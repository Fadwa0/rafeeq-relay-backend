#!/usr/bin/env python3
"""
RAFEEQ — Render Cloud Relay Server
====================================
Role: Fallback pipeline when the Raspberry Pi fog node is offline.

Flow:
  ESP32 Watch → BLE → Flutter Phone App → HTTP POST → This Server → Firebase

This server replicates the exact same processing logic as the Pi fog node:
  - Validates sensor data
  - Runs local fusion / alert detection
  - Computes rolling buffers and trends
  - Writes live snapshot to Firebase RTDB
  - Writes reading history to Firestore
  - Writes alert logs to Firestore
  - Writes relay_events to Firestore (triggered on first packet after Pi silence)

Deploy on Render as a Web Service (Python, always-on).

Required environment variables on Render:
    RAFEEQ_DATABASE_URL        — Firebase RTDB URL  e.g. https://your-project.firebaseio.com
    RAFEEQ_SERVICE_ACCOUNT_JSON — Full JSON content of your serviceAccount.json (as a single-line string)
    RAFEEQ_RELAY_API_KEY        — A secret string; phone app must send it in X-API-Key header
    PORT                        — Set automatically by Render (default 10000)

Optional:
    RAFEEQ_UPLOAD_COOLDOWN_SEC  — Minimum seconds between uploads per device (default 2)
    RAFEEQ_ROLLING_WINDOW_SIZE  — Rolling buffer size for trend analysis (default 10)
    RAFEEQ_DEDUP_WINDOW_SEC     — Alert dedup window in seconds (default 3)
    RAFEEQ_LOG_LEVEL            — DEBUG / INFO / WARNING (default INFO)

Install:
    pip install flask firebase-admin gunicorn

Run locally:
    gunicorn rafeeq_render_relay:app --bind 0.0.0.0:10000 --workers 1 --threads 4

Render start command:
    gunicorn rafeeq_render_relay:app --bind 0.0.0.0:$PORT --workers 1 --threads 4
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import firebase_admin
from firebase_admin import credentials, db as rtdb, firestore
from flask import Flask, jsonify, request

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=getattr(logging, os.getenv("RAFEEQ_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("rafeeq.render")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG HELPERS  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_get(data: Dict[str, Any], key: str, default: Any = None, cast: Any = None) -> Any:
    try:
        value = data.get(key, default)
        if value is None:
            return default
        return cast(value) if cast is not None else value
    except Exception:
        return default


def parse_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "wearing", "on"}
    return default

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS  (mirror Pi fog node schema)
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_VERSION          = "4.0"
SOURCE_TYPE_PI          = "raspberry_pi"        # kept for schema compat
SOURCE_TYPE_RELAY       = "phone_relay"
SOURCE_PATH_RELAY       = "relay"
PROCESSED_BY_NAME       = "Rafeeq Render Relay"
LOCAL_DEVICE_ID         = "render-relay-node-01"
DEFAULT_RELAY_DEVICE_ID = os.getenv("RAFEEQ_DEFAULT_RELAY_DEVICE_ID", "unknown_phone_relay")

HEARTBEAT_TIMEOUT_SEC    = 15.0   # same as Pi: declare Pi offline after 15 s
PI_HEARTBEAT_INTERVAL_SEC = 5.0

UPLOAD_COOLDOWN_SEC  = env_float("RAFEEQ_UPLOAD_COOLDOWN_SEC", 2.0)
ROLLING_WINDOW_SIZE  = env_int("RAFEEQ_ROLLING_WINDOW_SIZE", 10)
DEDUP_WINDOW_SEC     = env_float("RAFEEQ_DEDUP_WINDOW_SEC", 3.0)

VALID_HR_RANGE   = (20, 250)
VALID_SPO2_RANGE = (50, 100)
VALID_TEMP_RANGE = (25.0, 45.0)

RELAY_API_KEY = os.getenv("RAFEEQ_RELAY_API_KEY", "")

# ══════════════════════════════════════════════════════════════════════════════
# FIREBASE INIT
# ══════════════════════════════════════════════════════════════════════════════

fs: Any = None   # Firestore client

def init_firebase() -> None:
    global fs
    database_url = os.getenv("RAFEEQ_DATABASE_URL", "").strip()
    sa_json_str  = os.getenv("RAFEEQ_SERVICE_ACCOUNT_JSON", "").strip()

    if not database_url:
        log.error("[FIREBASE] RAFEEQ_DATABASE_URL is not set — Firebase disabled")
        return
    if not sa_json_str:
        log.error("[FIREBASE] RAFEEQ_SERVICE_ACCOUNT_JSON is not set — Firebase disabled")
        return

    try:
        sa_dict = json.loads(sa_json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred, {"databaseURL": database_url})
        fs = firestore.client()
        log.info("[FIREBASE] Initialized (RTDB + Firestore)")
    except Exception as exc:
        fs = None
        log.error("[FIREBASE] Init failed: %s", exc)

# ══════════════════════════════════════════════════════════════════════════════
# ROLLING BUFFER  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

class RollingBuffer:
    def __init__(self, maxlen: int):
        self.values: Deque[float] = deque(maxlen=maxlen)

    def push(self, value: float) -> None:
        self.values.append(float(value))

    def mean(self) -> float:
        return sum(self.values) / len(self.values) if self.values else 0.0

    def std(self) -> float:
        if len(self.values) < 2:
            return 0.0
        m = self.mean()
        return math.sqrt(sum((x - m) ** 2 for x in self.values) / len(self.values))

    def trend(self) -> str:
        if len(self.values) < 3:
            return "stable"
        y = list(self.values)
        x = list(range(len(y)))
        xm = sum(x) / len(x)
        ym = sum(y) / len(y)
        den = sum((i - xm) ** 2 for i in x)
        if den == 0:
            return "stable"
        slope = sum((i - xm) * (v - ym) for i, v in zip(x, y)) / den
        if slope > 0.5:
            return "rising"
        if slope < -0.5:
            return "falling"
        return "stable"

# ══════════════════════════════════════════════════════════════════════════════
# ALERT DEDUPLICATOR  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

class AlertDeduplicator:
    def __init__(self, window_sec: float):
        self.window_sec = window_sec
        self._lock = threading.Lock()
        self.last_seen: Dict[str, float] = {}

    def should_write(self, key: str) -> bool:
        now = time.time()
        with self._lock:
            last = self.last_seen.get(key, 0.0)
            if now - last < self.window_sec:
                return False
            self.last_seen[key] = now
        return True

# ══════════════════════════════════════════════════════════════════════════════
# HEARTBEAT MONITOR
# Tracks when the Pi fog node last sent a heartbeat so we know it's offline.
# The phone app should forward the Pi heartbeat topic payload here if possible,
# but even without that, the relay server starts writing relay_events as soon
# as it receives a phone packet (which means Pi was already silent).
# ══════════════════════════════════════════════════════════════════════════════

class HeartbeatMonitor:
    def __init__(self, timeout_sec: float):
        self.timeout_sec = timeout_sec
        self._lock = threading.Lock()
        self.last_heartbeat_time: Optional[str] = None
        self._last_heartbeat_ts: float = 0.0

    def record(self) -> None:
        with self._lock:
            self._last_heartbeat_ts = time.time()
            self.last_heartbeat_time = utc_now_iso()

    def is_alive(self) -> bool:
        with self._lock:
            if self._last_heartbeat_ts == 0.0:
                return False
            return (time.time() - self._last_heartbeat_ts) < self.timeout_sec

    def seconds_since(self) -> float:
        with self._lock:
            if self._last_heartbeat_ts == 0.0:
                return float("inf")
            return time.time() - self._last_heartbeat_ts

hb_monitor = HeartbeatMonitor(HEARTBEAT_TIMEOUT_SEC)

# ══════════════════════════════════════════════════════════════════════════════
# PER-DEVICE STATE
# Each device keeps its own rolling buffers, deduplicator, and cooldown timer.
# Render is stateless between restarts, but state within a session is fine —
# the relay is a short-lived fallback path.
# ══════════════════════════════════════════════════════════════════════════════

class DeviceState:
    def __init__(self, device_id: str):
        self.device_id      = device_id
        self.buf_hr         = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_spo2       = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_temp       = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.deduplicator   = AlertDeduplicator(DEDUP_WINDOW_SEC)
        self.last_upload_ts = 0.0
        self.relay_event_written = False   # write once per relay session
        self.daily_readings = 0
        self.daily_alerts   = 0
        self.daily_relay    = 0

_device_states: Dict[str, DeviceState] = {}
_device_states_lock = threading.Lock()

def get_device_state(device_id: str) -> DeviceState:
    with _device_states_lock:
        if device_id not in _device_states:
            _device_states[device_id] = DeviceState(device_id)
        return _device_states[device_id]

# ══════════════════════════════════════════════════════════════════════════════
# SENSOR VALIDATION  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

def validate_sensor(hr: int, spo2: int, temp: float, wearing: bool) -> bool:
    if not wearing:
        return True
    if hr == 0 or spo2 == 0:
        return False
    if not (VALID_HR_RANGE[0] <= hr <= VALID_HR_RANGE[1]):
        return False
    if not (VALID_SPO2_RANGE[0] <= spo2 <= VALID_SPO2_RANGE[1]):
        return False
    if temp and not (VALID_TEMP_RANGE[0] <= temp <= VALID_TEMP_RANGE[1]):
        return False
    return True

# ══════════════════════════════════════════════════════════════════════════════
# CONFIDENCE SCORE  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

def compute_confidence(
    buf_hr: RollingBuffer,
    buf_spo2: RollingBuffer,
    buf_temp: RollingBuffer,
    hr: int,
    spo2: int,
    wearing: bool,
) -> int:
    if not wearing:
        return 0
    confidence = 100.0
    confidence -= min(buf_hr.std()   / 10.0, 1.0) * 15
    confidence -= min(buf_spo2.std() /  3.0, 1.0) * 15
    confidence -= min(buf_temp.std() /  0.5, 1.0) * 10
    if hr == 0 or spo2 == 0:
        confidence -= 30
    return max(0, min(100, int(confidence)))

# ══════════════════════════════════════════════════════════════════════════════
# LOCAL FUSION / ALERT LOGIC  (identical to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

def local_fusion(
    hr: int,
    spo2: int,
    temp: float,
    wearing: bool,
    imu_candidate: bool,
) -> Dict[str, Any]:
    if not wearing:
        return {"status": "NoData", "danger": False, "warning": False, "flags": ["NotWearing"]}

    flags: List[str] = []
    danger  = False
    warning = False

    if spo2 and spo2 < 90:
        danger = True
        flags.append("Severe SpO2 drop")
    elif spo2 and spo2 < 94:
        warning = True
        flags.append("Low SpO2")

    if hr and (hr < 45 or hr > 130):
        warning = True
        flags.append("Abnormal heart rate")

    if temp >= 39.5:
        danger = True
        flags.append("High fever")
    elif temp >= 38.5:
        warning = True
        flags.append("Fever")

    if imu_candidate:
        if danger or warning:
            danger = True
            flags.append("Confirmed fainting/fall with vital abnormality")
        else:
            warning = True
            flags.append("Possible fall - vitals stable")

    status = "Critical" if danger else "Warning" if warning else "Stable"
    return {"status": status, "danger": danger, "warning": warning, "flags": flags or ["Normal"]}

# ══════════════════════════════════════════════════════════════════════════════
# RELAY EVENT WRITER  (as specified in the requirements)
# ══════════════════════════════════════════════════════════════════════════════

def write_relay_event(
    uid: Optional[str],
    device_id: Optional[str],
    reason: str = "heartbeat_timeout_15s",
    detected_relay_device: Optional[str] = None,
) -> None:
    """
    Write a relay event document to:
      users/{uid}/devices/{device_id}/relay_events/{event_id}
    Called once per relay session when the first packet arrives via phone relay.
    """
    if not uid:
        log.warning("[RELAY EVENT] Skipped — uid is None or empty")
        return
    if not device_id:
        log.warning("[RELAY EVENT] Skipped — device_id is None or empty")
        return
    if fs is None:
        log.warning("[RELAY EVENT] Skipped — Firestore not initialised")
        return

    event_id        = str(uuid.uuid4())
    timestamp       = utc_now_iso()
    day_key         = timestamp[:10]
    month_key       = timestamp[:7]
    relay_device_id = detected_relay_device or DEFAULT_RELAY_DEVICE_ID or "unknown_phone_relay"

    log.warning(
        "[RELAY EVENT] Attempting write: uid=%s device_id=%s reason=%s relay_device=%s",
        uid, device_id, reason, relay_device_id,
    )
    try:
        fs.collection("users").document(uid) \
            .collection("devices").document(device_id) \
            .collection("relay_events").document(event_id).set({
                "event_id":                event_id,
                "event_type":              "FAILOVER_START",
                "occurred_at":             firestore.SERVER_TIMESTAMP,
                "iso_timestamp":           timestamp,
                "day_key":                 day_key,
                "month_key":               month_key,
                "relay_device_id":         relay_device_id,
                "trigger":                 reason,
                "pi_last_seen":            timestamp,
                "details":                 f"Pi missed heartbeat — silent for {hb_monitor.seconds_since():.1f}s",
                "source_type":             SOURCE_TYPE_PI,
                "source_id":               LOCAL_DEVICE_ID,
                "source_path":             SOURCE_PATH_RELAY,
                "processed_by":            PROCESSED_BY_NAME,
                "relay_source_type":       SOURCE_TYPE_RELAY,
                "relay_source_id":         relay_device_id,
                "heartbeat_last_time":     hb_monitor.last_heartbeat_time,
                "heartbeat_alive":         hb_monitor.is_alive(),
                "heartbeat_seconds_since": round(hb_monitor.seconds_since(), 2),
                "heartbeat_timeout_sec":   HEARTBEAT_TIMEOUT_SEC,
                "pi_heartbeat_interval":   PI_HEARTBEAT_INTERVAL_SEC,
            })
        state = get_device_state(device_id)
        state.daily_relay += 1
        log.warning(
            "[RELAY EVENT] Success: event_id=%s reason=%s relay_device=%s daily_relay_count=%d",
            event_id, reason, relay_device_id, state.daily_relay,
        )
    except Exception as exc:
        log.error("[RELAY EVENT] Write failed: %s", exc)

# ══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT BUILDER  (mirrors Pi fog node build_snapshot)
# ══════════════════════════════════════════════════════════════════════════════

def build_snapshot(
    uid: str,
    device_id: str,
    hr: int,
    spo2: int,
    temp: float,
    wearing: bool,
    imu_candidate: bool,
    imu_peak_svm: float,
    imu_motion_level: float,
    imu_orientation: float,
    accel_x: Optional[float],
    accel_y: Optional[float],
    accel_z: Optional[float],
    relay_device_id: str,
    state: DeviceState,
) -> Dict[str, Any]:
    timestamp = utc_now_iso()
    fusion    = local_fusion(hr, spo2, temp, wearing, imu_candidate)
    confidence = compute_confidence(
        state.buf_hr, state.buf_spo2, state.buf_temp, hr, spo2, wearing
    )

    return {
        "packet_id":            str(uuid.uuid4()),
        "timestamp":            timestamp,
        "day_key":              timestamp[:10],
        "month_key":            timestamp[:7],
        "device_id":            device_id,

        # Vitals
        "heart_rate":           hr   if wearing else 0,
        "spo2":                 spo2 if wearing else 0,
        "temperature":          temp if wearing else 0,
        "blood_pressure":       "0/0",
        "glucose":              0,

        # Motion
        "accel_x":              accel_x,
        "accel_y":              accel_y,
        "accel_z":              accel_z,
        "imu_candidate":        imu_candidate,
        "imu_peak_svm":         imu_peak_svm,

        # Wearing state
        "wearing":              wearing,
        "medical_data_status":  "Active"    if wearing else "NoData",
        "device_status":        "Wearing"   if wearing else "NotWearing",

        # Confidence + trends
        "confidence":           confidence,
        "hr_trend":             state.buf_hr.trend(),
        "spo2_trend":           state.buf_spo2.trend(),
        "temp_trend":           state.buf_temp.trend(),

        # Fusion / alerts
        "status":               fusion["status"],
        "danger":               fusion["danger"],
        "warning":              fusion["warning"],
        "alert_flags_list":     fusion["flags"],
        "alert_flags_str":      "; ".join(fusion["flags"]),

        # Relay metadata
        "pi_online":            False,           # Pi is offline — that's why we're here
        "pi_heartbeat":         hb_monitor.last_heartbeat_time,
        "pi_id":                None,
        "fog_pipeline_ok":      False,
        "cloud_connected":      True,            # we are the cloud path

        "connection_mode":      "BLE_PHONE_RELAY",
        "source_path":          SOURCE_PATH_RELAY,
        "source_type":          SOURCE_TYPE_PI,  # schema compat
        "source_id":            LOCAL_DEVICE_ID,
        "relay_source_type":    SOURCE_TYPE_RELAY,
        "relay_source_id":      relay_device_id,
        "processed_by":         PROCESSED_BY_NAME,
        "schema_version":       SCHEMA_VERSION,
    }

# ══════════════════════════════════════════════════════════════════════════════
# FIREBASE WRITERS  (identical structure to Pi fog node)
# ══════════════════════════════════════════════════════════════════════════════

def write_live_snapshot(uid: str, device_id: str, snap: Dict[str, Any]) -> None:
    base = rtdb.reference(f"live/{uid}/{device_id}")
    base.child("vitals").update({
        "heart_rate":       snap["heart_rate"],
        "spo2":             snap["spo2"],
        "temperature":      snap["temperature"],
        "blood_pressure":   snap["blood_pressure"],
        "glucose":          snap["glucose"],
        "accel_x":          snap["accel_x"],
        "accel_y":          snap["accel_y"],
        "accel_z":          snap["accel_z"],
        "confidence_score": snap["confidence"],
        "alert_flags":      snap["alert_flags_str"],
        "hr_trend":         snap["hr_trend"],
        "spo2_trend":       snap["spo2_trend"],
        "temp_trend":       snap["temp_trend"],
        "updated_at":       snap["timestamp"],
        "packet_id":        snap["packet_id"],
    })
    base.child("status").update({
        "pi_online":          False,
        "pi_last_seen":       snap["pi_heartbeat"],
        "pi_heartbeat":       snap["pi_heartbeat"],
        "relay_active":       True,
        "relay_source_id":    snap["relay_source_id"],
        "connection_mode":    snap["connection_mode"],
        "last_seen":          snap["timestamp"],
        "fog_pipeline_ok":    False,
        "cloud_connected":    True,
        "wearing":            snap["wearing"],
        "device_status":      snap["device_status"],
        "medical_data_status":snap["medical_data_status"],
        "data_stale":         not snap["wearing"],
        "source_path":        snap["source_path"],
        "source_type":        snap["source_type"],
        "source_id":          snap["source_id"],
        "processed_by":       snap["processed_by"],
        "schema_version":     snap["schema_version"],
    })
    base.child("alerts").update({
        "active":             bool(snap["danger"] or snap["warning"]),
        "latest_status":      snap["status"],
        "latest_alert_type":  snap["alert_flags_list"][0] if snap["alert_flags_list"] else None,
        "latest_alert_at":    snap["timestamp"] if snap["alert_flags_list"] and snap["alert_flags_list"][0] != "Normal" else None,
    })


def write_firestore_history(uid: str, device_id: str, snap: Dict[str, Any]) -> None:
    if not fs:
        return
    fs.collection("users").document(uid) \
        .collection("devices").document(device_id) \
        .collection("readings").document(snap["packet_id"]) \
        .set({**snap, "timestamp_server": firestore.SERVER_TIMESTAMP})


def write_alert_log_if_needed(
    uid: str, device_id: str, snap: Dict[str, Any], state: DeviceState
) -> None:
    if not fs:
        return
    flags = snap["alert_flags_str"]
    if flags in ("Normal", "NotWearing"):
        return
    if not state.deduplicator.should_write(flags):
        return
    fs.collection("users").document(uid) \
        .collection("devices").document(device_id) \
        .collection("alert_logs").document(snap["packet_id"]) \
        .set({
            "alert_id":          snap["packet_id"],
            "alert_types":       snap["alert_flags_list"],
            "alert_flags":       snap["alert_flags_str"],
            "status":            snap["status"],
            "triggered_at":      firestore.SERVER_TIMESTAMP,
            "iso_timestamp":     snap["timestamp"],
            "resolved":          False,
            "source_path":       snap["source_path"],
            "source_type":       snap["source_type"],
            "source_id":         snap["source_id"],
            "connection_mode":   snap["connection_mode"],
            "relay_source_id":   snap["relay_source_id"],
        })
    state.daily_alerts += 1

# ══════════════════════════════════════════════════════════════════════════════
# CORE PACKET PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_packet(payload: Dict[str, Any], relay_device_id: str) -> Dict[str, Any]:
    """
    Extract, validate, fuse, and upload one sensor packet.
    Returns a dict describing what was done (used in the HTTP response).
    """
    uid       = str(payload.get("uid", "")).strip()
    device_id = str(payload.get("device_id", "")).strip()

    if not uid or not device_id:
        return {"ok": False, "error": "Missing uid or device_id"}
    if not device_id.startswith("rafeeq-watch-"):
        return {"ok": False, "error": f"Unexpected device_id format: {device_id}"}

    state = get_device_state(device_id)

    # ── Upload cooldown ───────────────────────────────────────────────────────
    if time.time() - state.last_upload_ts < UPLOAD_COOLDOWN_SEC:
        return {"ok": True, "skipped": True, "reason": "cooldown"}

    # ── Extract vitals (nested or flat — accept both ESP packet formats) ─────
    vitals_obj  = payload.get("vitals", {})
    motion_obj  = payload.get("motion", {})

    wearing     = parse_bool(payload.get("wearing", vitals_obj.get("finger", True)), True)
    hr          = safe_get(vitals_obj or payload, "heart_rate", safe_get(payload, "hr", 0), int)
    spo2        = safe_get(vitals_obj or payload, "spo2", 0, int)
    temp        = safe_get(vitals_obj or payload, "temperature_c",
                           safe_get(payload, "temperature", 0.0), float)

    imu_candidate  = bool(safe_get(motion_obj or payload, "imu_candidate", False))
    imu_peak_svm   = safe_get(motion_obj or payload, "peak_svm",
                               safe_get(payload, "peakSVM", 0.0), float)
    imu_motion_lvl = safe_get(motion_obj or payload, "motion_level",
                               safe_get(motion_obj or payload, "stillness", 0.0), float)
    imu_orient     = safe_get(motion_obj or payload, "orientation", 0.0, float)
    accel_x        = safe_get(motion_obj or payload, "accel_x", None, float)
    accel_y        = safe_get(motion_obj or payload, "accel_y", None, float)
    accel_z        = safe_get(motion_obj or payload, "accel_z", None, float)

    # ── Validation ────────────────────────────────────────────────────────────
    if not validate_sensor(hr, spo2, temp, wearing):
        log.warning("[PROCESS] Rejected sensor hr=%s spo2=%s temp=%s", hr, spo2, temp)
        return {"ok": False, "error": "Sensor validation failed", "hr": hr, "spo2": spo2}

    # ── Update rolling buffers ────────────────────────────────────────────────
    if wearing:
        if hr:
            state.buf_hr.push(hr)
        if spo2:
            state.buf_spo2.push(spo2)
        if temp:
            state.buf_temp.push(temp)

    # ── Write relay_event once per relay session ──────────────────────────────
    if not state.relay_event_written:
        write_relay_event(
            uid=uid,
            device_id=device_id,
            reason="heartbeat_timeout_15s",
            detected_relay_device=relay_device_id,
        )
        state.relay_event_written = True

    # ── Build snapshot ────────────────────────────────────────────────────────
    snap = build_snapshot(
        uid=uid, device_id=device_id,
        hr=hr, spo2=spo2, temp=temp, wearing=wearing,
        imu_candidate=imu_candidate, imu_peak_svm=imu_peak_svm,
        imu_motion_level=imu_motion_lvl, imu_orientation=imu_orient,
        accel_x=accel_x, accel_y=accel_y, accel_z=accel_z,
        relay_device_id=relay_device_id, state=state,
    )

    # ── Write to Firebase ─────────────────────────────────────────────────────
    try:
        write_live_snapshot(uid, device_id, snap)
        write_firestore_history(uid, device_id, snap)
        write_alert_log_if_needed(uid, device_id, snap, state)
        state.daily_readings += 1
        state.last_upload_ts = time.time()
        log.info(
            "[UPLOAD] relay uid=%s device=%s status=%s flags=%s",
            uid, device_id, snap["status"], snap["alert_flags_str"],
        )
        return {
            "ok":          True,
            "packet_id":   snap["packet_id"],
            "status":      snap["status"],
            "alert_flags": snap["alert_flags_str"],
            "wearing":     wearing,
        }
    except Exception as exc:
        log.exception("[UPLOAD] Failed: %s", exc)
        return {"ok": False, "error": str(exc)}

# ══════════════════════════════════════════════════════════════════════════════
# FLASK APP
# ══════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)


def _check_api_key() -> Optional[str]:
    """Returns an error message if the API key is invalid, else None."""
    if not RELAY_API_KEY:
        return None   # key not configured — open (dev mode)
    provided = request.headers.get("X-API-Key", "")
    if provided != RELAY_API_KEY:
        return "Unauthorized"
    return None


# ── Health check ─────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":          "ok",
        "firebase":        fs is not None,
        "pi_alive":        hb_monitor.is_alive(),
        "pi_seconds_since": round(hb_monitor.seconds_since(), 1),
        "server":          PROCESSED_BY_NAME,
        "schema_version":  SCHEMA_VERSION,
    })


# ── Pi heartbeat receiver (optional — phone app can forward Pi heartbeats) ───

@app.route("/pi/heartbeat", methods=["POST"])
def pi_heartbeat():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401

    hb_monitor.record()
    log.info("[HB] Pi heartbeat received via relay endpoint")
    return jsonify({"ok": True, "pi_alive": True})


# ── Main relay endpoint — phone app POSTs ESP packets here ───────────────────

@app.route("/relay/packet", methods=["POST"])
def relay_packet():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401

    if not request.is_json:
        return jsonify({"ok": False, "error": "Content-Type must be application/json"}), 400

    payload = request.get_json(silent=True)
    if not payload or not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "Invalid or empty JSON body"}), 400

    # Relay device identity — phone sends its own device id in the header or payload
    relay_device_id = (
        request.headers.get("X-Relay-Device-Id")
        or str(payload.get("relay_device_id", DEFAULT_RELAY_DEVICE_ID))
    )

    result = process_packet(payload, relay_device_id)
    status_code = 200 if result.get("ok") else 422
    return jsonify(result), status_code


# ── Batch endpoint — phone can send multiple buffered packets at once ─────────

@app.route("/relay/batch", methods=["POST"])
def relay_batch():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401

    if not request.is_json:
        return jsonify({"ok": False, "error": "Content-Type must be application/json"}), 400

    body = request.get_json(silent=True)
    if not body or not isinstance(body, dict):
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    packets = body.get("packets", [])
    if not isinstance(packets, list) or len(packets) == 0:
        return jsonify({"ok": False, "error": "packets array is empty or missing"}), 400

    relay_device_id = (
        request.headers.get("X-Relay-Device-Id")
        or str(body.get("relay_device_id", DEFAULT_RELAY_DEVICE_ID))
    )

    results = []
    for i, pkt in enumerate(packets):
        if not isinstance(pkt, dict):
            results.append({"index": i, "ok": False, "error": "not a dict"})
            continue
        r = process_packet(pkt, relay_device_id)
        r["index"] = i
        results.append(r)

    ok_count   = sum(1 for r in results if r.get("ok"))
    fail_count = len(results) - ok_count
    log.info("[BATCH] processed=%d ok=%d fail=%d", len(results), ok_count, fail_count)
    return jsonify({"ok": True, "processed": len(results), "ok_count": ok_count,
                    "fail_count": fail_count, "results": results})


# ── Device reset endpoint — resets relay_event_written so a new relay_event
#    is written next time (useful when Pi comes back and goes offline again) ──

@app.route("/relay/reset_session/<device_id>", methods=["POST"])
def reset_relay_session(device_id: str):
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401

    with _device_states_lock:
        if device_id in _device_states:
            _device_states[device_id].relay_event_written = False
            log.info("[RELAY SESSION] Reset for device_id=%s", device_id)
    return jsonify({"ok": True, "device_id": device_id})


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

init_firebase()
log.info("[SYSTEM] Rafeeq Render relay server ready")
log.info("[SYSTEM] Firebase: %s", "OK" if fs else "DISABLED")
log.info("[SYSTEM] API key protection: %s", "ON" if RELAY_API_KEY else "OFF (dev mode)")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
