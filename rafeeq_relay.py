#!/usr/bin/env python3
"""
RAFEEQ — Render Cloud Relay Server v5.0
Role: fallback relay pipeline when Raspberry Pi fog node is offline.

Flow:
  ESP32 Watch -> BLE -> Flutter Phone App -> HTTP POST -> Render -> Firebase

Design copied from the Raspberry Pi fog-node logic:
  1) Receive flat or nested ESP packets from the phone relay.
  2) Validate vitals exactly like the Pi path.
  3) Apply local fusion / alert detection.
  4) Write RTDB live/{uid}/{device_id} immediately for app/web live updates.
  5) Queue Firestore history/audit/performance/relay-event writes in SQLite.
  6) Upload queued Firestore jobs slowly in a background worker so Firestore quota
     failures never block live relay processing.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
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

# ─────────────────────────────────────────
# LOGGING / HELPERS
# ─────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("RAFEEQ_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("rafeeq.render.relay")


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


def ensure_parent_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


# ─────────────────────────────────────────
# CONFIG — v5.0 schema, aligned with Pi fog node
# ─────────────────────────────────────────

SCHEMA_VERSION = "5.0"
SOURCE_TYPE_RELAY = "phone_relay"
SOURCE_PATH_RELAY = "relay"
PROCESSED_BY_NAME = "Rafeeq Render Relay"
RELAY_NODE_ID = os.getenv("RAFEEQ_RELAY_NODE_ID", "render-relay-node-01")
DEFAULT_RELAY_DEVICE_ID = os.getenv("RAFEEQ_DEFAULT_RELAY_DEVICE_ID", "unknown_phone_relay")

HEARTBEAT_TIMEOUT_SEC = env_float("RAFEEQ_HEARTBEAT_TIMEOUT_SEC", 15.0)
PI_HEARTBEAT_INTERVAL_SEC = env_float("RAFEEQ_PI_HEARTBEAT_INTERVAL_SEC", 5.0)
UPLOAD_COOLDOWN_SEC = env_float("RAFEEQ_UPLOAD_COOLDOWN_SEC", 2.0)
ROLLING_WINDOW_SIZE = env_int("RAFEEQ_ROLLING_WINDOW_SIZE", 10)
DEDUP_WINDOW_SEC = env_float("RAFEEQ_DEDUP_WINDOW_SEC", 3.0)

FIRESTORE_HISTORY_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_HISTORY_INTERVAL_SEC", 60.0)
FIRESTORE_PERF_LOG_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_PERF_LOG_INTERVAL_SEC", 60.0)
FIRESTORE_WORKER_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_WORKER_INTERVAL_SEC", 30.0)
FIRESTORE_WORKER_BATCH_SIZE = env_int("RAFEEQ_FIRESTORE_WORKER_BATCH_SIZE", 1)
SQLITE_QUEUE_PATH = os.getenv("RAFEEQ_SQLITE_QUEUE", "/tmp/rafeeq_render_firestore_queue.db")

VALID_HR_RANGE: Tuple[int, int] = (20, 250)
VALID_SPO2_RANGE: Tuple[int, int] = (50, 100)
VALID_TEMP_RANGE: Tuple[float, float] = (20.0, 45.0)

RELAY_API_KEY = os.getenv("RAFEEQ_RELAY_API_KEY", "")

fs: Any = None


# ─────────────────────────────────────────
# FIREBASE INIT
# ─────────────────────────────────────────


def init_firebase() -> None:
    global fs
    database_url = os.getenv("RAFEEQ_DATABASE_URL", "").strip()
    service_account_json = os.getenv("RAFEEQ_SERVICE_ACCOUNT_JSON", "").strip()

    if not database_url:
        log.error("[FIREBASE] RAFEEQ_DATABASE_URL is missing")
        return
    if not service_account_json:
        log.error("[FIREBASE] RAFEEQ_SERVICE_ACCOUNT_JSON is missing")
        return

    try:
        service_account = json.loads(service_account_json)
        if not firebase_admin._apps:
            cred = credentials.Certificate(service_account)
            firebase_admin.initialize_app(cred, {"databaseURL": database_url})
        fs = firestore.client()
        log.info("[FIREBASE] Initialized")
    except Exception as exc:
        fs = None
        log.error("[FIREBASE] Init failed: %s", exc)


# ─────────────────────────────────────────
# ROLLING / DEDUP / SQLITE QUEUE
# ─────────────────────────────────────────

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
        mean_v = self.mean()
        return math.sqrt(sum((x - mean_v) ** 2 for x in self.values) / len(self.values))

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


class AlertDeduplicator:
    def __init__(self, window_sec: float):
        self.window_sec = window_sec
        self.lock = threading.Lock()
        self.last_seen: Dict[str, float] = {}

    def should_write(self, key: str) -> bool:
        now = time.time()
        with self.lock:
            last = self.last_seen.get(key, 0.0)
            if now - last < self.window_sec:
                return False
            self.last_seen[key] = now
        return True


class LocalQueue:
    """SQLite-backed Firestore queue.

    RTDB live writes are direct. Firestore writes are queued and uploaded by a
    worker. If quota is exceeded, the worker backs off; live relay remains alive.
    """

    def __init__(self, path: str):
        self.path = path
        ensure_parent_dir(path)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS firestore_jobs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "job_type TEXT NOT NULL,"
            "uid TEXT NOT NULL,"
            "device_id TEXT NOT NULL,"
            "payload_json TEXT NOT NULL,"
            "created_at REAL NOT NULL,"
            "attempt_count INTEGER DEFAULT 0,"
            "next_attempt_at REAL DEFAULT 0,"
            "delivered INTEGER DEFAULT 0)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_firestore_jobs_ready "
            "ON firestore_jobs(delivered,next_attempt_at,created_at)"
        )
        self.conn.commit()

    def enqueue_job(self, job_type: str, uid: str, device_id: str, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO firestore_jobs(job_type,uid,device_id,payload_json,created_at,attempt_count,next_attempt_at,delivered) "
                "VALUES(?,?,?,?,?,?,?,0)",
                (job_type, uid, device_id, json.dumps(payload), time.time(), 0, 0.0),
            )
            self.conn.commit()

    def count(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) FROM firestore_jobs WHERE delivered=0").fetchone()
            return int(row[0])

    def oldest_age_ms(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT MIN(created_at) FROM firestore_jobs WHERE delivered=0").fetchone()
            if not row or row[0] is None:
                return 0
            return int((time.time() - float(row[0])) * 1000)

    def next_batch(self, limit: int) -> List[Tuple[int, str, str, str, Dict[str, Any], int]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id,job_type,uid,device_id,payload_json,attempt_count "
                "FROM firestore_jobs WHERE delivered=0 AND next_attempt_at<=? "
                "ORDER BY CASE job_type "
                "WHEN 'alert' THEN 1 WHEN 'relay_event' THEN 2 WHEN 'reading' THEN 3 "
                "WHEN 'perf' THEN 4 WHEN 'audit' THEN 5 ELSE 9 END, created_at ASC LIMIT ?",
                (time.time(), limit),
            ).fetchall()
        out = []
        for row_id, job_type, uid, device_id, payload, attempts in rows:
            try:
                out.append((int(row_id), str(job_type), str(uid), str(device_id), json.loads(payload), int(attempts)))
            except Exception:
                pass
        return out

    def mark_delivered(self, row_id: int) -> None:
        with self.lock:
            self.conn.execute("UPDATE firestore_jobs SET delivered=1 WHERE id=?", (row_id,))
            self.conn.commit()

    def mark_failed(self, row_id: int, attempt_count: int) -> None:
        delays = [300, 600, 1800, 3600, 3600]
        delay = delays[min(attempt_count, len(delays) - 1)]
        with self.lock:
            self.conn.execute(
                "UPDATE firestore_jobs SET attempt_count=attempt_count+1,next_attempt_at=? WHERE id=?",
                (time.time() + delay, row_id),
            )
            self.conn.commit()

    def purge_delivered(self, older_than_sec: float = 86400.0) -> None:
        with self.lock:
            self.conn.execute(
                "DELETE FROM firestore_jobs WHERE delivered=1 AND created_at<?",
                (time.time() - older_than_sec,),
            )
            self.conn.commit()


local_queue = LocalQueue(SQLITE_QUEUE_PATH)
history_sync_status = "ok"
last_firestore_error: Optional[str] = None
last_firestore_error_at: Optional[str] = None
last_firestore_success_at: Optional[str] = None
queue_state_lock = threading.Lock()


def update_history_sync_state(status: str, error: Optional[str] = None) -> None:
    global history_sync_status, last_firestore_error, last_firestore_error_at, last_firestore_success_at
    with queue_state_lock:
        history_sync_status = status
        if error:
            last_firestore_error = str(error)[:500]
            last_firestore_error_at = utc_now_iso()
        elif status == "ok":
            last_firestore_error = None
            last_firestore_error_at = None
            last_firestore_success_at = utc_now_iso()


def history_sync_snapshot() -> Dict[str, Any]:
    depth = local_queue.count()
    with queue_state_lock:
        status = history_sync_status
        err = last_firestore_error
        err_at = last_firestore_error_at
        ok_at = last_firestore_success_at
    if depth == 0 and status != "delayed":
        status = "ok"
    elif depth > 0 and status == "ok":
        status = "pending"
    return {
        "history_sync_status": status,
        "queue_depth": depth,
        "queue_oldest_ms": local_queue.oldest_age_ms(),
        "last_firestore_error": err,
        "last_firestore_error_at": err_at,
        "last_firestore_success_at": ok_at,
    }


def enqueue_firestore_job(job_type: str, uid: Optional[str], device_id: Optional[str], payload: Dict[str, Any]) -> None:
    if not uid or not device_id:
        return
    local_queue.enqueue_job(job_type, uid, device_id, payload)
    if history_sync_status == "ok":
        update_history_sync_state("pending")


# ─────────────────────────────────────────
# HEARTBEAT / DEVICE STATE
# ─────────────────────────────────────────

class HeartbeatMonitor:
    def __init__(self, timeout_sec: float):
        self.timeout_sec = timeout_sec
        self.lock = threading.Lock()
        self.last_heartbeat_time: Optional[str] = None
        self.last_heartbeat_ts: float = 0.0
        self.last_pi_id: Optional[str] = None

    def record(self, data: Optional[Dict[str, Any]] = None) -> None:
        with self.lock:
            self.last_heartbeat_ts = time.time()
            self.last_heartbeat_time = utc_now_iso()
            if data and data.get("pi_id"):
                self.last_pi_id = str(data.get("pi_id"))

    def is_alive(self) -> bool:
        with self.lock:
            return self.last_heartbeat_ts > 0 and (time.time() - self.last_heartbeat_ts) < self.timeout_sec

    def seconds_since(self) -> float:
        with self.lock:
            if self.last_heartbeat_ts == 0:
                return float("inf")
            return time.time() - self.last_heartbeat_ts


hb_monitor = HeartbeatMonitor(HEARTBEAT_TIMEOUT_SEC)


class DeviceState:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.buf_hr = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_spo2 = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_temp = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.deduplicator = AlertDeduplicator(DEDUP_WINDOW_SEC)
        self.last_upload_ts = 0.0
        self.last_history_enqueue_ts = 0.0
        self.last_perf_log_ts = 0.0
        self.relay_event_written = False
        self.seen_packet_ids: Dict[str, float] = {}
        self.lat_phone_to_render: Deque[float] = deque(maxlen=60)
        self.lat_render_processing: Deque[float] = deque(maxlen=60)
        self.lat_firebase: Deque[float] = deque(maxlen=60)
        self.lat_total: Deque[float] = deque(maxlen=60)
        self.daily_readings = 0
        self.daily_alerts = 0
        self.daily_relay = 0

    def duplicate_packet(self, esp_packet_id: Optional[str]) -> bool:
        if not esp_packet_id:
            return False
        now = time.time()
        for key, ts in list(self.seen_packet_ids.items()):
            if now - ts > 60:
                self.seen_packet_ids.pop(key, None)
        if esp_packet_id in self.seen_packet_ids:
            return True
        self.seen_packet_ids[esp_packet_id] = now
        return False


_device_states: Dict[str, DeviceState] = {}
_device_states_lock = threading.Lock()


def get_device_state(device_id: str) -> DeviceState:
    with _device_states_lock:
        if device_id not in _device_states:
            _device_states[device_id] = DeviceState(device_id)
        return _device_states[device_id]


# ─────────────────────────────────────────
# PI-COMPATIBLE PROCESSING LOGIC
# ─────────────────────────────────────────


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


def compute_confidence(state: DeviceState, hr: int, spo2: int, wearing: bool) -> int:
    if not wearing:
        return 0
    confidence = 100.0
    confidence -= min(state.buf_hr.std() / 10.0, 1.0) * 15
    confidence -= min(state.buf_spo2.std() / 3.0, 1.0) * 15
    confidence -= min(state.buf_temp.std() / 0.5, 1.0) * 10
    if hr == 0 or spo2 == 0:
        confidence -= 30
    return max(0, min(100, int(confidence)))


def quality_tier(score: int) -> str:
    if score >= 85:
        return "excellent"
    if score >= 70:
        return "good"
    if score >= 45:
        return "fair"
    if score > 0:
        return "poor"
    return "invalid"


def local_fusion(hr: int, spo2: int, temp: float, wearing: bool, imu_candidate: bool,
                 hr_spike: bool, spo2_drop: bool, fault_flags: int) -> Dict[str, Any]:
    flags: List[str] = []
    danger = False
    warning = False

    if not wearing:
        return {"status": "NoData", "danger": False, "warning": False, "flags": ["NotWearing"]}

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

    if hr_spike:
        warning = True
        flags.append("Rapid HR rise")
    if spo2_drop:
        warning = True
        flags.append("Rapid SpO2 drop")
    if fault_flags:
        warning = True
        flags.append("Hardware fault")

    status = "Critical" if danger else "Warning" if warning else "Stable"
    return {"status": status, "danger": danger, "warning": warning, "flags": flags or ["Normal"]}


def extract_packet(payload: Dict[str, Any]) -> Dict[str, Any]:
    vitals = payload.get("vitals") if isinstance(payload.get("vitals"), dict) else {}
    motion = payload.get("motion") if isinstance(payload.get("motion"), dict) else {}
    device = payload.get("device") if isinstance(payload.get("device"), dict) else {}

    wearing = parse_bool(payload.get("wearing", vitals.get("wearing", vitals.get("finger", True))), True)

    return {
        "uid": str(payload.get("uid", "")).strip(),
        "device_id": str(payload.get("device_id", "")).strip(),
        "esp_packet_id": str(payload.get("packet_id", payload.get("esp_packet_id", ""))).strip() or None,
        "heart_rate": safe_get(vitals or payload, "heart_rate", safe_get(payload, "hr", 0), int),
        "spo2": safe_get(vitals or payload, "spo2", 0, int),
        "temperature": safe_get(vitals or payload, "temperature", safe_get(vitals or payload, "temperature_c", safe_get(payload, "tempC", 0.0)), float),
        "wearing": wearing,
        "imu_candidate": bool(safe_get(motion or payload, "imu_candidate", safe_get(payload, "candidate", False))),
        "imu_peak_svm": safe_get(motion or payload, "peak_svm", safe_get(payload, "peakSVM", 0.0), float),
        "imu_stillness": safe_get(motion or payload, "motion_level", safe_get(motion or payload, "stillness", 0.0), float),
        "imu_orientation": safe_get(motion or payload, "orientation", 0.0, float),
        "accel_x": safe_get(motion or payload, "accel_x", None, float),
        "accel_y": safe_get(motion or payload, "accel_y", None, float),
        "accel_z": safe_get(motion or payload, "accel_z", None, float),
        "esp_sent_at_ms": safe_get(payload, "sent_at_ms", safe_get(payload, "esp_sent_at_ms", None), int),
        "esp_sent_at_epoch_ms": safe_get(payload, "sent_at_epoch_ms", safe_get(payload, "esp_sent_at_epoch_ms", None), int),
        "esp_uptime_ms": safe_get(payload, "esp_uptime_ms", None, int),
        "esp_boot_count": safe_get(payload, "esp_boot_count", device.get("esp_boot_count"), int),
        "esp_confidence_score": safe_get(payload, "confidence_score", safe_get(vitals, "confidence_score", None), int),
        "wear_confidence": str(payload.get("wear_confidence", vitals.get("wear_confidence", "unknown"))),
        "battery_pct": safe_get(payload, "battery_pct", device.get("battery_pct"), int),
        "power_mode": str(payload.get("power_mode", device.get("power_mode", "POWER_NORMAL"))),
        "charging": parse_bool(payload.get("charging", device.get("charging", False)), False),
        "fault_flags": safe_get(payload, "fault_flags", device.get("fault_flags", 0), int),
        "time_synced": parse_bool(payload.get("time_synced", payload.get("ntp_synced", False)), False),
        "ntp_last_sync_ms_ago": safe_get(payload, "ntp_last_sync_ms_ago", None, int),
        "hr_spike": parse_bool(payload.get("hr_spike", False), False),
        "spo2_drop": parse_bool(payload.get("spo2_drop", False), False),
        "data_smoothed": parse_bool(payload.get("data_smoothed", False), False),
        "phone_sent_at_epoch_ms": safe_get(payload, "phone_sent_at_epoch_ms", None, int),
    }


def build_snapshot(packet: Dict[str, Any], relay_device_id: str, state: DeviceState) -> Dict[str, Any]:
    timestamp = utc_now_iso()
    confidence = compute_confidence(state, packet["heart_rate"], packet["spo2"], packet["wearing"])
    fusion = local_fusion(
        packet["heart_rate"], packet["spo2"], packet["temperature"], packet["wearing"],
        packet["imu_candidate"], packet["hr_spike"], packet["spo2_drop"], packet["fault_flags"],
    )
    pi_received_at_ms = int(time.time() * 1000)

    return {
        "packet_id": str(uuid.uuid4()),
        "timestamp": timestamp,
        "day_key": timestamp[:10],
        "month_key": timestamp[:7],
        "device_id": packet["device_id"],
        "heart_rate": packet["heart_rate"] if packet["wearing"] else 0,
        "spo2": packet["spo2"] if packet["wearing"] else 0,
        "temperature": packet["temperature"] if packet["wearing"] else 0,
        "blood_pressure": "0/0",
        "glucose": 0,
        "accel_x": packet["accel_x"],
        "accel_y": packet["accel_y"],
        "accel_z": packet["accel_z"],
        "imu_candidate": packet["imu_candidate"],
        "imu_peak_svm": packet["imu_peak_svm"],
        "wearing": packet["wearing"],
        "medical_data_status": "Active" if packet["wearing"] else "NoData",
        "device_status": "Wearing" if packet["wearing"] else "NotWearing",
        "confidence": confidence,
        "esp_confidence_score": packet["esp_confidence_score"],
        "quality_tier": quality_tier(confidence),
        "esp_packet_id": packet["esp_packet_id"],
        "esp_sent_at_ms": packet["esp_sent_at_ms"],
        "esp_sent_at_epoch_ms": packet["esp_sent_at_epoch_ms"],
        "pi_received_at_ms": pi_received_at_ms,
        "esp_uptime_ms": packet["esp_uptime_ms"],
        "esp_boot_count": packet["esp_boot_count"],
        "wear_confidence": packet["wear_confidence"],
        "battery_pct": packet["battery_pct"],
        "power_mode": packet["power_mode"],
        "charging": packet["charging"],
        "fault_flags": packet["fault_flags"],
        "time_synced": packet["time_synced"],
        "ntp_last_sync_ms_ago": packet["ntp_last_sync_ms_ago"],
        "hr_spike": packet["hr_spike"],
        "spo2_drop": packet["spo2_drop"],
        "data_smoothed": packet["data_smoothed"],
        "status": fusion["status"],
        "danger": fusion["danger"],
        "warning": fusion["warning"],
        "alert_flags_list": fusion["flags"],
        "alert_flags_str": "; ".join(fusion["flags"]),
        "hr_trend": state.buf_hr.trend(),
        "spo2_trend": state.buf_spo2.trend(),
        "temp_trend": state.buf_temp.trend(),
        "pi_online": False,
        "pi_heartbeat": hb_monitor.last_heartbeat_time,
        "pi_id": hb_monitor.last_pi_id,
        "fog_pipeline_ok": True,
        "cloud_connected": fs is not None,
        "connection_mode": "relay",
        "source_path": SOURCE_PATH_RELAY,
        "source_type": SOURCE_TYPE_RELAY,
        "source_id": RELAY_NODE_ID,
        "relay_source_type": SOURCE_TYPE_RELAY,
        "relay_source_id": relay_device_id,
        "processed_by": PROCESSED_BY_NAME,
        "schema_version": SCHEMA_VERSION,
    }


# ─────────────────────────────────────────
# FIREBASE WRITES
# ─────────────────────────────────────────


def write_live_snapshot(uid: str, device_id: str, snap: Dict[str, Any]) -> None:
    base = rtdb.reference(f"live/{uid}/{device_id}")
    base.child("vitals").update({
        "heart_rate": snap["heart_rate"],
        "spo2": snap["spo2"],
        "temperature": snap["temperature"],
        "blood_pressure": snap["blood_pressure"],
        "glucose": snap["glucose"],
        "accel_x": snap["accel_x"],
        "accel_y": snap["accel_y"],
        "accel_z": snap["accel_z"],
        "confidence_score": snap["confidence"],
        "esp_confidence_score": snap["esp_confidence_score"],
        "quality_tier": snap["quality_tier"],
        "esp_packet_id": snap["esp_packet_id"],
        "esp_sent_at_ms": snap["esp_sent_at_ms"],
        "esp_sent_at_epoch_ms": snap["esp_sent_at_epoch_ms"],
        "pi_received_at_ms": snap["pi_received_at_ms"],
        "esp_uptime_ms": snap["esp_uptime_ms"],
        "esp_boot_count": snap["esp_boot_count"],
        "wear_confidence": snap["wear_confidence"],
        "battery_pct": snap["battery_pct"],
        "power_mode": snap["power_mode"],
        "charging": snap["charging"],
        "fault_flags": snap["fault_flags"],
        "time_synced": snap["time_synced"],
        "ntp_last_sync_ms_ago": snap["ntp_last_sync_ms_ago"],
        "hr_spike": snap["hr_spike"],
        "spo2_drop": snap["spo2_drop"],
        "data_smoothed": snap["data_smoothed"],
        "alert_flags": snap["alert_flags_str"],
        "hr_trend": snap["hr_trend"],
        "spo2_trend": snap["spo2_trend"],
        "temp_trend": snap["temp_trend"],
        "updated_at": snap["timestamp"],
        "packet_id": snap["packet_id"],
    })
    base.child("status").update({
        "pi_online": False,
        "pi_last_seen": snap["pi_heartbeat"],
        "pi_heartbeat": snap["pi_heartbeat"],
        "pi_id": snap["pi_id"],
        "relay_active": True,
        "relay_source_id": snap["relay_source_id"],
        "connection_mode": "relay",
        "last_seen": snap["timestamp"],
        "fog_pipeline_ok": True,
        "cloud_connected": fs is not None,
        "wearing": snap["wearing"],
        "device_status": snap["device_status"],
        "medical_data_status": snap["medical_data_status"],
        "data_stale": not snap["wearing"],
        "source_path": snap["source_path"],
        "source_type": snap["source_type"],
        "source_id": snap["source_id"],
        "processed_by": snap["processed_by"],
        "schema_version": snap["schema_version"],
        "operating_mode": "relay",
        **history_sync_snapshot(),
    })
    base.child("alerts").update({
        "active": bool(snap["danger"] or snap["warning"]),
        "latest_status": snap["status"],
        "latest_alert_type": snap["alert_flags_list"][0] if snap["alert_flags_list"] else None,
        "latest_alert_at": snap["timestamp"] if snap["alert_flags_list"] and snap["alert_flags_list"][0] != "Normal" else None,
    })
    base.child("device").update({
        "battery_pct": snap["battery_pct"],
        "power_mode": snap["power_mode"],
        "charging": snap["charging"],
        "esp_boot_count": snap["esp_boot_count"],
        "fault_flags": snap["fault_flags"],
        "schema_version": snap["schema_version"],
        "updated_at": snap["timestamp"],
    })
    base.child("system_health").update({
        "band_connected": True,
        "pi_online": False,
        "firebase_reachable": fs is not None,
        "render_relay_alive": True,
        "last_packet_at": snap["timestamp"],
        "operating_mode": "relay",
        **history_sync_snapshot(),
    })


def firestore_set_job(job_type: str, uid: str, device_id: str, payload: Dict[str, Any]) -> None:
    if not fs:
        raise RuntimeError("Firestore client is not available")

    if job_type == "reading":
        doc_id = str(payload.get("packet_id") or payload.get("esp_packet_id") or uuid.uuid4())
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("readings").document(doc_id).set({**payload, "timestamp_server": firestore.SERVER_TIMESTAMP})
        # Optional alias for dashboards that expect vital_readings.
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("vital_readings").document(doc_id).set({**payload, "timestamp_server": firestore.SERVER_TIMESTAMP})
        return

    if job_type == "alert":
        doc_id = str(payload.get("alert_id") or payload.get("packet_id") or uuid.uuid4())
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("alert_logs").document(doc_id).set({**payload, "timestamp_server": firestore.SERVER_TIMESTAMP})
        return

    if job_type == "relay_event":
        doc_id = str(payload.get("event_id") or uuid.uuid4())
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("relay_events").document(doc_id).set({**payload, "occurred_at": firestore.SERVER_TIMESTAMP})
        return

    if job_type == "perf":
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("performance_logs").document().set({**payload, "timestamp_server": firestore.SERVER_TIMESTAMP})
        return

    if job_type == "audit":
        fs.collection("users").document(uid).collection("devices").document(device_id).collection("audit_logs").document().set({**payload, "timestamp_server": firestore.SERVER_TIMESTAMP})
        return

    raise ValueError(f"Unknown Firestore job type: {job_type}")


def write_firestore_history(uid: str, device_id: str, snap: Dict[str, Any], state: DeviceState) -> None:
    now = time.time()
    if now - state.last_history_enqueue_ts < FIRESTORE_HISTORY_INTERVAL_SEC:
        return
    state.last_history_enqueue_ts = now
    enqueue_firestore_job("reading", uid, device_id, snap)


def write_alert_log_if_needed(uid: str, device_id: str, snap: Dict[str, Any], state: DeviceState) -> None:
    flags = snap["alert_flags_str"]
    if flags in ("Normal", "NotWearing"):
        return
    if not state.deduplicator.should_write(flags):
        return
    enqueue_firestore_job("alert", uid, device_id, {
        "alert_id": snap["packet_id"],
        "alert_types": snap["alert_flags_list"],
        "alert_flags": snap["alert_flags_str"],
        "status": snap["status"],
        "iso_timestamp": snap["timestamp"],
        "resolved": False,
        "source_path": snap["source_path"],
        "source_type": snap["source_type"],
        "source_id": snap["source_id"],
        "connection_mode": snap["connection_mode"],
        "relay_source_id": snap["relay_source_id"],
        "esp_packet_id": snap.get("esp_packet_id"),
    })
    state.daily_alerts += 1


def write_relay_event_once(uid: str, device_id: str, relay_device_id: str, state: DeviceState) -> None:
    if state.relay_event_written:
        return
    event_id = str(uuid.uuid4())
    timestamp = utc_now_iso()
    enqueue_firestore_job("relay_event", uid, device_id, {
        "event_id": event_id,
        "event_type": "FAILOVER_START",
        "iso_timestamp": timestamp,
        "day_key": timestamp[:10],
        "month_key": timestamp[:7],
        "relay_device_id": relay_device_id,
        "trigger": "pi_unavailable_phone_relay_active",
        "pi_last_seen": hb_monitor.last_heartbeat_time,
        "details": f"Phone relay packet received. Pi heartbeat alive={hb_monitor.is_alive()} silent_for={hb_monitor.seconds_since():.1f}s",
        "source_type": SOURCE_TYPE_RELAY,
        "source_id": RELAY_NODE_ID,
        "source_path": SOURCE_PATH_RELAY,
        "processed_by": PROCESSED_BY_NAME,
        "relay_source_type": SOURCE_TYPE_RELAY,
        "relay_source_id": relay_device_id,
        "heartbeat_last_time": hb_monitor.last_heartbeat_time,
        "heartbeat_alive": hb_monitor.is_alive(),
        "heartbeat_seconds_since": round(hb_monitor.seconds_since(), 2),
        "heartbeat_timeout_sec": HEARTBEAT_TIMEOUT_SEC,
        "pi_heartbeat_interval": PI_HEARTBEAT_INTERVAL_SEC,
        "schema_version": SCHEMA_VERSION,
    })
    state.relay_event_written = True
    state.daily_relay += 1


def percentile(values: Deque[float], p: float) -> Optional[float]:
    if not values:
        return None
    arr = sorted(values)
    idx = int(round((p / 100.0) * (len(arr) - 1)))
    return arr[max(0, min(idx, len(arr) - 1))]


def avg(values: Deque[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def write_performance_metrics(uid: str, device_id: str, snap: Dict[str, Any], state: DeviceState,
                              render_processing_ms: float, firebase_write_ms: float,
                              phone_to_render_ms: Optional[float], total_pipeline_ms: Optional[float]) -> None:
    if phone_to_render_ms is not None:
        state.lat_phone_to_render.append(phone_to_render_ms)
    state.lat_render_processing.append(render_processing_ms)
    state.lat_firebase.append(firebase_write_ms)
    state.lat_total.append(total_pipeline_ms if total_pipeline_ms is not None else render_processing_ms + firebase_write_ms)

    sample = {
        "phone_to_render_ms": round(phone_to_render_ms, 2) if phone_to_render_ms is not None else None,
        "render_processing_ms": round(render_processing_ms, 2),
        "firebase_write_ms": round(firebase_write_ms, 2),
        "total_pipeline_ms": round(total_pipeline_ms, 2) if total_pipeline_ms is not None else None,
        "avg_phone_to_render_ms": round(avg(state.lat_phone_to_render), 2) if state.lat_phone_to_render else None,
        "avg_render_processing_ms": round(avg(state.lat_render_processing), 2),
        "avg_firebase_ms": round(avg(state.lat_firebase), 2),
        "avg_total_ms": round(avg(state.lat_total), 2),
        "p50_ms": round(percentile(state.lat_total, 50), 2),
        "p95_ms": round(percentile(state.lat_total, 95), 2),
        "p99_ms": round(percentile(state.lat_total, 99), 2),
        "samples": len(state.lat_total),
        "last_transport": "PHONE_RELAY",
        "last_esp_packet_id": snap.get("esp_packet_id"),
        "time_synced": snap.get("time_synced"),
        "updated_at": utc_now_iso(),
    }
    try:
        rtdb.reference(f"live/{uid}/{device_id}/performance").update(sample)
    except Exception as exc:
        log.debug("[PERF] RTDB update failed: %s", exc)

    now = time.time()
    if now - state.last_perf_log_ts >= FIRESTORE_PERF_LOG_INTERVAL_SEC:
        state.last_perf_log_ts = now
        enqueue_firestore_job("perf", uid, device_id, sample)


# ─────────────────────────────────────────
# CORE PROCESSOR
# ─────────────────────────────────────────


def process_packet(payload: Dict[str, Any], relay_device_id: str) -> Dict[str, Any]:
    request_start_ms = int(time.time() * 1000)
    packet = extract_packet(payload)
    uid = packet["uid"]
    device_id = packet["device_id"]

    if not uid or not device_id:
        return {"ok": False, "error": "Missing uid or device_id"}
    if not device_id.startswith("rafeeq-watch-"):
        return {"ok": False, "error": f"Unexpected device_id format: {device_id}"}

    state = get_device_state(device_id)

    if state.duplicate_packet(packet["esp_packet_id"]):
        return {"ok": True, "skipped": True, "reason": "duplicate", "esp_packet_id": packet["esp_packet_id"]}

    if time.time() - state.last_upload_ts < UPLOAD_COOLDOWN_SEC:
        return {"ok": True, "skipped": True, "reason": "cooldown"}

    if not validate_sensor(packet["heart_rate"], packet["spo2"], packet["temperature"], packet["wearing"]):
        log.warning("[VALIDATION] Rejected hr=%s spo2=%s temp=%s", packet["heart_rate"], packet["spo2"], packet["temperature"])
        return {"ok": False, "error": "Sensor validation failed", "hr": packet["heart_rate"], "spo2": packet["spo2"]}

    if packet["wearing"]:
        if packet["heart_rate"]:
            state.buf_hr.push(packet["heart_rate"])
        if packet["spo2"]:
            state.buf_spo2.push(packet["spo2"])
        if packet["temperature"]:
            state.buf_temp.push(packet["temperature"])

    write_relay_event_once(uid, device_id, relay_device_id, state)
    snap = build_snapshot(packet, relay_device_id, state)

    phone_to_render_ms = None
    phone_sent = packet.get("phone_sent_at_epoch_ms")
    if phone_sent:
        phone_to_render_ms = max(0, request_start_ms - int(phone_sent))

    esp_to_cloud_total_ms = None
    if packet.get("esp_sent_at_epoch_ms") and packet.get("time_synced"):
        esp_to_cloud_total_ms = max(0, request_start_ms - int(packet["esp_sent_at_epoch_ms"]))

    try:
        write_start_ms = int(time.time() * 1000)
        write_live_snapshot(uid, device_id, snap)
        write_firestore_history(uid, device_id, snap, state)
        write_alert_log_if_needed(uid, device_id, snap, state)
        write_done_ms = int(time.time() * 1000)

        write_performance_metrics(
            uid, device_id, snap, state,
            render_processing_ms=max(0, write_start_ms - request_start_ms),
            firebase_write_ms=max(0, write_done_ms - write_start_ms),
            phone_to_render_ms=phone_to_render_ms,
            total_pipeline_ms=esp_to_cloud_total_ms,
        )
        state.daily_readings += 1
        state.last_upload_ts = time.time()
        log.info("[UPLOAD] relay uid=%s device=%s status=%s flags=%s", uid, device_id, snap["status"], snap["alert_flags_str"])
        return {
            "ok": True,
            "packet_id": snap["packet_id"],
            "status": snap["status"],
            "alert_flags": snap["alert_flags_str"],
            "wearing": snap["wearing"],
            "connection_mode": "relay",
            "history_sync": history_sync_snapshot(),
        }
    except Exception as exc:
        log.exception("[UPLOAD] RTDB live write failed: %s", exc)
        return {"ok": False, "error": str(exc)}


# ─────────────────────────────────────────
# BACKGROUND FIRESTORE WORKER
# ─────────────────────────────────────────

shutdown_event = threading.Event()


def queue_retry_loop() -> None:
    while not shutdown_event.is_set():
        shutdown_event.wait(FIRESTORE_WORKER_INTERVAL_SEC)
        if shutdown_event.is_set():
            break
        if not fs:
            continue

        jobs = local_queue.next_batch(FIRESTORE_WORKER_BATCH_SIZE)
        if not jobs:
            local_queue.purge_delivered()
            update_history_sync_state("ok")
            continue

        for row_id, job_type, uid, device_id, payload, attempts in jobs:
            try:
                firestore_set_job(job_type, uid, device_id, payload)
                local_queue.mark_delivered(row_id)
                update_history_sync_state("ok")
                log.info("[QUEUE] delivered id=%s type=%s", row_id, job_type)
            except Exception as exc:
                local_queue.mark_failed(row_id, attempts)
                update_history_sync_state("delayed", str(exc))
                log.warning("[QUEUE] delayed id=%s type=%s attempts=%s error=%s", row_id, job_type, attempts + 1, exc)


# ─────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────

app = Flask(__name__)


def _check_api_key() -> Optional[str]:
    if not RELAY_API_KEY:
        return None
    provided = request.headers.get("X-API-Key", "")
    if provided != RELAY_API_KEY:
        return "Unauthorized"
    return None


def _relay_device_id_from_request(data: Dict[str, Any]) -> str:
    return request.headers.get("X-Relay-Device-Id") or str(data.get("relay_device_id", DEFAULT_RELAY_DEVICE_ID))


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "firebase": fs is not None,
        "server": PROCESSED_BY_NAME,
        "schema_version": SCHEMA_VERSION,
        "connection_mode_written": "relay",
        "pi_alive": hb_monitor.is_alive(),
        "pi_seconds_since": None if math.isinf(hb_monitor.seconds_since()) else round(hb_monitor.seconds_since(), 1),
        "history_sync": history_sync_snapshot(),
    })


@app.route("/pi/heartbeat", methods=["POST"])
def pi_heartbeat():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401
    data = request.get_json(silent=True) if request.is_json else {}
    hb_monitor.record(data if isinstance(data, dict) else None)
    return jsonify({"ok": True, "pi_alive": True})


@app.route("/relay/packet", methods=["POST"])
def relay_packet():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401
    if not request.is_json:
        return jsonify({"ok": False, "error": "Content-Type must be application/json"}), 400
    data = request.get_json(silent=True)
    if not data or not isinstance(data, dict):
        return jsonify({"ok": False, "error": "Invalid or empty JSON body"}), 400
    result = process_packet(data, _relay_device_id_from_request(data))
    return jsonify(result), 200 if result.get("ok") else 422


@app.route("/relay-ingest", methods=["POST"])
def relay_ingest():
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401
    if not request.is_json:
        return jsonify({"success": False, "error": "Content-Type must be application/json"}), 400
    data = request.get_json(silent=True)
    if not data or not isinstance(data, dict):
        return jsonify({"success": False, "error": "Invalid or empty JSON body"}), 400
    result = process_packet(data, _relay_device_id_from_request(data))
    return jsonify({"success": result.get("ok", False), "received": data, "detail": result}), 200


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
    if not isinstance(packets, list) or not packets:
        return jsonify({"ok": False, "error": "packets array is empty or missing"}), 400
    relay_device_id = _relay_device_id_from_request(body)
    results = []
    for i, pkt in enumerate(packets):
        if not isinstance(pkt, dict):
            results.append({"index": i, "ok": False, "error": "not a dict"})
            continue
        r = process_packet(pkt, relay_device_id)
        r["index"] = i
        results.append(r)
    ok_count = sum(1 for r in results if r.get("ok"))
    return jsonify({"ok": True, "processed": len(results), "ok_count": ok_count, "fail_count": len(results) - ok_count, "results": results})


@app.route("/relay/reset_session/<device_id>", methods=["POST"])
def reset_relay_session(device_id: str):
    err = _check_api_key()
    if err:
        return jsonify({"ok": False, "error": err}), 401
    with _device_states_lock:
        if device_id in _device_states:
            _device_states[device_id].relay_event_written = False
    return jsonify({"ok": True, "device_id": device_id})


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────

init_firebase()
worker_thread = threading.Thread(target=queue_retry_loop, name="firestore_queue_worker", daemon=True)
worker_thread.start()

log.info("[SYSTEM] Rafeeq Render relay server ready")
log.info("[SYSTEM] Firebase: %s", "OK" if fs else "DISABLED")
log.info("[SYSTEM] API key protection: %s", "ON" if RELAY_API_KEY else "OFF (dev mode)")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
