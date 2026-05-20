#!/usr/bin/env python3
"""
RAFEEQ — Render Cloud Relay Server v5.3
Role: fallback relay pipeline when Raspberry Pi fog node is offline.

Flow:
  ESP32 Watch -> BLE -> Flutter Phone App -> HTTP POST -> Render -> Firebase

Render owns:
  - relay mode writes (connection_mode = "relay")
  - offline_sync recovery writes (connection_mode = "recovery", recovery_reason = "offline_sync")
  - historical vital_readings in Firestore
  - queue/audit/performance/mode_history/relay_logs Firestore collections

Render does NOT own:
  - primary mode
  - pi_handover recovery
  - system_health RTDB branch
  - Raspberry Pi logic

Required env vars:
  RAFEEQ_DATABASE_URL
  RAFEEQ_SERVICE_ACCOUNT_JSON  (full JSON string) OR GOOGLE_APPLICATION_CREDENTIALS (path)
  RAFEEQ_RELAY_API_KEY
  RAFEEQ_BACKEND_VERSION  (optional, default "5.3.0")
  PORT  (provided by Render automatically)
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
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, os.getenv("RAFEEQ_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("rafeeq.render.relay")


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

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
        return value.strip().lower() in {"true", "1", "yes", "wearing", "active", "on"}
    return default


def ensure_parent_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

SCHEMA_VERSION = "5.0"
SOURCE_TYPE_RELAY = "render_relay"
SOURCE_PATH_RELAY = "relay"
PROCESSED_BY_NAME = "Render Relay Backend"
RELAY_NODE_ID = os.getenv("RAFEEQ_RELAY_NODE_ID", "render-relay-node-01")
BACKEND_VERSION = os.getenv("RAFEEQ_BACKEND_VERSION", "5.3.0")

UPLOAD_COOLDOWN_SEC = env_float("RAFEEQ_UPLOAD_COOLDOWN_SEC", 2.0)
ROLLING_WINDOW_SIZE = env_int("RAFEEQ_ROLLING_WINDOW_SIZE", 10)
DEDUP_WINDOW_SEC = env_float("RAFEEQ_DEDUP_WINDOW_SEC", 60.0)  # wider for cross-restart safety

FIRESTORE_HISTORY_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_HISTORY_INTERVAL_SEC", 60.0)
FIRESTORE_PERF_LOG_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_PERF_LOG_INTERVAL_SEC", 60.0)
FIRESTORE_WORKER_INTERVAL_SEC = env_float("RAFEEQ_FIRESTORE_WORKER_INTERVAL_SEC", 30.0)
FIRESTORE_WORKER_BATCH_SIZE = env_int("RAFEEQ_FIRESTORE_WORKER_BATCH_SIZE", 3)
SQLITE_QUEUE_PATH = os.getenv("RAFEEQ_SQLITE_QUEUE", "/tmp/rafeeq_render_firestore_queue.db")

VALID_HR_RANGE: Tuple[int, int] = (20, 250)
VALID_SPO2_RANGE: Tuple[int, int] = (50, 100)
VALID_TEMP_RANGE: Tuple[float, float] = (20.0, 45.0)

RELAY_API_KEY = os.getenv("RAFEEQ_RELAY_API_KEY", "")
ALLOW_INSECURE_DEV = os.getenv("RAFEEQ_ALLOW_INSECURE_DEV", "false").strip().lower() == "true"

# Guard: write mode_history at most once per N seconds per device to avoid spam
MODE_HISTORY_COOLDOWN_SEC = env_float("RAFEEQ_MODE_HISTORY_COOLDOWN_SEC", 30.0)
AUDIT_LOG_COOLDOWN_SEC = env_float("RAFEEQ_AUDIT_LOG_COOLDOWN_SEC", 60.0)

fs: Any = None  # Firestore client


# ─────────────────────────────────────────
# FIREBASE INIT
# ─────────────────────────────────────────

def init_firebase() -> None:
    global fs

    database_url = os.getenv("RAFEEQ_DATABASE_URL", "").strip()
    service_account_json = os.getenv("RAFEEQ_SERVICE_ACCOUNT_JSON", "").strip()
    google_creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if not database_url:
        log.error("[FIREBASE] RAFEEQ_DATABASE_URL is missing")
        return

    try:
        if not firebase_admin._apps:
            if service_account_json:
                service_account = json.loads(service_account_json)
                cred = credentials.Certificate(service_account)
            elif google_creds_path:
                cred = credentials.Certificate(google_creds_path)
            else:
                log.error("[FIREBASE] No service account credentials found")
                return
            firebase_admin.initialize_app(cred, {"databaseURL": database_url})

        fs = firestore.client()
        log.info("[FIREBASE] Initialized OK")
    except Exception as exc:
        fs = None
        log.error("[FIREBASE] Init failed: %s", exc)


# ─────────────────────────────────────────
# ROLLING STATS
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


class CooldownTracker:
    """Thread-safe cooldown tracker for deduplicating log/event writes."""

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


# ─────────────────────────────────────────
# SQLITE FIRESTORE QUEUE
# ─────────────────────────────────────────

class LocalQueue:
    """SQLite-backed Firestore job queue.

    RTDB live writes are direct and fast.
    Firestore history/log writes are queued here and processed by a background worker.
    This means Firebase Firestore quota bursts do not block the live relay path.
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
            row = self.conn.execute(
                "SELECT MIN(created_at) FROM firestore_jobs WHERE delivered=0"
            ).fetchone()
            if not row or row[0] is None:
                return 0
            return int((time.time() - float(row[0])) * 1000)

    def next_batch(self, limit: int) -> List[Tuple[int, str, str, str, Dict[str, Any], int]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id,job_type,uid,device_id,payload_json,attempt_count "
                "FROM firestore_jobs WHERE delivered=0 AND next_attempt_at<=? "
                "ORDER BY CASE job_type "
                "WHEN 'alert_log' THEN 1 "
                "WHEN 'relay_log' THEN 2 "
                "WHEN 'vital_reading' THEN 3 "
                "WHEN 'mode_history' THEN 4 "
                "WHEN 'recovery_event' THEN 5 "
                "WHEN 'queue_log' THEN 6 "
                "WHEN 'system_event' THEN 7 "
                "WHEN 'audit_log' THEN 8 "
                "WHEN 'perf_log' THEN 9 "
                "ELSE 10 END, created_at ASC LIMIT ?",
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


def enqueue_job(job_type: str, uid: str, device_id: str, payload: Dict[str, Any]) -> None:
    if not uid or not device_id:
        return
    local_queue.enqueue_job(job_type, uid, device_id, payload)
    with queue_state_lock:
        if history_sync_status == "ok":
            pass  # will be updated lazily
    update_history_sync_state("pending")


# ─────────────────────────────────────────
# DEVICE STATE
# ─────────────────────────────────────────

class DeviceState:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.buf_hr = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_spo2 = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.buf_temp = RollingBuffer(ROLLING_WINDOW_SIZE)
        self.alert_dedup = CooldownTracker(DEDUP_WINDOW_SEC)
        self.mode_history_dedup = CooldownTracker(MODE_HISTORY_COOLDOWN_SEC)
        self.audit_dedup = CooldownTracker(AUDIT_LOG_COOLDOWN_SEC)
        self.last_upload_ts = 0.0
        self.last_history_enqueue_ts = 0.0
        self.last_perf_log_ts = 0.0
        self.relay_activated_written = False
        self.seen_packet_ids: Dict[str, float] = {}  # in-memory dedup
        self.lat_phone_to_render: Deque[float] = deque(maxlen=60)
        self.lat_render_processing: Deque[float] = deque(maxlen=60)
        self.lat_firebase: Deque[float] = deque(maxlen=60)
        self.lat_total: Deque[float] = deque(maxlen=60)
        self.daily_readings = 0
        self.daily_alerts = 0
        # Worn time tracking
        self.worn_day_key = ""
        self.worn_today_sec = 0.0
        self.last_worn_sample_time: Optional[float] = None
        self.max_worn_gap_sec = 120.0
        self.last_valid_reading: Dict[str, Any] = {}
        # Mode tracking for mode_history
        self.last_known_mode: Optional[str] = None

    def is_duplicate_packet(self, packet_id: Optional[str], esp_packet_id: Optional[str]) -> bool:
        """In-memory dedup. Firestore dedup (by doc ID) is the authoritative cross-restart guard."""
        now = time.time()
        # Clean stale entries
        stale_keys = [k for k, ts in self.seen_packet_ids.items() if now - ts > DEDUP_WINDOW_SEC]
        for k in stale_keys:
            self.seen_packet_ids.pop(k, None)

        key = packet_id or esp_packet_id
        if not key:
            return False
        if key in self.seen_packet_ids:
            return True
        self.seen_packet_ids[key] = now
        return False


_device_states: Dict[str, DeviceState] = {}
_device_states_lock = threading.Lock()


def get_device_state(device_id: str) -> DeviceState:
    with _device_states_lock:
        if device_id not in _device_states:
            _device_states[device_id] = DeviceState(device_id)
        return _device_states[device_id]


# ─────────────────────────────────────────
# PACKET PARSING + VALIDATION
# ─────────────────────────────────────────

def validate_relay_request(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Returns error dict if invalid, None if valid."""
    if not data.get("uid"):
        return {"ok": False, "error": "missing_uid", "message": "uid is required"}
    if not data.get("device_id"):
        return {"ok": False, "error": "missing_device_id", "message": "device_id is required"}

    vitals = data.get("vitals") if isinstance(data.get("vitals"), dict) else {}
    pid = data.get("packet_id") or data.get("esp_packet_id") or vitals.get("packet_id")
    if not pid:
        return {"ok": False, "error": "missing_packet_id", "message": "packet_id or esp_packet_id is required"}

    sv = str(data.get("schema_version", "5.0"))
    if not sv.startswith("5"):
        return {"ok": False, "error": "invalid_schema_version", "message": f"schema_version must be 5.x, got {sv}"}

    hr = safe_get(vitals or data, "heart_rate", safe_get(data, "hr", None), int)
    spo2 = safe_get(vitals or data, "spo2", None, int)
    temp = safe_get(vitals or data, "temperature", None, float)

    if hr is not None and not isinstance(hr, (int, float)):
        return {"ok": False, "error": "invalid_heart_rate", "message": "heart_rate must be numeric"}
    if spo2 is not None and not isinstance(spo2, (int, float)):
        return {"ok": False, "error": "invalid_spo2", "message": "spo2 must be numeric"}
    if temp is not None and not isinstance(temp, (int, float)):
        return {"ok": False, "error": "invalid_temperature", "message": "temperature must be numeric"}

    return None


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


def extract_packet(payload: Dict[str, Any]) -> Dict[str, Any]:
    vitals = payload.get("vitals") if isinstance(payload.get("vitals"), dict) else {}
    motion = payload.get("motion") if isinstance(payload.get("motion"), dict) else {}
    device = payload.get("device") if isinstance(payload.get("device"), dict) else {}

    wearing = parse_bool(payload.get("wearing", vitals.get("wearing", vitals.get("finger", True))), True)

    # packet_id: prefer top-level packet_id, fallback to esp_packet_id
    packet_id = str(payload.get("packet_id", payload.get("esp_packet_id", ""))).strip() or None
    esp_packet_id = str(payload.get("esp_packet_id", payload.get("packet_id", ""))).strip() or None

    return {
        "uid": str(payload.get("uid", "")).strip(),
        "device_id": str(payload.get("device_id", "")).strip(),
        "packet_id": packet_id,
        "esp_packet_id": esp_packet_id,
        "seq": safe_get(payload, "seq", None, int),
        "schema_version": str(payload.get("schema_version", SCHEMA_VERSION)),
        "heart_rate": safe_get(vitals or payload, "heart_rate", safe_get(payload, "hr", 0), int),
        "spo2": safe_get(vitals or payload, "spo2", 0, int),
        "temperature": safe_get(
            vitals or payload, "temperature",
            safe_get(vitals or payload, "temperature_c", safe_get(payload, "tempC", safe_get(payload, "temp", 0.0))),
            float,
        ),
        "blood_pressure": str(payload.get("blood_pressure", vitals.get("blood_pressure", "0/0"))),
        "glucose": safe_get(vitals or payload, "glucose", 0, int),
        "wearing": wearing,
        "confidence_score": safe_get(payload, "confidence_score", safe_get(vitals, "confidence_score", None), int),
        "wear_confidence": str(payload.get("wear_confidence", vitals.get("wear_confidence", "unknown"))),
        "reading_valid": parse_bool(payload.get("reading_valid", vitals.get("reading_valid", None)), None),
        "imu_candidate": bool(safe_get(motion or payload, "imu_candidate", safe_get(payload, "candidate", False))),
        "imu_peak_svm": safe_get(motion or payload, "peak_svm", safe_get(payload, "peakSVM", 0.0), float),
        "imu_stillness": safe_get(motion or payload, "motion_level", safe_get(motion or payload, "stillness", 0.0), float),
        "hr_spike": parse_bool(payload.get("hr_spike", False), False),
        "spo2_drop": parse_bool(payload.get("spo2_drop", False), False),
        "data_smoothed": parse_bool(payload.get("data_smoothed", False), False),
        "esp_sent_at_ms": safe_get(payload, "sent_at_ms", safe_get(payload, "esp_sent_at_ms", None), int),
        "esp_sent_at_epoch_ms": safe_get(payload, "sent_at_epoch_ms", safe_get(payload, "esp_sent_at_epoch_ms", None), int),
        "esp_uptime_ms": safe_get(payload, "esp_uptime_ms", None, int),
        "esp_boot_count": safe_get(payload, "esp_boot_count", device.get("esp_boot_count"), int),
        "battery_pct": safe_get(payload, "battery_pct", device.get("battery_pct"), int),
        "power_mode": str(payload.get("power_mode", device.get("power_mode", "POWER_NORMAL"))),
        "charging": parse_bool(payload.get("charging", device.get("charging", False)), False),
        "fault_flags": safe_get(payload, "fault_flags", device.get("fault_flags", 0), int),
        "time_synced": parse_bool(payload.get("time_synced", payload.get("ntp_synced", False)), False),
        "ntp_last_sync_ms_ago": safe_get(payload, "ntp_last_sync_ms_ago", None, int),
        "phone_sent_at_epoch_ms": safe_get(payload, "phone_sent_at_epoch_ms", None, int),
        "app_sent_at": payload.get("app_sent_at") or payload.get("sent_at"),
        "app_instance_id": payload.get("app_instance_id"),
        "transport_mode": str(payload.get("transport_mode", "BLE_RELAY")),
        "is_queued": parse_bool(payload.get("is_queued", False), False),
        "latest_live": parse_bool(payload.get("latest_live", False), False),
        "firmware_version": str(payload.get("firmware_version", device.get("firmware_version", ""))),
    }


# ─────────────────────────────────────────
# VITALS ANALYTICS
# ─────────────────────────────────────────

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


def local_fusion(hr: int, spo2: int, temp: float, wearing: bool,
                 imu_candidate: bool, hr_spike: bool, spo2_drop: bool, fault_flags: int
                 ) -> Dict[str, Any]:
    if not wearing:
        return {"status": "NoData", "danger": False, "warning": False, "flags": ["NotWearing"]}

    flags: List[str] = []
    danger = False
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


def reading_valid_for_display(wearing: bool, hr: int, spo2: int, temp: float) -> bool:
    try:
        return bool(wearing and int(hr or 0) > 0 and int(spo2 or 0) > 0 and float(temp or 0) > 0)
    except Exception:
        return False


def format_duration_sec(seconds: float) -> str:
    total_minutes = max(0, int(seconds // 60))
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"


def compute_display_fields(packet: Dict[str, Any], state: DeviceState) -> Dict[str, Any]:
    """Compute display fields and update state.last_valid_reading. Returns display dict."""
    now = time.time()
    wearing = packet["wearing"]
    hr = packet["heart_rate"]
    spo2 = packet["spo2"]
    temp = packet["temperature"]
    is_valid = reading_valid_for_display(wearing, hr, spo2, temp)

    day_key = utc_now_iso()[:10]
    if state.worn_day_key != day_key:
        state.worn_day_key = day_key
        state.worn_today_sec = 0.0
        state.last_worn_sample_time = None

    if wearing and is_valid:
        if state.last_worn_sample_time is not None:
            gap = max(0.0, now - state.last_worn_sample_time)
            state.worn_today_sec += min(gap, state.max_worn_gap_sec)
        state.last_worn_sample_time = now
        state.last_valid_reading = {
            "heart_rate": hr,
            "spo2": spo2,
            "temperature": temp,
            "blood_pressure": packet.get("blood_pressure", "0/0"),
            "glucose": packet.get("glucose", 0),
            "updated_at": utc_now_iso(),
            "esp_packet_id": packet.get("esp_packet_id"),
        }
        return {
            "reading_valid": True,
            "reading_status_label": "Live",
            "display_heart_rate": hr,
            "display_spo2": spo2,
            "display_temperature": temp,
            "display_blood_pressure": packet.get("blood_pressure", "0/0"),
            "display_glucose": packet.get("glucose", 0),
            "display_updated_at": utc_now_iso(),
            "last_valid_reading": dict(state.last_valid_reading),
        }

    elif not wearing:
        state.last_worn_sample_time = None
        lvr = state.last_valid_reading
        if lvr:
            status_label = "Cached / Watch not worn"
            display = {k: lvr.get(k) for k in ["heart_rate", "spo2", "temperature", "blood_pressure", "glucose", "updated_at"]}
        else:
            status_label = "Watch not worn"
            display = {"heart_rate": None, "spo2": None, "temperature": None,
                       "blood_pressure": None, "glucose": None, "updated_at": None}
        return {
            "reading_valid": False,
            "reading_status_label": status_label,
            "display_heart_rate": display.get("heart_rate"),
            "display_spo2": display.get("spo2"),
            "display_temperature": display.get("temperature"),
            "display_blood_pressure": display.get("blood_pressure"),
            "display_glucose": display.get("glucose"),
            "display_updated_at": display.get("updated_at"),
            "last_valid_reading": dict(state.last_valid_reading),
        }

    else:
        lvr = state.last_valid_reading
        return {
            "reading_valid": False,
            "reading_status_label": "Waiting for valid reading",
            "display_heart_rate": lvr.get("heart_rate"),
            "display_spo2": lvr.get("spo2"),
            "display_temperature": lvr.get("temperature"),
            "display_blood_pressure": lvr.get("blood_pressure"),
            "display_glucose": lvr.get("glucose"),
            "display_updated_at": lvr.get("updated_at"),
            "last_valid_reading": dict(lvr),
        }


# ─────────────────────────────────────────
# PI HANDOVER PROTECTION
# ─────────────────────────────────────────

def read_current_firebase_status(uid: str, device_id: str) -> Dict[str, Any]:
    """Read current live status from RTDB. Returns empty dict on failure."""
    try:
        ref = rtdb.reference(f"live/{uid}/{device_id}/status")
        val = ref.get()
        return val if isinstance(val, dict) else {}
    except Exception as exc:
        log.warning("[STATUS_READ] Failed to read current status: %s", exc)
        return {}


def is_pi_handover_active(status: Dict[str, Any]) -> bool:
    return (
        status.get("connection_mode") == "recovery"
        and status.get("recovery_reason") == "pi_handover"
    )


def is_primary_active(status: Dict[str, Any]) -> bool:
    return status.get("connection_mode") == "primary"


def should_render_write_relay(current_status: Dict[str, Any], packet: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    """
    Determines whether Render is permitted to write connection_mode = "relay".

    Rules:
    - Never if Pi handover recovery is active.
    - If Pi has primary: only allow relay if the app explicitly signals Pi is unavailable/stale
      AND the packet is not a queued/historical packet.
    - Otherwise (offline, relay, unknown): allow.
    """
    if is_pi_handover_active(current_status):
        return False

    if current_status.get("connection_mode") == "primary":
        app_says_pi_offline = (
            payload.get("app_pi_online") is False
            or payload.get("pi_online") is False
            or payload.get("force_relay") is True
            or payload.get("relay_active") is True
        )
        return app_says_pi_offline and not packet.get("is_queued")

    return True


def should_write_live_rtdb(current_status: Dict[str, Any], packet: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    """
    Determines whether Render is permitted to write live RTDB branches
    (vitals, device, alerts, status).

    Rules:
    - Never if Pi handover recovery is active.
    - If Pi has primary: only allow live RTDB writes when the packet is explicitly
      marked latest_live=True AND the app has proven Pi is unavailable/stale.
    - Otherwise: allow.
    """
    if is_pi_handover_active(current_status):
        return False

    if current_status.get("connection_mode") == "primary":
        app_says_pi_offline = (
            payload.get("app_pi_online") is False
            or payload.get("pi_online") is False
            or payload.get("force_relay") is True
            or payload.get("relay_active") is True
        )
        return bool(payload.get("latest_live") is True and app_says_pi_offline)

    return True


# ─────────────────────────────────────────
# FIREBASE WRITES — RTDB BRANCHES
# ─────────────────────────────────────────

def write_vitals_branch(uid: str, device_id: str, packet: Dict[str, Any],
                         display: Dict[str, Any], confidence: int, quality: str,
                         timestamp: str, packet_id: str) -> None:
    """Write only vitals-relevant fields to live/{uid}/{device_id}/vitals."""
    ref = rtdb.reference(f"live/{uid}/{device_id}/vitals")
    ref.update({
        # Raw vitals
        "heart_rate": packet["heart_rate"],
        "spo2": packet["spo2"],
        "temperature": packet["temperature"],
        "blood_pressure": packet.get("blood_pressure", "0/0"),
        "glucose": packet.get("glucose", 0),
        # Display vitals (no zeros if invalid)
        "display_heart_rate": display.get("display_heart_rate"),
        "display_spo2": display.get("display_spo2"),
        "display_temperature": display.get("display_temperature"),
        "display_blood_pressure": display.get("display_blood_pressure"),
        "display_glucose": display.get("display_glucose"),
        "display_updated_at": display.get("display_updated_at"),
        # Reading validity
        "reading_valid": display.get("reading_valid", False),
        "reading_status_label": display.get("reading_status_label", "Waiting for valid reading"),
        "last_valid_reading": display.get("last_valid_reading", {}),
        # Quality
        "confidence_score": confidence,
        "quality_tier": quality,
        # Identifiers
        "packet_id": packet_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        # Timestamps
        "updated_at": timestamp,
    })


def write_status_branch(uid: str, device_id: str, timestamp: str,
                         connection_mode: str, request_id: str,
                         transport_mode: str = "BLE_PHONE_RELAY") -> None:
    """Write relay status fields. Caller must check Pi handover protection first."""
    ref = rtdb.reference(f"live/{uid}/{device_id}/status")
    ref.update({
        "connection_mode": connection_mode,
        "relay_active": True,
        "pi_online": False,
        "cloud_connected": fs is not None,
        "data_stale": False,
        "relay_last_seen": timestamp,
        "app_last_seen": timestamp,
        "last_seen": timestamp,
        "source_path": SOURCE_PATH_RELAY,
        "source_type": SOURCE_TYPE_RELAY,
        "source_id": RELAY_NODE_ID,
        "transport_mode": transport_mode,
        "processed_by": PROCESSED_BY_NAME,
        "schema_version": SCHEMA_VERSION,
        # Render-specific metadata
        "render_last_seen": timestamp,
        "render_request_id": request_id,
        "render_backend_version": BACKEND_VERSION,
    })


def write_device_branch(uid: str, device_id: str, packet: Dict[str, Any], timestamp: str) -> None:
    """Write device hardware fields to live/{uid}/{device_id}/device."""
    ref = rtdb.reference(f"live/{uid}/{device_id}/device")
    update: Dict[str, Any] = {
        "device_id": packet.get("device_id"),
        "wearing": packet.get("wearing"),
        "wear_confidence": packet.get("wear_confidence"),
        "battery_pct": packet.get("battery_pct"),
        "power_mode": packet.get("power_mode"),
        "charging": packet.get("charging"),
        "fault_flags": packet.get("fault_flags"),
        "esp_packet_id": packet.get("esp_packet_id"),
        "ntp_synced": packet.get("time_synced"),
        "time_synced": packet.get("time_synced"),
        "ntp_last_sync_ms_ago": packet.get("ntp_last_sync_ms_ago"),
        "esp_uptime_ms": packet.get("esp_uptime_ms"),
        "esp_boot_count": packet.get("esp_boot_count"),
        "firmware_version": packet.get("firmware_version"),
        "updated_at": timestamp,
    }
    ref.update({k: v for k, v in update.items() if v is not None})


def write_queue_branch(uid: str, device_id: str, queue_depth: int = 0,
                        queue_oldest_ms: int = 0, sync_status: str = "ok") -> None:
    ref = rtdb.reference(f"live/{uid}/{device_id}/queue")
    ref.update({
        "queue_depth": queue_depth,
        "queue_oldest_ms": queue_oldest_ms,
        "sync_status": sync_status,
        "last_sync_at": utc_now_iso(),
        "source": "render_relay",
        "history_sync_status": history_sync_snapshot().get("history_sync_status", "ok"),
    })


def write_alerts_branch(uid: str, device_id: str, fusion: Dict[str, Any], timestamp: str) -> None:
    ref = rtdb.reference(f"live/{uid}/{device_id}/alerts")
    flags = fusion.get("flags", [])
    ref.update({
        "active": bool(fusion.get("danger") or fusion.get("warning")),
        "latest_status": fusion.get("status"),
        "latest_alert_type": flags[0] if flags and flags[0] != "Normal" else None,
        "latest_alert_value": None,
        "latest_alert_at": timestamp if flags and flags[0] not in ("Normal", "NotWearing") else None,
        "emergency": bool(fusion.get("danger")),
    })


def write_performance_branch(uid: str, device_id: str, state: DeviceState,
                               render_processing_ms: float, firebase_write_ms: float,
                               phone_to_render_ms: Optional[float],
                               total_pipeline_ms: Optional[float]) -> None:
    if phone_to_render_ms is not None:
        state.lat_phone_to_render.append(phone_to_render_ms)
    state.lat_render_processing.append(render_processing_ms)
    state.lat_firebase.append(firebase_write_ms)
    total = total_pipeline_ms if total_pipeline_ms is not None else render_processing_ms + firebase_write_ms
    state.lat_total.append(total)

    def avg_dq(dq: Deque[float]) -> Optional[float]:
        return round(sum(dq) / len(dq), 2) if dq else None

    sample: Dict[str, Any] = {
        "app_to_render_ms": round(phone_to_render_ms, 2) if phone_to_render_ms is not None else None,
        "render_processing_ms": round(render_processing_ms, 2),
        "firebase_write_ms": round(firebase_write_ms, 2),
        "total_pipeline_ms": round(total, 2),
        "avg_total_ms": avg_dq(state.lat_total),
        "samples": len(state.lat_total),
        "updated_at": utc_now_iso(),
    }

    try:
        rtdb.reference(f"live/{uid}/{device_id}/performance").update(sample)
    except Exception as exc:
        log.debug("[PERF] RTDB performance update failed: %s", exc)

    now = time.time()
    if now - state.last_perf_log_ts >= FIRESTORE_PERF_LOG_INTERVAL_SEC:
        state.last_perf_log_ts = now
        enqueue_job("perf_log", uid, device_id, {
            **sample,
            "device_id": device_id,
            "source_type": SOURCE_TYPE_RELAY,
            "created_at": utc_now_iso(),
        })


# ─────────────────────────────────────────
# FIRESTORE WRITES (via job queue)
# ─────────────────────────────────────────

def firestore_set_job(job_type: str, uid: str, device_id: str, payload: Dict[str, Any]) -> None:
    """Execute a Firestore write job. Called by the background worker."""
    if not fs:
        raise RuntimeError("Firestore client is not available")

    device_doc = fs.collection("users").document(uid).collection("devices").document(device_id)

    if job_type == "vital_reading":
        doc_id = str(payload.get("packet_id") or payload.get("esp_packet_id") or uuid.uuid4())
        device_doc.collection("vital_readings").document(doc_id).set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP},
            merge=True,  # idempotent: merge so re-upload doesn't wipe existing fields
        )
        return

    if job_type == "alert_log":
        doc_id = str(payload.get("alert_id") or payload.get("packet_id") or uuid.uuid4())
        device_doc.collection("alert_logs").document(doc_id).set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "relay_log":
        doc_id = str(payload.get("request_id") or uuid.uuid4())
        device_doc.collection("relay_logs").document(doc_id).set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "system_event":
        device_doc.collection("system_events").document().set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "mode_history":
        device_doc.collection("mode_history").document().set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "recovery_event":
        doc_id = str(payload.get("recovery_id") or uuid.uuid4())
        device_doc.collection("recovery_events").document(doc_id).set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        return

    if job_type == "queue_log":
        device_doc.collection("queue_logs").document().set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "audit_log":
        device_doc.collection("audit_logs").document().set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "perf_log":
        device_doc.collection("performance_logs").document().set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    if job_type == "fault_log":
        doc_id = str(payload.get("esp_packet_id") or uuid.uuid4())
        device_doc.collection("fault_logs").document(doc_id).set(
            {**payload, "timestamp_server": firestore.SERVER_TIMESTAMP}
        )
        return

    raise ValueError(f"Unknown Firestore job type: {job_type}")


# ─────────────────────────────────────────
# SMART FIRESTORE HELPERS
# ─────────────────────────────────────────

def enqueue_vital_reading(uid: str, device_id: str, packet: Dict[str, Any],
                           display: Dict[str, Any], confidence: int, quality: str,
                           packet_id: str, timestamp: str, state: DeviceState) -> None:
    """Enqueue historical vital reading for live relay. Skips if called too recently (interval-based)."""
    now = time.time()
    if now - state.last_history_enqueue_ts < FIRESTORE_HISTORY_INTERVAL_SEC:
        return
    state.last_history_enqueue_ts = now

    enqueue_job("vital_reading", uid, device_id, {
        "packet_id": packet_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        "seq": packet.get("seq"),
        "timestamp": timestamp,
        "device_id": packet.get("device_id"),
        "heart_rate": packet.get("heart_rate"),
        "spo2": packet.get("spo2"),
        "temperature": packet.get("temperature"),
        "blood_pressure": packet.get("blood_pressure"),
        "glucose": packet.get("glucose"),
        "wearing": packet.get("wearing"),
        "reading_valid": display.get("reading_valid", False),
        "confidence_score": confidence,
        "quality_tier": quality,
        "source_type": SOURCE_TYPE_RELAY,
        "source_path": SOURCE_PATH_RELAY,
        "transport_mode": packet.get("transport_mode", "BLE_RELAY"),
        "schema_version": SCHEMA_VERSION,
        "relay_uploaded_at": timestamp,
        "app_sent_at": packet.get("app_sent_at"),
        "render_received_at": timestamp,
    })


def enqueue_vital_reading_force(uid: str, device_id: str, packet: Dict[str, Any],
                                  display: Dict[str, Any], confidence: int, quality: str,
                                  packet_id: str, timestamp: str) -> None:
    """
    Enqueue a vital_reading unconditionally — no cooldown, no state update.
    Used for offline sync where every queued packet must be stored regardless of interval.
    The Firestore document ID is packet_id/esp_packet_id, so duplicates are naturally
    handled by Firestore set(merge=True).
    """
    enqueue_job("vital_reading", uid, device_id, {
        "packet_id": packet_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        "seq": packet.get("seq"),
        "timestamp": timestamp,
        "device_id": packet.get("device_id"),
        "heart_rate": packet.get("heart_rate"),
        "spo2": packet.get("spo2"),
        "temperature": packet.get("temperature"),
        "blood_pressure": packet.get("blood_pressure"),
        "glucose": packet.get("glucose"),
        "wearing": packet.get("wearing"),
        "reading_valid": display.get("reading_valid", False),
        "confidence_score": confidence,
        "quality_tier": quality,
        "source_type": SOURCE_TYPE_RELAY,
        "source_path": SOURCE_PATH_RELAY,
        "transport_mode": packet.get("transport_mode", "BLE_RELAY"),
        "schema_version": SCHEMA_VERSION,
        "relay_uploaded_at": timestamp,
        "app_sent_at": packet.get("app_sent_at"),
        "render_received_at": timestamp,
        "offline_queued": True,  # mark as originally queued offline packet
    })


def enqueue_alert_log_if_needed(uid: str, device_id: str, fusion: Dict[str, Any],
                                  packet: Dict[str, Any], packet_id: str,
                                  timestamp: str, state: DeviceState) -> None:
    flags = fusion.get("flags", [])
    flags_str = "; ".join(flags)
    if not flags or flags_str in ("Normal", "NotWearing"):
        return
    if not state.alert_dedup.should_write(flags_str):
        return

    enqueue_job("alert_log", uid, device_id, {
        "alert_id": packet_id,
        "packet_id": packet_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        "alert_type": flags[0] if flags else None,
        "alert_title": flags[0] if flags else None,
        "alert_description": flags_str,
        "severity": "danger" if fusion.get("danger") else "warning",
        "status": fusion.get("status"),
        "value": None,
        "threshold": None,
        "created_at": timestamp,
        "resolved_at": None,
        "source_type": SOURCE_TYPE_RELAY,
        "device_id": packet.get("device_id"),
        "metadata": {"all_flags": flags},
    })


def enqueue_relay_activated_log(uid: str, device_id: str, packet: Dict[str, Any],
                                  packet_id: str, timestamp: str, request_id: str,
                                  state: DeviceState) -> None:
    """Write relay_log and system_event once when relay activates for this session."""
    if state.relay_activated_written:
        return

    enqueue_job("relay_log", uid, device_id, {
        "request_id": request_id,
        "packet_id": packet_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        "uid": uid,
        "device_id": packet.get("device_id"),
        "received_at": timestamp,
        "status": "accepted",
        "deduplicated": False,
        "source_type": SOURCE_TYPE_RELAY,
        "app_instance_id": packet.get("app_instance_id"),
        "metadata": {"schema_version": SCHEMA_VERSION, "backend_version": BACKEND_VERSION},
    })

    if state.audit_dedup.should_write(f"relay_activated_{device_id}"):
        enqueue_job("audit_log", uid, device_id, {
            "event_type": "RELAY_ACTIVATED",
            "event_title": "Relay mode activated",
            "event_description": "Render relay backend started receiving live relay packets",
            "actor_type": "render",
            "actor_uid": uid,
            "device_id": device_id,
            "timestamp": timestamp,
            "severity": "info",
            "metadata": {"backend_version": BACKEND_VERSION},
        })

        enqueue_job("system_event", uid, device_id, {
            "event_type": "RELAY_ACTIVATED",
            "event_title": "Relay mode activated",
            "event_description": "Phone relay path is active via Render",
            "actor": SOURCE_TYPE_RELAY,
            "source_type": SOURCE_TYPE_RELAY,
            "created_at": timestamp,
            "device_id": device_id,
            "metadata": {},
        })

    state.relay_activated_written = True


def enqueue_mode_history(uid: str, device_id: str, from_mode: str, to_mode: str,
                          reason: str, timestamp: str, state: DeviceState,
                          esp_packet_id: Optional[str] = None) -> None:
    """Write mode_history only if mode changed and cooldown passed."""
    if not state.mode_history_dedup.should_write(f"{from_mode}->{to_mode}_{device_id}"):
        return
    if from_mode == to_mode:
        return

    enqueue_job("mode_history", uid, device_id, {
        "from_mode": from_mode,
        "to_mode": to_mode,
        "reason": reason,
        "trigger": "render_relay_backend",
        "actor": "render",
        "started_at": timestamp,
        "completed_at": timestamp,
        "status": "completed",
        "device_id": device_id,
        "esp_packet_id": esp_packet_id,
        "metadata": {"backend_version": BACKEND_VERSION},
    })

    state.last_known_mode = to_mode


def enqueue_fault_log_if_needed(uid: str, device_id: str, packet: Dict[str, Any],
                                  packet_id: str, timestamp: str) -> None:
    fault_flags = packet.get("fault_flags", 0)
    if not fault_flags:
        return

    fault_names = []
    for bit, name in enumerate(["SENSOR_ERR", "BLE_TIMEOUT", "BATTERY_LOW", "NTP_FAIL",
                                  "IMU_FAULT", "STORAGE_FULL", "WATCHDOG", "UNKNOWN"]):
        if fault_flags & (1 << bit):
            fault_names.append(name)

    enqueue_job("fault_log", uid, device_id, {
        "fault_flags": fault_flags,
        "fault_names": fault_names,
        "severity": "warning",
        "detected_at": timestamp,
        "resolved_at": None,
        "device_id": device_id,
        "esp_packet_id": packet.get("esp_packet_id"),
        "battery_pct": packet.get("battery_pct"),
        "power_mode": packet.get("power_mode"),
        "metadata": {},
    })


# ─────────────────────────────────────────
# CORE LIVE RELAY PROCESSOR
# ─────────────────────────────────────────

def process_live_relay_packet(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main processor for live relay packets from the Flutter app.
    Handles Pi handover protection, writes RTDB live data, queues Firestore history.
    """
    request_start_ms = int(time.time() * 1000)
    request_id = str(uuid.uuid4())
    timestamp = utc_now_iso()

    packet = extract_packet(payload)
    uid = packet["uid"]
    device_id = packet["device_id"]
    packet_id = packet.get("packet_id") or str(uuid.uuid4())

    state = get_device_state(device_id)

    # ── In-memory dedup ──────────────────────────────────────
    if state.is_duplicate_packet(packet.get("packet_id"), packet.get("esp_packet_id")):
        log.info("[RELAY] Duplicate packet skipped uid=%s device=%s pid=%s", uid, device_id, packet_id)
        return {
            "ok": True,
            "role": "render_relay_backend",
            "schema_version": SCHEMA_VERSION,
            "deduplicated": True,
            "reason": "duplicate_packet_no_status_write",
            "packet_id": packet_id,
            "connection_mode_written": None,
            "history_sync": history_sync_snapshot(),
        }

    # ── Sensor validation ────────────────────────────────────
    if not validate_sensor(packet["heart_rate"], packet["spo2"], packet["temperature"], packet["wearing"]):
        log.warning("[RELAY] Sensor validation failed uid=%s hr=%s spo2=%s temp=%s",
                    uid, packet["heart_rate"], packet["spo2"], packet["temperature"])
        return {
            "ok": False,
            "error": "sensor_validation_failed",
            "message": "Sensor values out of valid range",
            "connection_mode_written": None,
        }

    # ── Read current Firebase status (Pi handover + primary protection) ──
    current_status = read_current_firebase_status(uid, device_id)
    pi_handover = is_pi_handover_active(current_status)
    primary_active = is_primary_active(current_status)
    prev_mode = current_status.get("connection_mode")
    permit_relay_write = should_render_write_relay(current_status, packet, payload)
    permit_live_write = should_write_live_rtdb(current_status, packet, payload)

    # ── Compute analytics ─────────────────────────────────────
    if packet["wearing"]:
        if packet["heart_rate"]:
            state.buf_hr.push(packet["heart_rate"])
        if packet["spo2"]:
            state.buf_spo2.push(packet["spo2"])
        if packet["temperature"]:
            state.buf_temp.push(packet["temperature"])

    confidence = compute_confidence(state, packet["heart_rate"], packet["spo2"], packet["wearing"])
    quality = quality_tier(confidence)
    fusion = local_fusion(
        packet["heart_rate"], packet["spo2"], packet["temperature"], packet["wearing"],
        packet["imu_candidate"], packet["hr_spike"], packet["spo2_drop"], packet["fault_flags"],
    )
    display = compute_display_fields(packet, state)

    phone_to_render_ms: Optional[float] = None
    if packet.get("phone_sent_at_epoch_ms"):
        phone_to_render_ms = max(0, request_start_ms - int(packet["phone_sent_at_epoch_ms"]))

    total_pipeline_ms: Optional[float] = None
    if packet.get("esp_sent_at_epoch_ms") and packet.get("time_synced"):
        total_pipeline_ms = max(0, request_start_ms - int(packet["esp_sent_at_epoch_ms"]))

    try:
        write_start_ms = int(time.time() * 1000)

        # ── Determine live write permission ──────────────────
        # permit_live_write gates ALL live RTDB branches (vitals, device, alerts, status).
        # If False (primary active, or pi_handover), only Firestore history is written.
        connection_mode_written: Optional[str] = None
        protection_reason: Optional[str] = None

        if not permit_live_write:
            # Primary is active (or pi_handover) — no live RTDB writes at all.
            if pi_handover:
                # Pi handover: still safe to update relay timing fields only.
                log.info("[RELAY] Pi handover active — skipping live RTDB uid=%s device=%s", uid, device_id)
                rtdb.reference(f"live/{uid}/{device_id}/status").update({
                    "relay_last_seen": timestamp,
                    "app_last_seen": timestamp,
                    "render_last_seen": timestamp,
                    "render_request_id": request_id,
                })
                protection_reason = "pi_handover_active"
            else:
                # Primary protected: skip ALL live RTDB writes (vitals/device/alerts/status).
                log.info("[RELAY] Primary protected — history only uid=%s device=%s", uid, device_id)
                protection_reason = "primary_protected"

        else:
            # ── Write RTDB live data ─────────────────────────
            write_vitals_branch(uid, device_id, packet, display, confidence, quality, timestamp, packet_id)
            write_device_branch(uid, device_id, packet, timestamp)
            write_alerts_branch(uid, device_id, fusion, timestamp)

            # ── Status branch ────────────────────────────────
            if permit_relay_write:
                write_status_branch(uid, device_id, timestamp, "relay", request_id,
                                    transport_mode=packet.get("transport_mode", "BLE_PHONE_RELAY"))
                connection_mode_written = "relay"

                # Write mode_history only if mode actually changed.
                if prev_mode and prev_mode != "relay":
                    enqueue_mode_history(uid, device_id, prev_mode, "relay",
                                         "relay_packet_received", timestamp, state,
                                         esp_packet_id=packet.get("esp_packet_id"))
                    state.last_known_mode = "relay"
            else:
                # live_write permitted but relay status write is not (primary + latest_live edge case)
                log.info("[RELAY] Live written but relay status blocked uid=%s device=%s", uid, device_id)
                protection_reason = "primary_protected_status_only"

        write_done_ms = int(time.time() * 1000)

        # ── Firestore history + logs ─────────────────────────
        enqueue_vital_reading(uid, device_id, packet, display, confidence, quality,
                               packet_id, timestamp, state)
        enqueue_alert_log_if_needed(uid, device_id, fusion, packet, packet_id, timestamp, state)
        enqueue_relay_activated_log(uid, device_id, packet, packet_id, timestamp, request_id, state)
        enqueue_fault_log_if_needed(uid, device_id, packet, packet_id, timestamp)

        # ── Performance ──────────────────────────────────────
        write_performance_branch(
            uid, device_id, state,
            render_processing_ms=max(0, write_start_ms - request_start_ms),
            firebase_write_ms=max(0, write_done_ms - write_start_ms),
            phone_to_render_ms=phone_to_render_ms,
            total_pipeline_ms=total_pipeline_ms,
        )

        state.daily_readings += 1
        state.last_upload_ts = time.time()

        log.info("[RELAY] Processed uid=%s device=%s pid=%s mode=%s reason=%s live=%s",
                 uid, device_id, packet_id, connection_mode_written, protection_reason, permit_live_write)

        return {
            "ok": True,
            "role": "render_relay_backend",
            "schema_version": SCHEMA_VERSION,
            "connection_mode_written": connection_mode_written,
            "protection_reason": protection_reason,
            "live_written": permit_live_write,
            "deduplicated": False,
            "packet_id": packet_id,
            "history_sync": {
                "status": "ok",
                "queue_depth": local_queue.count(),
                "synced_count": state.daily_readings,
                "failed_count": 0,
            },
        }

    except Exception as exc:
        log.exception("[RELAY] Write failed uid=%s device=%s: %s", uid, device_id, exc)
        return {"ok": False, "error": str(exc), "connection_mode_written": None}


# ─────────────────────────────────────────
# OFFLINE SYNC LOGIC
# ─────────────────────────────────────────

def handle_offline_sync_start(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /offline-sync/start
    App signals it's about to flush its local phone queue.
    Render sets connection_mode = recovery, recovery_reason = offline_sync.
    """
    uid = str(data.get("uid", "")).strip()
    device_id = str(data.get("device_id", "")).strip()

    if not uid or not device_id:
        return {"ok": False, "error": "missing_uid_or_device_id", "message": "uid and device_id are required"}

    timestamp = utc_now_iso()
    queue_depth = int(data.get("queue_depth", 0))
    queue_oldest_ms = int(data.get("queue_oldest_ms", 0))
    recovery_id = str(uuid.uuid4())

    # Pi handover protection
    current_status = read_current_firebase_status(uid, device_id)
    if is_pi_handover_active(current_status):
        log.info("[OFFLINE_SYNC] Pi handover active — not overwriting uid=%s device=%s", uid, device_id)
        return {
            "ok": True,
            "role": "render_relay_backend",
            "status": "skipped",
            "reason": "pi_handover_active",
            "message": "Pi handover recovery is active; offline_sync write skipped",
        }

    prev_mode = current_status.get("connection_mode", "offline")

    # Write RTDB status
    rtdb.reference(f"live/{uid}/{device_id}/status").update({
        "connection_mode": "recovery",
        "recovery_reason": "offline_sync",
        "recovery_status": "preparing",
        "handover_status": "none",
        "recovery_started_at": timestamp,
        "cloud_connected": True,
        "app_last_seen": timestamp,
        "relay_last_seen": timestamp,
        "source_type": SOURCE_TYPE_RELAY,
        "processed_by": PROCESSED_BY_NAME,
        "render_last_seen": timestamp,
        "schema_version": SCHEMA_VERSION,
    })

    # Write RTDB queue
    rtdb.reference(f"live/{uid}/{device_id}/queue").update({
        "queue_depth": queue_depth,
        "queue_oldest_ms": queue_oldest_ms,
        "sync_status": "syncing",
        "last_sync_at": timestamp,
        "source": "render_relay",
    })

    state = get_device_state(device_id)

    # Firestore recovery_event (start)
    enqueue_job("recovery_event", uid, device_id, {
        "recovery_id": recovery_id,
        "recovery_reason": "offline_sync",
        "recovery_status": "preparing",
        "started_at": timestamp,
        "completed_at": None,
        "failed_at": None,
        "failure_reason": None,
        "actor": "render",
        "device_id": device_id,
        "last_packet_id": data.get("first_packet_id"),
        "synced_count": 0,
        "failed_count": 0,
        "queue_depth": queue_depth,
        "queue_oldest_ms": queue_oldest_ms,
        "metadata": {"app_instance_id": data.get("app_instance_id")},
    })

    # Firestore mode_history
    if prev_mode != "recovery":
        enqueue_mode_history(uid, device_id, prev_mode, "recovery",
                             "offline_sync_starting", timestamp, state)

    # Firestore queue_log
    enqueue_job("queue_log", uid, device_id, {
        "queue_type": "phone_local_queue",
        "event_type": "sync_started",
        "queue_depth": queue_depth,
        "queue_oldest_ms": queue_oldest_ms,
        "pending_count": queue_depth,
        "failed_count": 0,
        "sync_status": "syncing",
        "started_at": timestamp,
        "completed_at": None,
        "error": None,
        "actor": "render",
        "device_id": device_id,
        "metadata": {},
    })

    log.info("[OFFLINE_SYNC] Start uid=%s device=%s queue_depth=%s", uid, device_id, queue_depth)

    return {
        "ok": True,
        "role": "render_relay_backend",
        "schema_version": SCHEMA_VERSION,
        "connection_mode_written": "recovery",
        "recovery_reason": "offline_sync",
        "recovery_status": "preparing",
        "recovery_id": recovery_id,
    }


def handle_offline_sync_packet(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /offline-sync/packet
    Upload a single queued packet from phone local storage.

    - Requires packet_id or esp_packet_id (Issue 3).
    - Writes to Firestore vital_readings unconditionally via enqueue_vital_reading_force (Issue 2).
    - Does NOT overwrite live RTDB vitals unless packet is marked latest_live=True
      AND is newer than current live timestamp.
    """
    uid = str(data.get("uid", "")).strip()
    device_id = str(data.get("device_id", "")).strip()

    if not uid or not device_id:
        return {"ok": False, "error": "missing_uid_or_device_id",
                "message": "uid and device_id are required"}

    # Issue 3: require original packet_id or esp_packet_id for offline queued packets
    raw_packet_id = data.get("packet_id") or data.get("esp_packet_id")
    vitals_dict = data.get("vitals") if isinstance(data.get("vitals"), dict) else {}
    raw_packet_id = raw_packet_id or vitals_dict.get("packet_id") or vitals_dict.get("esp_packet_id")
    if not raw_packet_id:
        return {
            "ok": False,
            "error": "missing_packet_id",
            "message": "offline sync packet must include packet_id or esp_packet_id",
            "status": "failed",
        }

    packet = extract_packet(data)
    packet_id = packet.get("packet_id") or packet.get("esp_packet_id") or str(raw_packet_id)
    timestamp = utc_now_iso()
    state = get_device_state(device_id)

    # Dedup check (in-memory; Firestore set(merge=True) is the authoritative dedup)
    if state.is_duplicate_packet(packet.get("packet_id"), packet.get("esp_packet_id")):
        return {
            "ok": True,
            "deduplicated": True,
            "packet_id": packet_id,
            "status": "duplicate",
        }

    confidence = compute_confidence(state, packet["heart_rate"], packet["spo2"], packet["wearing"])
    quality = quality_tier(confidence)
    display = compute_display_fields(packet, state)

    # Issue 2: always store every queued packet — no cooldown
    enqueue_vital_reading_force(uid, device_id, packet, display, confidence, quality, packet_id, timestamp)

    # Update RTDB queue progress
    try:
        rtdb.reference(f"live/{uid}/{device_id}/queue").update({
            "sync_status": "syncing",
            "last_sync_at": timestamp,
        })
    except Exception as exc:
        log.warning("[OFFLINE_PACKET] Queue RTDB update failed: %s", exc)

    # Optionally update live RTDB vitals if this is the newest packet and marked latest_live
    if packet.get("latest_live"):
        try:
            current_vitals = rtdb.reference(f"live/{uid}/{device_id}/vitals").get() or {}
            current_ts = current_vitals.get("updated_at", "")
            if not current_ts or timestamp >= current_ts:
                write_vitals_branch(uid, device_id, packet, display, confidence, quality, timestamp, packet_id)
        except Exception as exc:
            log.warning("[OFFLINE_PACKET] latest_live vitals write failed: %s", exc)

    state.daily_readings += 1

    return {
        "ok": True,
        "deduplicated": False,
        "packet_id": packet_id,
        "status": "accepted",
    }


def handle_offline_sync_batch(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /offline-sync/batch
    Upload a batch of queued offline packets.
    Returns per-packet results.
    """
    uid = str(data.get("uid", "")).strip()
    device_id = str(data.get("device_id", "")).strip()

    if not uid or not device_id:
        return {"ok": False, "error": "missing_uid_or_device_id"}

    packets = data.get("packets", [])
    if not isinstance(packets, list):
        return {"ok": False, "error": "packets must be a list"}

    results = []
    accepted = 0
    duplicate = 0
    failed = 0

    for i, pkt in enumerate(packets):
        if not isinstance(pkt, dict):
            results.append({"index": i, "status": "failed", "error": "not a dict"})
            failed += 1
            continue

        pkt["uid"] = uid
        pkt["device_id"] = device_id
        r = handle_offline_sync_packet(pkt)
        r["index"] = i
        results.append(r)
        if r.get("deduplicated"):
            duplicate += 1
        elif r.get("ok"):
            accepted += 1
        else:
            failed += 1

    return {
        "ok": True,
        "role": "render_relay_backend",
        "schema_version": SCHEMA_VERSION,
        "processed": len(packets),
        "accepted": accepted,
        "duplicate": duplicate,
        "failed": failed,
        "results": results,
    }


def handle_offline_sync_complete(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /offline-sync/complete
    App signals offline sync is done. Render chooses final mode:
    - If Pi already wrote primary → keep primary
    - If Pi handover is active → do not overwrite
    - If phone relay is still active → set relay
    - Otherwise leave for app to decide (do not write)
    """
    uid = str(data.get("uid", "")).strip()
    device_id = str(data.get("device_id", "")).strip()

    if not uid or not device_id:
        return {"ok": False, "error": "missing_uid_or_device_id"}

    timestamp = utc_now_iso()
    total_packets = int(data.get("total_packets", 0))
    synced_count = int(data.get("synced_count", 0))
    failed_count = int(data.get("failed_count", 0))
    remaining = int(data.get("remaining_queue_depth", 0))
    last_packet_id = data.get("last_packet_id")

    # Read current status
    current_status = read_current_firebase_status(uid, device_id)
    pi_handover = is_pi_handover_active(current_status)
    primary = is_primary_active(current_status)
    prev_mode = current_status.get("connection_mode", "recovery")
    state = get_device_state(device_id)

    # Step 1: Write recovery_status = completed
    status_update: Dict[str, Any] = {
        "recovery_status": "completed",
        "recovery_completed_at": timestamp,
        "app_last_seen": timestamp,
        "render_last_seen": timestamp,
        "cloud_connected": True,
    }

    final_mode: Optional[str] = None

    if pi_handover:
        # Pi handover in progress: do not touch connection_mode
        log.info("[OFFLINE_COMPLETE] Pi handover active — not setting final mode uid=%s", uid)

    elif primary:
        # Pi has already restored primary: keep it
        log.info("[OFFLINE_COMPLETE] Pi is primary — keeping primary uid=%s", uid)
        final_mode = "primary"  # just for logging; we don't write it

    elif data.get("relay_still_active"):
        # App says relay is active → set relay
        status_update["connection_mode"] = "relay"
        status_update["relay_active"] = True
        status_update["source_type"] = SOURCE_TYPE_RELAY
        status_update["processed_by"] = PROCESSED_BY_NAME
        final_mode = "relay"
        log.info("[OFFLINE_COMPLETE] Relay still active → setting relay uid=%s", uid)
    else:
        # Pi unavailable, no relay → leave connection_mode as recovery/completed
        # Let the app settle the final mode
        log.info("[OFFLINE_COMPLETE] No active relay path — leaving mode for app uid=%s", uid)

    if not pi_handover:
        rtdb.reference(f"live/{uid}/{device_id}/status").update(status_update)

    # Queue branch
    sync_status = "ok" if remaining == 0 else "partial"
    rtdb.reference(f"live/{uid}/{device_id}/queue").update({
        "queue_depth": remaining,
        "failed_count": failed_count,
        "sync_status": sync_status,
        "last_sync_at": timestamp,
    })

    # Firestore recovery_event (completed)
    enqueue_job("recovery_event", uid, device_id, {
        "recovery_reason": "offline_sync",
        "recovery_status": "completed",
        "started_at": None,  # already written at start
        "completed_at": timestamp,
        "failed_at": None,
        "failure_reason": None,
        "actor": "render",
        "device_id": device_id,
        "last_packet_id": last_packet_id,
        "synced_count": synced_count,
        "failed_count": failed_count,
        "queue_depth": remaining,
        "queue_oldest_ms": 0,
        "metadata": {"total_packets": total_packets},
    })

    # Firestore mode_history
    if final_mode and final_mode != prev_mode and not pi_handover:
        enqueue_mode_history(uid, device_id, "recovery", final_mode,
                             "offline_sync_completed", timestamp, state)

    # Firestore queue_log
    enqueue_job("queue_log", uid, device_id, {
        "queue_type": "phone_local_queue",
        "event_type": "sync_completed",
        "queue_depth": remaining,
        "queue_oldest_ms": 0,
        "pending_count": remaining,
        "failed_count": failed_count,
        "sync_status": sync_status,
        "started_at": None,
        "completed_at": timestamp,
        "error": None,
        "actor": "render",
        "device_id": device_id,
        "metadata": {"total_packets": total_packets, "synced_count": synced_count},
    })

    # Audit log
    enqueue_job("audit_log", uid, device_id, {
        "event_type": "OFFLINE_SYNC_COMPLETED",
        "event_title": "Offline sync completed",
        "event_description": f"Synced {synced_count}/{total_packets} packets. Remaining: {remaining}.",
        "actor_type": "render",
        "actor_uid": uid,
        "device_id": device_id,
        "timestamp": timestamp,
        "severity": "info",
        "metadata": {
            "total_packets": total_packets,
            "synced_count": synced_count,
            "failed_count": failed_count,
            "remaining_queue_depth": remaining,
            "final_mode": final_mode,
        },
    })

    log.info("[OFFLINE_COMPLETE] uid=%s device=%s synced=%s failed=%s remaining=%s final_mode=%s",
             uid, device_id, synced_count, failed_count, remaining, final_mode)

    return {
        "ok": True,
        "role": "render_relay_backend",
        "schema_version": SCHEMA_VERSION,
        "recovery_status": "completed",
        "final_mode": final_mode,
        "synced_count": synced_count,
        "failed_count": failed_count,
        "remaining_queue_depth": remaining,
        "history_sync": history_sync_snapshot(),
    }


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
                log.info("[QUEUE] Delivered id=%s type=%s", row_id, job_type)
            except Exception as exc:
                local_queue.mark_failed(row_id, attempts)
                update_history_sync_state("delayed", str(exc))
                log.warning("[QUEUE] Delayed id=%s type=%s attempts=%s error=%s",
                            row_id, job_type, attempts + 1, exc)


# ─────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────

app = Flask(__name__)


def _check_api_key() -> Optional[str]:
    """
    Returns error string if unauthorized, None if OK.

    Security rules:
    - If RAFEEQ_RELAY_API_KEY is set: require it on every non-health request.
    - If RAFEEQ_RELAY_API_KEY is NOT set and RAFEEQ_ALLOW_INSECURE_DEV=true: allow (dev mode).
    - If RAFEEQ_RELAY_API_KEY is NOT set and RAFEEQ_ALLOW_INSECURE_DEV is not true:
      reject with security_not_configured (503).
    """
    if not RELAY_API_KEY:
        if ALLOW_INSECURE_DEV:
            return None  # explicit dev mode opt-in
        return "security_not_configured"  # production: reject

    # Accept X-RAFEEQ-RELAY-KEY (canonical), X-API-Key (backward compat), Authorization: Bearer
    provided = (
        request.headers.get("X-RAFEEQ-RELAY-KEY", "")
        or request.headers.get("X-Rafeeq-Relay-Key", "")
        or request.headers.get("X-API-Key", "")
    )

    if not provided:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            provided = auth_header[7:]

    if provided != RELAY_API_KEY:
        return "Unauthorized"
    return None


def _require_json() -> Optional[Tuple[Any, int]]:
    if not request.is_json:
        return jsonify({"ok": False, "error": "invalid_content_type",
                        "message": "Content-Type must be application/json"}), 400
    return None


def _parse_json() -> Optional[Dict[str, Any]]:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else None


# ── Health ────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    api_key_configured = bool(RELAY_API_KEY)
    insecure_dev_mode = (not RELAY_API_KEY) and ALLOW_INSECURE_DEV
    return jsonify({
        "ok": True,
        "role": "render_relay_backend",
        "schema_version": SCHEMA_VERSION,
        "backend_version": BACKEND_VERSION,
        "firebase_ready": fs is not None,
        "api_key_configured": api_key_configured,
        "insecure_dev_mode": insecure_dev_mode,
        "history_sync": history_sync_snapshot(),
    })


def _auth_error_response(err: str):
    """Return appropriate error response for auth failures."""
    if err == "security_not_configured":
        return jsonify({
            "ok": False,
            "error": "security_not_configured",
            "message": "RAFEEQ_RELAY_API_KEY is not set. Set it or enable RAFEEQ_ALLOW_INSECURE_DEV=true for local dev.",
        }), 503
    return jsonify({"ok": False, "error": "unauthorized", "message": err}), 401


# ── Live relay endpoints ─────────────────

@app.route("/relay-ingest", methods=["POST"])
@app.route("/relay/packet", methods=["POST"])
@app.route("/ingest", methods=["POST"])          # compatibility alias
@app.route("/relay", methods=["POST"])           # compatibility alias
@app.route("/upload", methods=["POST"])          # compatibility alias
def relay_ingest():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    data = _parse_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json", "message": "Empty or invalid JSON body"}), 400

    # Validate required fields
    val_err = validate_relay_request(data)
    if val_err:
        return jsonify(val_err), 400

    result = process_live_relay_packet(data)
    return jsonify(result), 200 if result.get("ok") else 422


@app.route("/relay-batch", methods=["POST"])
@app.route("/relay/batch", methods=["POST"])
def relay_batch():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    body = _parse_json()
    if not body:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    packets = body.get("packets", [])
    if not isinstance(packets, list) or not packets:
        return jsonify({"ok": False, "error": "empty_packets", "message": "packets array is required and non-empty"}), 400

    results = []
    for i, pkt in enumerate(packets):
        if not isinstance(pkt, dict):
            results.append({"index": i, "ok": False, "status": "failed", "error": "not a dict"})
            continue

        val_err = validate_relay_request(pkt)
        if val_err:
            val_err["index"] = i
            val_err["status"] = "failed"
            results.append(val_err)
            continue

        r = process_live_relay_packet(pkt)
        r["index"] = i
        r["status"] = "accepted" if (r.get("ok") and not r.get("deduplicated")) else (
            "duplicate" if r.get("deduplicated") else "failed"
        )
        results.append(r)

    accepted = sum(1 for r in results if r.get("status") == "accepted")
    duplicate = sum(1 for r in results if r.get("status") == "duplicate")
    failed = sum(1 for r in results if r.get("status") == "failed")

    return jsonify({
        "ok": True,
        "role": "render_relay_backend",
        "schema_version": SCHEMA_VERSION,
        "processed": len(results),
        "accepted": accepted,
        "duplicate": duplicate,
        "failed": failed,
        "results": results,
    })


# ── Offline sync endpoints ────────────────

@app.route("/offline-sync/start", methods=["POST"])
def offline_sync_start():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    data = _parse_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    if not data.get("uid") or not data.get("device_id"):
        return jsonify({"ok": False, "error": "missing_uid_or_device_id",
                        "message": "uid and device_id are required"}), 400

    try:
        result = handle_offline_sync_start(data)
        return jsonify(result), 200 if result.get("ok") else 422
    except Exception as exc:
        log.exception("[OFFLINE_SYNC/START] Error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/offline-sync/packet", methods=["POST"])
def offline_sync_packet():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    data = _parse_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    try:
        result = handle_offline_sync_packet(data)
        return jsonify(result), 200 if result.get("ok") else 422
    except Exception as exc:
        log.exception("[OFFLINE_SYNC/PACKET] Error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/offline-sync/batch", methods=["POST"])
def offline_sync_batch():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    data = _parse_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    try:
        result = handle_offline_sync_batch(data)
        return jsonify(result), 200 if result.get("ok") else 422
    except Exception as exc:
        log.exception("[OFFLINE_SYNC/BATCH] Error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/offline-sync/complete", methods=["POST"])
def offline_sync_complete():
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    json_err = _require_json()
    if json_err:
        return json_err

    data = _parse_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    if not data.get("uid") or not data.get("device_id"):
        return jsonify({"ok": False, "error": "missing_uid_or_device_id"}), 400

    try:
        result = handle_offline_sync_complete(data)
        return jsonify(result), 200 if result.get("ok") else 422
    except Exception as exc:
        log.exception("[OFFLINE_SYNC/COMPLETE] Error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Debug / session reset ─────────────────

@app.route("/relay/reset_session/<device_id>", methods=["POST"])
def reset_relay_session(device_id: str):
    err = _check_api_key()
    if err:
        return _auth_error_response(err)

    with _device_states_lock:
        if device_id in _device_states:
            s = _device_states[device_id]
            s.relay_activated_written = False
            s.seen_packet_ids.clear()
            s.last_upload_ts = 0.0
            s.last_known_mode = None

    return jsonify({"ok": True, "device_id": device_id})


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────

init_firebase()

worker_thread = threading.Thread(
    target=queue_retry_loop,
    name="firestore_queue_worker",
    daemon=True,
)
worker_thread.start()

log.info("[SYSTEM] Rafeeq Render relay backend v%s ready", BACKEND_VERSION)
log.info("[SYSTEM] Firebase: %s", "OK" if fs else "DISABLED")
if RELAY_API_KEY:
    log.info("[SYSTEM] API key protection: ON")
elif ALLOW_INSECURE_DEV:
    log.warning("[SYSTEM] API key protection: OFF — INSECURE DEV MODE (RAFEEQ_ALLOW_INSECURE_DEV=true)")
else:
    log.error("[SYSTEM] SECURITY NOT CONFIGURED: RAFEEQ_RELAY_API_KEY missing and RAFEEQ_ALLOW_INSECURE_DEV not set — all non-health requests will be rejected with 503")
log.info("[SYSTEM] Source type: %s", SOURCE_TYPE_RELAY)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
