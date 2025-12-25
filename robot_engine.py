# robot_engine.py
import sqlite3
import time
import random
from dataclasses import dataclass

from api_client import AnemAPIClient


class ResultType:
    HAS_DATES = "HAS_DATES"
    NO_DATES = "NO_DATES"
    HAS_RDV = "HAS_RDV"
    INVALID = "INVALID"
    RATE_LIMIT = "RATE_LIMIT"
    NETWORK = "NETWORK"
    SERVER_ERROR = "SERVER_ERROR"
    UNKNOWN = "UNKNOWN"


@dataclass
class MemberState:
    last_check_at: float = 0.0
    next_allowed_check_at: float = 0.0
    last_result_type: str = ResultType.UNKNOWN
    consecutive_failures: int = 0
    cooldown_level: int = 0
    last_round_processed: int = -1


class RateLimiter:
    def __init__(self, min_interval_seconds=1.5):
        self.min_interval_seconds = min_interval_seconds
        self.next_allowed_at = 0.0

    def allow(self, now_ts):
        return now_ts >= self.next_allowed_at

    def reserve(self, now_ts):
        self.next_allowed_at = max(self.next_allowed_at, now_ts + self.min_interval_seconds)


class ResultClassifier:
    def classify(self, status_text, error_text, data=None):
        status = status_text or ""
        error = (error_text or "").lower()

        if "429" in error or "طلبات كثيرة" in error:
            return ResultType.RATE_LIMIT
        if "timeout" in error or "connection" in error or "network" in error:
            return ResultType.NETWORK
        if "500" in error or "server" in error:
            return ResultType.SERVER_ERROR
        if status in ["لا توجد مواعيد"]:
            return ResultType.NO_DATES
        if status in ["تم الحجز", "لديه موعد مسبق", "مكتمل"]:
            return ResultType.HAS_RDV
        if status in ["غير مؤهل للحجز", "بيانات الإدخال خاطئة", "غير مؤهل مبدئيًا"]:
            return ResultType.INVALID
        if status in ["جاري البحث عن مواعيد...", "جاري حجز الموعد..."] and not error:
            return ResultType.HAS_DATES
        return ResultType.UNKNOWN


class HarClient:
    def __init__(self, api_client: AnemAPIClient):
        self.api_client = api_client

    def validate_candidate(self, wassit_number, identity_doc_number):
        return self.api_client.validate_candidate(wassit_number, identity_doc_number)

    def get_pre_inscription(self, pre_inscription_id):
        return self.api_client.get_pre_inscription_info(pre_inscription_id)

    def get_available_dates(self, structure_id, pre_inscription_id):
        return self.api_client.get_available_dates(structure_id, pre_inscription_id)

    def create_rendezvous(self, pre_inscription_id, demandeur_id, rdv_date, ccp, nom_ccp, prenom_ccp):
        return self.api_client.create_rendezvous(
            pre_inscription_id, ccp, nom_ccp, prenom_ccp, rdv_date, demandeur_id
        )


class RobotRepository:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS members (
                    member_key TEXT PRIMARY KEY,
                    name TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS member_state (
                    member_key TEXT PRIMARY KEY,
                    last_check_at REAL,
                    next_allowed_check_at REAL,
                    last_result_type TEXT,
                    consecutive_failures INTEGER,
                    cooldown_level INTEGER,
                    last_round_processed INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS checks_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    member_key TEXT,
                    result_type TEXT,
                    created_at REAL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    created_at REAL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rules_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT,
                    value TEXT
                )
                """
            )
            conn.commit()

    def upsert_member_state(self, member_key, state: MemberState):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO member_state (
                    member_key, last_check_at, next_allowed_check_at, last_result_type,
                    consecutive_failures, cooldown_level, last_round_processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(member_key) DO UPDATE SET
                    last_check_at=excluded.last_check_at,
                    next_allowed_check_at=excluded.next_allowed_check_at,
                    last_result_type=excluded.last_result_type,
                    consecutive_failures=excluded.consecutive_failures,
                    cooldown_level=excluded.cooldown_level,
                    last_round_processed=excluded.last_round_processed
                """,
                (
                    member_key,
                    state.last_check_at,
                    state.next_allowed_check_at,
                    state.last_result_type,
                    state.consecutive_failures,
                    state.cooldown_level,
                    state.last_round_processed,
                ),
            )
            conn.commit()

    def log_check(self, member_key, result_type):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO checks_log (member_key, result_type, created_at) VALUES (?, ?, ?)",
                (member_key, result_type, time.time()),
            )
            conn.commit()

    def add_alert(self, message):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO alerts (message, created_at) VALUES (?, ?)",
                (message, time.time()),
            )
            conn.commit()

    def get_last_alert(self):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT message FROM alerts ORDER BY created_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return row[0] if row else "لا توجد تنبيهات"


class SmartScheduler:
    def __init__(self, global_min_interval=1.5):
        self.global_rate = RateLimiter(global_min_interval)
        self.round_id = 0
        self.member_states = {}
        self.mode = "sleep"

    def start_round(self):
        self.round_id += 1

    def _state_for(self, member_key):
        if member_key not in self.member_states:
            self.member_states[member_key] = MemberState()
        return self.member_states[member_key]

    def can_process_member(self, member_key, now_ts):
        state = self._state_for(member_key)
        if state.last_round_processed == self.round_id:
            return False
        if now_ts < state.next_allowed_check_at:
            return False
        if not self.global_rate.allow(now_ts):
            return False
        return True

    def next_wait_seconds(self, member_key, now_ts):
        state = self._state_for(member_key)
        return max(0, int(max(state.next_allowed_check_at, self.global_rate.next_allowed_at) - now_ts))

    def record_result(self, member_key, result_type, now_ts):
        state = self._state_for(member_key)
        state.last_round_processed = self.round_id
        state.last_result_type = result_type
        state.last_check_at = now_ts

        jitter = random.uniform(5, 15)
        cooldown_seconds = 0

        if result_type == ResultType.NO_DATES:
            cooldown_seconds = (2 * 60 * 60) + random.uniform(5 * 60, 20 * 60)
            state.cooldown_level = max(state.cooldown_level - 1, 0)
            self.mode = "sleep"
        elif result_type == ResultType.RATE_LIMIT:
            state.cooldown_level = min(state.cooldown_level + 1, 6)
            cooldown_seconds = (60 * (2 ** state.cooldown_level)) + random.uniform(20, 60)
            self.global_rate.next_allowed_at = max(self.global_rate.next_allowed_at, now_ts + cooldown_seconds)
            self.mode = "sleep"
        elif result_type == ResultType.NETWORK:
            state.cooldown_level = min(state.cooldown_level + 1, 4)
            cooldown_seconds = (5 * 60 * (state.cooldown_level + 1)) + random.uniform(30, 120)
            self.mode = "sleep"
        elif result_type == ResultType.HAS_RDV:
            cooldown_seconds = 12 * 60 * 60
            self.mode = "sleep"
        elif result_type == ResultType.INVALID:
            cooldown_seconds = 24 * 60 * 60
            self.mode = "sleep"
        elif result_type == ResultType.HAS_DATES:
            cooldown_seconds = (5 * 60) + random.uniform(30, 120)
            state.cooldown_level = max(state.cooldown_level - 1, 0)
            self.mode = "burst"
        else:
            cooldown_seconds = 10 * 60
            self.mode = "sleep"

        state.next_allowed_check_at = now_ts + cooldown_seconds + jitter
        self.global_rate.reserve(now_ts)
        return cooldown_seconds, self.mode


def explain_result(result_type, cooldown_seconds):
    if result_type == ResultType.RATE_LIMIT:
        return f"تهدئة تلقائية ({int(cooldown_seconds/60)} د)"
    if result_type == ResultType.NO_DATES:
        return "لا توجد مواعيد"
    if result_type == ResultType.NETWORK:
        return "شبكة غير مستقرة"
    if result_type == ResultType.HAS_DATES:
        return "توفر موعد"
    if result_type == ResultType.HAS_RDV:
        return "لديه موعد"
    if result_type == ResultType.INVALID:
        return "غير مؤهل"
    return "متابعة"
