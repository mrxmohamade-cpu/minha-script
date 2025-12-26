# robot/robot_repository.py
import sqlite3
import time

from config import APP_DATA_DIR
from .robot_scheduler import MemberState
from .robot_classifier import ResultType


class RobotRepository:
    def __init__(self, db_path=None):
        self.db_path = db_path or f"{APP_DATA_DIR}/robot_state.db"
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS member_state (
                    member_id TEXT PRIMARY KEY,
                    last_check_at INTEGER,
                    next_allowed_check_at INTEGER,
                    consecutive_failures INTEGER,
                    last_result_type TEXT,
                    last_http_status INTEGER,
                    cooldown_level INTEGER,
                    priority_score REAL,
                    last_change_signature TEXT,
                    monitoring_mode TEXT,
                    final_status TEXT,
                    updated_at INTEGER
                )
                """
            )
            cur.execute("PRAGMA table_info(member_state)")
            columns = {row[1] for row in cur.fetchall()}
            if "monitoring_mode" not in columns:
                cur.execute("ALTER TABLE member_state ADD COLUMN monitoring_mode TEXT")
            if "final_status" not in columns:
                cur.execute("ALTER TABLE member_state ADD COLUMN final_status TEXT")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS checks_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER,
                    member_id TEXT,
                    step TEXT,
                    http_status INTEGER,
                    result_type TEXT,
                    duration_ms INTEGER,
                    short_detail TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER,
                    severity TEXT,
                    member_id TEXT,
                    title TEXT,
                    message TEXT,
                    is_read INTEGER DEFAULT 0
                )
                """
            )
            conn.commit()

    def load_member_states(self):
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT member_id, last_check_at, next_allowed_check_at, consecutive_failures,
                       last_result_type, last_http_status, cooldown_level, priority_score,
                       last_change_signature, monitoring_mode, final_status
                FROM member_state
                """
            )
            states = {}
            for row in cur.fetchall():
                member_id = row[0]
                states[member_id] = MemberState(
                    member_id=member_id,
                    last_check_at=row[1] or 0,
                    next_allowed_check_at=row[2] or 0,
                    consecutive_failures=row[3] or 0,
                    last_result_type=row[4] or ResultType.UNKNOWN,
                    last_http_status=row[5],
                    cooldown_level=row[6] or 0,
                    priority_score=row[7] or 0.0,
                    last_change_signature=row[8] or "",
                    monitoring_mode=row[9] or "ACTIVE",
                    final_status=row[10] or ResultType.UNKNOWN,
                )
            return states

    def upsert_member_state(self, state: MemberState):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO member_state (
                    member_id, last_check_at, next_allowed_check_at, consecutive_failures,
                    last_result_type, last_http_status, cooldown_level, priority_score,
                    last_change_signature, monitoring_mode, final_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(member_id) DO UPDATE SET
                    last_check_at=excluded.last_check_at,
                    next_allowed_check_at=excluded.next_allowed_check_at,
                    consecutive_failures=excluded.consecutive_failures,
                    last_result_type=excluded.last_result_type,
                    last_http_status=excluded.last_http_status,
                    cooldown_level=excluded.cooldown_level,
                    priority_score=excluded.priority_score,
                    last_change_signature=excluded.last_change_signature,
                    monitoring_mode=excluded.monitoring_mode,
                    final_status=excluded.final_status,
                    updated_at=excluded.updated_at
                """,
                (
                    state.member_id,
                    int(state.last_check_at),
                    int(state.next_allowed_check_at),
                    state.consecutive_failures,
                    state.last_result_type,
                    state.last_http_status,
                    state.cooldown_level,
                    state.priority_score,
                    state.last_change_signature,
                    state.monitoring_mode,
                    state.final_status,
                    int(time.time()),
                ),
            )
            conn.commit()

    def insert_check_log(self, member_id, step, http_status, result_type, duration_ms, short_detail):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checks_log (ts, member_id, step, http_status, result_type, duration_ms, short_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (int(time.time()), member_id, step, http_status, result_type, duration_ms, short_detail),
            )
            conn.execute(
                "DELETE FROM checks_log WHERE ts < ?",
                (int(time.time()) - (60 * 60 * 24 * 60),),
            )
            conn.commit()

    def insert_alert(self, severity, title, message, member_id=None):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO alerts (ts, severity, member_id, title, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(time.time()), severity, member_id, title, message),
            )
            conn.commit()

    def get_last_alert(self):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT message FROM alerts ORDER BY ts DESC LIMIT 1"
            )
            row = cur.fetchone()
            return row[0] if row else "لا توجد تنبيهات"
