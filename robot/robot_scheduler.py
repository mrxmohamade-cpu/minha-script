# robot/robot_scheduler.py
import random
import time
from dataclasses import dataclass

from .robot_classifier import ResultType


@dataclass
class MemberState:
    member_id: str
    last_check_at: float = 0.0
    next_allowed_check_at: float = 0.0
    consecutive_failures: int = 0
    last_result_type: str = ResultType.UNKNOWN
    last_http_status: int | None = None
    cooldown_level: int = 0
    priority_score: float = 0.0
    last_change_signature: str = ""
    last_round_processed: int = -1
    last_ui_update_at: float = 0.0
    monitoring_mode: str = "ACTIVE"  # ACTIVE | SKIP_LONG | DISABLED
    final_status: str = "UNKNOWN"


class RateLimiter:
    def __init__(self, min_interval_seconds=8.0, jitter_range=(0.15, 0.25)):
        self.min_interval_seconds = min_interval_seconds
        self.jitter_range = jitter_range
        self.next_allowed_at = 0.0

    def allow(self, now_ts):
        return now_ts >= self.next_allowed_at

    def reserve(self, now_ts):
        jitter = self.min_interval_seconds * random.uniform(*self.jitter_range)
        self.next_allowed_at = max(self.next_allowed_at, now_ts + self.min_interval_seconds + jitter)


class SmartScheduler:
    def __init__(self, settings):
        self.settings = settings
        self.round_id = 0
        self.member_states = {}
        self.rate_limiter = RateLimiter(
            min_interval_seconds=settings["base_global_interval_sec"],
            jitter_range=(0.15, 0.25),
        )
        self.mode = "sleep"
        self.burst_until = 0.0
        self.pause_until = 0.0
        self.rate_limit_hits = []

    def is_paused(self, now_ts=None):
        now_ts = now_ts or time.time()
        return now_ts < self.pause_until

    def start_round(self):
        self.round_id += 1

    def hydrate(self, states):
        self.member_states = states or {}

    def state_for(self, member_id):
        if member_id not in self.member_states:
            self.member_states[member_id] = MemberState(member_id=member_id)
        return self.member_states[member_id]

    def can_process(self, member_id, now_ts):
        state = self.state_for(member_id)
        if state.monitoring_mode != "ACTIVE":
            return False
        if self.is_paused(now_ts):
            return False
        if state.last_round_processed == self.round_id:
            return False
        if now_ts < state.next_allowed_check_at:
            return False
        if not self.rate_limiter.allow(now_ts):
            return False
        return True

    def next_wait_seconds(self, now_ts):
        active_states = [
            state for state in self.member_states.values() if state.monitoring_mode == "ACTIVE"
        ]
        if not active_states:
            return 0
        if self.is_paused(now_ts):
            return max(0, int(self.pause_until - now_ts))
        earliest = min(
            (state.next_allowed_check_at for state in active_states if state.next_allowed_check_at),
            default=0,
        )
        if earliest <= 0:
            return 0
        return max(0, int(earliest - now_ts))

    def record_result(self, member_id, result_type, http_status=None):
        now_ts = time.time()
        state = self.state_for(member_id)
        state.last_round_processed = self.round_id
        state.last_result_type = result_type
        state.last_check_at = now_ts
        state.last_http_status = http_status
        state.final_status = result_type

        cooldown = 0
        jitter = random.uniform(0.15, 0.25)

        if result_type == ResultType.HAS_DATES:
            cooldown = random.uniform(5 * 60, 10 * 60)
            self.mode = "burst"
            burst_min = self.settings["burst_duration_min"]
            self.burst_until = max(self.burst_until, now_ts + (burst_min * 60))
        elif result_type == ResultType.NO_DATES:
            cooldown = random.uniform(8 * 60 * 60, 18 * 60 * 60)
            self.mode = "sleep"
        elif result_type == ResultType.NETWORK_ERROR:
            cooldown = random.uniform(10 * 60, 25 * 60)
            self.mode = "sleep"
        elif result_type == ResultType.RATE_LIMIT:
            state.cooldown_level = min(state.cooldown_level + 1, 6)
            cooldown = random.uniform(
                self.settings["rate_limit_pause_min_sec"],
                self.settings["rate_limit_pause_max_sec"],
            ) * (1 + state.cooldown_level * 0.15)
            self.mode = "PAUSED_RATE_LIMIT"
            self.rate_limiter.min_interval_seconds = min(
                self.rate_limiter.min_interval_seconds + 3, 20
            )
            window_sec = self.settings["rate_limit_window_sec"]
            now_ts = time.time()
            self.rate_limit_hits = [ts for ts in self.rate_limit_hits if now_ts - ts < window_sec]
            self.rate_limit_hits.append(now_ts)
            if len(self.rate_limit_hits) >= self.settings["rate_limit_threshold"]:
                cooldown *= 1.5
            self.pause_until = max(self.pause_until, now_ts + cooldown)
        elif result_type == ResultType.HAS_RDV:
            cooldown = self.settings["freeze_has_rdv_days"] * 24 * 60 * 60
            self.mode = "sleep"
            state.monitoring_mode = "SKIP_LONG"
        elif result_type == ResultType.NEEDS_PREINSCRIPTION:
            cooldown = random.uniform(3 * 24 * 60 * 60, 7 * 24 * 60 * 60)
            self.mode = "sleep"
            state.monitoring_mode = "SKIP_LONG"
        elif result_type == ResultType.INVALID:
            cooldown = 30 * 24 * 60 * 60
            self.mode = "sleep"
            state.monitoring_mode = "DISABLED"
        elif result_type == ResultType.SERVER_ERROR:
            cooldown = random.uniform(20 * 60, 60 * 60)
            self.mode = "sleep"
        elif result_type == ResultType.PROTECTED_STEP:
            cooldown = 24 * 60 * 60
            self.mode = "paused"
            state.monitoring_mode = "DISABLED"
        else:
            cooldown = random.uniform(30 * 60, 60 * 60)
            self.mode = "sleep"

        state.next_allowed_check_at = now_ts + cooldown * (1 + jitter)
        self.rate_limiter.reserve(now_ts)
        return cooldown, self.mode

    def update_mode(self):
        if self.mode == "burst" and time.time() > self.burst_until:
            self.mode = "sleep"

    def excluded_summary(self):
        excluded = {"SKIP_LONG": 0, "DISABLED": 0}
        for state in self.member_states.values():
            if state.monitoring_mode in excluded:
                excluded[state.monitoring_mode] += 1
        return excluded
