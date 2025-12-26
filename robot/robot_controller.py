# robot/robot_controller.py
import time

from .robot_client import HarApiClient
from .robot_classifier import ResultClassifier, ResultType
from .robot_repository import RobotRepository
from .robot_scheduler import SmartScheduler


class RobotController:
    def __init__(self, api_client, settings):
        self.client = HarApiClient(api_client)
        self.classifier = ResultClassifier()
        self.repo = RobotRepository()
        self.scheduler = SmartScheduler(settings)
        self.scheduler.hydrate(self.repo.load_member_states())
        self.round_id = 0

    def start_round(self):
        self.scheduler.start_round()
        self.round_id = self.scheduler.round_id

    def can_process(self, member_id):
        return self.scheduler.can_process(member_id, time.time())

    def next_wait_seconds(self):
        return self.scheduler.next_wait_seconds(time.time())

    def can_manual_check(self, member_id):
        return self.scheduler.can_process(member_id, time.time())

    def update_mode(self):
        self.scheduler.update_mode()

    def round_status(self):
        return self.round_id

    def record_result(self, member_id, result_type, http_status=None, step="cycle", detail="", duration_ms=0):
        cooldown, mode = self.scheduler.record_result(member_id, result_type, http_status=http_status)
        state = self.scheduler.state_for(member_id)
        self.repo.upsert_member_state(state)
        self.repo.insert_check_log(member_id, step, http_status, result_type, duration_ms, detail)
        if result_type in {
            ResultType.HAS_DATES,
            ResultType.RATE_LIMIT,
            ResultType.PROTECTED_STEP,
            ResultType.NETWORK_ERROR,
            ResultType.SERVER_ERROR,
        }:
            severity = "warn" if result_type in {ResultType.RATE_LIMIT, ResultType.PROTECTED_STEP} else "info"
            self.repo.insert_alert(
                severity,
                "تنبيه الروبوت",
                self.explain_result(result_type, cooldown),
                member_id=member_id,
            )
        return cooldown, mode

    def explain_result(self, result_type, cooldown_seconds):
        minutes = max(1, int(cooldown_seconds / 60))
        if result_type == ResultType.NO_DATES:
            return f"لا توجد مواعيد. تهدئة {minutes} د"
        if result_type == ResultType.RATE_LIMIT:
            return "الخادم مشغول (طلبات كثيرة). تم إيقاف الروبوت مؤقتًا لحماية الحساب."
        if result_type in {ResultType.ERROR_RETRYABLE, ResultType.NETWORK_ERROR}:
            return f"شبكة غير مستقرة. تهدئة {minutes} د"
        if result_type == ResultType.HAS_DATES:
            return "تواريخ متاحة. وضع الطوارئ"
        if result_type == ResultType.HAS_RDV:
            return "تم العثور على موعد"
        if result_type == ResultType.NEEDS_PREINSCRIPTION:
            return "يتطلب تسجيل مسبق"
        if result_type == ResultType.INELIGIBLE:
            return "غير مؤهل"
        if result_type == ResultType.COMPLETED:
            return "مكتمل"
        if result_type == ResultType.BENEFICIARY:
            return "مستفيد"
        if result_type == ResultType.PROTECTED_STEP:
            return "الخطوة محمية وتتطلب تدخل يدوي"
        return "متابعة ذكية"

    def last_alert(self):
        return self.repo.get_last_alert()
