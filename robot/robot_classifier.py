# robot/robot_classifier.py
from enum import Enum


class ResultType(str, Enum):
    HAS_DATES = "HAS_DATES"
    NO_DATES = "NO_DATES"
    HAS_RDV = "HAS_RDV"
    NEEDS_PREINSCRIPTION = "NEEDS_PREINSCRIPTION"
    INELIGIBLE = "INELIGIBLE"
    COMPLETED = "COMPLETED"
    BENEFICIARY = "BENEFICIARY"
    RATE_LIMIT = "RATE_LIMIT"
    ERROR_RETRYABLE = "ERROR_RETRYABLE"
    NETWORK_ERROR = "NETWORK_ERROR"
    SERVER_ERROR = "SERVER_ERROR"
    PROTECTED_STEP = "PROTECTED_STEP"
    UNKNOWN = "UNKNOWN"


class ResultClassifier:
    def classify(self, status_text, error_text, data=None, http_status=None):
        status = (status_text or "").strip()
        normalized_status = status.replace("حاليًا", "حاليا")
        error = (error_text or "").lower()

        if http_status in {401, 403} or "captcha" in error or "forbidden" in error:
            return ResultType.PROTECTED_STEP
        if http_status == 429 or "rate_limit_429" in error or "429" in error or "طلبات كثيرة" in error:
            return ResultType.RATE_LIMIT
        if http_status and 500 <= http_status <= 599:
            return ResultType.SERVER_ERROR
        if "timeout" in error or "connection" in error or "network" in error:
            return ResultType.NETWORK_ERROR

        error_statuses = {
            "فشل التحقق",
            "فشل جلب المعلومات",
            "فشل جلب التواريخ",
            "فشل الحجز",
            "خطأ في المعالجة",
        }
        if normalized_status in error_statuses:
            return ResultType.ERROR_RETRYABLE
        if normalized_status in ["جاري البحث عن مواعيد..."] and data and data.get("dates"):
            return ResultType.HAS_DATES
        no_date_statuses = {
            "لا توجد مواعيد",
            "تم التحقق",
            "تم جلب المعلومات",
            "جاري جلب الاسم...",
            "جاري البحث عن مواعيد...",
        }
        if normalized_status in no_date_statuses:
            return ResultType.NO_DATES
        if normalized_status in ["تم الحجز", "لديه موعد مسبق"]:
            return ResultType.HAS_RDV
        if normalized_status in ["مكتمل"]:
            return ResultType.COMPLETED
        if normalized_status in ["مستفيد حاليا من المنحة"]:
            return ResultType.BENEFICIARY
        if normalized_status in ["يتطلب تسجيل مسبق"]:
            return ResultType.NEEDS_PREINSCRIPTION
        if normalized_status in ["غير مؤهل للحجز", "بيانات الإدخال خاطئة", "غير مؤهل مبدئيًا"]:
            return ResultType.INELIGIBLE
        return ResultType.UNKNOWN
