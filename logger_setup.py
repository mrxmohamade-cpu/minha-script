# logger_setup.py
import logging
import sys
import os # تمت الإضافة للتأكد من وجود المجلد
from logging.handlers import RotatingFileHandler # تمت إضافة هذا السطر
from config import LOG_FILE # LOG_FILE يتم استيراده من config.py ويحتوي الآن على المسار الكامل

# تحديد حجم أقصى لملف السجل (50 ميجابايت) وعدم الاحتفاظ بنسخ احتياطية
MAX_LOG_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB
LOG_BACKUP_COUNT = 0  # عدم الاحتفاظ بأي ملفات احتياطية، سيتم الكتابة فوق الملف الأصلي

def setup_logging():
    """Configures and returns a logger instance with log rotation (single file)."""
    # الحصول على الـ root logger
    root_logger = logging.getLogger()

    # فقط قم بالتهيئة إذا لم تكن هناك معالجات بالفعل لتجنب التكرار
    if not root_logger.hasHandlers():
        root_logger.setLevel(logging.INFO) # تعيين المستوى العام هنا

        # --- معالج ملفات السجل الدوار ---
        # التأكد من وجود مجلد السجلات
        log_dir = os.path.dirname(LOG_FILE)
        if not os.path.exists(log_dir) and log_dir: # تحقق أن log_dir ليس فارغًا
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                # في حالة فشل إنشاء المجلد، اطبع خطأ ولكن استمر
                sys.stderr.write(f"Failed to create log directory {log_dir}: {e}\n")
                # يمكنك اختيار استخدام مسار احتياطي هنا إذا أردت

        # إنشاء RotatingFileHandler
        # عندما يكون backupCount=0، يتم الكتابة فوق الملف الأصلي عند الوصول إلى maxBytes
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=MAX_LOG_SIZE_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding='utf-8',
            delay=True # تأخير فتح الملف حتى أول كتابة
        )
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(threadName)s - %(filename)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        # --- معالج الإخراج إلى الكونسول ---
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
             "%(asctime)s - %(levelname)s - [%(threadName)s] - %(filename)s:%(lineno)d - %(message)s" # تنسيق مختلف قليلاً للكونسول للتمييز
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # رسالة أولية بعد التهيئة
        root_logger.info(f"Logging initialized. Log file: {LOG_FILE}, MaxSize: {MAX_LOG_SIZE_BYTES / (1024*1024):.1f}MB, BackupCount: {LOG_BACKUP_COUNT} (single rotating file)")
    else:
        # إذا كانت هناك معالجات بالفعل، افترض أنه تم تهيئتها
        root_logger.info("Logging already initialized.")

    # إرجاع الـ root logger أو logger محدد إذا كنت تفضل
    return logging.getLogger(__name__)


# Example of how this might be used in another file (e.g., main_app.py at the beginning):
# from logger_setup import setup_logging
# logger = setup_logging() # Call this once at the start of your application
# logger.info("Logging is configured.")
