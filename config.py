# config.py
import requests
import logging
import os # تمت الإضافة
from PyQt5.QtCore import QStandardPaths # تمت الإضافة

# --- Application Specific Name for AppData folder ---
APP_NAME_FOR_DATA_DIR = "AnemAppUserData" # يمكنك تغيير هذا إذا أردت

def get_app_data_dir():
    """
    Returns the application-specific data directory.
    Creates it if it doesn't exist.
    """
    try:
        # QStandardPaths.AppLocalDataLocation هو الأنسب للبيانات التي لا يجب أن يتجول بها المستخدم
        # أو QStandardPaths.AppDataLocation إذا كنت تفضل ذلك (أكثر شيوعًا للتجوال)
        path = QStandardPaths.writableLocation(QStandardPaths.AppLocalDataLocation)
        if not path: # في حالة عدم تمكن PyQt من تحديد المسار (نادر جدًا)
            # fallback to a directory next to the executable, but inside a hidden folder
            path = os.path.join(os.path.abspath("."), "." + APP_NAME_FOR_DATA_DIR.lower() + "_data")
            
        app_data_path = os.path.join(path, APP_NAME_FOR_DATA_DIR)
        
        if not os.path.exists(app_data_path):
            os.makedirs(app_data_path, exist_ok=True)
        return app_data_path
    except Exception as e:
        # Fallback in case of any error with QStandardPaths or directory creation
        fallback_path = os.path.join(os.path.abspath("."), "." + APP_NAME_FOR_DATA_DIR.lower() + "_data_fallback")
        try:
            if not os.path.exists(fallback_path):
                 os.makedirs(fallback_path, exist_ok=True)
            return fallback_path
        except Exception as fe:
            # Ultimate fallback: current directory (not ideal, but better than crashing)
            # Log this critical failure
            critical_fallback_logger = logging.getLogger(__name__ + ".config_critical_fallback")
            critical_fallback_logger.error(f"CRITICAL: Could not create any app data directory. Error with QStandardPaths: {e}, Error with fallback: {fe}. Using current directory.")
            return os.path.abspath(".")


APP_DATA_DIR = get_app_data_dir() # الحصول على المسار مرة واحدة

# --- File Names and Paths (Updated to use APP_DATA_DIR) ---
LOG_FILE = os.path.join(APP_DATA_DIR, "anem_app.log")
DATA_FILE = os.path.join(APP_DATA_DIR, "members_data.json")
SETTINGS_FILE = os.path.join(APP_DATA_DIR, "app_settings.json")
ACTIVATION_STATUS_FILE = os.path.join(APP_DATA_DIR, "activation_status.json")
DEVICE_ID_FILE = os.path.join(APP_DATA_DIR, "device_id.dat") # ملف جديد لـ device_id

# --- Temporary and Backup File Names (Updated to use APP_DATA_DIR) ---
DATA_FILE_TMP = DATA_FILE + ".tmp"
DATA_FILE_BAK = DATA_FILE + ".bak"
SETTINGS_FILE_TMP = SETTINGS_FILE + ".tmp"
SETTINGS_FILE_BAK = SETTINGS_FILE + ".bak"

# --- Resource Files (These should remain relative to the app or be handled by resource_path) ---
STYLESHEET_FILE = "styles_dark.txt" # يبقى كما هو، يُفترض أنه مورد
FIREBASE_SERVICE_ACCOUNT_KEY_FILE = "firebase_service_account_key.json" # يبقى كما هو، يُفترض أنه مورد

# --- API Configuration ---
BASE_API_URL = "https://ac-controle.anem.dz/AllocationChomage/api"
MAIN_SITE_CHECK_URL = "https://ac-controle.anem.dz/"

# --- Session Object (shared across API clients if needed) ---
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'ar-DZ,ar;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
    'Origin': 'https://minha.anem.dz',
    'Referer': 'https://minha.anem.dz/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
})

# --- Settings Keys (used for consistency in accessing settings dict) ---
SETTING_MIN_MEMBER_DELAY = "min_member_delay"
SETTING_MAX_MEMBER_DELAY = "max_member_delay"
SETTING_MONITORING_INTERVAL = "monitoring_interval"
SETTING_BACKOFF_429 = "backoff_429"
SETTING_BACKOFF_GENERAL = "backoff_general"
SETTING_REQUEST_TIMEOUT = "request_timeout"
SETTING_ROBOT_ENABLED = "robot_enabled"
SETTING_ROBOT_BASE_INTERVAL = "robot_base_interval_sec"
SETTING_ROBOT_BURST_MIN = "robot_burst_duration_min"
SETTING_ROBOT_FREEZE_HAS_RDV_DAYS = "robot_freeze_has_rdv_days"
SETTING_ROBOT_RATE_LIMIT_PAUSE_MIN = "robot_rate_limit_pause_min_sec"
SETTING_ROBOT_RATE_LIMIT_PAUSE_MAX = "robot_rate_limit_pause_max_sec"
SETTING_ROBOT_RATE_LIMIT_WINDOW = "robot_rate_limit_window_sec"
SETTING_ROBOT_RATE_LIMIT_THRESHOLD = "robot_rate_limit_threshold"

# --- Request pacing configuration (per endpoint) ---
# هذه القيم تضيف حداً أدنى للفاصل الزمني بين الطلبات الحساسة لتجنب الحظر
ENDPOINT_MIN_REQUEST_INTERVALS = {
    "RendezVous/GetAvailableDates": 3.0,  # ثانية
    "RendezVous/Create": 6.0,
}

# مدة التهدئة الإضافية بعد استلام 429 لنفس المسار (ثواني)
ENDPOINT_429_COOLDOWN = 90.0

# مدى العشوائية المضافة للفواصل الزمنية لتقليد المتصفح وتخفيف الحظر
PACING_JITTER_RANGE = (0.25, 0.9)

# --- Default Settings (if settings file is missing or corrupted) ---
DEFAULT_SETTINGS = {
    SETTING_MIN_MEMBER_DELAY: 5,
    SETTING_MAX_MEMBER_DELAY: 10,
    SETTING_MONITORING_INTERVAL: 1,
    SETTING_BACKOFF_429: 60,
    SETTING_BACKOFF_GENERAL: 5,
    SETTING_REQUEST_TIMEOUT: 30,
    SETTING_ROBOT_ENABLED: True,
    SETTING_ROBOT_BASE_INTERVAL: 8,
    SETTING_ROBOT_BURST_MIN: 25,
    SETTING_ROBOT_FREEZE_HAS_RDV_DAYS: 7,
    SETTING_ROBOT_RATE_LIMIT_PAUSE_MIN: 30 * 60,
    SETTING_ROBOT_RATE_LIMIT_PAUSE_MAX: 120 * 60,
    SETTING_ROBOT_RATE_LIMIT_WINDOW: 15 * 60,
    SETTING_ROBOT_RATE_LIMIT_THRESHOLD: 2,
}

# --- Retry Mechanism Constants (used by AnemAPIClient) ---
MAX_RETRIES = 3
MAX_BACKOFF_DELAY = 120

# --- Other Application Constants ---
MAX_ERROR_DISPLAY_LENGTH = 70
APP_ID_FALLBACK = 'anem-booking-app-pyqt14-refactored-v2' # تم تغيير الـ fallback قليلاً للتمييز

# --- Firebase Activation Constants ---
FIRESTORE_ACTIVATION_CODES_COLLECTION = "activation_codes"
# --- Firebase Messaging Constants ---
FIRESTORE_MESSAGES_COLLECTION = "app_messages" # New collection for messages/updates
FIRESTORE_USER_READ_MESSAGES_SUBCOLLECTION = "read_by_users" # Subcollection to track read messages per user/device

try:
    APP_ID = __app_id
except NameError:
    config_logger = logging.getLogger(__name__ + ".config_fallback")
    config_logger.info("Global variable __app_id not found, using fallback APP_ID.")
    APP_ID = APP_ID_FALLBACK

# تسجيل مسار بيانات التطبيق المستخدم
startup_logger = logging.getLogger(__name__ + ".startup_paths")
startup_logger.info(f"Application Data Directory set to: {APP_DATA_DIR}")
startup_logger.info(f"Log file path: {LOG_FILE}")
startup_logger.info(f"Data file path: {DATA_FILE}")
startup_logger.info(f"Settings file path: {SETTINGS_FILE}")
