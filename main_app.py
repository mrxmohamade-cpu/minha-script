# main_app.py (User App - Enhanced Activation & Error Handling - Data Safety V2 - Activation Thread & UI Fixes V2 - AppData Paths Confirmed - Auto Check & AttributeError Fix - Improved Auto Check Reliability - Corrected Auto Check Sequencing - Messaging Integration - Revamped Notifications UI)
import sys
import json
import os
import logging
import random
import time
import datetime # noqa
import shutil
import re # For stripping HTML from titles

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTableWidget, QTableWidgetItem,
    QMessageBox, QHeaderView, QStatusBar, QFrame, QAction, QStyle,
    QMenu, QLineEdit, QComboBox, QAbstractItemView, QDesktopWidget, QDialog,
    QListWidget, QListWidgetItem, QDialogButtonBox, QTextBrowser,
    QSizePolicy, QToolButton 
)
from PyQt5.QtCore import QTimer, Qt, QDateTime, QLocale, QStandardPaths, QUrl, pyqtSignal, QThread, QSize, QRegularExpression
from PyQt5.QtGui import QIcon, QColor, QPalette, QDesktopServices, QFontDatabase, QFont, QTextDocument

from firebase_service import FirebaseService
from gui_components import (
    ToastNotification, AddMemberDialog, EditMemberDialog,
    SettingsDialog, ViewMemberDialog, ActivationDialog, SubscriptionDetailsDialog,
    MessagesDialog # تمت إضافة MessagesDialog
)

from api_client import AnemAPIClient
from member import Member
from threads import FetchInitialInfoThread, MonitoringThread, SingleMemberCheckThread, DownloadAllPdfsThread
from config import (
    DATA_FILE,
    SETTINGS_FILE,
    DATA_FILE_TMP, DATA_FILE_BAK,
    SETTINGS_FILE_TMP, SETTINGS_FILE_BAK,
    STYLESHEET_FILE, 
    DEFAULT_SETTINGS, SETTING_MIN_MEMBER_DELAY, SETTING_MAX_MEMBER_DELAY,
    SETTING_MONITORING_INTERVAL, SETTING_BACKOFF_429, SETTING_BACKOFF_GENERAL,
    SETTING_REQUEST_TIMEOUT, MAX_ERROR_DISPLAY_LENGTH,
    FIREBASE_SERVICE_ACCOUNT_KEY_FILE, 
    FIRESTORE_ACTIVATION_CODES_COLLECTION,
    ACTIVATION_STATUS_FILE, 
    DEVICE_ID_FILE, 
    FIRESTORE_MESSAGES_COLLECTION, # تمت إضافته
    FIRESTORE_USER_READ_MESSAGES_SUBCOLLECTION # تمت إضافته
)
from logger_setup import setup_logging 
from utils import QColorConstants, get_icon_name_for_status, resource_path

logger = setup_logging()


def load_custom_fonts():
    font_dir = resource_path("fonts")
    if not os.path.isdir(font_dir):
        logger.warning(f"مجلد الخطوط '{font_dir}' غير موجود. لن يتم تحميل الخطوط المخصصة.")
        return

    font_files = [
        "Tajawal-Regular.ttf", "Tajawal-Medium.ttf", "Tajawal-Bold.ttf",
        "Tajawal-ExtraBold.ttf", "Tajawal-Light.ttf",
        "Tajawal-ExtraLight.ttf", "Tajawal-Black.ttf"
    ]
    loaded_fonts_count = 0
    for font_file in font_files:
        font_path = os.path.join(font_dir, font_file)
        if os.path.exists(font_path):
            font_id = QFontDatabase.addApplicationFont(font_path)
            if font_id != -1:
                loaded_fonts_count +=1
            else: logger.warning(f"فشل تحميل الخط: {font_file} من المسار: {font_path}")
        else: logger.warning(f"ملف الخط غير موجود: {font_path}")
    if loaded_fonts_count > 0: logger.info(f"تم تحميل {loaded_fonts_count} خطوط مخصصة بنجاح.")
    else: logger.warning("لم يتم تحميل أي خطوط مخصصة.")

class ActivationProcessingThread(QThread):
    activation_finished = pyqtSignal(bool, str, object)

    def __init__(self, firebase_service_instance, code_to_activate, parent=None):
        super().__init__(parent)
        self.firebase_service = firebase_service_instance
        self.code = code_to_activate
        self.is_running = True

    def run(self):
        if not self.is_running:
            return
        try:
            logger.info(f"ActivationProcessingThread: بدء معالجة الكود {self.code}")
            success, message, server_data = self.firebase_service.activate_code_on_current_device(self.code)
            if self.is_running:
                self.activation_finished.emit(success, message, server_data)
            logger.info(f"ActivationProcessingThread: انتهاء معالجة الكود {self.code}. النجاح: {success}")
        except Exception as e:
            logger.exception(f"ActivationProcessingThread: خطأ غير متوقع أثناء معالجة الكود {self.code}: {e}")
            if self.is_running:
                self.activation_finished.emit(False, f"خطأ غير متوقع في الخيط: {e}", None)

    def stop(self):
        self.is_running = False

class AnemApp(QMainWindow):
    COL_ICON, COL_FULL_NAME_AR, COL_NIN, COL_WASSIT, COL_CCP, COL_PHONE_NUMBER, COL_STATUS, COL_RDV_DATE, COL_DETAILS = range(9)
    subscription_updated_signal = pyqtSignal(object, str)
    new_app_messages_signal = pyqtSignal(list, str) # إشارة جديدة للرسائل


    def __init__(self):
        super().__init__()
        self._should_initialize_ui = False
        self.activation_successful = False
        self.firebase_service = FirebaseService() 
        self.activated_code_id = None
        self.current_subscription_data = None
        self.current_device_id = self.firebase_service.current_device_id_for_messaging
        self.activation_dialog_open = False
        self.toast_notifications = []
        self.settings = {}
        self.activation_thread = None

        # متغيرات خاصة بالرسائل والإشعارات
        self.app_messages = [] 
        self.unread_message_count = 0
        self.messages_dialog_instance = None 
        self.messages_button_status_bar = None 
        self.toast_shown_for_message_ids = set() # لتتبع إشعارات Toast المعروضة

        self._initialize_and_check_activation()

        if not self.activation_successful:
            logger.critical("AnemApp __init__: فشل تفعيل البرنامج. لن يتم إكمال تهيئة واجهة المستخدم.")
            return
        self._should_initialize_ui = True

        load_custom_fonts()
        QApplication.setLayoutDirection(Qt.RightToLeft)
        self.setWindowTitle("برنامج إدارة مواعيد منحة البطالة")

        desktop = QApplication.desktop()
        available_geometry = desktop.availableGeometry(self)
        self.setGeometry(available_geometry)
        self.setWindowState(Qt.WindowMaximized)
        logger.info(f"تم تعيين النافذة الرئيسية لملء الشاشة المتاحة: {available_geometry.width()}x{available_geometry.height()}")

        self.load_app_settings() 

        self.suppress_initial_messages = True
        self.members_list = []
        self.filtered_members_list = []
        self.is_filter_active = False
        self.last_added_member_index = None
        self.member_overview_selected_index = None

        self.api_client = AnemAPIClient(
            initial_backoff_general=self.settings.get(SETTING_BACKOFF_GENERAL, DEFAULT_SETTINGS[SETTING_BACKOFF_GENERAL]),
            initial_backoff_429=self.settings.get(SETTING_BACKOFF_429, DEFAULT_SETTINGS[SETTING_BACKOFF_429]),
            request_timeout=self.settings.get(SETTING_REQUEST_TIMEOUT, DEFAULT_SETTINGS[SETTING_REQUEST_TIMEOUT])
        )

        self.initial_fetch_threads = []
        self.single_check_thread = None
        self.active_download_all_pdfs_threads = {}
        self.active_spinner_row_in_view = -1
        self.spinner_char_idx = 0
        self.spinner_chars = ['◐', '◓', '◑', '◒']
        self.latest_countdown_text = "لا يوجد انتظار حالي"
        self.activity_feed = []  # يحتفظ بآخر الرسائل لعرضها للمستخدم

        # عناصر شريط الحالة المحسّنة
        self.status_spinner_idx = 0
        self.status_spinner_chars = ['⏳', '⌛', '⏰', '🕒']
        self.status_spinner_timer = QTimer(self)
        self.status_spinner_timer.setInterval(350)
        self.status_spinner_timer.timeout.connect(self._advance_status_spinner)
        self.row_spinner_timer = QTimer(self)
        self.row_spinner_timer.timeout.connect(self.update_active_row_spinner_display)
        self.row_spinner_timer_interval = 150

        self.monitoring_thread = MonitoringThread(self.members_list, self.settings.copy())
        self.monitoring_thread.update_member_gui_signal.connect(self.update_member_gui_in_table)
        self.monitoring_thread.new_data_fetched_signal.connect(self.update_member_name_in_table)
        self.monitoring_thread.global_log_signal.connect(self.update_status_bar_message)
        self.monitoring_thread.member_being_processed_signal.connect(self.handle_member_processing_signal)
        self.monitoring_thread.countdown_update_signal.connect(self.update_countdown_timer_display)

        self.subscription_updated_signal.connect(self._handle_subscription_update_from_signal)
        self.new_app_messages_signal.connect(self._handle_incoming_app_messages_on_main_thread) # ربط الإشارة الجديدة


        self.init_ui()
        self.load_stylesheet()
        self.load_members_data()
        QTimer.singleShot(0, self.apply_app_settings)
        
        if self.activation_successful:
            self._start_message_listener() # بدء مستمع الرسائل

        self._refresh_subscription_badge()

        logger.info("AnemApp __init__: اكتملت التهيئة.")

    def close_app_due_to_error(self, message=""):
        logger.info(f"AnemApp: Closing application due to critical error. Message: {message}")
        if self.activation_thread and self.activation_thread.isRunning():
            self.activation_thread.stop()
            self.activation_thread.wait(1000)

        if message and not self.activation_dialog_open :
             QMessageBox.critical(self, "خطأ حاسم", message, QMessageBox.Ok)
        self.close() 

    def _initialize_and_check_activation(self):
        self.activation_successful = self._perform_activation_check_logic()
        if not self.activation_successful:
            logger.info("AnemApp: _initialize_and_check_activation determined activation failed.")

    def _recheck_activation_state(self):
        """إعادة التحقق يدويًا من حالة الاشتراك وإظهار نتيجة واضحة للمستخدم."""
        self.update_status_bar_message("إعادة التحقق من الاشتراك...", is_general_message=True)
        success = self._perform_activation_check_logic()
        self.activation_successful = success
        self._refresh_subscription_badge()
        if success:
            self._show_toast("تمت إعادة التحقق بنجاح.", type="success", title="الاشتراك")
            self.update_status_bar_message("الاشتراك نشط وتمت المزامنة.", is_general_message=True)
        else:
            self._show_toast("فشلت إعادة التحقق. يرجى التحقق من الاتصال أو إعادة إدخال الكود.", type="error", title="الاشتراك")

    def _perform_activation_check_logic(self):
        logger.info("AnemApp: بدء التحقق من تفعيل البرنامج...")
        if not self.firebase_service.is_initialized(): 
            logger.critical(f"AnemApp: خدمة Firebase غير مهيأة. تأكد من وجود ملف '{FIREBASE_SERVICE_ACCOUNT_KEY_FILE}'.")
            if not hasattr(self, 'toast_notifications'): self.toast_notifications = []
            QMessageBox.critical(self, "خطأ فادح في الاتصال",
                                 f"لا يمكن تهيئة خدمة المصادقة.\nالرجاء التأكد من وجود ملف '{FIREBASE_SERVICE_ACCOUNT_KEY_FILE}' وأنه صالح, ومن وجود اتصال بالإنترنت.\nسيتم إغلاق البرنامج.",
                                 QMessageBox.Ok)
            return False

        is_locally_activated, local_code, local_device_id, local_data = self.firebase_service.check_local_activation()

        if is_locally_activated and local_code and local_device_id:
            logger.info(f"AnemApp: البرنامج مفعل محليًا بالكود: {local_code} للجهاز: {local_device_id}. يتم التحقق من الصلاحية عبر الإنترنت...")
            is_still_valid_online, online_message, server_code_data = self.firebase_service.verify_online_status_and_device(local_code, local_device_id)

            if is_still_valid_online and server_code_data:
                logger.info(f"AnemApp: الكود المحلي '{local_code}' صالح وحالته '{server_code_data.get('status', 'UNKNOWN')}' في Firebase.")
                self.current_subscription_data = server_code_data
                self.activated_code_id = local_code
                self.firebase_service.listen_to_activation_code_changes(self.activated_code_id, self._pass_subscription_update_to_signal)
                return True 
            else:
                user_facing_message = "فشل التحقق من التفعيل المحلي عبر الإنترنت. قد يكون الاشتراك قد انتهى أو تم إلغاؤه."
                if "لم يعد هذا الجهاز مصرحًا له" in online_message:
                    user_facing_message = "لم يعد هذا الجهاز مصرحًا له باستخدام هذا الكود."
                elif "تم إلغاء هذا الاشتراك" in online_message:
                     user_facing_message = "تم إلغاء هذا الاشتراك من قبل المسؤول."
                elif "الاشتراك منتهي الصلاحية" in online_message or "قد انتهت صلاحيته" in online_message:
                    user_facing_message = "صلاحية اشتراكك الحالي قد انتهت."

                logger.warning(f"AnemApp: الكود المحلي '{local_code}' لم يعد صالحًا عبر الإنترنت: {online_message}.")
                self._clear_local_activation_and_state(f"الكود المحلي ({local_code}) لم يعد صالحًا: {online_message}")
                return self._show_activation_dialog_loop(initial_message=user_facing_message, initial_is_error=True)

        logger.info("AnemApp: البرنامج غير مفعل محليًا أو التحقق المحلي فشل. يتطلب التفعيل عبر الإنترنت.")
        return self._show_activation_dialog_loop()

    def _clear_local_activation_and_state(self, reason=""):
        logger.info(f"AnemApp: مسح بيانات التفعيل المحلية. السبب: {reason}")
        if os.path.exists(ACTIVATION_STATUS_FILE):
            try:
                os.remove(ACTIVATION_STATUS_FILE)
                logger.info(f"AnemApp: تم حذف ملف التفعيل المحلي: {ACTIVATION_STATUS_FILE}")
            except Exception as e_remove:
                logger.error(f"AnemApp: خطأ أثناء حذف ملف التفعيل المحلي {ACTIVATION_STATUS_FILE}: {e_remove}")
        self.current_subscription_data = None
        if self.activated_code_id and self.firebase_service.is_initialized():
            self.firebase_service.stop_listening_to_code_changes(self.activated_code_id)
        self.activated_code_id = None

    def _process_activation_result(self, success, message_from_service, server_code_data, dialog_instance):
        logger.info(f"AnemApp: Activation thread finished. Success: {success}, Msg: '{message_from_service}'")
        if not dialog_instance or not hasattr(dialog_instance, 'isVisible') or not dialog_instance.isVisible():
            logger.warning("AnemApp: _process_activation_result called but dialog_instance is None or not visible.")
            return False 

        dialog_instance.show_status_message("", is_waiting=False) 

        if success and server_code_data:
            self.current_subscription_data = server_code_data
            self.activated_code_id = server_code_data.get("id", dialog_instance.get_activation_code())

            dialog_instance.show_status_message(message_from_service or "تم تفعيل البرنامج بنجاح!", is_success=True)
            QMessageBox.information(dialog_instance, "نجاح التفعيل", message_from_service or "تم تفعيل البرنامج بنجاح!")

            self._refresh_subscription_badge()
            self.firebase_service.listen_to_activation_code_changes(self.activated_code_id, self._pass_subscription_update_to_signal)
            dialog_instance.accept()
            return True
        else:
            if server_code_data: self.current_subscription_data = server_code_data
            user_friendly_error = "فشل تفعيل الكود. يرجى المحاولة مرة أخرى."
            if message_from_service:
                if "غير صحيح أو غير موجود" in message_from_service: user_friendly_error = "الكود الذي أدخلته غير صحيح أو غير موجود."
                elif "صلاحية هذا الكود قد انتهت" in message_from_service: user_friendly_error = "عذرًا، صلاحية الكود الذي أدخلته قد انتهت."
                elif "تم إلغاؤه من قبل المسؤول" in message_from_service: user_friendly_error = "تم إلغاء صلاحية هذا الكود."
                elif "تم الوصول للحد الأقصى لعدد الأجهزة" in message_from_service: user_friendly_error = "تم استخدام هذا الكود على الحد الأقصى من الأجهزة."
                elif "فشل الاتصال" in message_from_service or "خدمة Firebase غير مهيأة" in message_from_service or "خطأ غير متوقع في الخيط" in message_from_service:
                     user_friendly_error = "فشل الاتصال بالخادم أو حدث خطأ أثناء التحقق. يرجى التحقق من اتصالك بالإنترنت والمحاولة مرة أخرى."
                else: user_friendly_error = message_from_service

            dialog_instance.show_status_message(user_friendly_error, is_error=True)
            return False 


    def _show_activation_dialog_loop(self, initial_message=None, initial_is_error=False):
        if self.activation_dialog_open:
            logger.warning("AnemApp: Activation dialog is already open. Skipping new instance.")
            return self.activation_successful

        self.activation_dialog_open = True
        activation_dialog = ActivationDialog(self) 

        activation_processing_started_locally = False
        activation_outcome_from_processing = False

        def handle_activation_attempt(code_from_dialog):
            nonlocal activation_processing_started_locally
            nonlocal activation_outcome_from_processing 

            if activation_processing_started_locally:
                logger.warning("Activation attempt while another is in progress. Ignoring.")
                return

            if not code_from_dialog:
                activation_dialog.show_status_message("الرجاء إدخال كود التفعيل.", is_error=True)
                return

            if not self.firebase_service.is_initialized():
                activation_dialog.show_status_message("فشل الاتصال بخادم التفعيل. يرجى التحقق من اتصالك بالإنترنت.", is_error=True)
                return

            activation_processing_started_locally = True
            activation_dialog.show_status_message("جاري التحقق وتفعيل الكود...", is_waiting=True)

            if self.activation_thread and self.activation_thread.isRunning():
                self.activation_thread.stop()
                self.activation_thread.wait(500)

            self.activation_thread = ActivationProcessingThread(self.firebase_service, code_from_dialog, self)

            def on_activation_processed(success, msg, data):
                nonlocal activation_outcome_from_processing
                activation_outcome_from_processing = self._process_activation_result(success, msg, data, activation_dialog)

            self.activation_thread.activation_finished.connect(on_activation_processed)

            def on_thread_actually_finished():
                nonlocal activation_processing_started_locally
                activation_processing_started_locally = False
                if hasattr(activation_dialog, 'isVisible') and activation_dialog.isVisible():
                    if activation_dialog.result() != QDialog.Accepted:
                         current_status_text = activation_dialog.status_message_area.toPlainText()
                         if "⏳" in current_status_text and not activation_outcome_from_processing: 
                            activation_dialog.show_status_message(current_status_text.replace("⏳", "❌").strip(), is_error=True, is_waiting=False)
                         elif "⏳" in current_status_text: 
                            activation_dialog.show_status_message(current_status_text, is_waiting=False)


            self.activation_thread.finished.connect(on_thread_actually_finished)
            self.activation_thread.start()

        activation_dialog.activation_attempted.connect(handle_activation_attempt)

        current_message_for_dialog = initial_message if initial_message else "الرجاء إدخال كود التفعيل للمتابعة."
        current_is_error_for_dialog = initial_is_error
        activation_dialog.show_status_message(current_message_for_dialog, is_error=current_is_error_for_dialog)

        parent_window_for_centering = self if self.isVisible() else QApplication.desktop()
        screen_geometry = QApplication.desktop().availableGeometry(parent_window_for_centering)
        activation_dialog.adjustSize()
        x_pos = screen_geometry.left() + (screen_geometry.width() - activation_dialog.width()) // 2
        y_pos = screen_geometry.top() + (screen_geometry.height() - activation_dialog.height()) // 2
        activation_dialog.move(x_pos, y_pos)
        logger.info(f"تم تعيين موقع حوار التفعيل إلى: ({x_pos}, {y_pos}) على الشاشة.")

        dialog_result_code = activation_dialog.exec_() 

        if self.activation_thread and self.activation_thread.isRunning():
            logger.info("Activation dialog closed, stopping activation thread if running.")
            self.activation_thread.stop()
            self.activation_thread.wait(1000)

        self.activation_dialog_open = False

        if dialog_result_code == QDialog.Accepted:
            return True 
        else:
            logger.warning(f"AnemApp: عملية التفعيل ألغيت من قبل المستخدم أو أُغلقت (Result: {dialog_result_code}).")
            return False 

    def _show_subscription_details_dialog(self):
        if self.current_subscription_data:
            dialog = SubscriptionDetailsDialog(self.current_subscription_data, self)
            dialog.exec_()
        elif self.activated_code_id and self.firebase_service.is_initialized():
            self._show_toast("جاري جلب تفاصيل الاشتراك...", type="info", title="الاشتراك")
            QApplication.processEvents()
            code_data, error_msg = self.firebase_service.get_activation_code_details(self.activated_code_id)
            if code_data:
                self.current_subscription_data = code_data
                dialog = SubscriptionDetailsDialog(self.current_subscription_data, self)
                dialog.exec_()
            else:
                QMessageBox.warning(self, "خطأ", f"لم يتم العثور على تفاصيل الاشتراك للكود {self.activated_code_id}: {error_msg}")
        else:
             QMessageBox.information(self, "لا توجد تفاصيل", "لم يتم تفعيل البرنامج أو لا يمكن الوصول لمعلومات الاشتراك حاليًا.")

    def _pass_subscription_update_to_signal(self, updated_data, error_message):
        self.subscription_updated_signal.emit(updated_data, error_message)

    def _handle_subscription_update_from_signal(self, updated_data, error_message):
        logger.info(f"AnemApp: Subscription update received via signal: Data={bool(updated_data)}, Error='{error_message}'")

        critical_error_occurred = False
        critical_message = ""

        if error_message:
            logger.error(f"AnemApp: خطأ في مستمع تحديثات الاشتراك: {error_message}")
            if error_message == "DocumentDeleted":
                critical_message = "تم حذف كود التفعيل الخاص بك من الخادم."
                critical_error_occurred = True
            else:
                self.update_status_bar_message(f"خطأ في تحديث الاشتراك: {error_message}", is_general_message=True)
                self._show_toast(f"خطأ في تحديث حالة الاشتراك: {error_message}", type="error", title="خطأ اشتراك")
                if not updated_data: 
                    critical_message = f"حدث خطأ أثناء تحديث حالة الاشتراك ({error_message}). قد تحتاج لإعادة التفعيل."
                    critical_error_occurred = True


        if updated_data:
            old_status = self.current_subscription_data.get("status", "UNKNOWN") if self.current_subscription_data else "UNKNOWN"
            self.current_subscription_data = updated_data 

            new_status_from_server = updated_data.get("status", "UNKNOWN").upper()
            actual_expires_at = updated_data.get("actualExpiresAt") 
            activated_devices_list = updated_data.get("activatedDevices", [])

            effective_status = new_status_from_server 

            current_device_still_active = False
            if self.current_device_id:
                for dev_entry in activated_devices_list:
                    if isinstance(dev_entry, dict) and dev_entry.get("generated_device_id") == self.current_device_id:
                        current_device_still_active = True
                        break

            if effective_status == "ACTIVE" and not current_device_still_active and self.activated_code_id: 
                if not critical_error_occurred: 
                    critical_message = "لم يعد هذا الجهاز مصرحًا له باستخدام هذا الكود. قد يكون تم تفعيله على أجهزة أخرى أو تم إزالته من قبل المسؤول."
                critical_error_occurred = True
                effective_status = "DEVICE_REMOVED" 

            if effective_status == "ACTIVE" and actual_expires_at and actual_expires_at <= datetime.datetime.now(datetime.timezone.utc):
                if not critical_error_occurred:
                    critical_message = "صلاحية اشتراكك قد انتهت."
                critical_error_occurred = True
                effective_status = "EXPIRED" 
                logger.warning(f"AnemApp: Listener detected expiry for code {self.activated_code_id}. Server status was {new_status_from_server}.")


            if new_status_from_server == "REVOKED":
                if not critical_error_occurred:
                    critical_message = "تم إلغاء اشتراكك من قبل المسؤول."
                critical_error_occurred = True
                effective_status = "REVOKED" 

            elif new_status_from_server == "EXPIRED": 
                 if not critical_error_occurred:
                    critical_message = "صلاحية اشتراكك قد انتهت (وفقًا للخادم)."
                 critical_error_occurred = True
                 effective_status = "EXPIRED"


            expiry_text_display = "لا يوجد تاريخ انتهاء"
            remaining_time_str_display = ""
            if actual_expires_at and isinstance(actual_expires_at, datetime.datetime):
                q_dt_local_expiry = QDateTime.fromSecsSinceEpoch(int(actual_expires_at.timestamp()), Qt.UTC).toLocalTime()
                expiry_text_display = q_dt_local_expiry.toString("yyyy/MM/dd hh:mm AP")
                if effective_status == "ACTIVE" and actual_expires_at > datetime.datetime.now(datetime.timezone.utc): 
                    remaining_delta = actual_expires_at - datetime.datetime.now(datetime.timezone.utc)
                    days, hours, minutes = remaining_delta.days, remaining_delta.seconds // 3600, (remaining_delta.seconds // 60) % 60
                    if days > 0: remaining_time_str_display = f" (متبقي: {days}ي {hours}س)"
                    elif hours > 0: remaining_time_str_display = f" (متبقي: {hours}س {minutes}د)"
                    else: remaining_time_str_display = f" (متبقي: {minutes}د)"


            status_display_map = {"ACTIVE": "نشط", "EXPIRED": "منتهي الصلاحية",
                                  "REVOKED": "تم إلغاؤه", "UNUSED": "غير مستخدم",
                                  "DEVICE_REMOVED": "الجهاز غير مصرح له"}
            status_text_for_gui = status_display_map.get(effective_status, effective_status) 
            self.update_status_bar_message(f"الاشتراك: {status_text_for_gui}, ينتهي في: {expiry_text_display}{remaining_time_str_display}", is_general_message=True)


            if effective_status != "ACTIVE":
                if not critical_error_occurred: 
                    if effective_status == "EXPIRED": critical_message = "صلاحية اشتراكك قد انتهت."
                    elif effective_status == "DEVICE_REMOVED": critical_message = "لم يعد هذا الجهاز مصرحًا له باستخدام هذا الكود."
                    elif old_status == "ACTIVE": 
                         critical_message = f"تغيرت حالة اشتراكك إلى '{status_text_for_gui}'. لا يمكن متابعة استخدام البرنامج."
                critical_error_occurred = True 
            elif old_status != "ACTIVE" and not critical_error_occurred: 
                self._enable_app_functions()
                self._show_toast("تم تحديث معلومات الاشتراك بنجاح. البرنامج نشط.", type="success", title="تحديث الاشتراك")

        elif not error_message: 
            logger.warning("AnemApp: Received empty data in subscription update without error from listener.")


        if critical_error_occurred:
            final_critical_msg = critical_message if critical_message else "حدث خطأ في حالة الاشتراك يمنع استخدام البرنامج."
            self._show_critical_subscription_error(final_critical_msg)
            self._clear_local_activation_and_state(f"Critical subscription issue: {final_critical_msg}")
            if not self.activation_dialog_open:
                logger.info("AnemApp: Attempting to re-show activation dialog due to critical subscription error.")
                QTimer.singleShot(100, lambda: self._initialize_and_check_activation())

        self._refresh_subscription_badge()

    def _show_critical_subscription_error(self, message):
        logger.critical(f"AnemApp: Critical subscription error: {message}")
        if not self.activation_dialog_open:
            QMessageBox.critical(self, "خطأ في الاشتراك", message + "\nسيتم تعطيل وظائف البرنامج.", QMessageBox.Ok)
        self._disable_app_functions()

    def _refresh_subscription_badge(self):
        if not self.subscription_badge:
            return
        status = (self.current_subscription_data or {}).get("status", "UNKNOWN").upper()
        label = self.current_subscription_data.get("activation_code") if self.current_subscription_data else None
        status_map = {
            "ACTIVE": ("الاشتراك: مفعل", "#1dd1a1"),
            "PENDING": ("الاشتراك: قيد التفعيل", "#f2c94c"),
            "EXPIRED": ("الاشتراك: منتهي", "#ff6b6b"),
            "REVOKED": ("الاشتراك: ملغي", "#ff6b6b"),
            "DEVICE_REMOVED": ("الجهاز غير مصرح", "#f28482"),
        }
        text, color = status_map.get(status, ("الاشتراك: غير معروف", "#74c0fc"))
        if label:
            text = f"{text} ({label})"
        self.subscription_badge.setText(text)
        self.subscription_badge.setStyleSheet(
            f"QLabel#SubscriptionBadge {{ background: {color}22; color: {color}; padding: 6px 10px;"
            f" border: 1px solid {color}; border-radius: 10px; font-weight: 700; }}"
        )

    def _disable_app_functions(self):
        logger.info("AnemApp: Disabling application functions.")
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.add_member_button.setEnabled(False)
        self.remove_member_button.setEnabled(False)
        if hasattr(self, 'check_now_button'):
            self.check_now_button.setEnabled(False)
        if hasattr(self, 'settings_action'): self.settings_action.setEnabled(False)
        if self.monitoring_thread.isRunning():
            self.stop_monitoring()

    def _enable_app_functions(self):
        logger.info("AnemApp: Enabling application functions.")
        self.start_button.setEnabled(True)
        self.add_member_button.setEnabled(True)
        self.remove_member_button.setEnabled(True)
        if hasattr(self, 'check_now_button'):
            self.check_now_button.setEnabled(True)
        if hasattr(self, 'settings_action'): self.settings_action.setEnabled(True)

    def _start_message_listener(self):
        if self.firebase_service and self.firebase_service.is_initialized():
            logger.info("AnemApp: Starting app messages listener.")
            # استدعاء مستمع الرسائل من FirebaseService
            self.firebase_service.listen_to_app_messages(self.new_app_messages_signal.emit, limit_count=25) # حددنا 25 رسالة كحد أقصى
        else:
            logger.warning("AnemApp: Cannot start message listener, Firebase service not ready.")

    def _handle_incoming_app_messages_on_main_thread(self, messages_list, error_message):
        """يعالج الرسائل المستلمة من Firebase، يعمل على الخيط الرئيسي."""
        if error_message:
            logger.error(f"AnemApp: Error receiving app messages: {error_message}")
            self._show_toast(f"خطأ في استقبال الرسائل: {error_message}", type="error", title="خطأ رسائل")
            return

        if messages_list is None:
            logger.info("AnemApp: Received None for messages_list, possibly initial call or listener stop.")
            return

        logger.info(f"AnemApp: Received {len(messages_list)} app messages.")
        self.app_messages = messages_list # تم الفرز بالفعل في FirebaseService
        
        new_unread_count = 0
        latest_unread_message_to_toast = None
        high_priority_message_received = False 

        for msg in self.app_messages:
            is_active = msg.get('active', False)
            expires_at = msg.get('expiresAt')
            is_expired = False
            if isinstance(expires_at, datetime.datetime):
                if expires_at.tzinfo is None: expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
                if expires_at < datetime.datetime.now(datetime.timezone.utc):
                    is_expired = True
            
            if is_active and not is_expired:
                if not msg.get('is_read_by_current_device', False):
                    new_unread_count += 1
                    # عرض إشعار Toast للرسالة الأحدث غير المقروءة فقط لتجنب إغراق المستخدم
                    if latest_unread_message_to_toast is None: 
                         latest_unread_message_to_toast = msg
                    if msg.get('priority', 'normal').lower() == 'high':
                        high_priority_message_received = True
        
        self.unread_message_count = new_unread_count
        self._update_messages_action_ui() 
        self._update_messages_button_status_bar() 

        if latest_unread_message_to_toast:
            msg_id_for_toast = latest_unread_message_to_toast.get('id')
            # النقطة 4: دعم عرض التنبيه (Toast) عند وصول رسالة جديدة، وعدم تكراره
            if msg_id_for_toast not in self.toast_shown_for_message_ids:
                toast_title = latest_unread_message_to_toast.get('title', 'رسالة جديدة')
                plain_toast_title = MessagesDialog._strip_html(None, toast_title) 
                short_toast_title = (plain_toast_title[:35] + '...') if len(plain_toast_title) > 35 else plain_toast_title
                if not short_toast_title.strip(): short_toast_title = "رسالة جديدة"

                toast_content_short = MessagesDialog._strip_html(None, latest_unread_message_to_toast.get('content', '')) 
                toast_content_short = (toast_content_short[:70] + "...") if len(toast_content_short) > 70 else toast_content_short
                
                toast_type = "info"
                priority_icon_toast = ""
                if latest_unread_message_to_toast.get('priority', 'normal').lower() == 'high':
                    toast_type = "warning" 
                    priority_icon_toast = "⚠️ " # النقطة 6: تمييزها بأيقونة
                    short_toast_title = f"{priority_icon_toast}{short_toast_title}"


                self._show_toast(toast_content_short, title=short_toast_title, type=toast_type, duration=10000, message_id=msg_id_for_toast)
                
                # النقطة 6: فتح مركز الرسائل تلقائيًا إذا وصلت رسالة ذات أولوية عالية
                if high_priority_message_received:
                    logger.info("AnemApp: High priority message received. Opening messages dialog automatically.")
                    QTimer.singleShot(3000, self._show_messages_dialog) # فتح بعد 3 ثوانٍ
        
        # تحديث واجهة عرض الرسائل إذا كانت مفتوحة
        if self.messages_dialog_instance and self.messages_dialog_instance.isVisible():
            self.messages_dialog_instance.messages = self.app_messages 
            self.messages_dialog_instance._populate_message_list() 


    def _update_messages_action_ui(self):
        """تحديث عنصر القائمة الخاص بالرسائل (النقطة 2 - جزء من Badge)."""
        if not self.messages_action_menu:
            return
        base_text = "الرسائل والتحديثات"
        icon_theme_name = "mail-unread-new" if self.unread_message_count > 0 else "mail-read"

        action_icon = QIcon.fromTheme(icon_theme_name, self.style().standardIcon(QStyle.SP_MessageBoxInformation))
        self.messages_action_menu.setIcon(action_icon)

        if self.unread_message_count > 0:
            self.messages_action_menu.setText(f"{base_text} ({self.unread_message_count})")
            font = self.messages_action_menu.font()
            font.setBold(True)
            self.messages_action_menu.setFont(font)
        else:
            self.messages_action_menu.setText(base_text)
            font = self.messages_action_menu.font()
            font.setBold(False)
            self.messages_action_menu.setFont(font)

    def _update_messages_button_status_bar(self):
        """تحديث زر الرسائل في شريط الحالة (النقطة 2 - Badge)."""
        if hasattr(self, 'messages_button_status_bar') and self.messages_button_status_bar:
            if self.unread_message_count > 0:
                # استخدام أيقونة تحذير للإشارة إلى رسائل جديدة ومهمة
                icon = self.style().standardIcon(QStyle.SP_MessageBoxWarning) 
                # تغيير لون النص أو الخلفية للزر لجعله أكثر وضوحًا
                self.messages_button_status_bar.setStyleSheet("QToolButton { color: #FFD700; font-weight: bold; border: none; padding: 2px; background-color: #C0392B; border-radius: 4px;} QToolButton:hover { background-color: #E74C3C; }") 
                self.messages_button_status_bar.setText(f" {self.unread_message_count} ") # عرض العدد داخل دائرة حمراء (عبر QSS)
                self.messages_button_status_bar.setToolTip(f"لديك {self.unread_message_count} رسالة جديدة. انقر لعرض الرسائل.")
            else:
                icon = self.style().standardIcon(QStyle.SP_MessageBoxInformation) # أيقونة معلومات عادية
                self.messages_button_status_bar.setStyleSheet("QToolButton { color: #D8DEE9; border: none; padding: 2px; } QToolButton:hover { background-color: #4C566A; }") # النمط الافتراضي
                self.messages_button_status_bar.setText("") 
                self.messages_button_status_bar.setToolTip("عرض الرسائل والتحديثات (لا توجد رسائل جديدة).")
            
            self.messages_button_status_bar.setIcon(icon)
            self.messages_button_status_bar.setIconSize(QSize(16,16)) 


    def _show_messages_dialog(self):
        """إنشاء وعرض حوار الرسائل."""
        if self.messages_dialog_instance and self.messages_dialog_instance.isVisible():
            self.messages_dialog_instance.activateWindow()
            self.messages_dialog_instance.raise_()
            return

        self.messages_dialog_instance = MessagesDialog(self.app_messages, self.firebase_service, self)
        self.messages_dialog_instance.message_read_signal.connect(self._handle_message_marked_as_read_in_dialog)
        self.messages_dialog_instance.exec_() 
        self.messages_dialog_instance = None # مسح المثيل بعد الإغلاق


    def _handle_message_marked_as_read_in_dialog(self, message_id_read):
        """معالجة قراءة رسالة من خلال حوار الرسائل."""
        found = False
        for msg in self.app_messages:
            if msg.get('id') == message_id_read:
                if not msg.get('is_read_by_current_device', False): 
                    msg['is_read_by_current_device'] = True # تحديث الحالة محليًا
                    if self.unread_message_count > 0 : self.unread_message_count -=1
                found = True
                break
        if found:
            self._update_messages_action_ui()
            self._update_messages_button_status_bar() 
        else: 
            logger.warning(f"AnemApp: Message {message_id_read} marked as read in dialog, but not found in local app_messages list for UI update.")
            # قد نحتاج لإعادة جلب الرسائل إذا حدث عدم تطابق
            self._update_messages_action_ui()
            self._update_messages_button_status_bar()



    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(12)

        # عناصر الواجهة (تصريحات أولية)
        self.search_filter_frame = None
        self.search_input = None
        self.filter_by_combo = None
        self.filter_value_combo = None
        self.clear_filter_button = None

        self.subscription_badge = None
        self.subscription_manage_button = None
        self.refresh_activation_button = None

        self.operation_panel_frame = None
        self.operation_current_label = None
        self.operation_next_label = None
        self.operation_timer_label = None

        self.insight_frame = None
        self.stat_total_card = None
        self.stat_ready_card = None
        self.stat_monitor_card = None
        self.activity_list = None

        self.member_overview_frame = None
        self.member_overview_title = None
        self.member_overview_state = None
        self.member_overview_next = None
        self.member_overview_wait = None
        self.member_overview_last = None
        self.member_overview_hint = None

        menubar = self.menuBar()
        file_menu = menubar.addMenu("ملف")
        self.settings_action = QAction(QIcon.fromTheme("preferences-system"), "الإعدادات...", self)
        self.settings_action.triggered.connect(self.open_settings_dialog)
        file_menu.addAction(self.settings_action)

        # واجهة مبسطة بدون قوائم أدوات إضافية
        self.toggle_search_filter_action = None
        self.toggle_details_action = None
        self.messages_action_menu = None

        file_menu.addSeparator()
        exit_action = QAction(QIcon.fromTheme("application-exit"), "خروج", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # رأس منظم مع شارة اشتراك وإجراءات واضحة
        header_frame = QFrame()
        header_frame.setObjectName("HeaderFrame")
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(14, 12, 14, 10)
        header_layout.setSpacing(16)

        title_column = QVBoxLayout()
        title_column.setSpacing(2)
        title_label = QLabel("إدارة مواعيد منحة البطالة")
        title_label.setObjectName("HeaderTitle")
        subtitle_label = QLabel("متابعة الأعضاء والحجوزات برسائل واضحة")
        subtitle_label.setObjectName("HeaderSubtitle")
        title_column.addWidget(title_label)
        title_column.addWidget(subtitle_label)
        header_layout.addLayout(title_column, 2)

        badge_container = QHBoxLayout()
        badge_container.setSpacing(8)

        self.subscription_badge = QLabel("الاشتراك: غير معروف")
        self.subscription_badge.setObjectName("SubscriptionBadge")
        badge_container.addWidget(self.subscription_badge)

        self.subscription_manage_button = QPushButton("إدارة الاشتراك")
        self.subscription_manage_button.setObjectName("SubscriptionManageButton")
        self.subscription_manage_button.setIcon(self.style().standardIcon(QStyle.SP_ComputerIcon))
        self.subscription_manage_button.clicked.connect(self._show_subscription_details_dialog)
        badge_container.addWidget(self.subscription_manage_button)

        self.refresh_activation_button = QPushButton("إعادة التحقق")
        self.refresh_activation_button.setObjectName("SubscriptionRefreshButton")
        self.refresh_activation_button.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        self.refresh_activation_button.clicked.connect(self._recheck_activation_state)
        badge_container.addWidget(self.refresh_activation_button)

        header_layout.addLayout(badge_container, 1)
        main_layout.addWidget(header_frame)

        # شريط أدوات البحث والفلترة
        self.search_filter_frame = QFrame()
        self.search_filter_frame.setObjectName("FilterBar")
        filter_layout = QHBoxLayout(self.search_filter_frame)
        filter_layout.setContentsMargins(10, 6, 10, 6)
        filter_layout.setSpacing(10)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("بحث بالاسم، الهاتف، أو الرقم...")
        self.search_input.textChanged.connect(self.apply_filter_and_search)
        self.search_input.setClearButtonEnabled(True)
        filter_layout.addWidget(self.search_input, 2)

        self.filter_by_combo = QComboBox()
        self.filter_by_combo.setObjectName("FilterCombo")
        self.filter_by_combo.addItem("تصفية حسب...", None)
        self.filter_by_combo.addItem("الحالة", "status")
        self.filter_by_combo.addItem("حجز موعد", "has_rdv")
        self.filter_by_combo.addItem("مستفيد", "have_allocation")
        self.filter_by_combo.addItem("PDF التزام", "pdf_honneur")
        self.filter_by_combo.addItem("PDF موعد", "pdf_rdv")
        self.filter_by_combo.currentIndexChanged.connect(self.on_filter_by_changed)
        filter_layout.addWidget(self.filter_by_combo)

        self.filter_value_combo = QComboBox()
        self.filter_value_combo.setObjectName("FilterValueCombo")
        self.filter_value_combo.setVisible(False)
        self.filter_value_combo.currentIndexChanged.connect(self.apply_filter_and_search)
        filter_layout.addWidget(self.filter_value_combo)

        self.clear_filter_button = QPushButton("مسح")
        self.clear_filter_button.setObjectName("ClearFilterButton")
        self.clear_filter_button.setIcon(self.style().standardIcon(QStyle.SP_DialogResetButton))
        self.clear_filter_button.clicked.connect(self.clear_filter_and_search)
        filter_layout.addWidget(self.clear_filter_button)

        main_layout.addWidget(self.search_filter_frame)

        # صف إحصائيات ولوحة عمليات منظمة
        info_row = QHBoxLayout()
        info_row.setSpacing(10)

        self.insight_frame = QFrame()
        self.insight_frame.setObjectName("InsightFrame")
        insight_layout = QHBoxLayout(self.insight_frame)
        insight_layout.setContentsMargins(10, 6, 10, 6)
        insight_layout.setSpacing(10)

        self.stat_total_card = self._create_stat_card("عدد الأعضاء", "0", "إجمالي المسجلين", accent="#38bdf8")
        self.stat_ready_card = self._create_stat_card("جاهز للحجز", "0", "أعضاء مؤهلون ولديهم تسجيل مسبق", accent="#34d399")
        self.stat_monitor_card = self._create_stat_card("المراقبة", "--", "الحالة الحالية", accent="#fbbf24")

        insight_layout.addWidget(self.stat_total_card)
        insight_layout.addWidget(self.stat_ready_card)
        insight_layout.addWidget(self.stat_monitor_card)

        self.operation_panel_frame = QFrame()
        self.operation_panel_frame.setObjectName("OperationPanel")
        op_layout = QVBoxLayout(self.operation_panel_frame)
        op_layout.setContentsMargins(12, 10, 12, 10)
        op_layout.setSpacing(6)

        self.operation_current_label = QLabel("لا توجد عملية جارية")
        self.operation_current_label.setObjectName("OperationCurrent")
        self.operation_next_label = QLabel("الخطوة التالية: --")
        self.operation_next_label.setObjectName("OperationNext")
        self.operation_timer_label = QLabel("الانتظار: --")
        self.operation_timer_label.setObjectName("OperationTimer")

        op_layout.addWidget(self.operation_current_label)
        op_layout.addWidget(self.operation_next_label)
        op_layout.addWidget(self.operation_timer_label)

        info_row.addWidget(self.insight_frame, 2)
        info_row.addWidget(self.operation_panel_frame, 1)
        main_layout.addLayout(info_row)

        # شريط الحالة المبسط أسفل النافذة
        self.statusBar = QStatusBar()
        self.statusBar.setObjectName("MainStatusBar")
        self.setStatusBar(self.statusBar)

        self.status_bar_label = QLabel("التطبيق جاهز.")
        self.status_bar_label.setObjectName("StatusDetail")
        self.countdown_label = QLabel("لا يوجد عد تنازلي")
        self.countdown_label.setObjectName("CountdownLabel")
        self.last_scan_label = QLabel("آخر تحديث: --")
        self.last_scan_label.setObjectName("LastEventLabel")

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(10, 2, 10, 2)
        status_layout.setSpacing(12)
        status_container = QWidget()
        status_container.setLayout(status_layout)

        status_layout.addWidget(self.status_bar_label, 2)
        status_layout.addWidget(self.countdown_label, 1)
        status_layout.addWidget(self.last_scan_label, 1)

        self.statusBar.addPermanentWidget(status_container, 1)

        # المنطقة الرئيسية: الجدول مع لوحة العضو والنشاط
        self.table = QTableWidget(self)
        self.table.setColumnCount(self.COL_DETAILS + 1)
        self.table.setHorizontalHeaderLabels([
            "أيقونة", "الاسم", "رقم التعريف", "رقم الوسيط",
            "الحساب البريدي", "رقم الهاتف", "الحالة", "تاريخ الموعد", "آخر تحديث"
        ])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_table_context_menu)

        # إظهار الأعمدة الأساسية مع إبراز لأيقونات والهاتف لسهولة المتابعة
        self.table.setColumnHidden(self.COL_ICON, False)
        self.table.setColumnHidden(self.COL_NIN, False)
        self.table.setColumnHidden(self.COL_WASSIT, True)
        self.table.setColumnHidden(self.COL_CCP, True)
        self.table.setColumnHidden(self.COL_PHONE_NUMBER, False)

        header.setSectionResizeMode(self.COL_FULL_NAME_AR, QHeaderView.Stretch)
        header.setSectionResizeMode(self.COL_STATUS, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_RDV_DATE, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_DETAILS, QHeaderView.Stretch)

        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.verticalHeader().setDefaultSectionSize(30)
        self.table.itemDoubleClicked.connect(self.edit_member_details)

        self.member_overview_frame = QFrame()
        self.member_overview_frame.setObjectName("MemberOverview")
        overview_layout = QVBoxLayout(self.member_overview_frame)
        overview_layout.setContentsMargins(12, 10, 12, 10)
        overview_layout.setSpacing(8)

        self.member_overview_title = QLabel("نظرة سريعة")
        self.member_overview_title.setObjectName("MemberOverviewTitle")
        overview_layout.addWidget(self.member_overview_title)

        self.member_overview_state = QLabel("الحالة: --")
        self.member_overview_state.setObjectName("MemberOverviewState")
        overview_layout.addWidget(self.member_overview_state)

        self.member_overview_next = QLabel("الخطوة التالية: --")
        self.member_overview_next.setObjectName("MemberOverviewNext")
        overview_layout.addWidget(self.member_overview_next)

        self.member_overview_wait = QLabel("الانتظار/التحميل: --")
        self.member_overview_wait.setObjectName("MemberOverviewWait")
        overview_layout.addWidget(self.member_overview_wait)

        self.member_overview_last = QLabel("آخر تحديث: --")
        self.member_overview_last.setObjectName("MemberOverviewLast")
        overview_layout.addWidget(self.member_overview_last)

        self.member_overview_hint = QLabel("سيتم تحديث البيانات تلقائيًا عند اختيار عضو أو أثناء المعالجة.")
        self.member_overview_hint.setWordWrap(True)
        self.member_overview_hint.setObjectName("MemberOverviewHint")
        overview_layout.addWidget(self.member_overview_hint)

        overview_layout.addStretch()

        activity_frame = QFrame()
        activity_frame.setObjectName("ActivityPanel")
        activity_layout = QVBoxLayout(activity_frame)
        activity_layout.setContentsMargins(10, 8, 10, 8)
        activity_layout.setSpacing(6)
        activity_title = QLabel("سجل النشاط")
        activity_title.setObjectName("ActivityTitle")
        activity_layout.addWidget(activity_title)

        self.activity_list = QListWidget()
        self.activity_list.setObjectName("ActivityList")
        activity_layout.addWidget(self.activity_list)

        side_stack = QVBoxLayout()
        side_stack.setSpacing(10)
        side_stack.addWidget(self.member_overview_frame)
        side_stack.addWidget(activity_frame)

        content_frame = QFrame()
        content_frame.setObjectName("ContentFrame")
        content_layout = QHBoxLayout(content_frame)
        content_layout.setContentsMargins(10, 8, 10, 8)
        content_layout.setSpacing(10)
        content_layout.addWidget(self.table, 3)
        content_layout.addLayout(side_stack, 1)

        main_layout.addWidget(content_frame)

        bottom_controls_layout = QHBoxLayout()
        bottom_controls_layout.setSpacing(10)
        self.add_member_button = QPushButton("إضافة عضو", self)
        self.add_member_button.setObjectName("add_member_button")
        self.add_member_button.setIcon(self.style().standardIcon(QStyle.SP_FileDialogNewFolder))
        self.add_member_button.clicked.connect(self.add_member)
        bottom_controls_layout.addWidget(self.add_member_button)

        self.remove_member_button = QPushButton("حذف المحدد", self)
        self.remove_member_button.setObjectName("remove_member_button")
        self.remove_member_button.setIcon(self.style().standardIcon(QStyle.SP_TrashIcon))
        self.remove_member_button.clicked.connect(self.remove_member)
        bottom_controls_layout.addWidget(self.remove_member_button)

        bottom_controls_layout.addStretch()
        self.start_button = QPushButton("بدء المراقبة", self)
        self.start_button.setObjectName("start_button")
        self.start_button.setIcon(self.style().standardIcon(QStyle.SP_MediaPlay))
        self.start_button.clicked.connect(self.start_monitoring)
        bottom_controls_layout.addWidget(self.start_button)

        self.stop_button = QPushButton("إيقاف المراقبة", self)
        self.stop_button.setObjectName("stop_button")
        self.stop_button.setIcon(self.style().standardIcon(QStyle.SP_MediaStop))
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_monitoring)
        bottom_controls_layout.addWidget(self.stop_button)
        main_layout.addLayout(bottom_controls_layout)

        if not self.activation_successful or \
           (self.current_subscription_data and self.current_subscription_data.get("status", "").upper() != "ACTIVE"):
            self._disable_app_functions()
        else:
            self._enable_app_functions()

        self.update_status_bar_message("التطبيق جاهز.", is_general_message=True)
        self._update_insight_metrics()

    def toggle_search_filter_bar(self, checked):
        if not self.search_filter_frame:
            return
        self.search_filter_frame.setVisible(checked)
        if self.toggle_search_filter_action:
            self.toggle_search_filter_action.setChecked(checked)

    def _create_stat_card(self, title, value, subtitle, accent="#4dabf7"):
        card = QFrame(self)
        card.setObjectName("StatCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("StatCardTitle")
        value_label = QLabel(value)
        value_label.setObjectName("StatCardValue")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("StatCardSubtitle")
        subtitle_label.setWordWrap(True)

        layout.addWidget(title_label)
        layout.addWidget(value_label)
        layout.addWidget(subtitle_label)

        card.value_label = value_label
        card.setStyleSheet(
            f"""
            QFrame#StatCard {{ background: #101827; border: 1px solid #1f2a36; border-radius: 12px; }}
            QLabel#StatCardTitle {{ color: #cfd8e3; font-weight: 600; }}
            QLabel#StatCardValue {{ color: {accent}; font-weight: 800; font-size: 18px; }}
            QLabel#StatCardSubtitle {{ color: #8ca0b8; }}
            """
        )
        return card

    def _append_activity_entry(self, message, level="info"):
        """إضافة رسالة إلى لوحة النشاط مع تمييز اللون بحسب مستوى الرسالة."""
        if not getattr(self, "activity_list", None):
            return
        max_items = 12
        prefix = {"info": "ℹ️", "warn": "⚠️", "error": "❌"}.get(level, "ℹ️")
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        item = QListWidgetItem(f"{prefix} {timestamp} — {message}")
        if level == "error":
            item.setForeground(QColor("#ff6b6b"))
        elif level == "warn":
            item.setForeground(QColor("#f2c94c"))
        else:
            item.setForeground(QColor("#a5d8ff"))
        self.activity_feed.append(item)
        if len(self.activity_feed) > max_items:
            self.activity_feed.pop(0)
        self.activity_list.clear()
        for entry in self.activity_feed:
            self.activity_list.addItem(entry.clone())
        self.activity_list.scrollToBottom()

    def on_filter_by_changed(self, index):
        if not self.filter_by_combo or not self.filter_value_combo:
            return
        filter_key = self.filter_by_combo.itemData(index)
        self.filter_value_combo.clear()
        self.filter_value_combo.setVisible(False)

        if filter_key == "status":
            statuses = sorted(list(set(m.status for m in self.members_list)))
            self.filter_value_combo.addItem("اختر الحالة...", None)
            for status in statuses:
                self.filter_value_combo.addItem(status, status)
            self.filter_value_combo.setVisible(True)
        elif filter_key in ["has_rdv", "have_allocation", "pdf_honneur", "pdf_rdv"]:
            self.filter_value_combo.addItem("اختر القيمة...", None)
            self.filter_value_combo.addItem("نعم", True)
            self.filter_value_combo.addItem("لا", False)
            self.filter_value_combo.setVisible(True)
        self.apply_filter_and_search() 

    def clear_filter_and_search(self):
        if not self.search_input or not self.filter_by_combo:
            return
        self.search_input.clear()
        self.filter_by_combo.setCurrentIndex(0)
        self._show_toast("تم مسح الفلتر بنجاح.", type="info", title="فلتر")
        self.update_status_bar_message("تم مسح الفلتر.", is_general_message=True)

    def _update_insight_metrics(self):
        """تحديث البطاقات الإحصائية لتعكس حالة الأعضاء والمتابعة."""
        if not self.stat_total_card or not self.stat_ready_card or not self.stat_monitor_card:
            return
        total_members = len(self.members_list)
        ready_for_rdv = 0
        for member in self.members_list:
            if member.have_allocation:
                continue
            if member.already_has_rdv or member.rdv_date:
                continue
            if member.has_actual_pre_inscription or member.pre_inscription_id:
                ready_for_rdv += 1

        monitor_state = "نشط" if self.monitoring_thread.isRunning() else "موقوف"
        monitor_detail = "المراقبة الدورية تعمل" if monitor_state == "نشط" else "المراقبة متوقفة"

        self.stat_total_card.value_label.setText(str(total_members))
        self.stat_ready_card.value_label.setText(str(ready_for_rdv))
        self.stat_monitor_card.value_label.setText(monitor_state)
        subtitle_label = self.stat_monitor_card.findChild(QLabel, "StatCardSubtitle")
        if subtitle_label:
            subtitle_label.setText(monitor_detail)


    def apply_filter_and_search(self):
        if not self.search_input or not self.filter_by_combo or not self.filter_value_combo:
            self.is_filter_active = False
            self.filtered_members_list = list(self.members_list)
            self.update_table()
            return
        search_term = self.search_input.text().lower().strip()
        filter_key = self.filter_by_combo.itemData(self.filter_by_combo.currentIndex())
        filter_value_data = self.filter_value_combo.itemData(self.filter_value_combo.currentIndex())

        current_list_to_filter = list(self.members_list) 

        self.is_filter_active = bool(search_term or (filter_key and filter_value_data is not None))

        if not self.is_filter_active:
            self.filtered_members_list = list(self.members_list) 
            self.update_table()
            if hasattr(self, '_last_filter_applied') and self._last_filter_applied: 
                self.update_status_bar_message("تم عرض جميع الأعضاء.", is_general_message=True)
            self._last_filter_applied = False 
            return

        temp_filtered_list = []

        for member in current_list_to_filter:
            match_search = True
            if search_term:
                match_search = (search_term in (member.nin or "").lower() or
                                search_term in (member.wassit_no or "").lower() or
                                search_term in (member.get_full_name_ar() or "").lower() or
                                search_term in (member.nom_fr or "").lower() or
                                search_term in (member.prenom_fr or "").lower() or
                                search_term in (member.phone_number or "").lower() or
                                search_term in (member.ccp or "").lower())

            match_filter = True
            if filter_key and filter_value_data is not None: 
                if filter_key == "status":
                    match_filter = member.status == filter_value_data
                elif filter_key == "has_rdv":
                    match_filter = member.already_has_rdv == filter_value_data
                elif filter_key == "have_allocation":
                    match_filter = member.have_allocation == filter_value_data
                elif filter_key == "pdf_honneur":
                    match_filter = bool(member.pdf_honneur_path) == filter_value_data
                elif filter_key == "pdf_rdv":
                    match_filter = bool(member.pdf_rdv_path) == filter_value_data

            if match_search and match_filter:
                temp_filtered_list.append(member)

        self.filtered_members_list = temp_filtered_list
        self.update_table() 
        self.update_status_bar_message(f"تم تطبيق الفلتر. عدد النتائج: {len(self.filtered_members_list)}", is_general_message=True)
        self._last_filter_applied = True 

    def show_table_context_menu(self, position):
        selected_items = self.table.selectedItems()
        item_at_pos = self.table.itemAt(position) 

        if not item_at_pos and not selected_items: 
            return

        row_index_in_table = -1
        if item_at_pos: 
            row_index_in_table = item_at_pos.row()
        elif selected_items: 
            row_index_in_table = selected_items[0].row()

        if row_index_in_table < 0: return 

        current_list_for_context = self.filtered_members_list if self.is_filter_active else self.members_list
        if row_index_in_table >= len(current_list_for_context): return 

        member = current_list_for_context[row_index_in_table]
        try:
            original_member_index = self.members_list.index(member) 
        except ValueError:
            logger.error(f"العضو {member.nin} من القائمة المفلترة غير موجود في القائمة الرئيسية.")
            self._show_toast(f"خطأ: العضو {self._get_member_display_name_with_index(member, -1)} غير موجود بشكل صحيح.", type="error", title="خطأ داخلي")
            return

        menu = QMenu(self)
        member_display_name_with_index = self._get_member_display_name_with_index(member, original_member_index)

        view_action = QAction(QIcon.fromTheme("document-properties"), f"عرض معلومات {member_display_name_with_index}", self)
        view_action.triggered.connect(lambda: self.view_member_info(original_member_index))
        menu.addAction(view_action)

        check_now_action = QAction(QIcon.fromTheme("system-search"), f"فحص الآن لـ {member_display_name_with_index}", self)
        check_now_action.triggered.connect(lambda: self.check_member_now(original_member_index))
        menu.addAction(check_now_action)

        can_download_any_pdf = bool(member.pre_inscription_id) and \
                               member.status in ["لديه موعد مسبق", "تم الحجز", "مكتمل", "فشل تحميل PDF", "مستفيد حاليًا من المنحة"]

        download_all_action = QAction(QIcon.fromTheme("document-save-all", QIcon.fromTheme("document-save")), "تحميل جميع الشهادات", self)
        download_all_action.setEnabled(can_download_any_pdf)
        download_all_action.triggered.connect(lambda: self.download_all_member_pdfs(original_member_index))
        menu.addAction(download_all_action)

        menu.addSeparator()

        edit_action = QAction(QIcon.fromTheme("document-edit"), f"تعديل بيانات {member_display_name_with_index}", self)
        edit_action.triggered.connect(lambda: self.edit_member_details(self.table.item(row_index_in_table, 0)))
        menu.addAction(edit_action)

        delete_action = QAction(QIcon.fromTheme("edit-delete"), f"حذف {member_display_name_with_index}", self)
        delete_action.triggered.connect(lambda: self.remove_specific_member(original_member_index))
        menu.addAction(delete_action)

        menu.exec_(self.table.viewport().mapToGlobal(position))

    def view_member_info(self, original_member_index):
        if 0 <= original_member_index < len(self.members_list):
            member = self.members_list[original_member_index]
            member_display_name = self._get_member_display_name_with_index(member, original_member_index)
            logger.info(f"طلب عرض معلومات العضو: {member_display_name}")
            self.update_status_bar_message(f"عرض معلومات العضو: {member_display_name}", is_general_message=True)
            dialog = ViewMemberDialog(member, self)
            dialog.exec_()
        else:
            logger.warning(f"view_member_info: فهرس خاطئ {original_member_index}")
            self._show_toast("خطأ في عرض معلومات العضو (فهرس غير صالح).", type="error", title="خطأ")

    def check_member_now(self, original_member_index):
        if not self.activation_successful or (self.current_subscription_data and self.current_subscription_data.get("status","").upper() != "ACTIVE"):
            self._show_toast("لا يمكن إجراء الفحص. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="فحص فوري")
            return

        if 0 <= original_member_index < len(self.members_list):
            member = self.members_list[original_member_index]
            member_display_name = self._get_member_display_name_with_index(member, original_member_index)

            if member.is_processing: 
                 self._show_toast(f"العضو '{member_display_name}' قيد المعالجة حاليًا. يرجى الانتظار.", type="warning", title="فحص فوري")
                 return

            if self.single_check_thread and self.single_check_thread.isRunning():
                self._show_toast("فحص آخر قيد التنفيذ بالفعل. يرجى الانتظار.", type="warning", title="فحص فوري")
                return

            logger.info(f"طلب فحص فوري للعضو: {member_display_name}")
            self.update_status_bar_message(
                f"بدء الفحص الفوري للعضو: {member_display_name}...",
                is_general_message=False,
                busy=True,
                hint_text="جاري تنفيذ فحص فوري وتحديث الحالة"
            )
            self._show_toast(f"بدء الفحص الفوري للعضو: {member_display_name}", type="info", title="فحص فوري")

            self.single_check_thread = SingleMemberCheckThread(member, original_member_index, self.api_client, self.settings.copy())
            self.single_check_thread.update_member_gui_signal.connect(self.update_member_gui_in_table)
            self.single_check_thread.new_data_fetched_signal.connect(self.update_member_name_in_table)
            self.single_check_thread.member_processing_started_signal.connect(lambda idx: self.handle_member_processing_signal(idx, True))
            self.single_check_thread.member_processing_finished_signal.connect(lambda idx: self.handle_member_processing_signal(idx, False))
            self.single_check_thread.global_log_signal.connect(self.update_status_bar_message)
            self.single_check_thread.start()
        else:
            logger.warning(f"check_member_now: فهرس خاطئ {original_member_index}")
            self._show_toast("خطأ في بدء الفحص الفوري (فهرس غير صالح).", type="error", title="خطأ")

    def _select_row_by_member_index(self, original_member_index, ensure_visible=False):
        if not (0 <= original_member_index < len(self.members_list)):
            return -1

        member_obj = self.members_list[original_member_index]
        current_list_for_view = self.filtered_members_list if self.is_filter_active else self.members_list
        if member_obj not in current_list_for_view:
            return -1

        row_in_table = current_list_for_view.index(member_obj)
        self.table.selectRow(row_in_table)
        if ensure_visible:
            target_item = self.table.item(row_in_table, self.COL_FULL_NAME_AR) or self.table.item(row_in_table, self.COL_ICON)
            if target_item:
                self.table.scrollToItem(target_item, QAbstractItemView.PositionAtCenter)
        return row_in_table

    def _resolve_member_index_for_check(self):
        selected_index = None
        selection_model = self.table.selectionModel()
        if selection_model:
            selected_rows = selection_model.selectedRows()
            if selected_rows:
                row = selected_rows[0].row()
                current_list_for_view = self.filtered_members_list if self.is_filter_active else self.members_list
                if 0 <= row < len(current_list_for_view):
                    member_from_view = current_list_for_view[row]
                    try:
                        selected_index = self.members_list.index(member_from_view)
                    except ValueError:
                        logger.error("فشل العثور على العضو المحدد في القائمة الرئيسية أثناء تجهيز الفحص الفوري عبر الزر.")

        if selected_index is not None:
            return selected_index

        if self.last_added_member_index is not None and 0 <= self.last_added_member_index < len(self.members_list):
            return self.last_added_member_index

        return None

    def _refresh_member_overview_from_selection(self):
        if not self.member_overview_frame:
            return
        selection_model = self.table.selectionModel()
        if not selection_model or not selection_model.selectedRows():
            self.member_overview_selected_index = None
            self._set_member_overview_content(None)
            return

        row = selection_model.selectedRows()[0].row()
        current_list_for_view = self.filtered_members_list if self.is_filter_active else self.members_list
        if not (0 <= row < len(current_list_for_view)):
            self.member_overview_selected_index = None
            self._set_member_overview_content(None)
            return

        member = current_list_for_view[row]
        try:
            self.member_overview_selected_index = self.members_list.index(member)
        except ValueError:
            self.member_overview_selected_index = None
            self._set_member_overview_content(None)
            return

        self._set_member_overview_content(member)

    def _badge_for_status(self, status_text):
        if not status_text:
            return "ℹ️"
        if any(keyword in status_text for keyword in ["خطأ", "فشل"]):
            return "⚠️"
        if any(keyword in status_text for keyword in ["جاري", "انتظار", "معالجة", "تحميل"]):
            return "🔄"
        if any(keyword in status_text for keyword in ["تم الحجز", "مكتمل", "موعد", "مستفيد"]):
            return "✅"
        if "جديد" in status_text:
            return "🆕"
        return "ℹ️"

    def _describe_member_next_step(self, member):
        if member.have_allocation:
            return "المستخدم مستفيد حاليًا، لا حاجة للحجز."
        if member.already_has_rdv or member.rdv_date:
            return "موعد مؤكد، تأكد من تنزيل وطباعة الشهادات."
        if member.has_actual_pre_inscription:
            return "يتم البحث عن موعد وحجزه فور توفره."
        if member.pre_inscription_id:
            return "بيانات التسجيل المسبق جاهزة، بانتظار فتح مواعيد."
        return "يحتاج للتأكد من بيانات التسجيل المسبق ثم بدء البحث عن موعد."

    def _get_member_wait_text(self, member):
        if member.is_processing:
            return "المعالجة جارية لهذا العضو الآن."
        return self.latest_countdown_text

    def _set_member_overview_content(self, member, status_override=None, next_text=None, wait_text=None, last_msg=None, hint_text=None):
        if not getattr(self, 'member_overview_title', None):
            return

        if member is None:
            self.member_overview_title.setText("👤 لا يوجد عضو محدد")
            self.member_overview_state.setText("الحالة: --")
            self.member_overview_next.setText("الخطوة القادمة: --")
            self.member_overview_wait.setText("الانتظار/التحميل: --")
            self.member_overview_last.setText("آخر رسالة: --")
            self.member_overview_hint.setText("اختر عضوًا لمعرفة تفاصيل حالته وخطوته القادمة.")
            return

        try:
            member_index_display = self.members_list.index(member) + 1
        except ValueError:
            member_index_display = "?"

        display_name = member.get_full_name_ar() or member.nin
        self.member_overview_title.setText(f"👤 {display_name} (رقم {member_index_display})")

        status_text = status_override or member.status or "غير محدد"
        badge = self._badge_for_status(status_text)
        self.member_overview_state.setText(f"الحالة: {badge} {status_text}")

        computed_next = next_text or self._describe_member_next_step(member)
        self.member_overview_next.setText(f"الخطوة القادمة: {computed_next}")

        wait_display = wait_text or self._get_member_wait_text(member)
        self.member_overview_wait.setText(f"الانتظار/التحميل: {wait_display}")

        last_detail = last_msg or member.full_last_activity_detail or member.last_activity_detail or "لا توجد رسائل بعد."
        self.member_overview_last.setText(f"آخر رسالة: {last_detail}")

        if hint_text:
            self.member_overview_hint.setText(hint_text)
        else:
            hint_parts = []
            if member.already_has_rdv or member.rdv_date:
                hint_parts.append("يُمكن تحميل شهادات الموعد والتعهد عند الحاجة.")
            elif member.has_actual_pre_inscription:
                hint_parts.append("سيتم الحجز تلقائيًا عند ظهور مواعيد.")
            else:
                hint_parts.append("تأكد من اكتمال بيانات التسجيل المسبق ورفع الدقة للاتصال.")
            if member.have_allocation:
                hint_parts.append("الحالة مكتملة كمستفيد حالي، لا حاجة لإعادة الحجز.")
            self.member_overview_hint.setText(" • ".join(hint_parts))

    def _ensure_table_selection_after_refresh(self):
        if self.table.rowCount() == 0:
            self.member_overview_selected_index = None
            self._set_member_overview_content(None)
            return

        selection_model = self.table.selectionModel()
        if selection_model and selection_model.hasSelection():
            self._refresh_member_overview_from_selection()
            return

        # حاول اختيار آخر عضو مضاف أو أول صف
        target_index = None
        if self.last_added_member_index is not None and 0 <= self.last_added_member_index < len(self.members_list):
            target_index = self.last_added_member_index
        else:
            target_index = 0

        row_selected = self._select_row_by_member_index(target_index, ensure_visible=False)
        if row_selected >= 0:
            self.member_overview_selected_index = target_index
            self._refresh_member_overview_from_selection()

    def trigger_manual_check_for_selected(self):
        target_index = self._resolve_member_index_for_check()
        if target_index is None:
            self._show_toast("يرجى اختيار عضو لفحصه أو إضافة عضو جديد أولًا.", type="warning", title="فحص فوري")
            self.update_status_bar_message(
                "لم يتم العثور على عضو محدد لإجراء الفحص.",
                is_general_message=True,
                hint_text="حدد عضو من الجدول ثم اضغط فحص",
                busy=False,
            )
            return

        self._select_row_by_member_index(target_index, ensure_visible=True)
        self.check_member_now(target_index)

    def download_all_member_pdfs(self, original_member_index):
        if not self.activation_successful or (self.current_subscription_data and self.current_subscription_data.get("status","").upper() != "ACTIVE"):
            self._show_toast("لا يمكن تحميل الشهادات. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="تحميل الشهادات")
            return

        if not (0 <= original_member_index < len(self.members_list)):
            self._show_toast("فهرس عضو غير صالح لتحميل الشهادات.", type="error", title="خطأ")
            return

        member = self.members_list[original_member_index]
        member_display_name = self._get_member_display_name_with_index(member, original_member_index)

        if member.is_processing and self.active_download_all_pdfs_threads.get(original_member_index): 
            self._show_toast(f"تحميل شهادات العضو '{member_display_name}' قيد التنفيذ بالفعل.", type="warning", title="تحميل الشهادات")
            return

        if self.active_download_all_pdfs_threads.get(original_member_index) and self.active_download_all_pdfs_threads[original_member_index].isRunning():
            self._show_toast(f"تحميل شهادات العضو '{member_display_name}' قيد التنفيذ بالفعل.", type="warning", title="تحميل الشهادات")
            return

        if not member.pre_inscription_id:
            self._show_toast(f"ID التسجيل المسبق مفقود للعضو {member_display_name}. لا يمكن تحميل الشهادات.", type="error", title="تحميل الشهادات")
            return

        logger.info(f"طلب تحميل جميع الشهادات للعضو: {member_display_name}")
        self.update_status_bar_message(
            f"بدء تحميل جميع الشهادات لـ {member_display_name}...",
            is_general_message=False,
            busy=True,
            hint_text="جاري تحميل الشهادات وتجهيز الملفات"
        )
        self._show_toast(f"بدء تحميل جميع الشهادات لـ {member_display_name}", type="info", title="تحميل الشهادات")

        all_pdfs_thread = DownloadAllPdfsThread(member, original_member_index, self.api_client)
        all_pdfs_thread.all_pdfs_download_finished_signal.connect(self.handle_all_pdfs_download_finished)
        all_pdfs_thread.individual_pdf_status_signal.connect(self.handle_individual_pdf_status)
        all_pdfs_thread.member_processing_started_signal.connect(lambda idx: self.handle_member_processing_signal(idx, True))
        all_pdfs_thread.member_processing_finished_signal.connect(lambda idx, ri=original_member_index: self._clear_active_download_thread(ri))
        all_pdfs_thread.global_log_signal.connect(self.update_status_bar_message)

        self.active_download_all_pdfs_threads[original_member_index] = all_pdfs_thread
        all_pdfs_thread.start()

    def _clear_active_download_thread(self, original_member_index):
        if original_member_index in self.active_download_all_pdfs_threads:
            del self.active_download_all_pdfs_threads[original_member_index]
        self.handle_member_processing_signal(original_member_index, False) 
        if 0 <= original_member_index < len(self.members_list):
            member = self.members_list[original_member_index]
            member_display_name = self._get_member_display_name_with_index(member, original_member_index)
            self.update_status_bar_message(
                f"انتهت معالجة تحميل الشهادات للعضو: {member_display_name}",
                is_general_message=True,
                level="success",
                busy=False,
                hint_text="اكتمل تحميل الشهادات"
            )


    def handle_individual_pdf_status(self, original_member_index, pdf_type, file_path_or_status_msg_from_thread, success, error_msg_for_toast_from_thread):
        if not (0 <= original_member_index < len(self.members_list)):
            return
        member = self.members_list[original_member_index]

        pdf_type_ar = "التعهد" if pdf_type == "HonneurEngagementReport" else "الموعد"
        member_name_display = self._get_member_display_name_with_index(member, original_member_index)

        if success:
            file_path = file_path_or_status_msg_from_thread 
            if pdf_type == "HonneurEngagementReport":
                member.pdf_honneur_path = file_path
            elif pdf_type == "RdvReport":
                member.pdf_rdv_path = file_path

            activity_detail = f"تم تحميل شهادة {pdf_type_ar} بنجاح إلى {os.path.basename(file_path)}."
            member.set_activity_detail(file_path_or_status_msg_from_thread if os.path.exists(file_path_or_status_msg_from_thread) else activity_detail)
            toast_msg = f"{activity_detail}\nالمسار: {file_path}"
            self._show_toast(toast_msg, type="success", duration=5000, title=f"تحميل شهادة {pdf_type_ar}")
            self.update_status_bar_message(
                f"تم تحميل شهادة {pdf_type_ar} للعضو {member_name_display}.",
                is_general_message=True,
                level="success",
                busy=False,
                hint_text="آخر عملية تحميل مكتملة"
            )
        else:
            activity_detail = file_path_or_status_msg_from_thread 
            member.set_activity_detail(activity_detail, is_error=True)
            toast_msg = f"فشل تحميل شهادة {pdf_type_ar}. السبب: {error_msg_for_toast_from_thread or activity_detail}"
            self._show_toast(toast_msg, type="error", duration=6000, title=f"فشل تحميل شهادة {pdf_type_ar}")
            self.update_status_bar_message(
                f"فشل تحميل شهادة {pdf_type_ar} للعضو {member_name_display}.",
                is_general_message=True,
                level="error",
                busy=False,
                hint_text="يرجى إعادة المحاولة بعد التحقق من الاتصال"
            )

        self.update_member_gui_in_table(original_member_index, member.status, member.last_activity_detail, get_icon_name_for_status(member.status))
        self.save_members_data()


    def handle_all_pdfs_download_finished(self, original_member_index, honneur_path, rdv_path, overall_status_msg, all_success, first_error_msg):
        if not (0 <= original_member_index < len(self.members_list)):
            logger.warning(f"handle_all_pdfs_download_finished: فهرس خاطئ {original_member_index}")
            return

        member = self.members_list[original_member_index]
        member_name_display = self._get_member_display_name_with_index(member, original_member_index)

        if honneur_path: member.pdf_honneur_path = honneur_path
        if rdv_path: member.pdf_rdv_path = rdv_path

        if all_success:
            if member.status != "مستفيد حاليًا من المنحة": 
                if (member.pdf_honneur_path and member.pdf_rdv_path) or \
                   (member.pdf_honneur_path and not (member.already_has_rdv or member.rdv_id or member.status == "تم الحجز")):
                    member.status = "مكتمل"
                elif member.pdf_honneur_path or member.pdf_rdv_path : 
                     member.status = "تم الحجز" 

            member.set_activity_detail(overall_status_msg)
            final_toast_msg = f"{overall_status_msg}"
            self._show_toast(final_toast_msg, type="success", duration=7000, title=f"تحميل شهادات {member_name_display}")
            self.update_status_bar_message(f"اكتمل تحميل شهادات العضو {member_name_display}.", is_general_message=True)

            folder_to_open = None
            if honneur_path: folder_to_open = os.path.dirname(honneur_path)
            elif rdv_path: folder_to_open = os.path.dirname(rdv_path)

            if not folder_to_open and member.pre_inscription_id: 
                documents_location = QStandardPaths.writableLocation(QStandardPaths.DocumentsLocation)
                base_app_dir_name = "ملفات_المنحة_البرنامج"
                member_name_for_folder_path = member.get_full_name_ar()
                if not member_name_for_folder_path or member_name_for_folder_path.isspace():
                    member_name_for_folder_path = member.nin 
                safe_folder_name_part = "".join(c for c in member_name_for_folder_path if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
                if not safe_folder_name_part: safe_folder_name_part = member.nin 
                folder_to_open = os.path.join(documents_location, base_app_dir_name, safe_folder_name_part)


            if folder_to_open and os.path.exists(folder_to_open):
                reply = QMessageBox.question(self, 'فتح المجلد', f"تم حفظ الملفات بنجاح في المجلد:\n{folder_to_open}\n\nهل تريد فتح هذا المجلد؟",
                                           QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
                if reply == QMessageBox.Yes:
                    try:
                        QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.realpath(folder_to_open)))
                    except Exception as e_open:
                        logger.error(f"فشل فتح مجلد الملفات: {e_open}")
                        self._show_toast(f"فشل فتح مجلد الملفات: {e_open}", type="warning", title="خطأ فتح مجلد")
        else:
            if "فشل تحميل PDF" not in member.status and member.status != "مستفيد حاليًا من المنحة": 
                member.status = "فشل تحميل PDF"

            final_detail_msg = overall_status_msg
            if first_error_msg and first_error_msg not in final_detail_msg: 
                final_detail_msg += f" (الخطأ الأول: {first_error_msg.split(':')[0]})" 
            member.set_activity_detail(final_detail_msg, is_error=True)
            final_toast_msg = f"فشل تحميل بعض الشهادات.\n{overall_status_msg}"
            self._show_toast(final_toast_msg, type="error", duration=7000, title=f"فشل تحميل شهادات {member_name_display}")
            self.update_status_bar_message(f"فشل تحميل بعض شهادات العضو {member_name_display}.", is_general_message=True)

        self.update_member_gui_in_table(original_member_index, member.status, member.last_activity_detail, get_icon_name_for_status(member.status))
        self.save_members_data()


    def remove_specific_member(self, original_member_index):
        if not (0 <= original_member_index < len(self.members_list)):
            self._show_toast("فهرس عضو غير صالح للحذف.", type="error", title="خطأ")
            return

        member_to_remove = self.members_list[original_member_index]
        member_display_name = self._get_member_display_name_with_index(member_to_remove, original_member_index)
        confirm_delete = QMessageBox.question(self, "تأكيد الحذف",
                                              f"هل أنت متأكد أنك تريد حذف العضو '{member_display_name}'؟",
                                              QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if confirm_delete == QMessageBox.No:
            return

        self.members_list.pop(original_member_index) 

        if self.is_filter_active: 
            self.apply_filter_and_search()
        else: 
            self.update_table()

        logger.info(f"تم حذف العضو: {member_display_name}")
        self.update_status_bar_message(f"تم حذف العضو: {member_display_name}", is_general_message=True)
        self._show_toast(f"تم حذف العضو: {member_display_name}", type="info", title="حذف عضو")
        self.save_members_data() 

        if self.monitoring_thread and self.monitoring_thread.current_member_index_to_process >= len(self.members_list):
            self.monitoring_thread.current_member_index_to_process = 0 


    def load_app_settings(self):
        loaded_settings = None
        primary_path = SETTINGS_FILE 
        backup_path = SETTINGS_FILE_BAK 

        if os.path.exists(primary_path):
            try:
                with open(primary_path, 'r', encoding='utf-8') as f:
                    loaded_settings = json.load(f)
                logger.info(f"تم تحميل الإعدادات من الملف الأساسي: {primary_path}")
            except json.JSONDecodeError:
                logger.error(f"خطأ في فك تشفير JSON للملف الأساسي {primary_path}. محاولة تحميل النسخة الاحتياطية.")
                loaded_settings = None 
            except Exception as e:
                logger.exception(f"خطأ غير متوقع عند تحميل الإعدادات من {primary_path}: {e}")
                loaded_settings = None

        if loaded_settings is None and os.path.exists(backup_path): 
            try:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    loaded_settings = json.load(f)
                logger.info(f"تم تحميل الإعدادات من الملف الاحتياطي: {backup_path}")
                try:
                    shutil.copy2(backup_path, primary_path)
                    logger.info(f"تم استعادة الملف الأساسي {primary_path} من النسخة الاحتياطية {backup_path}.")
                except Exception as e_copy:
                    logger.error(f"فشل في استعادة الملف الأساسي من النسخة الاحتياطية: {e_copy}")
            except json.JSONDecodeError:
                logger.error(f"خطأ في فك تشفير JSON للملف الاحتياطي {backup_path}.")
                loaded_settings = None
            except Exception as e:
                logger.exception(f"خطأ غير متوقع عند تحميل الإعدادات من {backup_path}: {e}")
                loaded_settings = None

        if loaded_settings is not None:
            self.settings = loaded_settings
            for key, default_value in DEFAULT_SETTINGS.items():
                if key not in self.settings:
                    self.settings[key] = default_value
                    logger.info(f"تمت إضافة المفتاح المفقود '{key}' إلى الإعدادات بالقيمة الافتراضية.")
            if any(key not in loaded_settings for key in DEFAULT_SETTINGS.keys()):
                self.save_app_settings() 
        else:
            logger.warning(f"لم يتم العثور على ملف الإعدادات ({primary_path}) أو الملف الاحتياطي ({backup_path})، أو كلاهما تالف. تم استخدام الإعدادات الافتراضية.")
            self.settings = DEFAULT_SETTINGS.copy()
            if not hasattr(self, 'toast_notifications'): 
                self.toast_notifications = []
            self._show_toast("ملف الإعدادات غير موجود أو تالف. تم استخدام الإعدادات الافتراضية.", type="warning", duration=5000, title="الإعدادات")
            self.save_app_settings() 


    def save_app_settings(self):
        primary_path = SETTINGS_FILE 
        tmp_path = SETTINGS_FILE_TMP 
        bak_path = SETTINGS_FILE_BAK 
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=4)
            if os.path.exists(primary_path):
                try:
                    shutil.copy2(primary_path, bak_path) 
                    logger.debug(f"تم إنشاء نسخة احتياطية من {primary_path} إلى {bak_path}")
                except Exception as e_bak:
                    logger.error(f"فشل في إنشاء نسخة احتياطية لملف الإعدادات {primary_path}: {e_bak}")
            os.replace(tmp_path, primary_path)
            logger.info(f"تم حفظ الإعدادات بنجاح في {primary_path}")
        except Exception as e:
            logger.exception(f"خطأ عند حفظ الإعدادات في {primary_path}: {e}")
            self._show_toast(f"فشل حفظ الإعدادات: {e}", type="error", title="خطأ حفظ")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception as e_del_tmp:
                    logger.error(f"فشل في حذف الملف المؤقت للإعدادات {tmp_path} بعد خطأ في الحفظ: {e_del_tmp}")

    def open_settings_dialog(self):
        dialog = SettingsDialog(self.settings.copy(), self) 
        if dialog.exec_() == SettingsDialog.Accepted:
            new_settings = dialog.get_settings()
            self.settings.update(new_settings) 
            self.save_app_settings() 
            self.apply_app_settings() 
            self._show_toast("تم حفظ الإعدادات بنجاح وتطبيقها.", type="success", title="الإعدادات")
            logger.info("تم تحديث إعدادات التطبيق.")
            self.update_status_bar_message("تم تحديث الإعدادات.", is_general_message=True)


    def apply_app_settings(self):
        self.api_client = AnemAPIClient(
            initial_backoff_general=self.settings.get(SETTING_BACKOFF_GENERAL, DEFAULT_SETTINGS[SETTING_BACKOFF_GENERAL]),
            initial_backoff_429=self.settings.get(SETTING_BACKOFF_429, DEFAULT_SETTINGS[SETTING_BACKOFF_429]),
            request_timeout=self.settings.get(SETTING_REQUEST_TIMEOUT, DEFAULT_SETTINGS[SETTING_REQUEST_TIMEOUT])
        )

        if self.monitoring_thread.isRunning():
            self.monitoring_thread.update_thread_settings(self.settings.copy())
            monitoring_interval_minutes = self.settings.get(SETTING_MONITORING_INTERVAL, DEFAULT_SETTINGS[SETTING_MONITORING_INTERVAL])
            self.update_status_bar_message(f"المراقبة جارية (الدورة كل {monitoring_interval_minutes} دقيقة)...", is_general_message=False)
        else: 
            self.monitoring_thread.settings = self.settings.copy()
            self.monitoring_thread._apply_settings() 


    def _get_member_display_name_with_index(self, member, original_index):
        name_part = member.get_full_name_ar()
        if not name_part or name_part.isspace(): 
            name_part = member.nin 
        return f"{name_part} (رقم {original_index + 1})"


    def _show_toast(self, message, title=None, type="info", duration=4000, member_obj=None, original_idx_if_member=None, message_id=None):
        max_toast_len = 150 
        display_message = message
        display_title = title

        if member_obj is not None and original_idx_if_member is not None:
            member_display_intro = self._get_member_display_name_with_index(member_obj, original_idx_if_member)
            if not display_title: 
                display_title = member_display_intro
            else: 
                display_title = f"{member_display_intro}: {title}"

        if len(message) > max_toast_len: 
            display_message = message[:max_toast_len] + "..."
        
        if display_title and len(display_title) > 70:
            display_title = display_title[:67] + "..."

        # النقطة 4: عدم تكرار إشعار Toast لنفس الرسالة (إذا كان message_id موجودًا)
        if message_id and message_id in self.toast_shown_for_message_ids:
            logger.debug(f"Toast for message_id '{message_id}' already shown. Skipping.")
            return

        toast = ToastNotification(self) 
        self.toast_notifications.append(toast) 
        toast.showMessage(display_message, title=display_title, type=type, duration=duration, parent_window=self, message_id=message_id) 
        
        if message_id:
            self.toast_shown_for_message_ids.add(message_id)
            # إزالة المعرف بعد مدة أطول قليلاً من مدة الإشعار لتمكين ظهوره مرة أخرى لاحقًا إذا لزم الأمر
            QTimer.singleShot(duration + 5000, lambda: self.toast_shown_for_message_ids.discard(message_id))


    def _remove_toast_reference(self, toast_instance): 
        if toast_instance in self.toast_notifications:
            try:
                self.toast_notifications.remove(toast_instance)
            except ValueError:
                pass 


    def load_stylesheet(self):
        try:
            resolved_stylesheet_file = resource_path(STYLESHEET_FILE) 
            with open(resolved_stylesheet_file, "r", encoding="utf-8") as f:
                style = f.read()
                self.setStyleSheet(style)
        except FileNotFoundError:
            logger.warning(f"ملف التنسيق {STYLESHEET_FILE} (المسار المحلول: {resolved_stylesheet_file if 'resolved_stylesheet_file' in locals() else 'غير محدد'}) غير موجود. سيتم استخدام التنسيق الافتراضي.")
            self._show_toast(f"ملف التنسيق {STYLESHEET_FILE} غير موجود.", type="warning", title="خطأ تحميل")
        except Exception as e:
            logger.error(f"خطأ في تحميل ملف التنسيق {STYLESHEET_FILE}: {e}")
            self._show_toast(f"خطأ في تحميل ملف التنسيق: {e}", type="error", title="خطأ تحميل")

    def update_datetime(self):
        now = QDateTime.currentDateTime()
        arabic_locale = QLocale(QLocale.Arabic, QLocale.Algeria)
        if self.datetime_label:
            self.datetime_label.setText(arabic_locale.toString(now, "dddd, dd MMMM yy - hh:mm:ss AP"))


    def toggle_column_visibility(self, checked):
        if not self.table:
            return
        self.table.setColumnHidden(self.COL_NIN, not checked)
        self.table.setColumnHidden(self.COL_WASSIT, not checked)
        self.table.setColumnHidden(self.COL_CCP, not checked)
        self.table.setColumnHidden(self.COL_PHONE_NUMBER, not checked)
        if self.toggle_details_action:
            self.toggle_details_action.setText("إخفاء التفاصيل" if checked else "إظهار التفاصيل")
        self.update_status_bar_message(f"تم {'إظهار' if checked else 'إخفاء'} الأعمدة التفصيلية.", is_general_message=True)


    def update_active_row_spinner_display(self):
        if self.active_spinner_row_in_view == -1 or not (0 <= self.active_spinner_row_in_view < self.table.rowCount()):
            return 

        current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list

        if self.active_spinner_row_in_view >= len(current_list_displayed):
            self.row_spinner_timer.stop()
            self.active_spinner_row_in_view = -1
            return

        member = current_list_displayed[self.active_spinner_row_in_view]
        try:
            original_member_index = self.members_list.index(member) 
        except ValueError: 
            self.row_spinner_timer.stop()
            self.active_spinner_row_in_view = -1
            return

        if not member.is_processing:
            is_still_pdf_downloading = self.active_download_all_pdfs_threads.get(original_member_index) and \
                                       self.active_download_all_pdfs_threads[original_member_index].isRunning()
            is_still_single_checking = self.single_check_thread and \
                                       self.single_check_thread.isRunning() and \
                                       self.single_check_thread.index == original_member_index

            if not is_still_pdf_downloading and not is_still_single_checking: 
                self.row_spinner_timer.stop()
                self.active_spinner_row_in_view = -1 
            self.update_member_gui_in_table(original_member_index, member.status, member.last_activity_detail, get_icon_name_for_status(member.status))
            return

        self.spinner_char_idx = (self.spinner_char_idx + 1) % len(self.spinner_chars)
        char = self.spinner_chars[self.spinner_char_idx]

        icon_item_in_table = self.table.item(self.active_spinner_row_in_view, self.COL_ICON)
        if icon_item_in_table:
            icon_item_in_table.setText(char) 
            icon_item_in_table.setIcon(QIcon()) 


    def handle_member_processing_signal(self, original_member_index, is_processing_now):
        if not (0 <= original_member_index < len(self.members_list)):
            logger.warning(f"HMP Signal: فهرس العضو الأصلي غير صالح {original_member_index}")
            return

        member = self.members_list[original_member_index]
        member.is_processing = is_processing_now 

        row_in_table_to_update = -1
        current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list
        try:
            row_in_table_to_update = current_list_displayed.index(member)
        except ValueError: 
            return

        if not (0 <= row_in_table_to_update < self.table.rowCount()):
             logger.warning(f"HMP Signal: فهرس الجدول المحسوب {row_in_table_to_update} خارج الحدود لـ {self.table.rowCount()} صفوف.")
             return

        member_display_name = self._get_member_display_name_with_index(member, original_member_index)
        if is_processing_now:
            self.active_spinner_row_in_view = row_in_table_to_update
            self.spinner_char_idx = 0

            self.table.selectRow(row_in_table_to_update)
            first_column_item = self.table.item(row_in_table_to_update, 0) 
            if first_column_item:
                self.table.scrollToItem(first_column_item, QAbstractItemView.EnsureVisible)

            icon_item = self.table.item(row_in_table_to_update, self.COL_ICON)
            if icon_item:
                icon_item.setText(self.spinner_chars[self.spinner_char_idx]) 
                icon_item.setIcon(QIcon()) 

            self.highlight_processing_row(row_in_table_to_update, force_processing_display=True) 

            if not self.row_spinner_timer.isActive():
                self.row_spinner_timer.start(self.row_spinner_timer_interval)

            self.update_status_bar_message(f"جاري معالجة العضو: {member_display_name}...", is_general_message=False)
            self._set_member_overview_content(
                member,
                status_override=f"جاري المعالجة ({member.status})",
                wait_text="المعالجة جارية لهذا العضو الآن.",
                last_msg=member.full_last_activity_detail or member.last_activity_detail,
            )

        else:
            is_still_pdf_downloading = self.active_download_all_pdfs_threads.get(original_member_index) and \
                                       self.active_download_all_pdfs_threads[original_member_index].isRunning()
            is_still_single_checking = self.single_check_thread and \
                                       self.single_check_thread.isRunning() and \
                                       self.single_check_thread.index == original_member_index

            if not is_still_pdf_downloading and not is_still_single_checking:
                if self.active_spinner_row_in_view == row_in_table_to_update:
                    self.row_spinner_timer.stop()
                    self.active_spinner_row_in_view = -1
                    icon_item = self.table.item(row_in_table_to_update, self.COL_ICON)
                    if icon_item:
                        icon_item.setText("")

            self.highlight_processing_row(row_in_table_to_update, force_processing_display=False)
            if self.member_overview_selected_index == original_member_index:
                self._set_member_overview_content(
                    member,
                    status_override=member.status,
                    wait_text=self.latest_countdown_text,
                    last_msg=member.full_last_activity_detail or member.last_activity_detail,
                )


    def highlight_processing_row(self, row_index_in_table, force_processing_display=None):
        if not (0 <= row_index_in_table < self.table.rowCount()):
            return 

        current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list
        if row_index_in_table >= len(current_list_displayed): 
            return

        member = current_list_displayed[row_index_in_table]
        is_processing_flag = force_processing_display if force_processing_display is not None else member.is_processing

        item_for_selection_check = self.table.item(row_index_in_table, 0) 
        is_row_selected_by_user_or_code = False
        if item_for_selection_check:
            is_row_selected_by_user_or_code = item_for_selection_check.isSelected() 

        default_bg_color = self.table.palette().color(QPalette.Base)
        alternate_bg_color = QColor(self.table.palette().color(QPalette.AlternateBase)) if self.table.alternatingRowColors() else default_bg_color
        processing_bg_color = QColorConstants.PROCESSING_ROW_DARK_THEME
        selection_bg_color_from_qss = QColor("#00A2E8") 

        for col in range(self.table.columnCount()):
            item = self.table.item(row_index_in_table, col)
            if item:
                if is_processing_flag: 
                    item.setBackground(processing_bg_color)
                    item.setForeground(Qt.white)
                elif is_row_selected_by_user_or_code: 
                    if item.background() != selection_bg_color_from_qss: 
                         item.setBackground(selection_bg_color_from_qss)
                    if item.foreground().color() != Qt.white: 
                         item.setForeground(Qt.white)
                else: 
                    status_text_for_color = member.status
                    specific_color = None
                    if status_text_for_color == "مستفيد حاليًا من المنحة": specific_color = QColorConstants.BENEFITING_GREEN_DARK_THEME
                    elif status_text_for_color == "بيانات الإدخال خاطئة": specific_color = QColorConstants.PINK_DARK_THEME
                    elif status_text_for_color == "لديه موعد مسبق": specific_color = QColorConstants.LIGHT_BLUE_DARK_THEME
                    elif status_text_for_color == "غير مؤهل للحجز": specific_color = QColorConstants.ORANGE_RED_DARK_THEME
                    elif status_text_for_color == "مكتمل": specific_color = QColorConstants.LIGHT_GREEN_DARK_THEME
                    elif "فشل" in status_text_for_color or "غير مؤهل" in status_text_for_color or "خطأ" in status_text_for_color:
                        specific_color = QColorConstants.LIGHT_PINK_DARK_THEME
                    elif "يتطلب تسجيل مسبق" in status_text_for_color: specific_color = QColorConstants.LIGHT_YELLOW_DARK_THEME

                    if specific_color:
                        item.setBackground(specific_color)
                    else: 
                        if self.table.alternatingRowColors() and row_index_in_table % 2 != 0 :
                            item.setBackground(alternate_bg_color)
                        else:
                            item.setBackground(default_bg_color)
                    item.setForeground(self.table.palette().color(QPalette.Text)) 


    def add_member(self):
        if not self.activation_successful or (self.current_subscription_data and self.current_subscription_data.get("status","").upper() != "ACTIVE"):
            self._show_toast("لا يمكن إضافة أعضاء. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="إضافة عضو")
            return

        dialog = AddMemberDialog(self)
        if dialog.exec_() == AddMemberDialog.Accepted:
            data = dialog.get_data()
            if not (data["nin"] and data["wassit_no"] and data["ccp"]):
                self._show_toast("يرجى ملء حقول رقم التعريف، رقم الوسيط، والحساب البريدي.", type="warning", title="بيانات ناقصة")
                return
            if len(data["nin"]) != 18:
                self._show_toast("رقم التعريف الوطني يجب أن يتكون من 18 رقمًا.", type="error", title="خطأ في الإدخال")
                return
            if len(data["ccp"]) != 12: 
                self._show_toast("رقم الحساب البريدي يجب أن يتكون من 12 رقمًا (10 للحساب + 2 للمفتاح).", type="error", title="خطأ في الإدخال")
                return

            for idx, m in enumerate(self.members_list):
                if m.nin == data["nin"] or m.wassit_no == data["wassit_no"]:
                    member_name_display = self._get_member_display_name_with_index(m, idx)
                    msg = f"العضو '{member_name_display}' موجود بالفعل ببيانات مشابهة."
                    self._show_toast(msg, type="warning", title="عضو مكرر")
                    logger.warning(f"محاولة إضافة عضو مكرر: {data['nin']}/{data['wassit_no']} - {msg}")
                    return

            member = Member(data["nin"], data["wassit_no"], data["ccp"], data["phone_number"])
            self.members_list.append(member)

            if self.is_filter_active:
                self.apply_filter_and_search()
            else:
                self.update_table()

            current_original_index = self.members_list.index(member)
            self.last_added_member_index = current_original_index
            self._select_row_by_member_index(current_original_index, ensure_visible=True)
            member_display_name_add = self._get_member_display_name_with_index(member, current_original_index)
            logger.info(f"تمت إضافة العضو: {member_display_name_add}, Phone={data['phone_number']}")
            self.update_status_bar_message(f"تمت إضافة العضو: {member_display_name_add}. جاري جلب المعلومات الأولية...", is_general_message=False)
            self._show_toast(f"تمت إضافة العضو. جاري جلب المعلومات الأولية...", type="info", title=member_display_name_add)

            fetch_thread = FetchInitialInfoThread(member, current_original_index, self.api_client, self.settings.copy())
            fetch_thread.update_member_gui_signal.connect(self.update_member_gui_in_table)
            fetch_thread.new_data_fetched_signal.connect(self.update_member_name_in_table)
            fetch_thread.member_processing_started_signal.connect(lambda idx: self.handle_member_processing_signal(idx, True))
            
            fetch_thread.member_processing_finished_signal.connect(
                lambda idx, new_member_original_idx=current_original_index: self._handle_fetch_initial_info_finished(new_member_original_idx)
            )

            self.initial_fetch_threads.append(fetch_thread)
            fetch_thread.start()

    def _handle_fetch_initial_info_finished(self, original_member_index):
        logger.debug(f"FetchInitialInfoThread finished for member index {original_member_index}. Setting processing to False.")
        if 0 <= original_member_index < len(self.members_list):
            self.handle_member_processing_signal(original_member_index, False) 
            self._trigger_auto_check_after_add(original_member_index)
        else:
            logger.warning(f"_handle_fetch_initial_info_finished: Member at index {original_member_index} no longer exists.")


    def _trigger_auto_check_after_add(self, original_member_index):
        if 0 <= original_member_index < len(self.members_list):
            member = self.members_list[original_member_index]
            member_display_name = self._get_member_display_name_with_index(member, original_member_index)
            logger.info(f"اكتمل جلب المعلومات الأولية للعضو {member_display_name}. بدء الفحص التلقائي الفوري...")
            
            if member.is_processing: 
                logger.warning(f"_trigger_auto_check_after_add: العضو {member_display_name} لا يزال قيد المعالجة (غير متوقع). تأجيل الفحص الفوري.")
                self._show_toast(f"العضو لا يزال قيد المعالجة (غير متوقع). سيتأخر الفحص الفوري قليلاً.", type="warning", title=member_display_name)
                QTimer.singleShot(500, lambda: self._trigger_auto_check_after_add(original_member_index)) 
                return

            self._show_toast(f"بدء الفحص التلقائي الفوري...", type="info", title=member_display_name)
            self.check_member_now(original_member_index) 
        else:
            logger.warning(f"_trigger_auto_check_after_add: فهرس خاطئ {original_member_index}")


    def edit_member_details(self, item):
        if not self.activation_successful or (self.current_subscription_data and self.current_subscription_data.get("status","").upper() != "ACTIVE"):
            self._show_toast("لا يمكن تعديل الأعضاء. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="تعديل عضو")
            return

        row_in_table = -1
        if not item: 
            selected_rows = self.table.selectionModel().selectedRows()
            if not selected_rows: return 
            row_in_table = selected_rows[0].row() 
        else:
            row_in_table = item.row() 

        current_list_for_edit = self.filtered_members_list if self.is_filter_active else self.members_list
        if not (0 <= row_in_table < len(current_list_for_edit)): return 

        member_to_edit_from_display = current_list_for_edit[row_in_table]
        try:
            original_member_index = self.members_list.index(member_to_edit_from_display) 
            member_to_edit = self.members_list[original_member_index] 
        except ValueError:
            member_display_name_err = self._get_member_display_name_with_index(member_to_edit_from_display, -1)
            logger.error(f"فشل العثور على العضو {member_display_name_err} في القائمة الرئيسية عند التعديل.")
            self._show_toast(f"خطأ: فشل العثور على العضو {member_display_name_err} للتعديل.", type="error", title="خطأ داخلي")
            return

        member_display_name_edit_title = self._get_member_display_name_with_index(member_to_edit, original_member_index)
        dialog = EditMemberDialog(member_to_edit, self)
        if dialog.exec_() == EditMemberDialog.Accepted:
            new_data = dialog.get_data()
            if not (new_data["nin"] and new_data["wassit_no"] and new_data["ccp"]):
                self._show_toast("يرجى ملء حقول رقم التعريف، رقم الوسيط، والحساب البريدي.", type="warning", title="بيانات ناقصة")
                return
            if len(new_data["nin"]) != 18:
                self._show_toast("رقم التعريف الوطني يجب أن يتكون من 18 رقمًا.", type="error", title="خطأ في الإدخال")
                return
            if len(new_data["ccp"]) != 12:
                self._show_toast("رقم الحساب البريدي يجب أن يتكون من 12 رقمًا (10 للحساب + 2 للمفتاح).", type="error", title="خطأ في الإدخال")
                return

            nin_changed = member_to_edit.nin != new_data["nin"]
            wassit_changed = member_to_edit.wassit_no != new_data["wassit_no"]

            if nin_changed or wassit_changed:
                for idx, m in enumerate(self.members_list):
                    if idx == original_member_index: 
                        continue
                    if m.nin == new_data["nin"] or m.wassit_no == new_data["wassit_no"]:
                        conflicting_member_display = self._get_member_display_name_with_index(m, idx)
                        self._show_toast(f"البيانات الجديدة (NIN أو رقم الوسيط) تتعارض مع العضو '{conflicting_member_display}'. لم يتم الحفظ.", type="error", title="بيانات مكررة")
                        logger.warning(f"فشل تعديل العضو {member_display_name_edit_title} بسبب تكرار مع {conflicting_member_display}")
                        return

            member_to_edit.nin = new_data["nin"]
            member_to_edit.wassit_no = new_data["wassit_no"]
            member_to_edit.ccp = new_data["ccp"]
            member_to_edit.phone_number = new_data["phone_number"]

            member_display_after_edit = self._get_member_display_name_with_index(member_to_edit, original_member_index)

            if nin_changed or wassit_changed: 
                logger.info(f"تم تغيير المعرفات الرئيسية للعضو {member_display_after_edit}. إعادة تعيين الحالة وجلب المعلومات.")
                member_to_edit.status = "جديد" 
                member_to_edit.set_activity_detail("تم تعديل المعرفات، يتطلب إعادة التحقق.")
                member_to_edit.nom_fr = ""
                member_to_edit.prenom_fr = ""
                member_to_edit.nom_ar = ""
                member_to_edit.prenom_ar = ""
                member_to_edit.pre_inscription_id = None
                member_to_edit.demandeur_id = None
                member_to_edit.structure_id = None
                member_to_edit.rdv_date = None
                member_to_edit.rdv_id = None
                member_to_edit.rdv_source = None
                member_to_edit.pdf_honneur_path = None
                member_to_edit.pdf_rdv_path = None
                member_to_edit.has_actual_pre_inscription = False
                member_to_edit.already_has_rdv = False
                member_to_edit.consecutive_failures = 0
                member_to_edit.is_processing = False 
                member_to_edit.have_allocation = False
                member_to_edit.allocation_details = {}

                if self.is_filter_active: self.apply_filter_and_search()
                else: self.update_table_row(original_member_index, member_to_edit) 

                self.update_status_bar_message(f"تم تعديل بيانات العضو {member_display_after_edit}. جاري إعادة جلب المعلومات...", is_general_message=False)
                self._show_toast(f"تم تعديل البيانات. جاري إعادة جلب المعلومات...", type="info", title=member_display_after_edit)

                fetch_thread = FetchInitialInfoThread(member_to_edit, original_member_index, self.api_client, self.settings.copy())
                fetch_thread.update_member_gui_signal.connect(self.update_member_gui_in_table)
                fetch_thread.new_data_fetched_signal.connect(self.update_member_name_in_table)
                fetch_thread.member_processing_started_signal.connect(lambda idx: self.handle_member_processing_signal(idx, True))
                fetch_thread.member_processing_finished_signal.connect(
                     lambda idx, edited_member_idx=original_member_index: self._handle_fetch_initial_info_finished(edited_member_idx) 
                )
                self.initial_fetch_threads.append(fetch_thread)
                fetch_thread.start()
            else: 
                if self.is_filter_active: self.apply_filter_and_search()
                else: self.update_table_row(original_member_index, member_to_edit) 
                self.update_status_bar_message(f"تم تعديل بيانات العضو: {member_display_after_edit}", is_general_message=True)
                self._show_toast(f"تم تعديل بيانات العضو.", type="success", title=member_display_after_edit)

            self.save_members_data() 


    def remove_member(self):
        if not self.activation_successful or (self.current_subscription_data and self.current_subscription_data.get("status","").upper() != "ACTIVE"):
            self._show_toast("لا يمكن حذف الأعضاء. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="حذف عضو")
            return

        selected_rows_in_table = self.table.selectionModel().selectedRows()
        if not selected_rows_in_table:
            self._show_toast("يرجى تحديد عضو واحد على الأقل لحذفه.", type="warning", title="حذف عضو")
            return

        confirm_msg = f"هل أنت متأكد أنك تريد حذف {len(selected_rows_in_table)} عضو/أعضاء محددين؟"
        if len(selected_rows_in_table) == 1: 
            row_in_table = selected_rows_in_table[0].row()
            current_list_for_display = self.filtered_members_list if self.is_filter_active else self.members_list
            if 0 <= row_in_table < len(current_list_for_display):
                member_to_remove_display_obj = current_list_for_display[row_in_table]
                original_idx_for_display_remove = -1
                try:
                    original_idx_for_display_remove = self.members_list.index(member_to_remove_display_obj)
                except ValueError:
                    pass 
                member_to_remove_display_name = self._get_member_display_name_with_index(member_to_remove_display_obj, original_idx_for_display_remove)
                confirm_msg = f"هل أنت متأكد أنك تريد حذف العضو '{member_to_remove_display_name}'؟"


        confirm_delete = QMessageBox.question(self, "تأكيد الحذف", confirm_msg, QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if confirm_delete == QMessageBox.No:
            return

        members_to_delete_from_display = [] 
        for index_obj in selected_rows_in_table:
            row_in_table = index_obj.row()
            current_list_for_display = self.filtered_members_list if self.is_filter_active else self.members_list
            if 0 <= row_in_table < len(current_list_for_display):
                members_to_delete_from_display.append(current_list_for_display[row_in_table])

        deleted_count = 0
        for member_to_delete in members_to_delete_from_display:
            if member_to_delete in self.members_list: 
                original_idx_before_delete = self.members_list.index(member_to_delete)
                deleted_member_display_name = self._get_member_display_name_with_index(member_to_delete, original_idx_before_delete)
                self.members_list.remove(member_to_delete) 
                logger.info(f"تم حذف العضو: {deleted_member_display_name}")
                deleted_count +=1
            else:
                logger.warning(f"محاولة حذف عضو {member_to_delete.nin} غير موجود في القائمة الرئيسية.")

        if self.is_filter_active:
            self.apply_filter_and_search()
        else:
            self.update_table()

        if self.members_list:
            if self.last_added_member_index is not None and self.last_added_member_index >= len(self.members_list):
                self.last_added_member_index = len(self.members_list) - 1
        else:
            self.last_added_member_index = None

        if deleted_count > 0:
            self.update_status_bar_message(f"تم حذف {deleted_count} عضو/أعضاء.", is_general_message=True)
            self._show_toast(f"تم حذف {deleted_count} عضو/أعضاء بنجاح.", type="info", title="حذف أعضاء")

        self.save_members_data() 
        if self.monitoring_thread and self.monitoring_thread.current_member_index_to_process >= len(self.members_list):
            self.monitoring_thread.current_member_index_to_process = 0 


    def update_table(self):
        self.table.setRowCount(0)
        list_to_display = self.filtered_members_list if self.is_filter_active else self.members_list
        for row_idx, member_obj in enumerate(list_to_display):
            self.table.insertRow(row_idx)
            self.update_table_row(row_idx, member_obj)
        if not self.is_filter_active:
            self.save_members_data()
        self._ensure_table_selection_after_refresh()
        self._update_insight_metrics()

    def update_table_row(self, row_in_table, member):
        item_icon = QTableWidgetItem()
        item_icon.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row_in_table, self.COL_ICON, item_icon) 

        item_full_name_ar = QTableWidgetItem(member.get_full_name_ar())
        item_full_name_ar.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_FULL_NAME_AR, item_full_name_ar)

        item_nin = QTableWidgetItem(member.nin)
        item_nin.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_NIN, item_nin)

        item_wassit = QTableWidgetItem(member.wassit_no)
        item_wassit.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_WASSIT, item_wassit)

        ccp_display = member.ccp
        if len(member.ccp) == 12: 
             ccp_display = f"{member.ccp[:10]} {member.ccp[10:]}"
        item_ccp = QTableWidgetItem(ccp_display)
        item_ccp.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_CCP, item_ccp)

        item_phone = QTableWidgetItem(member.phone_number or "") 
        item_phone.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_PHONE_NUMBER, item_phone)

        item_status = QTableWidgetItem()
        item_status.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_STATUS, item_status)

        rdv_date_display_text = member.rdv_date if member.rdv_date else ""
        if member.rdv_date:
            if member.rdv_source == "system":
                rdv_date_display_text += " (نظام)"
            elif member.rdv_source == "discovered":
                rdv_date_display_text += " (مكتشف)"
        item_rdv = QTableWidgetItem(rdv_date_display_text)
        item_rdv.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_RDV_DATE, item_rdv)

        detail_to_show = member.last_activity_detail 
        item_details = QTableWidgetItem(detail_to_show)
        item_details.setToolTip(member.full_last_activity_detail) 
        item_details.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table.setItem(row_in_table, self.COL_DETAILS, item_details)

        try:
            original_member_index = self.members_list.index(member) 
            self.update_member_gui_in_table(original_member_index, member.status, member.last_activity_detail, get_icon_name_for_status(member.status))
        except ValueError: 
            status_item = self.table.item(row_in_table, self.COL_STATUS)
            if status_item: status_item.setText(member.status)
            icon_item = self.table.item(row_in_table, self.COL_ICON)
            if icon_item:
                qt_icon = self.style().standardIcon(getattr(QStyle, get_icon_name_for_status(member.status), QStyle.SP_CustomBase))
                icon_item.setIcon(qt_icon)
                icon_item.setText("") 


    def update_member_gui_in_table(self, original_member_index, status_text, detail_text, icon_name_str):
        if not (0 <= original_member_index < len(self.members_list)):
            return 

        member = self.members_list[original_member_index] 

        row_in_table_to_update = -1
        current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list
        try:
            row_in_table_to_update = current_list_displayed.index(member)
        except ValueError: 
            return

        if not (0 <= row_in_table_to_update < self.table.rowCount()):
             return 

        self.table.item(row_in_table_to_update, self.COL_FULL_NAME_AR).setText(member.get_full_name_ar())
        self.table.item(row_in_table_to_update, self.COL_NIN).setText(member.nin)
        self.table.item(row_in_table_to_update, self.COL_WASSIT).setText(member.wassit_no)
        ccp_display = member.ccp
        if len(member.ccp) == 12: ccp_display = f"{member.ccp[:10]} {member.ccp[10:]}"
        self.table.item(row_in_table_to_update, self.COL_CCP).setText(ccp_display)
        self.table.item(row_in_table_to_update, self.COL_PHONE_NUMBER).setText(member.phone_number or "")

        rdv_date_display_text = member.rdv_date if member.rdv_date else ""
        if member.rdv_date:
            if member.rdv_source == "system":
                rdv_date_display_text += " (نظام)"
            elif member.rdv_source == "discovered":
                rdv_date_display_text += " (مكتشف)"
        self.table.item(row_in_table_to_update, self.COL_RDV_DATE).setText(rdv_date_display_text)

        detail_to_show_gui = member.last_activity_detail 
        self.table.item(row_in_table_to_update, self.COL_DETAILS).setText(detail_to_show_gui)
        self.table.item(row_in_table_to_update, self.COL_DETAILS).setToolTip(member.full_last_activity_detail) 

        icon_item = self.table.item(row_in_table_to_update, self.COL_ICON)
        status_text_item = self.table.item(row_in_table_to_update, self.COL_STATUS)
        status_text_item.setText(status_text) 

        if icon_item:
            if self.active_spinner_row_in_view == row_in_table_to_update and member.is_processing:
                icon_item.setIcon(QIcon())
            else:
                qt_icon = self.style().standardIcon(getattr(QStyle, icon_name_str, QStyle.SP_CustomBase))
                icon_item.setIcon(qt_icon)
                icon_item.setText("")

        self.highlight_processing_row(row_in_table_to_update, force_processing_display=None)

        if self.member_overview_selected_index == original_member_index:
            self._set_member_overview_content(
                member,
                status_override=status_text,
                wait_text=self._get_member_wait_text(member),
                last_msg=member.full_last_activity_detail or member.last_activity_detail,
            )

        # تحديث الإحصاءات السريعة فور تغير حالة العضو
        self._update_insight_metrics()

        msg_attr_prefix = f"_toast_shown_{original_member_index}_"
        if not self.suppress_initial_messages:
            current_status_for_toast = status_text
            toast_title_for_member = self._get_member_display_name_with_index(member, original_member_index)

            if "فشل" in current_status_for_toast or "خطأ" in current_status_for_toast or "غير مؤهل" in current_status_for_toast:
                error_attr = msg_attr_prefix + current_status_for_toast.replace(" ", "_") 
                if not hasattr(self, error_attr) or not getattr(self, error_attr): 
                    self._show_toast(f"{member.full_last_activity_detail}", type="error", duration=5000, title=toast_title_for_member)
                    setattr(self, error_attr, True) 
                    for attr_suffix in ["input_error", "has_rdv", "booking_ineligible", "completed_or_benefiting", "success_generic"]:
                        if hasattr(self, msg_attr_prefix + attr_suffix):
                            delattr(self, msg_attr_prefix + attr_suffix)
            elif current_status_for_toast == "مكتمل" or current_status_for_toast == "مستفيد حاليًا من المنحة" or current_status_for_toast == "تم الحجز":
                success_attr = msg_attr_prefix + "success_generic" 
                if not hasattr(self, success_attr) or not getattr(self, success_attr): 
                    self._show_toast(f"{detail_text}", type="success", duration=5000, title=toast_title_for_member)
                    setattr(self, success_attr, True) 
                    for attr_suffix in ["input_error", "has_rdv", "booking_ineligible", "error_generic"]:
                         if hasattr(self, msg_attr_prefix + attr_suffix):
                            delattr(self, msg_attr_prefix + attr_suffix)
            else: 
                for attr_suffix in ["input_error", "has_rdv", "booking_ineligible", "completed_or_benefiting", "success_generic", "error_generic"] + [current_status_for_toast.replace(" ", "_")]:
                    if hasattr(self, msg_attr_prefix + attr_suffix):
                        delattr(self, msg_attr_prefix + attr_suffix)


    def update_member_name_in_table(self, original_member_index, nom_ar, prenom_ar):
        if 0 <= original_member_index < len(self.members_list):
            member = self.members_list[original_member_index]
            member.nom_ar = nom_ar
            member.prenom_ar = prenom_ar

            row_in_table_to_update = -1
            current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list
            try:
                row_in_table_to_update = current_list_displayed.index(member)
                if 0 <= row_in_table_to_update < self.table.rowCount():
                    full_name_item = self.table.item(row_in_table_to_update, self.COL_FULL_NAME_AR)
                    if full_name_item:
                        full_name_item.setText(member.get_full_name_ar()) 
                    if not self.suppress_initial_messages: 
                        self._show_toast(f"تم تحديث اسم العضو.", type="info", title=self._get_member_display_name_with_index(member, original_member_index))
            except ValueError:
                pass
            self.save_members_data()


    def _update_status_chip(self, level="info", label_text="حالة عامة"):
        if not hasattr(self, 'status_chip_label'):
            return

        icon_map = {
            "info": "ℹ️",
            "success": "✅",
            "warning": "⚠️",
            "error": "⛔",
        }
        color_map = {
            "info": "#2d3436",
            "success": "#1e7e34",
            "warning": "#d35400",
            "error": "#c0392b",
        }
        icon = icon_map.get(level, "ℹ️")
        bg_color = color_map.get(level, "#2d3436")
        self.status_chip_label.setText(f"{icon} {label_text}")
        self.status_chip_label.setStyleSheet(
            f"QLabel#StatusChip {{ background: {bg_color}; color: #ecf0f1; border-radius: 8px; padding: 4px 10px; font-weight: 600; }}"
        )


    def _set_status_spinner_running(self, running):
        if not hasattr(self, 'status_spinner_label'):
            return
        if running:
            if not self.status_spinner_timer.isActive():
                self.status_spinner_timer.start()
            self.status_spinner_label.setVisible(True)
        else:
            self.status_spinner_timer.stop()
            self.status_spinner_label.setVisible(False)
            self.status_spinner_label.setText("")


    def _advance_status_spinner(self):
        if not getattr(self, 'status_spinner_label', None):
            return
        self.status_spinner_idx = (self.status_spinner_idx + 1) % len(self.status_spinner_chars)
        self.status_spinner_label.setText(self.status_spinner_chars[self.status_spinner_idx])


    def _update_refresh_hint(self, text=None, busy=False):
        if getattr(self, 'refresh_hint_label', None):
            if text is not None:
                self.refresh_hint_label.setText(text)
            elif not self.refresh_hint_label.text().strip():
                self.refresh_hint_label.setText("لا يوجد تحديث نشط")
        self._set_status_spinner_running(busy)


    def _format_wait_time(self, seconds):
        try:
            seconds = int(seconds)
        except (TypeError, ValueError):
            return None
        if seconds < 0:
            seconds = 0
        minutes, remaining_seconds = divmod(seconds, 60)
        hours, remaining_minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:02d}:{remaining_minutes:02d}:{remaining_seconds:02d}"
        return f"{remaining_minutes:02d}:{remaining_seconds:02d}"


    def _set_operation_panel(self, current_text=None, next_text=None, wait_seconds=None, level="info"):
        if not getattr(self, 'operation_current_label', None):
            return

        color_map = {
            "info": "#e9eef5",
            "success": "#b5e48c",
            "warning": "#f6c344",
            "error": "#f28b82",
        }
        badge_color = color_map.get(level, "#e9eef5")

        if current_text:
            self.operation_current_label.setText(f"حالة العمليات: {current_text}")
        if next_text:
            self.operation_next_label.setText(f"التالي: {next_text}")
        if wait_seconds is not None:
            formatted = self._format_wait_time(wait_seconds)
            self.operation_timer_label.setText(f"الزمن المتبقي: {formatted if formatted else '--'}")

        self.operation_current_label.setStyleSheet(
            f"QLabel#OperationCurrent {{ color: {badge_color}; font-weight: 700; }}"
        )


    def update_status_bar_message(self, message, is_general_message=True, member_obj=None, original_idx_if_member=None, level="info", busy=False, hint_text=None):
        final_message = message
        if member_obj and original_idx_if_member is not None and original_idx_if_member >= 0:
            member_display = self._get_member_display_name_with_index(member_obj, original_idx_if_member)
            final_message = f"{member_display}: {message}"

        if hasattr(self, 'status_bar_label'):
            self.status_bar_label.setText(final_message)

        if hasattr(self, 'last_scan_label'):
            self.last_scan_label.setText(f"آخر تحديث: {time.strftime('%H:%M:%S')}")

        if hasattr(self, 'countdown_label') and self.countdown_label.text().strip() == "":
            self.countdown_label.setText("لا يوجد عد تنازلي")


    def update_countdown_timer_display(self, time_remaining_str):
        if hasattr(self, 'countdown_label'):
            display_text = time_remaining_str.strip()
            if display_text:
                self.countdown_label.setText(f"⏳ {display_text}")
                self.countdown_label.setToolTip("الوقت المتبقي قبل تنفيذ الخطوة التالية")
                self.latest_countdown_text = display_text
            else:
                self.countdown_label.setText("لا يوجد عد تنازلي")
                self.countdown_label.setToolTip("لا يوجد انتظار حالي")
                self.latest_countdown_text = "لا يوجد انتظار حالي"



    def start_monitoring(self):
        if not self.activation_successful or not self.current_subscription_data or self.current_subscription_data.get("status","").upper() != "ACTIVE":
            self._show_toast("لا يمكن بدء المراقبة. البرنامج غير مفعل أو الاشتراك غير نشط.", type="error", title="بدء المراقبة")
            return

        if not self.members_list:
            self._show_toast("يرجى إضافة أعضاء أولاً لبدء المراقبة.", type="warning", title="بدء المراقبة")
            return
        if not self.monitoring_thread.isRunning():
            logger.info("بدء المراقبة...")
            self.monitoring_thread.members_list_ref = self.members_list 
            self.monitoring_thread.is_running = True
            self.monitoring_thread.is_connection_lost_mode = False 
            self.monitoring_thread.current_member_index_to_process = 0 
            self.monitoring_thread.consecutive_network_error_trigger_count = 0 
            self.monitoring_thread.update_thread_settings(self.settings.copy()) 
            self.monitoring_thread.start()
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            self.add_member_button.setEnabled(False) 
            self.remove_member_button.setEnabled(False) 
            monitoring_interval_minutes = self.settings.get(SETTING_MONITORING_INTERVAL, DEFAULT_SETTINGS[SETTING_MONITORING_INTERVAL])
            self.update_status_bar_message(
                f"بدأت المراقبة (الدورة كل {monitoring_interval_minutes} دقيقة)...",
                is_general_message=False,
                busy=True,
                hint_text=f"المراقبة نشطة - الدورة كل {monitoring_interval_minutes} دقيقة"
            )
            self._set_operation_panel(
                current_text="المراقبة الدورية قيد التنفيذ",
                next_text="الدورة القادمة حسب الإعداد",
                wait_seconds=int(float(monitoring_interval_minutes) * 60),
                level="info",
            )
            self._show_toast(f"بدأت المراقبة (الدورة كل {monitoring_interval_minutes} دقيقة).", type="info", title="المراقبة")
            self._update_insight_metrics()
        else:
            self._show_toast("المراقبة جارية بالفعل.", type="info", title="المراقبة")
            self.update_status_bar_message("المراقبة جارية بالفعل.", is_general_message=True, hint_text="المراقبة تعمل")


    def stop_monitoring(self):
        if self.monitoring_thread.isRunning():
            logger.info("تم طلب إيقاف المراقبة.")
            self.monitoring_thread.stop_monitoring() 
            if self.row_spinner_timer.isActive():
                self.row_spinner_timer.stop()
                if self.active_spinner_row_in_view != -1 and self.active_spinner_row_in_view < self.table.rowCount():
                    current_list_displayed = self.filtered_members_list if self.is_filter_active else self.members_list
                    if self.active_spinner_row_in_view < len(current_list_displayed):
                        member_at_spinner = current_list_displayed[self.active_spinner_row_in_view]
                        try:
                            original_member_index = self.members_list.index(member_at_spinner)
                            if 0 <= original_member_index < len(self.members_list):
                                member = self.members_list[original_member_index]
                                member.is_processing = False 
                                self.update_member_gui_in_table(original_member_index, member.status, member.last_activity_detail, get_icon_name_for_status(member.status))
                        except ValueError:
                             pass 
                self.active_spinner_row_in_view = -1 

            if self.activation_successful and self.current_subscription_data and self.current_subscription_data.get("status","").upper() == "ACTIVE":
                self._enable_app_functions()
            else:
                self._disable_app_functions()
                self.stop_button.setEnabled(False)

            self.update_status_bar_message("تم إيقاف المراقبة بنجاح.", is_general_message=True, level="warning", busy=False, hint_text="المراقبة متوقفة")
            self._set_operation_panel(current_text="المراقبة متوقفة", next_text="اضغط بدء للمراقبة", level="warning", wait_seconds=0)
            self._show_toast("تم إيقاف المراقبة.", type="info", title="المراقبة")
            self.update_countdown_timer_display("")
            for i in range(len(self.members_list)):
                if self.members_list[i].is_processing:
                    self.members_list[i].is_processing = False
                    self.update_member_gui_in_table(i, self.members_list[i].status, self.members_list[i].last_activity_detail, get_icon_name_for_status(self.members_list[i].status))
            self._update_insight_metrics()
        else:
            self._show_toast("المراقبة ليست جارية حاليًا.", type="info", title="المراقبة")
            self.update_status_bar_message("المراقبة ليست جارية.", is_general_message=True, hint_text="لا يوجد عمل مجدول")


    def load_members_data(self):
        self.suppress_initial_messages = True 
        loaded_successfully = False
        primary_path = DATA_FILE 
        backup_path = DATA_FILE_BAK 

        if os.path.exists(primary_path):
            try:
                with open(primary_path, 'r', encoding='utf-8') as f:
                    data_list = json.load(f)
                    self.members_list = [Member.from_dict(data) for data in data_list]
                    for member in self.members_list: 
                        member.is_processing = False
                    loaded_successfully = True
                    logger.info(f"تم تحميل بيانات {len(self.members_list)} أعضاء من {primary_path}")
            except json.JSONDecodeError:
                logger.error(f"خطأ في فك تشفير JSON للملف الأساسي {primary_path}. محاولة تحميل النسخة الاحتياطية.")
            except Exception as e:
                logger.exception(f"خطأ غير متوقع عند تحميل البيانات من {primary_path}: {e}")
        else:
            logger.info(f"الملف الأساسي {primary_path} غير موجود. محاولة تحميل النسخة الاحتياطية.")

        if not loaded_successfully and os.path.exists(backup_path): 
            try:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    data_list = json.load(f)
                    self.members_list = [Member.from_dict(data) for data in data_list]
                    for member in self.members_list: 
                        member.is_processing = False
                    loaded_successfully = True
                    logger.info(f"تم تحميل بيانات {len(self.members_list)} أعضاء من الملف الاحتياطي {backup_path}")
                    self.update_status_bar_message(f"تم استعادة البيانات من النسخة الاحتياطية.", is_general_message=True)
                    self._show_toast(f"تم استعادة بيانات الأعضاء من نسخة احتياطية: {backup_path}", type="info", duration=5000, title="تحميل البيانات")
                    try:
                        shutil.copy2(backup_path, primary_path)
                        logger.info(f"تم استعادة الملف الأساسي {primary_path} من النسخة الاحتياطية {backup_path}.")
                    except Exception as e_copy:
                        logger.error(f"فشل في استعادة الملف الأساسي من النسخة الاحتياطية: {e_copy}")
            except json.JSONDecodeError:
                logger.error(f"خطأ في فك تشفير JSON للملف الاحتياطي {backup_path}. قد يكون الملف تالفًا.")
                self.update_status_bar_message(f"خطأ في قراءة ملف البيانات الاحتياطي {backup_path}.", is_general_message=True)
                self._show_toast(f"خطأ في ملف البيانات الاحتياطي {backup_path}. قد يكون الملف تالفًا. تم بدء البرنامج بقائمة فارغة.", type="error", duration=6000, title="خطأ بيانات")
            except Exception as e:
                logger.exception(f"خطأ غير متوقع عند تحميل البيانات من الملف الاحتياطي {backup_path}: {e}")
                self.update_status_bar_message(f"خطأ غير متوقع عند تحميل البيانات الاحتياطية: {e}", is_general_message=True)
                self._show_toast(f"خطأ غير متوقع عند تحميل البيانات الاحتياطية: {e}", type="error", duration=6000, title="خطأ بيانات")

        if not loaded_successfully:
            self.members_list = []
            logger.info(f"لم يتم العثور على ملف البيانات ({primary_path}) أو الملف الاحتياطي ({backup_path})، أو كلاهما تالف. سيبدأ البرنامج بقائمة فارغة.")
            self.update_status_bar_message(f"ملف البيانات غير موجود أو تالف. يمكنك إضافة أعضاء جدد.", is_general_message=True)

        self.last_added_member_index = (len(self.members_list) - 1) if self.members_list else None
        self.filtered_members_list = list(self.members_list)
        self.update_table()

        QTimer.singleShot(200, lambda: setattr(self, 'suppress_initial_messages', False))


    def save_members_data(self):
        primary_path = DATA_FILE 
        tmp_path = DATA_FILE_TMP 
        bak_path = DATA_FILE_BAK 
        try:
            data_to_save = []
            for member in self.members_list:
                member_dict = member.to_dict()
                member_dict['is_processing'] = False 
                data_to_save.append(member_dict)

            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=4)

            if os.path.exists(primary_path):
                try:
                    shutil.copy2(primary_path, bak_path) 
                    logger.debug(f"تم إنشاء نسخة احتياطية من {primary_path} إلى {bak_path}")
                except Exception as e_bak:
                    logger.error(f"فشل في إنشاء نسخة احتياطية لملف بيانات الأعضاء {primary_path}: {e_bak}")

            os.replace(tmp_path, primary_path) 
            logger.info(f"تم حفظ بيانات الأعضاء بنجاح في {primary_path}")

        except Exception as e:
            logger.exception(f"خطأ عند حفظ بيانات الأعضاء في {primary_path}: {e}")
            self.update_status_bar_message(f"خطأ عند حفظ البيانات: {e}", is_general_message=True)
            self._show_toast(f"فشل حفظ بيانات الأعضاء: {e}", type="error", title="خطأ حفظ")
            if os.path.exists(tmp_path): 
                try:
                    os.remove(tmp_path)
                except Exception as e_del_tmp:
                    logger.error(f"فشل في حذف الملف المؤقت لبيانات الأعضاء {tmp_path} بعد خطأ في الحفظ: {e_del_tmp}")


    def closeEvent(self, event):
        logger.info("إغلاق التطبيق...")
        self.update_status_bar_message("جاري إغلاق التطبيق...", is_general_message=True)

        if self.activation_thread and self.activation_thread.isRunning():
            logger.info("Stopping active activation thread before closing...")
            self.activation_thread.stop()
            self.activation_thread.wait(1500) 

        if self.activated_code_id and self.firebase_service and self.firebase_service.is_initialized():
            logger.info(f"AnemApp: Stopping listener for activation code {self.activated_code_id} before closing.")
            self.firebase_service.stop_listening_to_code_changes(self.activated_code_id)
        
        # إيقاف مستمع الرسائل عند الإغلاق
        if self.firebase_service and self.firebase_service.is_initialized():
            logger.info("AnemApp: Stopping app messages listener before closing.")
            self.firebase_service.stop_listening_to_app_messages()


        if self.monitoring_thread.isRunning():
            logger.info("إيقاف المراقبة قبل الإغلاق...")
            self.monitoring_thread.stop_monitoring()
            if not self.monitoring_thread.wait(3000): 
                logger.warning("خيط المراقبة لم ينتهِ في الوقت المناسب.")

        self.save_members_data()
        self.save_app_settings()

        for thread in self.initial_fetch_threads:
            if thread.isRunning():
                thread.quit() 
                if not thread.wait(2000): 
                    logger.warning(f"الخيط {thread} لم ينتهِ في الوقت المناسب عند الإغلاق.")

        if self.single_check_thread and self.single_check_thread.isRunning():
            self.single_check_thread.quit()
            if not self.single_check_thread.wait(1000): 
                 logger.warning("خيط الفحص الفردي لم ينته في الوقت المناسب.")

        active_pdf_dl_threads_copy = list(self.active_download_all_pdfs_threads.values()) 
        if active_pdf_dl_threads_copy:
            for pdf_thread in active_pdf_dl_threads_copy:
                if pdf_thread.isRunning():
                    pdf_thread.stop() 
                    if not pdf_thread.wait(2000): 
                        logger.warning(f"خيط تحميل جميع ملفات PDF {pdf_thread} لم ينتهِ في الوقت المناسب.")
            self.active_download_all_pdfs_threads.clear() 

        if hasattr(self, 'datetime_timer') and self.datetime_timer.isActive(): self.datetime_timer.stop()
        if hasattr(self, 'row_spinner_timer') and self.row_spinner_timer.isActive(): self.row_spinner_timer.stop()
        logger.info("تم إغلاق التطبيق.")
        super().closeEvent(event)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = AnemApp()

    if not main_window.activation_successful or not main_window._should_initialize_ui:
        logger.critical("__main__: Activation failed or UI should not be initialized. Application will exit.")
    else:
        main_window.show()
        sys.exit(app.exec_())
