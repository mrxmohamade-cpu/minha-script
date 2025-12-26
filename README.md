# minha-script

## Android (Kotlin)

هذا المستودع يحتوي على مشروع Android جاهز لفتحه في Android Studio.

### طريقة التشغيل

1. افتح Android Studio واختر **Open**.
2. اختر المجلد `android-app`.
3. إذا ظهر تنبيه عن Gradle Wrapper مفقود، نفّذ أحد الأوامر التالية من داخل مجلد `android-app`:

   **Windows (PowerShell):**
   ```powershell
   ./scripts/restore-wrapper.ps1
   ```

   **macOS / Linux (Terminal):**
   ```bash
   ./scripts/restore-wrapper.sh
   ```

4. انتظر اكتمال مزامنة Gradle.
5. اضغط **Run** للتشغيل على المحاكي أو جهاز فعلي.

> ملاحظة: ملف `gradle-wrapper.jar` غير مضمَّن لتجنّب مشاكل الملفات الثنائية في إنشاء الـ PR. يتم توليده محليًا عبر السكربتات أعلاه.

### أهم الملفات

- `android-app/app/src/main/java/com/minhascript/app/MainActivity.kt`
- `android-app/app/src/main/res/layout/activity_main.xml`
