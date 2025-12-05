# Flutter App (Clean Rebuild)

هذا المشروع هو نسخة Flutter نظيفة وجاهزة للتشغيل على Android وWeb مع إعداد Firebase وخدمة خلفية حديثة.

## المتطلبات المسبقة
- تثبيت Flutter (3.22 أو أحدث) وDart SDK.
- Android Studio/SDK لأجهزة Android.
- حساب Firebase وتطبيق Android.

## إعداد المشروع
1. من داخل مجلد `flutter_app` شغّل:
   ```bash
   flutter pub get
   ```

2. أضف ملف `google-services.json` داخل `android/app/` بعد إنشائه من وحدة تحكم Firebase.

3. حدّث قيم `FirebaseOptions` في `lib/firebase_options.dart` ببيانات تطبيقك (يمكن نسخها من إعدادات Firebase > SDK setup & config > Flutter).

4. (اختياري للـ iOS) أضف `GoogleService-Info.plist` إلى `ios/Runner` ثم شغّل `pod install` داخل `ios/`.

## تشغيل التطبيق على Android
```bash
flutter run -d android
```
- تأكد من تمكين USB debugging وأن الجهاز متصل.
- سيحمل التطبيق Firebase باستخدام `DefaultFirebaseOptions` دون الحاجة لـ `google-services.json` في وضع التطوير، لكن وجود الملف يضمن تحميل الموارد تلقائياً.

## تشغيل التطبيق على الويب
```bash
flutter run -d chrome
```
- لا يتم تفعيل الخدمة الخلفية على الويب.

## خدمة الخلفية (Android فقط)
- يتم تهيئة الخدمة باستخدام الحزمة `flutter_background_service` وتعمل فقط على Android.
- لبدء الخدمة من التطبيق: زر "Start Service".
- لإيقافها: زر "Stop Service".
- لا يتم تشغيلها على الويب أو المنصات غير المدعومة.

## هيكلة المشروع
- `lib/main.dart`: نقطة الدخول، تهيئة Firebase، وواجهة المستخدم.
- `lib/services/background_service.dart`: منطق الخدمة الخلفية المخصّص لـ Android.
- `lib/firebase_options.dart`: خيارات Firebase لكل منصة (يجب تعبئتها).

## ملاحظات Firebase
- في حال ظهور الخطأ `PlatformException: Failed to load FirebaseOptions from resource` تأكد من:
  - وجود `google-services.json` في `android/app/`.
  - أو اكتمال قيم `FirebaseOptions` في `lib/firebase_options.dart`.
  - ثبّت `com.google.gms.google-services` في `android/app/build.gradle` كما هو موجود في المشروع.

