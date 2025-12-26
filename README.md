# Minha Script Flutter (Android)

هذا المشروع نسخة Flutter تعمل على Android فقط بنفس فكرة تطبيق سطح المكتب.

## المزايا الأساسية
- تفعيل/اشتراك عبر API (نموذج قابل للتوصيل).
- إدارة الأعضاء: إضافة / تعديل / حذف.
- تفاصيل العضو + البحث عن المواعيد المتاحة + تأكيد الحجز.
- حفظ البيانات محليًا باستخدام Hive.
- حفظ ملفات PDF وعرضها داخل التطبيق.
- واجهة عربية RTL مع ثيم داكن.
- بنية منظمة (Layers + Riverpod) + معالجة أخطاء واضحة.

## الهيكل العام
```
lib/
  app/
  core/
    errors.dart
    network/api_client.dart
    storage/local_storage.dart
    theme.dart
  features/
    auth/
    booking/
    members/
    pdf/
```

## خطوات التشغيل (Android)
1. تثبيت Flutter (إصدار SDK 3.3 أو أحدث).
2. إنشاء ملف `android/local.properties` وإضافة مسار Flutter SDK:
   ```properties
   flutter.sdk=/path/to/flutter
   ```
3. تشغيل الأمر التالي لجلب الاعتمادات:
   ```bash
   flutter pub get
   ```
4. تشغيل التطبيق على جهاز Android أو محاكي:
   ```bash
   flutter run
   ```

## ملاحظات الربط مع API
- يمكن استبدال روابط `https://api.example.com/...` في:
  - `lib/features/auth/data/activation_repository_impl.dart`
  - `lib/features/booking/data/booking_repository_impl.dart`
- استبدل الاستجابات بما يناسب API الحقيقي.

## التخزين المحلي
- البيانات تُحفظ في Hive (صندوق `members`).
- إعدادات التفعيل محفوظة في صندوق `settings`.

## ملفات PDF
- يتم إنشاء ملف PDF تجريبي وحفظه في مجلد المستندات للتطبيق.
- يتم فتح الملف داخل التطبيق عبر `pdfx`.
