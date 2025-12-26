import 'package:hive_flutter/hive_flutter.dart';

class LocalStorage {
  static const String membersBox = 'members';
  static const String settingsBox = 'settings';

  static Future<void> initialize() async {
    await Hive.initFlutter();
    await Hive.openBox<Map>(membersBox);
    await Hive.openBox(settingsBox);
  }

  static Box<Map> memberBox() => Hive.box<Map>(membersBox);
  static Box settings() => Hive.box(settingsBox);
}
