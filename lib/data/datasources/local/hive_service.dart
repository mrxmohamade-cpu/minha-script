import 'package:hive_flutter/hive_flutter.dart';

import '../../../core/constants.dart';
import '../../models/member.dart';
import '../../models/subscription.dart';

class HiveService {
  static Future<void> init() async {
    await Hive.initFlutter();
    Hive.registerAdapter(MemberAdapter());
    Hive.registerAdapter(SubscriptionAdapter());
    await Hive.openBox<Member>(AppConfig.membersBox);
    await Hive.openBox<Subscription>(AppConfig.subscriptionBox);
  }
}
