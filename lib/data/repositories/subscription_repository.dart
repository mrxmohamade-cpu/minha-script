import 'package:hive/hive.dart';

import '../../core/constants.dart';
import '../models/subscription.dart';

class SubscriptionRepository {
  Future<void> saveSubscription(Subscription subscription) async {
    final box = Hive.box<Subscription>(AppConfig.subscriptionBox);
    await box.put('current', subscription);
  }

  Future<Subscription?> fetchSubscription() async {
    final box = Hive.box<Subscription>(AppConfig.subscriptionBox);
    return box.get('current');
  }
}
