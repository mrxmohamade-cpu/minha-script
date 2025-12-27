import 'package:riverpod/riverpod.dart';

import '../../core/errors.dart';
import '../../data/models/subscription.dart';
import 'app_providers.dart';

class ActivationController extends StateNotifier<AsyncValue<Subscription?>> {
  ActivationController(this.ref) : super(const AsyncData(null)) {
    _loadSaved();
  }

  final Ref ref;

  Future<void> _loadSaved() async {
    final subscription = await ref.read(subscriptionRepositoryProvider).fetchSubscription();
    if (subscription != null) {
      state = AsyncData(subscription);
    }
  }

  Future<void> verifyActivation(String code) async {
    if (code.trim().isEmpty) {
      state = AsyncError(AppException('يرجى إدخال رمز التفعيل.'), StackTrace.current);
      return;
    }
    state = const AsyncLoading();
    try {
      final subscription = await ref.read(activationRepositoryProvider).verifyActivation(code.trim());
      await ref.read(subscriptionRepositoryProvider).saveSubscription(subscription);
      state = AsyncData(subscription);
    } catch (error, stack) {
      state = AsyncError(error, stack);
    }
  }
}

final activationControllerProvider =
    StateNotifierProvider<ActivationController, AsyncValue<Subscription?>>((ref) {
  return ActivationController(ref);
});
