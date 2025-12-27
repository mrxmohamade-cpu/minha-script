import 'package:riverpod/riverpod.dart';

import '../../data/datasources/remote/api_client.dart';
import '../../data/repositories/activation_repository.dart';
import '../../data/repositories/booking_repository.dart';
import '../../data/repositories/document_repository.dart';
import '../../data/repositories/member_repository.dart';
import '../../data/repositories/subscription_repository.dart';
import '../../core/constants.dart';

final apiClientProvider = Provider<ApiClient>((ref) {
  if (AppConfig.useMockApi) {
    return MockApiClient();
  }
  return DioApiClient(ApiClientFactory.createDio());
});

final activationRepositoryProvider = Provider<ActivationRepository>((ref) {
  return ActivationRepository(apiClient: ref.read(apiClientProvider));
});

final subscriptionRepositoryProvider = Provider<SubscriptionRepository>((ref) {
  return SubscriptionRepository();
});

final memberRepositoryProvider = Provider<MemberRepository>((ref) {
  return MemberRepository();
});

final bookingRepositoryProvider = Provider<BookingRepository>((ref) {
  return BookingRepository(apiClient: ref.read(apiClientProvider));
});

final documentRepositoryProvider = Provider<DocumentRepository>((ref) {
  return DocumentRepository();
});
