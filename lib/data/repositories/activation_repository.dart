import '../datasources/remote/api_client.dart';
import '../models/subscription.dart';

class ActivationRepository {
  ActivationRepository({required this.apiClient});

  final ApiClient apiClient;

  Future<Subscription> verifyActivation(String code) {
    return apiClient.verifyActivation(code);
  }
}
