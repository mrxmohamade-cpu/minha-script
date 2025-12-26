import 'package:dio/dio.dart';

import '../../../core/errors.dart';
import '../../../core/network/api_client.dart';
import '../../../core/storage/local_storage.dart';
import '../domain/activation_info.dart';
import '../domain/activation_repository.dart';

class ActivationRepositoryImpl implements ActivationRepository {
  ActivationRepositoryImpl(this._client);

  final ApiClient _client;

  @override
  Future<ActivationInfo> verifyActivation(String memberId) async {
    try {
      final response = await _client.post(
        'https://api.example.com/activation',
        data: {'memberId': memberId},
      );
      final data = response.data ?? {};
      final info = ActivationInfo(
        subscriptionId: data['subscriptionId']?.toString() ?? 'N/A',
        expiryDate: data['expiryDate']?.toString() ?? 'غير متاح',
      );
      await LocalStorage.settings().put('activation', {
        'subscriptionId': info.subscriptionId,
        'expiryDate': info.expiryDate,
      });
      return info;
    } on DioException catch (error) {
      throw Failure(error.message ?? 'تعذر التحقق من التفعيل');
    }
  }
}
