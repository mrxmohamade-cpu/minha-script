import 'package:dio/dio.dart';

import '../../core/constants.dart';
import '../datasources/remote/api_client.dart';
import '../models/appointment.dart';

class BookingRepository {
  BookingRepository({required this.apiClient});

  final ApiClient apiClient;

  Future<List<AppointmentSlot>> searchAppointments(String memberId) {
    return apiClient.searchAppointments(memberId);
  }

  Future<void> bookAppointment(String slotId, String memberId) {
    return apiClient.bookAppointment(slotId, memberId);
  }

  static ApiClient buildApiClient() {
    if (AppConfig.useMockApi) {
      return MockApiClient();
    }
    return DioApiClient(ApiClientFactory.createDio());
  }
}

class ApiClientFactory {
  static Dio createDio() {
    return DioInitializer.build();
  }
}

class DioInitializer {
  static Dio build() {
    return Dio(
      BaseOptions(
        baseUrl: AppConfig.apiBaseUrl,
        connectTimeout: const Duration(seconds: 10),
        receiveTimeout: const Duration(seconds: 10),
      ),
    );
  }
}
