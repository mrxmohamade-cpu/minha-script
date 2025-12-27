import 'package:dio/dio.dart';

import '../../../core/constants.dart';
import '../../../core/errors.dart';
import '../../models/appointment.dart';
import '../../models/subscription.dart';

abstract class ApiClient {
  Future<Subscription> verifyActivation(String code);
  Future<List<AppointmentSlot>> searchAppointments(String memberId);
  Future<void> bookAppointment(String slotId, String memberId);
}

class DioApiClient implements ApiClient {
  DioApiClient(this._dio);

  final Dio _dio;

  @override
  Future<Subscription> verifyActivation(String code) async {
    if (AppConfig.apiBaseUrl.isEmpty) {
      throw AppException('يرجى ضبط عنوان الـ API في AppConfig.');
    }
    final response = await _dio.post('/activation', data: {'code': code});
    final data = response.data as Map<String, dynamic>;
    return Subscription(
      activationCode: code,
      memberName: data['memberName'] as String,
      expiresAt: DateTime.parse(data['expiresAt'] as String),
      isActive: data['isActive'] as bool? ?? true,
    );
  }

  @override
  Future<List<AppointmentSlot>> searchAppointments(String memberId) async {
    final response = await _dio.get('/appointments', queryParameters: {
      'memberId': memberId,
    });
    final data = response.data as List<dynamic>;
    return data
        .map((item) => AppointmentSlot(
              id: item['id'] as String,
              date: DateTime.parse(item['date'] as String),
              startTime: item['startTime'] as String,
              endTime: item['endTime'] as String,
              available: item['available'] as bool,
            ))
        .toList();
  }

  @override
  Future<void> bookAppointment(String slotId, String memberId) async {
    await _dio.post('/appointments/$slotId/book', data: {'memberId': memberId});
  }
}

class MockApiClient implements ApiClient {
  @override
  Future<Subscription> verifyActivation(String code) async {
    await Future<void>.delayed(const Duration(milliseconds: 600));
    return Subscription(
      activationCode: code,
      memberName: 'العميل التجريبي',
      expiresAt: DateTime.now().add(const Duration(days: 30)),
      isActive: true,
    );
  }

  @override
  Future<List<AppointmentSlot>> searchAppointments(String memberId) async {
    await Future<void>.delayed(const Duration(milliseconds: 600));
    final now = DateTime.now();
    return List.generate(4, (index) {
      final date = now.add(Duration(days: index + 1));
      return AppointmentSlot(
        id: 'slot-$index',
        date: date,
        startTime: '0${index + 9}:00',
        endTime: '0${index + 10}:00',
        available: index.isEven,
      );
    });
  }

  @override
  Future<void> bookAppointment(String slotId, String memberId) async {
    await Future<void>.delayed(const Duration(milliseconds: 500));
  }
}
