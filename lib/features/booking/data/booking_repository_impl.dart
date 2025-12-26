import 'package:dio/dio.dart';

import '../../../core/errors.dart';
import '../../../core/network/api_client.dart';
import '../domain/appointment.dart';
import '../domain/booking_repository.dart';

class BookingRepositoryImpl implements BookingRepository {
  BookingRepositoryImpl(this._client);

  final ApiClient _client;

  @override
  Future<List<Appointment>> fetchAvailableAppointments(String memberId) async {
    try {
      final response = await _client.get(
        'https://api.example.com/appointments?memberId=$memberId',
      );
      final items = response.data?['items'] as List<dynamic>? ?? [];
      return items
          .map((item) => Appointment(
                id: item['id']?.toString() ?? '',
                date: item['date']?.toString() ?? '',
                time: item['time']?.toString() ?? '',
              ))
          .toList();
    } on DioException catch (error) {
      throw Failure(error.message ?? 'تعذر جلب المواعيد');
    }
  }

  @override
  Future<bool> bookAppointment(String appointmentId) async {
    try {
      await _client.post(
        'https://api.example.com/appointments/book',
        data: {'appointmentId': appointmentId},
      );
      return true;
    } on DioException catch (error) {
      throw Failure(error.message ?? 'تعذر تأكيد الحجز');
    }
  }
}
