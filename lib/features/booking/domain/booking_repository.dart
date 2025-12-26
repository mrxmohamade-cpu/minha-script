import 'appointment.dart';

abstract class BookingRepository {
  Future<List<Appointment>> fetchAvailableAppointments(String memberId);
  Future<bool> bookAppointment(String appointmentId);
}
