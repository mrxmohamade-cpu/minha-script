import 'package:equatable/equatable.dart';

class AppointmentSlot extends Equatable {
  const AppointmentSlot({
    required this.id,
    required this.date,
    required this.startTime,
    required this.endTime,
    required this.available,
  });

  final String id;
  final DateTime date;
  final String startTime;
  final String endTime;
  final bool available;

  @override
  List<Object?> get props => [id, date, startTime, endTime, available];
}
