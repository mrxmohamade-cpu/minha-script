import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../data/models/appointment.dart';
import 'app_providers.dart';

class AppointmentController extends StateNotifier<AsyncValue<List<AppointmentSlot>>> {
  AppointmentController(this.ref) : super(const AsyncData([]));

  final Ref ref;

  Future<void> searchAppointments(String memberId) async {
    state = const AsyncLoading();
    try {
      final slots = await ref.read(bookingRepositoryProvider).searchAppointments(memberId);
      state = AsyncData(slots);
    } catch (error, stack) {
      state = AsyncError(error, stack);
    }
  }

  Future<void> bookAppointment(String slotId, String memberId) async {
    try {
      await ref.read(bookingRepositoryProvider).bookAppointment(slotId, memberId);
    } catch (error) {
      rethrow;
    }
  }
}

final appointmentControllerProvider =
    StateNotifierProvider<AppointmentController, AsyncValue<List<AppointmentSlot>>>((ref) {
  return AppointmentController(ref);
});
