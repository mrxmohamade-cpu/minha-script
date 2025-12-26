import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../../core/errors.dart';
import '../../../core/network/api_client.dart';
import '../data/booking_repository_impl.dart';
import '../domain/appointment.dart';

final bookingRepositoryProvider = Provider(
  (ref) => BookingRepositoryImpl(ApiClient()),
);

class BookingState {
  const BookingState({
    this.isLoading = false,
    this.error,
    this.appointments = const [],
    this.bookedMessage,
  });

  final bool isLoading;
  final Failure? error;
  final List<Appointment> appointments;
  final String? bookedMessage;

  BookingState copyWith({
    bool? isLoading,
    Failure? error,
    List<Appointment>? appointments,
    String? bookedMessage,
  }) {
    return BookingState(
      isLoading: isLoading ?? this.isLoading,
      error: error,
      appointments: appointments ?? this.appointments,
      bookedMessage: bookedMessage,
    );
  }
}

class BookingController extends StateNotifier<BookingState> {
  BookingController(this._repository) : super(const BookingState());

  final BookingRepositoryImpl _repository;

  Future<void> loadAppointments(String memberId) async {
    state = state.copyWith(isLoading: true, error: null, bookedMessage: null);
    try {
      final appointments = await _repository.fetchAvailableAppointments(memberId);
      state = state.copyWith(isLoading: false, appointments: appointments);
    } on Failure catch (error) {
      state = state.copyWith(isLoading: false, error: error);
    }
  }

  Future<void> book(String appointmentId) async {
    state = state.copyWith(isLoading: true, error: null, bookedMessage: null);
    try {
      await _repository.bookAppointment(appointmentId);
      state = state.copyWith(isLoading: false, bookedMessage: 'تم الحجز بنجاح');
    } on Failure catch (error) {
      state = state.copyWith(isLoading: false, error: error);
    }
  }
}

final bookingControllerProvider =
    StateNotifierProvider<BookingController, BookingState>(
  (ref) => BookingController(ref.watch(bookingRepositoryProvider)),
);
