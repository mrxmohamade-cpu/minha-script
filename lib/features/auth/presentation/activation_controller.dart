import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../../core/errors.dart';
import '../../../core/network/api_client.dart';
import '../data/activation_repository_impl.dart';
import '../domain/activation_info.dart';

final activationRepositoryProvider = Provider(
  (ref) => ActivationRepositoryImpl(ApiClient()),
);

class ActivationState {
  const ActivationState({
    this.isLoading = false,
    this.error,
    this.info,
  });

  final bool isLoading;
  final Failure? error;
  final ActivationInfo? info;

  ActivationState copyWith({
    bool? isLoading,
    Failure? error,
    ActivationInfo? info,
  }) {
    return ActivationState(
      isLoading: isLoading ?? this.isLoading,
      error: error,
      info: info ?? this.info,
    );
  }
}

class ActivationController extends StateNotifier<ActivationState> {
  ActivationController(this._repository) : super(const ActivationState());

  final ActivationRepositoryImpl _repository;

  Future<void> verify(String memberId) async {
    state = state.copyWith(isLoading: true, error: null);
    try {
      final info = await _repository.verifyActivation(memberId);
      state = state.copyWith(isLoading: false, info: info);
    } on Failure catch (error) {
      state = state.copyWith(isLoading: false, error: error);
    }
  }
}

final activationControllerProvider =
    StateNotifierProvider<ActivationController, ActivationState>(
  (ref) => ActivationController(ref.watch(activationRepositoryProvider)),
);
