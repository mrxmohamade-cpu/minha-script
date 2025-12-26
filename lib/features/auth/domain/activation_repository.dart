import 'activation_info.dart';

abstract class ActivationRepository {
  Future<ActivationInfo> verifyActivation(String memberId);
}
