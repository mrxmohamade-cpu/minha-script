import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:uuid/uuid.dart';

import '../../data/models/member.dart';
import 'app_providers.dart';

class MembersController extends StateNotifier<AsyncValue<List<Member>>> {
  MembersController(this.ref) : super(const AsyncLoading()) {
    loadMembers();
  }

  final Ref ref;

  Future<void> loadMembers() async {
    state = const AsyncLoading();
    try {
      final members = await ref.read(memberRepositoryProvider).fetchMembers();
      state = AsyncData(members);
    } catch (error, stack) {
      state = AsyncError(error, stack);
    }
  }

  Future<void> addMember({
    required String fullName,
    required String phone,
    required String membershipNumber,
    required String notes,
  }) async {
    final member = Member(
      id: const Uuid().v4(),
      fullName: fullName,
      phone: phone,
      membershipNumber: membershipNumber,
      notes: notes,
      createdAt: DateTime.now(),
      documentPaths: const [],
    );
    await ref.read(memberRepositoryProvider).upsertMember(member);
    await loadMembers();
  }

  Future<void> updateMember(Member member) async {
    await ref.read(memberRepositoryProvider).upsertMember(member);
    await loadMembers();
  }

  Future<void> deleteMember(String memberId) async {
    await ref.read(memberRepositoryProvider).deleteMember(memberId);
    await loadMembers();
  }
}

final membersControllerProvider =
    StateNotifierProvider<MembersController, AsyncValue<List<Member>>>((ref) {
  return MembersController(ref);
});
