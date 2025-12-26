import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:uuid/uuid.dart';

import '../data/member_repository_impl.dart';
import '../domain/member.dart';

final memberRepositoryProvider = Provider((ref) => MemberRepositoryImpl());

class MemberState {
  const MemberState({this.items = const [], this.isLoading = false});

  final List<Member> items;
  final bool isLoading;

  MemberState copyWith({List<Member>? items, bool? isLoading}) {
    return MemberState(
      items: items ?? this.items,
      isLoading: isLoading ?? this.isLoading,
    );
  }
}

class MemberController extends StateNotifier<MemberState> {
  MemberController(this._repository) : super(const MemberState()) {
    load();
  }

  final MemberRepositoryImpl _repository;
  final _uuid = const Uuid();

  Future<void> load() async {
    state = state.copyWith(isLoading: true);
    final members = await _repository.fetchMembers();
    state = state.copyWith(items: members, isLoading: false);
  }

  Future<void> add({required String name, required String email}) async {
    final member = Member(
      id: _uuid.v4(),
      name: name,
      email: email,
      status: 'نشط',
    );
    await _repository.addMember(member);
    await load();
  }

  Future<void> update(Member member) async {
    await _repository.updateMember(member);
    await load();
  }

  Future<void> remove(String id) async {
    await _repository.deleteMember(id);
    await load();
  }
}

final memberControllerProvider =
    StateNotifierProvider<MemberController, MemberState>(
  (ref) => MemberController(ref.watch(memberRepositoryProvider)),
);
