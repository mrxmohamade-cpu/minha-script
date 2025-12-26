import '../../../core/storage/local_storage.dart';
import '../domain/member.dart';
import '../domain/member_repository.dart';

class MemberRepositoryImpl implements MemberRepository {
  @override
  Future<List<Member>> fetchMembers() async {
    final box = LocalStorage.memberBox();
    return box.values
        .map((data) => Member.fromMap(Map<String, dynamic>.from(data)))
        .toList();
  }

  @override
  Future<void> addMember(Member member) async {
    await LocalStorage.memberBox().put(member.id, member.toMap());
  }

  @override
  Future<void> updateMember(Member member) async {
    await LocalStorage.memberBox().put(member.id, member.toMap());
  }

  @override
  Future<void> deleteMember(String id) async {
    await LocalStorage.memberBox().delete(id);
  }
}
