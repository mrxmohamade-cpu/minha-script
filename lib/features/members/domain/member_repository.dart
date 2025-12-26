import 'member.dart';

abstract class MemberRepository {
  Future<List<Member>> fetchMembers();
  Future<void> addMember(Member member);
  Future<void> updateMember(Member member);
  Future<void> deleteMember(String id);
}
