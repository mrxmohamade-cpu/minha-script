import 'package:hive/hive.dart';

import '../../core/constants.dart';
import '../models/member.dart';

class MemberRepository {
  Future<List<Member>> fetchMembers() async {
    final box = Hive.box<Member>(AppConfig.membersBox);
    return box.values.toList();
  }

  Future<void> upsertMember(Member member) async {
    final box = Hive.box<Member>(AppConfig.membersBox);
    await box.put(member.id, member);
  }

  Future<void> deleteMember(String memberId) async {
    final box = Hive.box<Member>(AppConfig.membersBox);
    await box.delete(memberId);
  }

  Future<Member?> getMember(String memberId) async {
    final box = Hive.box<Member>(AppConfig.membersBox);
    return box.get(memberId);
  }
}
