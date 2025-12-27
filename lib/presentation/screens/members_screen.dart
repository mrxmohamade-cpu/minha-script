import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../data/models/member.dart';
import '../providers/members_controller.dart';
import '../widgets/app_error_view.dart';
import 'member_detail_screen.dart';
import 'member_form_screen.dart';

class MembersScreen extends ConsumerWidget {
  const MembersScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final membersState = ref.watch(membersControllerProvider);

    return Card(
      elevation: 0,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Text('إدارة الأعضاء', style: Theme.of(context).textTheme.titleLarge),
                const Spacer(),
                FilledButton.icon(
                  onPressed: () async {
                    final result = await Navigator.of(context).push<Member>(
                      MaterialPageRoute(
                        builder: (_) => const MemberFormScreen(),
                      ),
                    );
                    if (result != null) {
                      await ref.read(membersControllerProvider.notifier).addMember(
                            fullName: result.fullName,
                            phone: result.phone,
                            membershipNumber: result.membershipNumber,
                            notes: result.notes,
                          );
                    }
                  },
                  icon: const Icon(Icons.add),
                  label: const Text('عضو جديد'),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Expanded(
              child: membersState.when(
                data: (members) => _MembersList(members: members),
                loading: () => const Center(child: CircularProgressIndicator()),
                error: (error, _) => AppErrorView(
                  message: 'تعذر تحميل الأعضاء',
                  onRetry: () => ref.read(membersControllerProvider.notifier).loadMembers(),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _MembersList extends ConsumerWidget {
  const _MembersList({required this.members});

  final List<Member> members;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    if (members.isEmpty) {
      return const Center(child: Text('لا يوجد أعضاء بعد.'));
    }

    return ListView.separated(
      itemCount: members.length,
      separatorBuilder: (_, __) => const Divider(),
      itemBuilder: (context, index) {
        final member = members[index];
        return ListTile(
          title: Text(member.fullName),
          subtitle: Text('رقم العضوية: ${member.membershipNumber}'),
          trailing: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              IconButton(
                icon: const Icon(Icons.edit),
                onPressed: () async {
                  final updated = await Navigator.of(context).push<Member>(
                    MaterialPageRoute(
                      builder: (_) => MemberFormScreen(member: member),
                    ),
                  );
                  if (updated != null) {
                    await ref.read(membersControllerProvider.notifier).updateMember(updated);
                  }
                },
              ),
              IconButton(
                icon: const Icon(Icons.delete),
                onPressed: () async {
                  final confirmed = await showDialog<bool>(
                    context: context,
                    builder: (context) => AlertDialog(
                      title: const Text('حذف العضو'),
                      content: const Text('هل أنت متأكد من حذف العضو؟'),
                      actions: [
                        TextButton(
                          onPressed: () => Navigator.of(context).pop(false),
                          child: const Text('إلغاء'),
                        ),
                        FilledButton(
                          onPressed: () => Navigator.of(context).pop(true),
                          child: const Text('حذف'),
                        ),
                      ],
                    ),
                  );
                  if (confirmed == true) {
                    await ref.read(membersControllerProvider.notifier).deleteMember(member.id);
                  }
                },
              ),
            ],
          ),
          onTap: () {
            Navigator.of(context).push(
              MaterialPageRoute(
                builder: (_) => MemberDetailScreen(member: member),
              ),
            );
          },
        );
      },
    );
  }
}
