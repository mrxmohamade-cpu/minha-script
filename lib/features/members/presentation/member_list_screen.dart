import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../domain/member.dart';
import 'member_controller.dart';
import 'member_detail_screen.dart';

class MemberListScreen extends ConsumerWidget {
  const MemberListScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final state = ref.watch(memberControllerProvider);
    return Scaffold(
      appBar: AppBar(title: const Text('إدارة الأعضاء')),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () => _openForm(context, ref),
        icon: const Icon(Icons.person_add),
        label: const Text('إضافة عضو'),
      ),
      body: state.isLoading
          ? const Center(child: CircularProgressIndicator())
          : ListView.separated(
              padding: const EdgeInsets.all(16),
              itemBuilder: (context, index) {
                final member = state.items[index];
                return ListTile(
                  title: Text(member.name),
                  subtitle: Text(member.email),
                  trailing: PopupMenuButton<String>(
                    onSelected: (value) {
                      if (value == 'edit') {
                        _openForm(context, ref, member: member);
                      } else if (value == 'delete') {
                        ref.read(memberControllerProvider.notifier).remove(member.id);
                      }
                    },
                    itemBuilder: (context) => const [
                      PopupMenuItem(value: 'edit', child: Text('تعديل')),
                      PopupMenuItem(value: 'delete', child: Text('حذف')),
                    ],
                  ),
                  onTap: () => Navigator.of(context).push(
                    MaterialPageRoute(
                      builder: (_) => MemberDetailScreen(member: member),
                    ),
                  ),
                );
              },
              separatorBuilder: (_, __) => const Divider(),
              itemCount: state.items.length,
            ),
    );
  }

  Future<void> _openForm(BuildContext context, WidgetRef ref, {Member? member}) {
    return showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      builder: (_) => MemberFormSheet(member: member),
    );
  }
}

class MemberFormSheet extends ConsumerStatefulWidget {
  const MemberFormSheet({super.key, this.member});

  final Member? member;

  @override
  ConsumerState<MemberFormSheet> createState() => _MemberFormSheetState();
}

class _MemberFormSheetState extends ConsumerState<MemberFormSheet> {
  final _formKey = GlobalKey<FormState>();
  late final TextEditingController _nameController;
  late final TextEditingController _emailController;

  @override
  void initState() {
    super.initState();
    _nameController = TextEditingController(text: widget.member?.name ?? '');
    _emailController = TextEditingController(text: widget.member?.email ?? '');
  }

  @override
  void dispose() {
    _nameController.dispose();
    _emailController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: EdgeInsets.only(
        left: 16,
        right: 16,
        top: 16,
        bottom: MediaQuery.of(context).viewInsets.bottom + 16,
      ),
      child: Form(
        key: _formKey,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(
              widget.member == null ? 'إضافة عضو' : 'تعديل عضو',
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 16),
            TextFormField(
              controller: _nameController,
              decoration: const InputDecoration(labelText: 'اسم العضو'),
              validator: (value) {
                if (value == null || value.isEmpty) {
                  return 'الاسم مطلوب';
                }
                return null;
              },
            ),
            const SizedBox(height: 12),
            TextFormField(
              controller: _emailController,
              decoration: const InputDecoration(labelText: 'البريد الإلكتروني'),
              validator: (value) {
                if (value == null || value.isEmpty) {
                  return 'البريد مطلوب';
                }
                return null;
              },
            ),
            const SizedBox(height: 16),
            ElevatedButton(
              onPressed: () async {
                if (_formKey.currentState?.validate() ?? false) {
                  if (widget.member == null) {
                    await ref
                        .read(memberControllerProvider.notifier)
                        .add(
                          name: _nameController.text.trim(),
                          email: _emailController.text.trim(),
                        );
                  } else {
                    await ref.read(memberControllerProvider.notifier).update(
                          Member(
                            id: widget.member!.id,
                            name: _nameController.text.trim(),
                            email: _emailController.text.trim(),
                            status: widget.member!.status,
                          ),
                        );
                  }
                  if (context.mounted) {
                    Navigator.of(context).pop();
                  }
                }
              },
              child: const Text('حفظ'),
            ),
          ],
        ),
      ),
    );
  }
}
