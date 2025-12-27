import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:intl/intl.dart';

import '../../data/models/member.dart';
import '../providers/appointment_controller.dart';
import '../providers/app_providers.dart';
import '../providers/members_controller.dart';
import '../widgets/app_error_view.dart';
import 'pdf_viewer_screen.dart';

class MemberDetailScreen extends ConsumerStatefulWidget {
  const MemberDetailScreen({super.key, required this.member});

  final Member member;

  @override
  ConsumerState<MemberDetailScreen> createState() => _MemberDetailScreenState();
}

class _MemberDetailScreenState extends ConsumerState<MemberDetailScreen> {
  late Member _member;

  @override
  void initState() {
    super.initState();
    _member = widget.member;
  }

  Future<void> _addSamplePdf() async {
    final repository = ref.read(documentRepositoryProvider);
    final path = await repository.createSamplePdf(_member.fullName);
    final updated = _member.copyWith(documentPaths: [..._member.documentPaths, path]);
    await ref.read(membersControllerProvider.notifier).updateMember(updated);
    setState(() {
      _member = updated;
    });
  }

  @override
  Widget build(BuildContext context) {
    final appointmentState = ref.watch(appointmentControllerProvider);
    final dateFormatter = DateFormat.yMMMd('ar');

    return Scaffold(
      appBar: AppBar(title: const Text('تفاصيل العضو')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: ListView(
          children: [
            Text(_member.fullName, style: Theme.of(context).textTheme.headlineSmall),
            const SizedBox(height: 4),
            Text('رقم العضوية: ${_member.membershipNumber}'),
            Text('الهاتف: ${_member.phone}'),
            Text('تاريخ الإضافة: ${dateFormatter.format(_member.createdAt)}'),
            const SizedBox(height: 16),
            Text('ملاحظات', style: Theme.of(context).textTheme.titleMedium),
            Text(_member.notes.isEmpty ? 'لا توجد ملاحظات.' : _member.notes),
            const SizedBox(height: 16),
            Row(
              children: [
                Text('ملفات PDF', style: Theme.of(context).textTheme.titleMedium),
                const Spacer(),
                OutlinedButton.icon(
                  onPressed: _addSamplePdf,
                  icon: const Icon(Icons.picture_as_pdf),
                  label: const Text('إضافة PDF'),
                ),
              ],
            ),
            const SizedBox(height: 8),
            if (_member.documentPaths.isEmpty)
              const Text('لا توجد ملفات بعد.')
            else
              ..._member.documentPaths.map(
                (path) => ListTile(
                  leading: const Icon(Icons.picture_as_pdf),
                  title: Text(path.split('/').last),
                  onTap: () {
                    Navigator.of(context).push(
                      MaterialPageRoute(
                        builder: (_) => PdfViewerScreen(path: path),
                      ),
                    );
                  },
                ),
              ),
            const SizedBox(height: 20),
            Text('المواعيد المتاحة', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            ElevatedButton.icon(
              onPressed: () => ref
                  .read(appointmentControllerProvider.notifier)
                  .searchAppointments(_member.id),
              icon: const Icon(Icons.search),
              label: const Text('بحث عن المواعيد'),
            ),
            const SizedBox(height: 8),
            appointmentState.when(
              data: (slots) {
                if (slots.isEmpty) {
                  return const Text('لا توجد نتائج بعد.');
                }
                return Column(
                  children: slots.map((slot) {
                    return Card(
                      child: ListTile(
                        title: Text(dateFormatter.format(slot.date)),
                        subtitle: Text('${slot.startTime} - ${slot.endTime}'),
                        trailing: slot.available
                            ? FilledButton(
                                onPressed: () async {
                                  final confirmed = await showDialog<bool>(
                                    context: context,
                                    builder: (context) => AlertDialog(
                                      title: const Text('تأكيد الحجز'),
                                      content: const Text('هل ترغب بحجز هذا الموعد؟'),
                                      actions: [
                                        TextButton(
                                          onPressed: () => Navigator.of(context).pop(false),
                                          child: const Text('إلغاء'),
                                        ),
                                        FilledButton(
                                          onPressed: () => Navigator.of(context).pop(true),
                                          child: const Text('حجز'),
                                        ),
                                      ],
                                    ),
                                  );
                                  if (confirmed == true) {
                                    await ref
                                        .read(appointmentControllerProvider.notifier)
                                        .bookAppointment(slot.id, _member.id);
                                    if (mounted) {
                                      ScaffoldMessenger.of(context).showSnackBar(
                                        const SnackBar(content: Text('تم إرسال طلب الحجز.')),
                                      );
                                    }
                                  }
                                },
                                child: const Text('حجز'),
                              )
                            : const Text('غير متاح'),
                      ),
                    );
                  }).toList(),
                );
              },
              loading: () => const Center(child: CircularProgressIndicator()),
              error: (error, _) => AppErrorView(message: 'تعذر جلب المواعيد.'),
            ),
          ],
        ),
      ),
    );
  }
}
