import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../booking/presentation/booking_controller.dart';
import '../../pdf/data/pdf_service.dart';
import '../../pdf/presentation/pdf_viewer_screen.dart';
import '../domain/member.dart';

class MemberDetailScreen extends ConsumerWidget {
  const MemberDetailScreen({super.key, required this.member});

  final Member member;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final bookingState = ref.watch(bookingControllerProvider);
    return Scaffold(
      appBar: AppBar(title: const Text('تفاصيل العضو')),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Card(
            child: ListTile(
              title: Text(member.name),
              subtitle: Text(member.email),
              trailing: Text(member.status),
            ),
          ),
          const SizedBox(height: 16),
          ElevatedButton.icon(
            icon: const Icon(Icons.search),
            label: const Text('البحث عن المواعيد المتاحة'),
            onPressed: bookingState.isLoading
                ? null
                : () {
                    ref
                        .read(bookingControllerProvider.notifier)
                        .loadAppointments(member.id);
                  },
          ),
          const SizedBox(height: 8),
          if (bookingState.isLoading)
            const Center(child: CircularProgressIndicator()),
          if (bookingState.error != null)
            Text(
              bookingState.error!.message,
              style: TextStyle(color: Theme.of(context).colorScheme.error),
            ),
          if (bookingState.bookedMessage != null)
            Text(
              bookingState.bookedMessage!,
              style: const TextStyle(color: Colors.greenAccent),
            ),
          ...bookingState.appointments.map(
            (appointment) => Card(
              child: ListTile(
                title: Text('${appointment.date} - ${appointment.time}'),
                trailing: TextButton(
                  onPressed: () async {
                    final shouldBook = await showDialog<bool>(
                      context: context,
                      builder: (context) => AlertDialog(
                        title: const Text('تأكيد الحجز'),
                        content: const Text('هل تريد تأكيد الحجز؟'),
                        actions: [
                          TextButton(
                            onPressed: () => Navigator.pop(context, false),
                            child: const Text('إلغاء'),
                          ),
                          ElevatedButton(
                            onPressed: () => Navigator.pop(context, true),
                            child: const Text('تأكيد'),
                          ),
                        ],
                      ),
                    );
                    if (shouldBook == true) {
                      await ref
                          .read(bookingControllerProvider.notifier)
                          .book(appointment.id);
                    }
                  },
                  child: const Text('احجز'),
                ),
              ),
            ),
          ),
          const SizedBox(height: 16),
          ElevatedButton.icon(
            icon: const Icon(Icons.picture_as_pdf),
            label: const Text('حفظ وفتح ملف PDF'),
            onPressed: () async {
              final file = await PdfService().saveSamplePdf(
                'member_${member.id}.pdf',
              );
              if (context.mounted) {
                Navigator.of(context).push(
                  MaterialPageRoute(
                    builder: (_) => PdfViewerScreen(file: file),
                  ),
                );
              }
            },
          ),
        ],
      ),
    );
  }
}
