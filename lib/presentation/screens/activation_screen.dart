import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../core/errors.dart';
import '../providers/activation_controller.dart';
import '../widgets/app_error_view.dart';

class ActivationScreen extends ConsumerStatefulWidget {
  const ActivationScreen({super.key});

  @override
  ConsumerState<ActivationScreen> createState() => _ActivationScreenState();
}

class _ActivationScreenState extends ConsumerState<ActivationScreen> {
  final _controller = TextEditingController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final activationState = ref.watch(activationControllerProvider);

    return Card(
      elevation: 0,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('تفعيل الاشتراك', style: Theme.of(context).textTheme.titleLarge),
            const SizedBox(height: 12),
            TextField(
              controller: _controller,
              decoration: const InputDecoration(
                labelText: 'رمز التفعيل',
                prefixIcon: Icon(Icons.key),
              ),
            ),
            const SizedBox(height: 12),
            ElevatedButton.icon(
              onPressed: activationState is AsyncLoading
                  ? null
                  : () => ref
                      .read(activationControllerProvider.notifier)
                      .verifyActivation(_controller.text),
              icon: const Icon(Icons.check),
              label: const Text('تحقق'),
            ),
            const SizedBox(height: 16),
            activationState.when(
              data: (subscription) {
                if (subscription == null) {
                  return const Text('لم يتم تفعيل أي اشتراك بعد.');
                }
                return Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text('العميل: ${subscription.memberName}'),
                    Text('الحالة: ${subscription.isActive ? 'نشط' : 'غير نشط'}'),
                    Text('ينتهي في: ${subscription.expiresAt.toLocal()}'),
                  ],
                );
              },
              loading: () => const Center(child: CircularProgressIndicator()),
              error: (error, _) => AppErrorView(
                message: error is AppException ? error.message : 'حدث خطأ أثناء التفعيل',
              ),
            ),
          ],
        ),
      ),
    );
  }
}
