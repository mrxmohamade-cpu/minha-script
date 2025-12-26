import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import 'activation_controller.dart';

class ActivationScreen extends ConsumerStatefulWidget {
  const ActivationScreen({super.key});

  @override
  ConsumerState<ActivationScreen> createState() => _ActivationScreenState();
}

class _ActivationScreenState extends ConsumerState<ActivationScreen> {
  final _formKey = GlobalKey<FormState>();
  final _memberIdController = TextEditingController();

  @override
  void dispose() {
    _memberIdController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final state = ref.watch(activationControllerProvider);
    return Scaffold(
      appBar: AppBar(title: const Text('تفعيل/اشتراك')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Form(
              key: _formKey,
              child: TextFormField(
                controller: _memberIdController,
                decoration: const InputDecoration(
                  labelText: 'رقم العضو',
                ),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'أدخل رقم العضو';
                  }
                  return null;
                },
              ),
            ),
            const SizedBox(height: 16),
            ElevatedButton(
              onPressed: state.isLoading
                  ? null
                  : () {
                      if (_formKey.currentState?.validate() ?? false) {
                        ref
                            .read(activationControllerProvider.notifier)
                            .verify(_memberIdController.text.trim());
                      }
                    },
              child: state.isLoading
                  ? const SizedBox(
                      height: 18,
                      width: 18,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Text('تحقق من الاشتراك'),
            ),
            const SizedBox(height: 24),
            if (state.error != null)
              Text(
                state.error!.message,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            if (state.info != null)
              Card(
                child: ListTile(
                  title: Text('الاشتراك: ${state.info!.subscriptionId}'),
                  subtitle: Text('ينتهي في: ${state.info!.expiryDate}'),
                ),
              ),
          ],
        ),
      ),
    );
  }
}
