import 'package:flutter/material.dart';
import 'package:uuid/uuid.dart';

import '../../data/models/member.dart';

class MemberFormScreen extends StatefulWidget {
  const MemberFormScreen({super.key, this.member});

  final Member? member;

  @override
  State<MemberFormScreen> createState() => _MemberFormScreenState();
}

class _MemberFormScreenState extends State<MemberFormScreen> {
  late final TextEditingController _nameController;
  late final TextEditingController _phoneController;
  late final TextEditingController _membershipController;
  late final TextEditingController _notesController;
  final _formKey = GlobalKey<FormState>();

  @override
  void initState() {
    super.initState();
    _nameController = TextEditingController(text: widget.member?.fullName ?? '');
    _phoneController = TextEditingController(text: widget.member?.phone ?? '');
    _membershipController = TextEditingController(text: widget.member?.membershipNumber ?? '');
    _notesController = TextEditingController(text: widget.member?.notes ?? '');
  }

  @override
  void dispose() {
    _nameController.dispose();
    _phoneController.dispose();
    _membershipController.dispose();
    _notesController.dispose();
    super.dispose();
  }

  void _submit() {
    if (!_formKey.currentState!.validate()) {
      return;
    }
    final member = widget.member;
    final result = Member(
      id: member?.id ?? const Uuid().v4(),
      fullName: _nameController.text.trim(),
      phone: _phoneController.text.trim(),
      membershipNumber: _membershipController.text.trim(),
      notes: _notesController.text.trim(),
      createdAt: member?.createdAt ?? DateTime.now(),
      documentPaths: member?.documentPaths ?? const [],
    );
    Navigator.of(context).pop(result);
  }

  @override
  Widget build(BuildContext context) {
    final isEditing = widget.member != null;
    return Scaffold(
      appBar: AppBar(
        title: Text(isEditing ? 'تعديل عضو' : 'عضو جديد'),
      ),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Form(
          key: _formKey,
          child: ListView(
            children: [
              TextFormField(
                controller: _nameController,
                decoration: const InputDecoration(labelText: 'الاسم الكامل'),
                validator: (value) => value == null || value.trim().isEmpty ? 'مطلوب' : null,
              ),
              const SizedBox(height: 12),
              TextFormField(
                controller: _phoneController,
                decoration: const InputDecoration(labelText: 'رقم الهاتف'),
                validator: (value) => value == null || value.trim().isEmpty ? 'مطلوب' : null,
              ),
              const SizedBox(height: 12),
              TextFormField(
                controller: _membershipController,
                decoration: const InputDecoration(labelText: 'رقم العضوية'),
                validator: (value) => value == null || value.trim().isEmpty ? 'مطلوب' : null,
              ),
              const SizedBox(height: 12),
              TextFormField(
                controller: _notesController,
                maxLines: 3,
                decoration: const InputDecoration(labelText: 'ملاحظات'),
              ),
              const SizedBox(height: 16),
              FilledButton(
                onPressed: _submit,
                child: Text(isEditing ? 'حفظ' : 'إضافة'),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
