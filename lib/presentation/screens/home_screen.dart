import 'package:flutter/material.dart';

import 'activation_screen.dart';
import 'members_screen.dart';

class HomeScreen extends StatelessWidget {
  const HomeScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return DefaultTabController(
      length: 2,
      child: Scaffold(
        appBar: AppBar(
          title: const Text('منصة العضوية'),
          bottom: const TabBar(
            tabs: [
              Tab(text: 'تفعيل'),
              Tab(text: 'الأعضاء'),
            ],
          ),
        ),
        body: const TabBarView(
          children: [
            Padding(
              padding: EdgeInsets.all(12),
              child: ActivationScreen(),
            ),
            Padding(
              padding: EdgeInsets.all(12),
              child: MembersScreen(),
            ),
          ],
        ),
      ),
    );
  }
}
