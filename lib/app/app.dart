import 'package:flutter/material.dart';

import '../features/auth/presentation/activation_screen.dart';
import '../features/members/presentation/member_list_screen.dart';

class AppRoot extends StatefulWidget {
  const AppRoot({super.key});

  @override
  State<AppRoot> createState() => _AppRootState();
}

class _AppRootState extends State<AppRoot> {
  int _index = 0;

  @override
  Widget build(BuildContext context) {
    final screens = [
      const ActivationScreen(),
      const MemberListScreen(),
    ];
    return Scaffold(
      body: screens[_index],
      bottomNavigationBar: NavigationBar(
        selectedIndex: _index,
        onDestinationSelected: (value) {
          setState(() => _index = value);
        },
        destinations: const [
          NavigationDestination(icon: Icon(Icons.verified_user), label: 'التفعيل'),
          NavigationDestination(icon: Icon(Icons.people), label: 'الأعضاء'),
        ],
      ),
    );
  }
}
