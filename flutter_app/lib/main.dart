import 'dart:io';

import 'package:firebase_core/firebase_core.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_background_service/flutter_background_service.dart';

import 'firebase_options.dart';
import 'services/background_service.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  await Firebase.initializeApp(
    options: DefaultFirebaseOptions.currentPlatform,
  );

  await BackgroundServiceManager.instance.initialize();

  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Flutter Clean App',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.blue),
        useMaterial3: true,
      ),
      home: const HomePage(),
    );
  }
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  bool _running = false;
  bool get _supportsService => !kIsWeb && Platform.isAndroid;

  @override
  void initState() {
    super.initState();
    _loadState();
  }

  Future<void> _loadState() async {
    final isRunning = await BackgroundServiceManager.instance.isRunning();
    setState(() => _running = isRunning);
  }

  Future<void> _start() async {
    final started = await BackgroundServiceManager.instance.startService();
    setState(() => _running = started);
  }

  Future<void> _stop() async {
    await FlutterBackgroundService().invoke('stopService');
    final stopped = await BackgroundServiceManager.instance.stopService();
    setState(() => _running = !stopped);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Clean Flutter App'),
      ),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Firebase initialized: ${Firebase.apps.isNotEmpty}',
              style: Theme.of(context).textTheme.titleMedium,
            ),
            const SizedBox(height: 12),
            Text(
              _supportsService
                  ? 'Background service is ${_running ? 'running' : 'stopped'}.'
                  : 'Background service is only available on Android.',
            ),
            const SizedBox(height: 24),
            if (_supportsService) ...[
              ElevatedButton(
                onPressed: _running ? null : _start,
                child: const Text('Start Service'),
              ),
              const SizedBox(height: 8),
              ElevatedButton(
                onPressed: _running ? _stop : null,
                child: const Text('Stop Service'),
              ),
            ],
          ],
        ),
      ),
    );
  }
}
