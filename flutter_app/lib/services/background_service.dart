import 'dart:async';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter_background_service/flutter_background_service.dart';
import 'package:flutter_background_service_android/flutter_background_service_android.dart';

class BackgroundServiceManager {
  BackgroundServiceManager._();
  static final BackgroundServiceManager instance = BackgroundServiceManager._();

  final FlutterBackgroundService _service = FlutterBackgroundService();

  Future<void> initialize() async {
    if (!Platform.isAndroid) return;

    await _service.configure(
      androidConfiguration: AndroidConfiguration(
        onStart: _onStart,
        isForegroundMode: true,
        autoStart: false,
        notificationChannelId: 'flutter_background_service',
        initialNotificationTitle: 'Service Running',
        initialNotificationContent: 'Monitoring in background',
      ),
    );
  }

  Future<bool> startService() async {
    if (!Platform.isAndroid) return false;
    return _service.startService();
  }

  Future<bool> stopService() async {
    if (!Platform.isAndroid) return false;
    return _service.stopService();
  }

  Future<bool> isRunning() async {
    if (!Platform.isAndroid) return false;
    return _service.isRunning();
  }
}

@pragma('vm:entry-point')
void _onStart(ServiceInstance service) async {
  DartPluginRegistrant.ensureInitialized();

  if (service is AndroidServiceInstance) {
    service.on('stopService').listen((_) {
      service.stopSelf();
    });
  }

  Timer.periodic(const Duration(minutes: 15), (timer) async {
    if (service is AndroidServiceInstance) {
      await service.setForegroundNotificationInfo(
        title: 'Service Running',
        content: 'Last update: ${DateTime.now()}',
      );
    }
  });
}
