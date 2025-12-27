import 'package:equatable/equatable.dart';
import 'package:hive/hive.dart';

class Subscription extends Equatable {
  const Subscription({
    required this.activationCode,
    required this.memberName,
    required this.expiresAt,
    required this.isActive,
  });

  final String activationCode;
  final String memberName;
  final DateTime expiresAt;
  final bool isActive;

  Subscription copyWith({
    String? activationCode,
    String? memberName,
    DateTime? expiresAt,
    bool? isActive,
  }) {
    return Subscription(
      activationCode: activationCode ?? this.activationCode,
      memberName: memberName ?? this.memberName,
      expiresAt: expiresAt ?? this.expiresAt,
      isActive: isActive ?? this.isActive,
    );
  }

  @override
  List<Object?> get props => [activationCode, memberName, expiresAt, isActive];
}

class SubscriptionAdapter extends TypeAdapter<Subscription> {
  @override
  final int typeId = 2;

  @override
  Subscription read(BinaryReader reader) {
    final activationCode = reader.readString();
    final memberName = reader.readString();
    final expiresAt = DateTime.parse(reader.readString());
    final isActive = reader.readBool();
    return Subscription(
      activationCode: activationCode,
      memberName: memberName,
      expiresAt: expiresAt,
      isActive: isActive,
    );
  }

  @override
  void write(BinaryWriter writer, Subscription obj) {
    writer.writeString(obj.activationCode);
    writer.writeString(obj.memberName);
    writer.writeString(obj.expiresAt.toIso8601String());
    writer.writeBool(obj.isActive);
  }
}
