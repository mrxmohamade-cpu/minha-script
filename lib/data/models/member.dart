import 'package:equatable/equatable.dart';
import 'package:hive/hive.dart';

class Member extends Equatable {
  const Member({
    required this.id,
    required this.fullName,
    required this.phone,
    required this.membershipNumber,
    required this.notes,
    required this.createdAt,
    required this.documentPaths,
  });

  final String id;
  final String fullName;
  final String phone;
  final String membershipNumber;
  final String notes;
  final DateTime createdAt;
  final List<String> documentPaths;

  Member copyWith({
    String? fullName,
    String? phone,
    String? membershipNumber,
    String? notes,
    DateTime? createdAt,
    List<String>? documentPaths,
  }) {
    return Member(
      id: id,
      fullName: fullName ?? this.fullName,
      phone: phone ?? this.phone,
      membershipNumber: membershipNumber ?? this.membershipNumber,
      notes: notes ?? this.notes,
      createdAt: createdAt ?? this.createdAt,
      documentPaths: documentPaths ?? this.documentPaths,
    );
  }

  @override
  List<Object?> get props => [
        id,
        fullName,
        phone,
        membershipNumber,
        notes,
        createdAt,
        documentPaths,
      ];
}

class MemberAdapter extends TypeAdapter<Member> {
  @override
  final int typeId = 1;

  @override
  Member read(BinaryReader reader) {
    final id = reader.readString();
    final fullName = reader.readString();
    final phone = reader.readString();
    final membershipNumber = reader.readString();
    final notes = reader.readString();
    final createdAt = DateTime.parse(reader.readString());
    final documentPaths = reader.readList().cast<String>();
    return Member(
      id: id,
      fullName: fullName,
      phone: phone,
      membershipNumber: membershipNumber,
      notes: notes,
      createdAt: createdAt,
      documentPaths: documentPaths,
    );
  }

  @override
  void write(BinaryWriter writer, Member obj) {
    writer.writeString(obj.id);
    writer.writeString(obj.fullName);
    writer.writeString(obj.phone);
    writer.writeString(obj.membershipNumber);
    writer.writeString(obj.notes);
    writer.writeString(obj.createdAt.toIso8601String());
    writer.writeList(obj.documentPaths);
  }
}
