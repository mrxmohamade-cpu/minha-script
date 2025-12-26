class Member {
  Member({
    required this.id,
    required this.name,
    required this.email,
    required this.status,
  });

  final String id;
  final String name;
  final String email;
  final String status;

  Map<String, dynamic> toMap() => {
        'id': id,
        'name': name,
        'email': email,
        'status': status,
      };

  factory Member.fromMap(Map<String, dynamic> map) {
    return Member(
      id: map['id']?.toString() ?? '',
      name: map['name']?.toString() ?? '',
      email: map['email']?.toString() ?? '',
      status: map['status']?.toString() ?? '',
    );
  }
}
