import 'package:dio/dio.dart';

class ApiClient {
  ApiClient({Dio? dio}) : _dio = dio ?? Dio();

  final Dio _dio;

  Future<Response<Map<String, dynamic>>> post(
    String path, {
    Map<String, dynamic>? data,
  }) async {
    return _dio.post<Map<String, dynamic>>(path, data: data);
  }

  Future<Response<Map<String, dynamic>>> get(String path) async {
    return _dio.get<Map<String, dynamic>>(path);
  }
}
