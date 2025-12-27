import 'package:flutter/material.dart';

ThemeData buildDarkTheme() {
  const colorScheme = ColorScheme.dark(
    primary: Color(0xFF4CAF50),
    secondary: Color(0xFF03A9F4),
    surface: Color(0xFF121212),
    error: Color(0xFFE53935),
  );

  return ThemeData(
    useMaterial3: true,
    colorScheme: colorScheme,
    scaffoldBackgroundColor: colorScheme.surface,
    appBarTheme: const AppBarTheme(
      backgroundColor: Color(0xFF1E1E1E),
      foregroundColor: Colors.white,
      elevation: 0,
    ),
    inputDecorationTheme: InputDecorationTheme(
      filled: true,
      fillColor: const Color(0xFF2C2C2C),
      border: OutlineInputBorder(
        borderRadius: BorderRadius.circular(12),
      ),
    ),
  );
}
