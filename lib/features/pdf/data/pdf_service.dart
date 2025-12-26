import 'dart:io';
import 'dart:typed_data';

import 'package:path_provider/path_provider.dart';

class PdfService {
  Future<File> saveSamplePdf(String fileName) async {
    final directory = await getApplicationDocumentsDirectory();
    final file = File('${directory.path}/$fileName');
    final bytes = Uint8List.fromList(_minimalPdfBytes());
    return file.writeAsBytes(bytes, flush: true);
  }

  List<int> _minimalPdfBytes() {
    const pdf = '%PDF-1.4\n'
        '1 0 obj\n'
        '<< /Type /Catalog /Pages 2 0 R >>\n'
        'endobj\n'
        '2 0 obj\n'
        '<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n'
        'endobj\n'
        '3 0 obj\n'
        '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] '
        '/Contents 4 0 R >>\n'
        'endobj\n'
        '4 0 obj\n'
        '<< /Length 44 >>\n'
        'stream\n'
        'BT /F1 18 Tf 10 100 Td (Sample PDF) Tj ET\n'
        'endstream\n'
        'endobj\n'
        'xref\n'
        '0 5\n'
        '0000000000 65535 f\n'
        '0000000010 00000 n\n'
        '0000000060 00000 n\n'
        '0000000117 00000 n\n'
        '0000000200 00000 n\n'
        'trailer\n'
        '<< /Root 1 0 R /Size 5 >>\n'
        'startxref\n'
        '280\n'
        '%%EOF';
    return pdf.codeUnits;
  }
}
