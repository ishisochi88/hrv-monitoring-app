import 'dart:convert';
import 'dart:math' as math;
import 'dart:async';
import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

void main() {
  runApp(const BioSignalApp());
}

class BioSignalApp extends StatelessWidget {
  const BioSignalApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'BioSignal Monitor',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF050816),
        useMaterial3: true,
      ),
      home: const DashboardPage(),
    );
  }
}

class DashboardPage extends StatefulWidget {
  const DashboardPage({super.key});

  @override
  State<DashboardPage> createState() => _DashboardPageState();
}

class _DashboardPageState extends State<DashboardPage> {
  late final WebSocketChannel _channel;
  final List<FlSpot> _ppgData = [];

  double heartRate = 0;
  double rmssd = 0;
  double signalQuality = 0;
  double rmssdConfidence = 0;
  double polarRmssd = 0;
  double rmssdAgreement = 0;
  double rmssdErrorMs = 0;

  bool isMeasuring = false;
  bool connectedBitalino = false;
  bool connectedPolar = false;
  bool canSave = false;
  bool pendingAutoSave = false;

  String backendMessage = 'Press Start';
  String lastSavedSummaryFile = '';
  String lastSavedRawFile = '';
  String floatingMessage = '';
  Color floatingMessageColor = const Color(0xFF58B8FF);


  Timer? protocolTimer;

  bool isProtocolRunning = false;
  int protocolElapsedSec = 0;
  int protocolTotalSec = 240;

  String selectedProtocol = 'Daily Motion Sequence';
  String currentPhaseLabel = 'Ready';
  String currentInstruction = 'Start Protocol';

  final Map<String, List<Map<String, dynamic>>> protocolDefinitions = {
    'Rest 3 min': [
      {'start': 0, 'end': 180, 'label': 'Rest', 'instruction': '安静'},
    ],
    'Walking 2 km/h 3 min': [
      {'start': 0, 'end': 180, 'label': 'Walking 2 km/h', 'instruction': '歩行 2 km/h'},
    ],
    'Fast Walking 4 km/h 3 min': [
      {'start': 0, 'end': 180, 'label': 'Walking 4 km/h', 'instruction': '歩行 4 km/h'},
    ],
    'Running 6 km/h 3 min': [
      {'start': 0, 'end': 180, 'label': 'Running 6 km/h', 'instruction': '走行 6 km/h'},
    ],
    'Walking + Head Rotation 3 min': [
      {'start': 0, 'end': 180, 'label': 'Walking + Head Rotation', 'instruction': '左'},
    ],
    'Daily Motion Sequence': [
      {'start': 0, 'end': 30, 'label': 'Rest', 'instruction': '安静'},
      {'start': 30, 'end': 60, 'label': 'Walking', 'instruction': '歩行'},
      {
        'start': 60,
        'end': 90,
        'label': 'Walking + Head Rotation',
        'instruction': '左',
      },
      {'start': 90, 'end': 120, 'label': 'Stop Standing', 'instruction': '停止'},
      {'start': 120, 'end': 150, 'label': 'Walking', 'instruction': '歩行'},
      {
        'start': 150,
        'end': 180,
        'label': 'Walking + Head Rotation',
        'instruction': '左',
      },
      {
        'start': 180,
        'end': 240,
        'label': 'Rest Recovery',
        'instruction': '回復安静',
      },
    ],
  };

  List<Map<String, dynamic>> get currentProtocol =>
      protocolDefinitions[selectedProtocol] ?? protocolDefinitions['Daily Motion Sequence']!;

  static const double _displayWindowSec = 3.0;
  static const int _maxPoints = 4000;

  @override
  void initState() {
    super.initState();
    _connectWebSocket();
  }

  void _connectWebSocket() {
    _channel = WebSocketChannel.connect(Uri.parse('ws://192.168.137.1:8765'));

    _channel.stream.listen(
      (message) {
        final data = jsonDecode(message);

        if (data['type'] == 'ack') {
          final ok = data['ok'] == true;
          final msg = (data['message'] ?? '').toString();

          setState(() {
            if (data['is_measuring'] != null) {
              isMeasuring = data['is_measuring'] == true;
            }
            if (data['can_save'] != null) {
              canSave = data['can_save'] == true;
            }
            if (data['connected_bitalino'] != null) {
              connectedBitalino = data['connected_bitalino'] == true;
            }
            if (data['connected_polar'] != null) {
              connectedPolar = data['connected_polar'] == true;
            }
            if (data['last_saved_summary_file'] != null) {
              lastSavedSummaryFile = data['last_saved_summary_file'].toString();
            }
            if (data['last_saved_raw_file'] != null) {
              lastSavedRawFile = data['last_saved_raw_file'].toString();
            }
            if (msg.isNotEmpty) {
              backendMessage = msg;
            }
          });

          _handlePendingAutoSave();

          if (msg.isNotEmpty) {
            _showFloatingMessage(msg, ok: ok);
          }
          return;
        }

        final timestamp = data['timestamp'];
        final ppg = data['ppg'];

        final double hr = (data['heart_rate'] as num?)?.toDouble() ?? 0.0;
        final double currentRmssd = (data['rmssd'] as num?)?.toDouble() ?? 0.0;
        final double currentSignal =
            (data['signal_quality'] as num?)?.toDouble() ?? 0.0;
        final double currentRmssdConfidence =
            (data['rmssd_confidence'] as num?)?.toDouble() ?? 0.0;
        final double currentPolarRmssd =
            (data['polar_rmssd'] as num?)?.toDouble() ?? 0.0;
        final double currentAgreement =
            (data['rmssd_agreement'] as num?)?.toDouble() ?? 0.0;
        final double currentErrorMs =
            (data['rmssd_error_ms'] as num?)?.toDouble() ?? 0.0;

        setState(() {
          isMeasuring = data['is_measuring'] == true;
          connectedBitalino = data['connected_bitalino'] == true;
          connectedPolar = data['connected_polar'] == true;
          canSave = data['can_save'] == true;
          backendMessage = (data['status_message'] ?? backendMessage)
              .toString();

          if (data['last_saved_summary_file'] != null) {
            lastSavedSummaryFile = data['last_saved_summary_file'].toString();
          }
          if (data['last_saved_raw_file'] != null) {
            lastSavedRawFile = data['last_saved_raw_file'].toString();
          }

          if (timestamp != null && ppg != null) {
            final double t = (timestamp as num).toDouble();
            final double y = (ppg as num).toDouble();

            _ppgData.add(FlSpot(t, y));

            final double cutoff = t - _displayWindowSec;
            _ppgData.removeWhere((spot) => spot.x < cutoff);

            if (_ppgData.length > _maxPoints) {
              _ppgData.removeRange(0, _ppgData.length - _maxPoints);
            }
          }

          heartRate = hr;
          rmssd = currentRmssd;
          signalQuality = currentSignal;
          rmssdConfidence = currentRmssdConfidence;
          polarRmssd = currentPolarRmssd;
          rmssdAgreement = currentAgreement;
          rmssdErrorMs = currentErrorMs;
        });

        _handlePendingAutoSave();
      },
      onError: (error) {
        debugPrint('WebSocket error: $error');
      },
      onDone: () {
        debugPrint('WebSocket closed');
      },
    );
  }

  void _sendCommand(String command, {String? fileName}) {
    final payload = <String, dynamic>{"command": command};

    if (fileName != null && fileName.trim().isNotEmpty) {
      payload["file_name"] = fileName.trim();
    }

    _channel.sink.add(jsonEncode(payload));
  }

  void _showFloatingMessage(String message, {bool ok = true}) {
    setState(() {
      floatingMessage = message;
      floatingMessageColor = ok
          ? const Color(0xFF3DFF7A)
          : const Color(0xFFFF6363);
    });

    Future.delayed(const Duration(seconds: 2), () {
      if (!mounted) return;
      setState(() {
        if (floatingMessage == message) {
          floatingMessage = '';
        }
      });
    });
  }

  Future<void> _showSaveDialog({String? defaultFileName}) async {
    final controller = TextEditingController(text: defaultFileName ?? '');

    await showDialog(
      context: context,
      builder: (_) => AlertDialog(
        backgroundColor: const Color(0xFF0A1220),
        title: const Text('ファイル名入力'),
        content: TextField(
          controller: controller,
          autofocus: true,
          decoration: const InputDecoration(hintText: '例: subject01_rest'),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('キャンセル'),
          ),
          ElevatedButton(
            onPressed: () {
              final fileName = controller.text.trim();
              Navigator.pop(context);

              if (fileName.isEmpty) {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(
                    content: Text('ファイル名を入力して'),
                    backgroundColor: Colors.redAccent,
                  ),
                );
                return;
              }

              _sendCommand('save', fileName: fileName);
              _showFloatingMessage('Save command sent: $fileName', ok: true);
            },
            child: const Text('保存'),
          ),
        ],
      ),
    );
  }


  void _selectProtocol(String? value) {
    if (value == null || isProtocolRunning) return;

    final phases = protocolDefinitions[value];
    if (phases == null || phases.isEmpty) return;

    protocolTimer?.cancel();

    setState(() {
      selectedProtocol = value;
      isProtocolRunning = false;
      pendingAutoSave = false;
      protocolElapsedSec = 0;
      protocolTotalSec = phases.last['end'] as int;
      currentPhaseLabel = 'Ready';
      currentInstruction = 'Start Experiment';
    });
  }

  int _selectedProtocolTotalSec() {
    if (currentProtocol.isEmpty) return 0;
    return currentProtocol
        .map((p) => (p['end'] as int?) ?? 0)
        .reduce((a, b) => a > b ? a : b);
  }

  String _defaultExperimentFileName() {
    final now = DateTime.now();
    final y = now.year.toString().padLeft(4, '0');
    final mo = now.month.toString().padLeft(2, '0');
    final d = now.day.toString().padLeft(2, '0');
    final h = now.hour.toString().padLeft(2, '0');
    final mi = now.minute.toString().padLeft(2, '0');
    final safeProtocol = selectedProtocol
        .replaceAll(' ', '_')
        .replaceAll('/', '_')
        .replaceAll('+', 'plus')
        .replaceAll('-', '_');
    return '${y}${mo}${d}_${h}${mi}_$safeProtocol';
  }

  void _handlePendingAutoSave() {
    if (!pendingAutoSave) return;
    if (!canSave) return;
    if (isMeasuring) return;

    pendingAutoSave = false;

    Future.delayed(const Duration(milliseconds: 300), () {
      if (!mounted) return;
      _showSaveDialog(defaultFileName: _defaultExperimentFileName());
    });
  }

  void _startProtocolTimer() {
    protocolTimer?.cancel();

    setState(() {
      isProtocolRunning = true;
      pendingAutoSave = false;
      protocolElapsedSec = 0;
      protocolTotalSec = _selectedProtocolTotalSec();
      _ppgData.clear();
      currentPhaseLabel = currentProtocol.first['label'].toString();
      currentInstruction = currentProtocol.first['instruction'].toString();
    });

    if (!isMeasuring) {
      _sendCommand('start');
    }

    protocolTimer = Timer.periodic(const Duration(seconds: 1), (timer) {
      if (!mounted) return;

      final nextElapsed = protocolElapsedSec + 1;

      if (nextElapsed >= protocolTotalSec) {
        setState(() {
          protocolElapsedSec = protocolTotalSec;
        });
        _finishExperimentAndSave();
        return;
      }

      setState(() {
        protocolElapsedSec = nextElapsed;
        _updateCurrentProtocolPhase();
      });
    });
  }

  void _finishExperimentAndSave() {
    protocolTimer?.cancel();

    setState(() {
      isProtocolRunning = false;
      pendingAutoSave = true;
      currentPhaseLabel = 'Finished';
      currentInstruction = '終了・保存準備中';
    });

    _sendCommand('stop');

    _showFloatingMessage(
      'Measurement stopped. Waiting for save...',
      ok: true,
    );

    Future.delayed(const Duration(seconds: 3), () {
      if (!mounted) return;
      _handlePendingAutoSave();
      if (pendingAutoSave) {
        _showFloatingMessage(
          'Waiting for backend save-ready...',
          ok: true,
        );
      }
    });
  }

  void _stopProtocolTimer() {
    _finishExperimentAndSave();
  }

  void _resetProtocolTimer() {
    protocolTimer?.cancel();

    setState(() {
      isProtocolRunning = false;
      pendingAutoSave = false;
      protocolElapsedSec = 0;
      currentPhaseLabel = 'Ready';
      currentInstruction = 'Start Experiment';
      _ppgData.clear();
    });
  }

  void _updateCurrentProtocolPhase() {
    final phase = currentProtocol.firstWhere(
      (p) =>
          protocolElapsedSec >= p['start'] &&
          protocolElapsedSec < p['end'],
      orElse: () => currentProtocol.last,
    );

    currentPhaseLabel = phase['label'].toString();

    if (currentPhaseLabel == 'Walking + Head Rotation') {
      final directions = ['左', '正面', '右', '正面'];
      final index =
          ((protocolElapsedSec - (phase['start'] as int)) ~/ 2) %
          directions.length;
      currentInstruction = directions[index];
    } else {
      currentInstruction = phase['instruction'].toString();
    }
  }

  String _formatTime(int sec) {
    final m = sec ~/ 60;
    final s = sec % 60;
    return '${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}';
  }

  @override
  void dispose() {
    protocolTimer?.cancel();
    _channel.sink.close();
    super.dispose();
  }

  bool get _isConnected => connectedBitalino;

  String get _statusLabel {
    if (!isMeasuring) return 'Stopped';
    if (signalQuality >= 80 && rmssdConfidence >= 70) return 'Reliable';
    if (signalQuality >= 50 && rmssdConfidence >= 40) return 'Caution';
    return 'Unstable';
  }

  Color get _statusColor {
    switch (_statusLabel) {
      case 'Reliable':
        return const Color(0xFF3DFF7A);
      case 'Caution':
        return const Color(0xFFFFC94A);
      case 'Stopped':
        return const Color(0xFF58B8FF);
      default:
        return const Color(0xFFFF6363);
    }
  }

  Color _qualityColor(double value) {
    if (value >= 80) return const Color(0xFF3DFF7A);
    if (value >= 50) return const Color(0xFFFFC94A);
    return const Color(0xFFFF6363);
  }

  Color _agreementColor(double value) {
    if (value >= 80) return const Color(0xFF3DFF7A);
    if (value >= 60) return const Color(0xFFFFC94A);
    return const Color(0xFFFF6363);
  }

  String _smartFormat(double value, {int fraction = 1}) {
    if (value.isNaN || value.isInfinite) return '--';
    if (value.abs() >= 100) return value.toStringAsFixed(0);
    return value.toStringAsFixed(fraction);
  }

  Widget _glassCard({
    required Widget child,
    EdgeInsets padding = const EdgeInsets.all(14),
    double radius = 20,
    double? height,
  }) {
    return Container(
      height: height,
      padding: padding,
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(radius),
        gradient: const LinearGradient(
          colors: [Color(0xFF071524), Color(0xFF0A1220)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        border: Border.all(
          color: const Color(0xFF2C3E50).withOpacity(0.65),
          width: 1.1,
        ),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.28),
            blurRadius: 18,
            offset: const Offset(0, 8),
          ),
        ],
      ),
      child: child,
    );
  }

  Widget _sectionTitle(IconData icon, String title, {String? trailing}) {
    return Row(
      children: [
        Icon(icon, color: const Color(0xFF57FF77), size: 18),
        const SizedBox(width: 8),
        Expanded(
          child: Text(
            title,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 14,
              fontWeight: FontWeight.w700,
            ),
          ),
        ),
        if (trailing != null)
          Text(
            trailing,
            style: const TextStyle(
              color: Color(0xFF57FF77),
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
      ],
    );
  }

  Widget _metricCard({
    required IconData icon,
    required Color iconColor,
    required String title,
    required String value,
    required String unit,
    Color? valueColor,
    double height = 88,
  }) {
    return _glassCard(
      height: height,
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, color: iconColor, size: 16),
              const SizedBox(width: 6),
              Expanded(
                child: Text(
                  title,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
            ],
          ),
          const Spacer(),
          Row(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Expanded(
                child: FittedBox(
                  fit: BoxFit.scaleDown,
                  alignment: Alignment.centerLeft,
                  child: Text(
                    value,
                    style: TextStyle(
                      color: valueColor ?? Colors.white,
                      fontSize: 20,
                      fontWeight: FontWeight.w800,
                      height: 1.0,
                    ),
                  ),
                ),
              ),
              const SizedBox(width: 4),
              Padding(
                padding: const EdgeInsets.only(bottom: 2),
                child: Text(
                  unit,
                  style: const TextStyle(
                    color: Colors.white70,
                    fontSize: 12,
                    fontWeight: FontWeight.w500,
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 6),
          Container(
            height: 3,
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(999),
              color: Colors.white10,
            ),
            child: FractionallySizedBox(
              alignment: Alignment.centerLeft,
              widthFactor:
                  0.55 + (math.Random(title.hashCode).nextDouble() * 0.20),
              child: Container(
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(999),
                  color: iconColor.withOpacity(0.85),
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _barCard({
    required IconData icon,
    required Color iconColor,
    required String title,
    required double value,
    required String suffix,
    required double min,
    required double max,
    bool invert = false,
  }) {
    double normalized;
    if (max <= min) {
      normalized = 0;
    } else {
      normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    }
    if (invert) normalized = 1 - normalized;

    final displayColor = invert
        ? _agreementColor(100 - value)
        : _qualityColor(value);

    return _glassCard(
      height: 92,
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, color: iconColor, size: 16),
              const SizedBox(width: 6),
              Expanded(
                child: Text(
                  title,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
            ],
          ),
          const Spacer(),
          Row(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Expanded(
                child: FittedBox(
                  fit: BoxFit.scaleDown,
                  alignment: Alignment.centerLeft,
                  child: Text(
                    _smartFormat(value),
                    style: TextStyle(
                      color: displayColor,
                      fontSize: 22,
                      fontWeight: FontWeight.w800,
                    ),
                  ),
                ),
              ),
              const SizedBox(width: 4),
              Padding(
                padding: const EdgeInsets.only(bottom: 2),
                child: Text(
                  suffix,
                  style: const TextStyle(color: Colors.white70, fontSize: 11),
                ),
              ),
            ],
          ),
          const SizedBox(height: 6),
          ClipRRect(
            borderRadius: BorderRadius.circular(999),
            child: Container(
              height: 6,
              decoration: const BoxDecoration(color: Color(0xFF2A3240)),
              child: Stack(
                children: [
                  Container(
                    decoration: const BoxDecoration(
                      gradient: LinearGradient(
                        colors: [
                          Color(0xFFFF5F5F),
                          Color(0xFFFFC94A),
                          Color(0xFF3DFF7A),
                        ],
                      ),
                    ),
                  ),
                  Align(
                    alignment: Alignment.centerRight,
                    child: FractionallySizedBox(
                      widthFactor: 1 - normalized,
                      child: Container(color: const Color(0xFF2A3240)),
                    ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _statusCard() {
    return _glassCard(
      height: 92,
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Row(
            children: [
              Icon(Icons.shield_outlined, color: Color(0xFF57FF77), size: 16),
              SizedBox(width: 6),
              Text(
                'Status',
                style: TextStyle(
                  color: Colors.white,
                  fontSize: 12,
                  fontWeight: FontWeight.w700,
                ),
              ),
            ],
          ),
          const Spacer(),
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(10),
              color: _statusColor.withOpacity(0.16),
              border: Border.all(color: _statusColor.withOpacity(0.40)),
            ),
            child: Text(
              _statusLabel,
              style: TextStyle(
                color: _statusColor,
                fontSize: 16,
                fontWeight: FontWeight.w800,
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _actionButton({
    required String label,
    required Color borderColor,
    required IconData icon,
    required VoidCallback? onPressed,
  }) {
    return Expanded(
      child: SizedBox(
        height: 48,
        child: OutlinedButton.icon(
          onPressed: onPressed,
          icon: Icon(icon, color: borderColor, size: 18),
          label: Text(
            label,
            style: TextStyle(
              color: borderColor,
              fontWeight: FontWeight.w700,
              fontSize: 14,
            ),
          ),
          style: OutlinedButton.styleFrom(
            backgroundColor: borderColor.withOpacity(0.08),
            side: BorderSide(color: borderColor.withOpacity(0.7), width: 1.2),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(16),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildPpgChart() {
    if (_ppgData.isEmpty) {
      return _glassCard(
        radius: 24,
        child: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(
                isMeasuring ? Icons.sensors : Icons.play_circle_outline,
                size: 54,
                color: const Color(0xFF57FF77),
              ),
              const SizedBox(height: 14),
              Text(
                isMeasuring ? 'Receiving signal...' : 'Press Start to begin',
                style: const TextStyle(
                  color: Colors.white,
                  fontSize: 18,
                  fontWeight: FontWeight.w700,
                ),
              ),
              const SizedBox(height: 8),
              Text(
                isMeasuring ? 'センサーデータを待っています' : '計測開始前はここに波形が表示されます',
                style: const TextStyle(color: Colors.white54, fontSize: 13),
              ),
            ],
          ),
        ),
      );
    }

    final firstX = _ppgData.first.x;
    final relativeSpots = _ppgData
        .map((e) => FlSpot(e.x - firstX, e.y))
        .toList();

    final ys = relativeSpots.map((e) => e.y).toList();
    final minRaw = ys.reduce(math.min);
    final maxRaw = ys.reduce(math.max);

    final range = maxRaw - minRaw;
    final margin = range < 1 ? 5.0 : range * 0.15;

    final minY = minRaw - margin;
    final maxY = maxRaw + margin;

    return _glassCard(
      radius: 24,
      padding: const EdgeInsets.fromLTRB(16, 14, 16, 10),
      child: Column(
        children: [
          _sectionTitle(
            Icons.monitor_heart_outlined,
            'PPG Waveform (Live)',
            trailing: '${_displayWindowSec.toStringAsFixed(1)} s',
          ),
          const SizedBox(height: 10),
          Expanded(
            child: LineChart(
              LineChartData(
                minX: 0,
                maxX: _displayWindowSec,
                minY: minY,
                maxY: maxY,
                clipData: const FlClipData.all(),
                gridData: FlGridData(
                  show: true,
                  drawVerticalLine: true,
                  horizontalInterval: (maxY - minY) / 4,
                  verticalInterval: 0.5,
                  getDrawingHorizontalLine: (value) {
                    return FlLine(
                      color: const Color(0xFF57FF77).withOpacity(0.10),
                      strokeWidth: 1,
                    );
                  },
                  getDrawingVerticalLine: (value) {
                    return FlLine(
                      color: const Color(0xFF57FF77).withOpacity(0.07),
                      strokeWidth: 1,
                    );
                  },
                ),
                titlesData: FlTitlesData(
                  leftTitles: const AxisTitles(
                    sideTitles: SideTitles(showTitles: false),
                  ),
                  rightTitles: const AxisTitles(
                    sideTitles: SideTitles(showTitles: false),
                  ),
                  topTitles: const AxisTitles(
                    sideTitles: SideTitles(showTitles: false),
                  ),
                  bottomTitles: AxisTitles(
                    sideTitles: SideTitles(
                      showTitles: true,
                      reservedSize: 20,
                      interval: 0.5,
                      getTitlesWidget: (value, meta) {
                        return Text(
                          '${value.toStringAsFixed(1)} s',
                          style: const TextStyle(
                            color: Colors.white54,
                            fontSize: 9,
                          ),
                        );
                      },
                    ),
                  ),
                ),
                borderData: FlBorderData(
                  show: true,
                  border: Border.all(
                    color: const Color(0xFF57FF77).withOpacity(0.20),
                  ),
                ),
                lineBarsData: [
                  LineChartBarData(
                    spots: relativeSpots,
                    isCurved: false,
                    color: const Color(0xFF57FF77),
                    barWidth: 2.0,
                    dotData: const FlDotData(show: false),
                    belowBarData: BarAreaData(show: false),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildHeader(bool isMobile) {
    return Padding(
      padding: EdgeInsets.only(bottom: isMobile ? 12 : 10),
      child: Row(
        children: [
          const Icon(Icons.favorite_border, color: Color(0xFF57FF77), size: 26),
          const SizedBox(width: 10),
          const Expanded(
            child: Text(
              'BioSignal Monitor',
              style: TextStyle(
                fontSize: 19,
                fontWeight: FontWeight.w800,
                color: Colors.white,
              ),
            ),
          ),
          Column(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Container(
                padding: const EdgeInsets.symmetric(
                  horizontal: 12,
                  vertical: 8,
                ),
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(14),
                  color: const Color(0xFF0E1A25),
                  border: Border.all(
                    color:
                        (_isConnected
                                ? const Color(0xFF57FF77)
                                : Colors.white24)
                            .withOpacity(0.35),
                  ),
                ),
                child: Row(
                  children: [
                    Icon(
                      Icons.circle,
                      size: 9,
                      color: _isConnected
                          ? const Color(0xFF57FF77)
                          : Colors.white38,
                    ),
                    const SizedBox(width: 7),
                    Text(
                      _isConnected ? 'Connected' : 'Waiting',
                      style: TextStyle(
                        color: _isConnected
                            ? const Color(0xFF57FF77)
                            : Colors.white70,
                        fontWeight: FontWeight.w700,
                        fontSize: 12,
                      ),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 4),
              Text(
                'Polar: ${connectedPolar ? "OK" : "Waiting"}',
                style: TextStyle(
                  color: connectedPolar
                      ? const Color(0xFF58B8FF)
                      : Colors.white54,
                  fontSize: 11,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _desktopMetricGrid() {
    return Column(
      children: [
        Expanded(
          child: Row(
            children: [
              Expanded(
                child: _metricCard(
                  icon: Icons.favorite_border,
                  iconColor: const Color(0xFFFF6A5C),
                  title: 'Heart Rate',
                  value: _smartFormat(heartRate, fraction: 0),
                  unit: 'bpm',
                ),
              ),
              const SizedBox(width: 10),
              Expanded(
                child: _metricCard(
                  icon: Icons.show_chart,
                  iconColor: const Color(0xFF57FF77),
                  title: 'BITalino RMSSD',
                  value: _smartFormat(rmssd),
                  unit: 'ms',
                ),
              ),
            ],
          ),
        ),
        const SizedBox(height: 10),
        Expanded(
          child: Row(
            children: [
              Expanded(
                child: _metricCard(
                  icon: Icons.bluetooth_searching,
                  iconColor: const Color(0xFF58B8FF),
                  title: 'POLAR RMSSD',
                  value: _smartFormat(polarRmssd),
                  unit: 'ms',
                ),
              ),
              const SizedBox(width: 10),
              Expanded(
                child: _metricCard(
                  icon: Icons.handshake_outlined,
                  iconColor: const Color(0xFFFFC94A),
                  title: 'Agreement',
                  value: _smartFormat(rmssdAgreement),
                  unit: '%',
                  valueColor: _agreementColor(rmssdAgreement),
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _desktopReliabilityGrid() {
    return Row(
      children: [
        Expanded(
          child: _barCard(
            icon: Icons.network_check_outlined,
            iconColor: const Color(0xFF57FF77),
            title: 'Signal (10s)',
            value: signalQuality,
            suffix: '%',
            min: 0,
            max: 100,
          ),
        ),
        const SizedBox(width: 10),
        Expanded(
          child: _barCard(
            icon: Icons.star_border,
            iconColor: const Color(0xFF7CFF83),
            title: 'RMSSD Conf.',
            value: rmssdConfidence,
            suffix: '%',
            min: 0,
            max: 100,
          ),
        ),
        const SizedBox(width: 10),
        Expanded(
          child: _barCard(
            icon: Icons.warning_amber_rounded,
            iconColor: const Color(0xFFFFC94A),
            title: 'Error (ms)',
            value: rmssdErrorMs,
            suffix: 'ms',
            min: 0,
            max: 20,
            invert: true,
          ),
        ),
        const SizedBox(width: 10),
        Expanded(child: _statusCard()),
      ],
    );
  }

  Widget _buildDesktopLayout() {
    return Padding(
      padding: const EdgeInsets.fromLTRB(18, 14, 18, 14),
      child: Column(
        children: [
          _buildHeader(false),
          Expanded(
            child: Row(
              children: [
                Expanded(
                  flex: 7,
                  child: _buildPpgChart(),
                ),
                const SizedBox(width: 14),
                Expanded(
                  flex: 6,
                  child: Column(
                    children: [
                      Expanded(
                        flex: 5,
                        child: _buildProtocolTimer(),
                      ),
                      const SizedBox(height: 10),
                      Expanded(
                        flex: 5,
                        child: _glassCard(
                          radius: 24,
                          padding: const EdgeInsets.all(14),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              _sectionTitle(
                                Icons.analytics_outlined,
                                'Core Metrics',
                              ),
                              const SizedBox(height: 10),
                              Expanded(child: _desktopMetricGrid()),
                            ],
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(height: 12),
          SizedBox(height: 58, child: _buildActions(false)),
        ],
      ),
    );
  }

  Widget _buildMobileMetricGrid() {
    return GridView.count(
      crossAxisCount: 2,
      crossAxisSpacing: 10,
      mainAxisSpacing: 10,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      childAspectRatio: 1.25,
      children: <Widget>[
        _metricCard(
          icon: Icons.favorite_border,
          iconColor: const Color(0xFFFF6A5C),
          title: 'Heart Rate',
          value: _smartFormat(heartRate, fraction: 0),
          unit: 'bpm',
        ),
        _metricCard(
          icon: Icons.show_chart,
          iconColor: const Color(0xFF57FF77),
          title: 'BITalino RMSSD',
          value: _smartFormat(rmssd),
          unit: 'ms',
        ),
        _metricCard(
          icon: Icons.bluetooth_searching,
          iconColor: const Color(0xFF58B8FF),
          title: 'POLAR RMSSD',
          value: _smartFormat(polarRmssd),
          unit: 'ms',
        ),
        _metricCard(
          icon: Icons.handshake_outlined,
          iconColor: const Color(0xFFFFC94A),
          title: 'Agreement',
          value: _smartFormat(rmssdAgreement),
          unit: '%',
          valueColor: _agreementColor(rmssdAgreement),
        ),
      ],
    );
  }

  Widget _buildMobileReliability() {
    return Column(
      children: [
        _barCard(
          icon: Icons.network_check_outlined,
          iconColor: const Color(0xFF57FF77),
          title: 'Signal (10s)',
          value: signalQuality,
          suffix: '%',
          min: 0,
          max: 100,
        ),
        const SizedBox(height: 10),
        _barCard(
          icon: Icons.star_border,
          iconColor: const Color(0xFF7CFF83),
          title: 'RMSSD Conf.',
          value: rmssdConfidence,
          suffix: '%',
          min: 0,
          max: 100,
        ),
        const SizedBox(height: 10),
        _barCard(
          icon: Icons.warning_amber_rounded,
          iconColor: const Color(0xFFFFC94A),
          title: 'Error (ms)',
          value: rmssdErrorMs,
          suffix: 'ms',
          min: 0,
          max: 20,
          invert: true,
        ),
        const SizedBox(height: 10),
        _statusCard(),
      ],
    );
  }



  Widget _buildActions(bool isMobile) {
    if (isMobile) {
      return Column(
        children: [
          Row(
            children: [
              _actionButton(
                label: 'Start',
                borderColor: const Color(0xFF57FF77),
                icon: Icons.play_arrow_rounded,
                onPressed: isMeasuring ? null : () => _sendCommand('start'),
              ),
              const SizedBox(width: 10),
              _actionButton(
                label: 'Stop',
                borderColor: const Color(0xFFFF5F5F),
                icon: Icons.stop_rounded,
                onPressed: isMeasuring ? () => _sendCommand('stop') : null,
              ),
            ],
          ),
          const SizedBox(height: 10),
          Row(
            children: [
              _actionButton(
                label: 'Save',
                borderColor: const Color(0xFF58B8FF),
                icon: Icons.save_outlined,
                onPressed: canSave ? _showSaveDialog : null,
              ),
            ],
          ),
        ],
      );
    }

    return Row(
      children: [
        _actionButton(
          label: 'Start',
          borderColor: const Color(0xFF57FF77),
          icon: Icons.play_arrow_rounded,
          onPressed: isMeasuring ? null : () => _sendCommand('start'),
        ),
        const SizedBox(width: 12),
        _actionButton(
          label: 'Stop',
          borderColor: const Color(0xFFFF5F5F),
          icon: Icons.stop_rounded,
          onPressed: isMeasuring ? () => _sendCommand('stop') : null,
        ),
        const SizedBox(width: 12),
        _actionButton(
          label: 'Save',
          borderColor: const Color(0xFF58B8FF),
          icon: Icons.save_outlined,
          onPressed: canSave ? _showSaveDialog : null,
        ),
      ],
    );
  }

  Widget _buildProtocolTimer() {
    final remainingSec =
        (protocolTotalSec - protocolElapsedSec).clamp(0, protocolTotalSec);

    return _glassCard(
      radius: 24,
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _sectionTitle(Icons.science_outlined, 'Experiment Control'),
          const SizedBox(height: 8),
          Container(
            height: 38,
            padding: const EdgeInsets.symmetric(horizontal: 10),
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(12),
              color: const Color(0xFF0E1A25),
              border: Border.all(color: Colors.white24),
            ),
            child: DropdownButtonHideUnderline(
              child: DropdownButton<String>(
                value: selectedProtocol,
                isExpanded: true,
                dropdownColor: const Color(0xFF0E1A25),
                iconEnabledColor: const Color(0xFF57FF77),
                style: const TextStyle(
                  color: Colors.white,
                  fontSize: 12,
                  fontWeight: FontWeight.w700,
                ),
                items: protocolDefinitions.keys.map((name) {
                  return DropdownMenuItem<String>(
                    value: name,
                    child: Text(
                      name,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                  );
                }).toList(),
                onChanged: isProtocolRunning ? null : _selectProtocol,
              ),
            ),
          ),
          const SizedBox(height: 6),
          Row(
            children: [
              Expanded(
                child: Text(
                  currentPhaseLabel,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 22,
                    fontWeight: FontWeight.w900,
                  ),
                ),
              ),
              Text(
                _formatTime(remainingSec),
                style: const TextStyle(
                  color: Color(0xFF57FF77),
                  fontSize: 30,
                  fontWeight: FontWeight.w900,
                ),
              ),
            ],
          ),
          const SizedBox(height: 4),
          Center(
            child: Text(
              currentInstruction,
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: TextStyle(
                color: currentPhaseLabel == 'Walking + Head Rotation'
                    ? const Color(0xFFFFC94A)
                    : const Color(0xFF58B8FF),
                fontSize: currentPhaseLabel == 'Walking + Head Rotation'
                    ? 28
                    : 20,
                fontWeight: FontWeight.w900,
              ),
            ),
          ),
          const Spacer(),
          Row(
            children: [
              _smallProtocolButton(
                label: 'Start Experiment',
                borderColor: const Color(0xFF57FF77),
                icon: Icons.play_arrow_rounded,
                onPressed: isProtocolRunning ? null : _startProtocolTimer,
              ),
              const SizedBox(width: 8),
              _smallProtocolButton(
                label: 'Stop & Save',
                borderColor: const Color(0xFFFF5F5F),
                icon: Icons.stop_rounded,
                onPressed: isProtocolRunning || isMeasuring
                    ? _stopProtocolTimer
                    : null,
              ),
              const SizedBox(width: 8),
              _smallProtocolButton(
                label: 'Reset',
                borderColor: const Color(0xFF58B8FF),
                icon: Icons.restart_alt_rounded,
                onPressed: _resetProtocolTimer,
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _smallProtocolButton({
    required String label,
    required Color borderColor,
    required IconData icon,
    required VoidCallback? onPressed,
  }) {
    return Expanded(
      child: SizedBox(
        height: 38,
        child: OutlinedButton.icon(
          onPressed: onPressed,
          icon: Icon(icon, color: borderColor, size: 15),
          label: Text(
            label,
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: TextStyle(
              color: borderColor,
              fontWeight: FontWeight.w800,
              fontSize: 11,
            ),
          ),
          style: OutlinedButton.styleFrom(
            padding: const EdgeInsets.symmetric(horizontal: 5),
            backgroundColor: borderColor.withOpacity(0.08),
            side: BorderSide(color: borderColor.withOpacity(0.7), width: 1.0),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(14),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildMobileLayout() {
    return SafeArea(
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(14),
        child: Column(
          children: [
            _buildHeader(true),
            SizedBox(height: 210, child: _buildProtocolTimer()),
            const SizedBox(height: 14),
            SizedBox(height: 260, child: _buildPpgChart()),
            const SizedBox(height: 14),
            _buildMobileMetricGrid(),
            const SizedBox(height: 14),
            _buildActions(true),
            if (lastSavedSummaryFile.isNotEmpty ||
                lastSavedRawFile.isNotEmpty) ...[
              const SizedBox(height: 10),
              Text(
                'Summary: $lastSavedSummaryFile\nRaw: $lastSavedRawFile',
                maxLines: 3,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(color: Colors.white54, fontSize: 11),
              ),
            ],
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final width = MediaQuery.of(context).size.width;
    final isMobile = width < 900;

    return Scaffold(
      backgroundColor: const Color(0xFF050816),
      body: Stack(
        children: [
          isMobile ? _buildMobileLayout() : _buildDesktopLayout(),

          if (floatingMessage.isNotEmpty)
            Positioned(
              top: 20,
              left: 20,
              right: 20,
              child: SafeArea(
                child: Material(
                  color: Colors.transparent,
                  child: Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 16,
                      vertical: 14,
                    ),
                    decoration: BoxDecoration(
                      color: const Color(0xFF0E1A25),
                      borderRadius: BorderRadius.circular(16),
                      border: Border.all(
                        color: floatingMessageColor.withOpacity(0.7),
                        width: 1.2,
                      ),
                      boxShadow: [
                        BoxShadow(
                          color: Colors.black.withOpacity(0.25),
                          blurRadius: 14,
                          offset: const Offset(0, 6),
                        ),
                      ],
                    ),
                    child: Row(
                      children: [
                        Icon(
                          floatingMessageColor == const Color(0xFF3DFF7A)
                              ? Icons.check_circle_outline
                              : Icons.error_outline,
                          color: floatingMessageColor,
                        ),
                        const SizedBox(width: 12),
                        Expanded(
                          child: Text(
                            floatingMessage,
                            style: const TextStyle(
                              color: Colors.white,
                              fontSize: 14,
                              fontWeight: FontWeight.w700,
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }
}
