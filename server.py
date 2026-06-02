import asyncio
import csv
import json
import os
import re
import threading
import time
from collections import deque
from datetime import datetime

import numpy as np
import serial
import websockets
from bitalino import BITalino
from pylsl import StreamInlet, resolve_byprop

from ppg_processor_proposed import (
    get_nni_from_ppg_with_quality,
    get_filtered_and_raw_data,
)

HOST = "0.0.0.0"
PORT = 8765

# -----------------------------
# BITalino settings
# -----------------------------
BITALINO_MAC = "98:D3:71:FE:51:B1"
PPG_SAMPLING_RATE = 1000
PPG_CHANNEL = [0]
PPG_BUFFER_SIZE = 100

# -----------------------------
# Display / analysis settings
# -----------------------------
DISPLAY_DECIMATION = 10
ANALYSIS_WINDOW_SEC = 10.0
ANALYSIS_WINDOW_SIZE = int(PPG_SAMPLING_RATE * ANALYSIS_WINDOW_SEC)

RMSSD_WINDOW_SEC = 120.0
ANALYSIS_INTERVAL_SEC = 1.0

# -----------------------------
# Polar LSL settings
# -----------------------------
POLAR_LSL_TYPE = "HRV"
POLAR_RESOLVE_TIMEOUT = 3

# -----------------------------
# IMU Serial settings
# -----------------------------
IMU_PORT = "COM9"
IMU_BAUDRATE = 115200
IMU_SERIAL_TIMEOUT = 1.0

# -----------------------------
# Save settings
# -----------------------------
SAVE_DIR = os.path.join(os.path.dirname(__file__), "saved_measurements")


def compute_rmssd(rr_intervals_ms: np.ndarray) -> float:
    if len(rr_intervals_ms) < 2:
        return 0.0
    diff_rr = np.diff(rr_intervals_ms)
    if len(diff_rr) == 0:
        return 0.0
    return float(np.sqrt(np.mean(diff_rr ** 2)))


def compute_agreement_percent(bitalino_rmssd: float, polar_rmssd: float) -> float:
    if polar_rmssd <= 0:
        return 0.0
    rel_error = abs(bitalino_rmssd - polar_rmssd) / polar_rmssd * 100.0
    return float(np.clip(100.0 - rel_error, 0.0, 100.0))


def sanitize_file_name(name: str) -> str:
    name = name.strip()
    if not name:
        return ""
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:80]


def compute_motion_index(ax_g: float, ay_g: float, az_g: float,
                         gx_dps: float, gy_dps: float, gz_dps: float) -> float:
    """
    まずはシンプルに gyro ノルムを motion index とする。
    必要ならあとで accel も混ぜられる。
    """
    return float(np.sqrt(gx_dps**2 + gy_dps**2 + gz_dps**2))


class SharedState:
    def __init__(self):
        self.lock = threading.Lock()

        self.connected_bitalino = False
        self.connected_polar = False
        self.connected_imu = False

        self.is_measuring = False
        self.status_message = "Press Start"
        self.session_started_at = None
        self.last_saved_summary_file = ""
        self.last_saved_raw_file = ""

        # live analysis buffers
        self.raw_times = deque(maxlen=ANALYSIS_WINDOW_SIZE)
        self.raw_values = deque(maxlen=ANALYSIS_WINDOW_SIZE)
        self.display_wave = deque(maxlen=5000)

        # latest metrics
        self.latest_hr = 0.0
        self.latest_rmssd = 0.0
        self.latest_signal_quality = 0.0
        self.latest_rmssd_confidence = 0.0
        self.latest_polar_rmssd = 0.0
        self.latest_rmssd_agreement = 0.0
        self.latest_rmssd_error_ms = 0.0
        self.latest_motion_index = 0.0

        # rolling histories for metrics
        self.nni_history = deque()          # (timestamp_sec, nni_ms, trust_bool)
        self.polar_rr_history = deque()     # (timestamp_sec, rr_ms)

        # per-session full logs for save
        self.session_ppg_raw_log = []       # (timestamp_sec, value)
        self.session_ppg_filtered_log = []  # (timestamp_sec, value)
        self.session_polar_rr_log = []      # (timestamp_sec, rr_ms)
        self.session_nni_log = []           # (timestamp_sec, nni_ms, trust)
        self.session_imu_log = []           # (timestamp_sec, ax, ay, az, gx, gy, gz, motion)

        self.last_filtered_logged_ts = -1.0
        self.last_nni_logged_ts = -1.0

        # IMU timestamp sync
        self.imu_pc_t0 = None
        self.imu_arduino_t0_ms = None

        self.last_analysis_time = 0.0
        self.display_t = 0.0

    def has_session_data(self) -> bool:
        return (
            len(self.session_ppg_raw_log) > 0
            or len(self.session_ppg_filtered_log) > 0
            or len(self.session_polar_rr_log) > 0
            or len(self.session_imu_log) > 0
        )


shared_state = SharedState()
backend_stop_event = threading.Event()
backend_threads_started = False


def start_backend_threads_once():
    global backend_threads_started

    if backend_threads_started:
        return

    backend_threads_started = True

    threading.Thread(
        target=bitalino_reader,
        args=(backend_stop_event, shared_state),
        daemon=True
    ).start()

    threading.Thread(
        target=polar_lsl_reader,
        args=(backend_stop_event, shared_state),
        daemon=True
    ).start()

    threading.Thread(
        target=imu_serial_reader,
        args=(backend_stop_event, shared_state),
        daemon=True
    ).start()

    print("Backend sensor threads started")


def reset_measurement_state(state: SharedState):
    with state.lock:
        state.raw_times.clear()
        state.raw_values.clear()
        state.display_wave.clear()

        state.nni_history.clear()
        state.polar_rr_history.clear()

        state.latest_hr = 0.0
        state.latest_rmssd = 0.0
        state.latest_signal_quality = 0.0
        state.latest_rmssd_confidence = 0.0
        state.latest_polar_rmssd = 0.0
        state.latest_rmssd_agreement = 0.0
        state.latest_rmssd_error_ms = 0.0
        state.latest_motion_index = 0.0

        state.session_ppg_raw_log.clear()
        state.session_ppg_filtered_log.clear()
        state.session_polar_rr_log.clear()
        state.session_nni_log.clear()
        state.session_imu_log.clear()

        state.last_filtered_logged_ts = -1.0
        state.last_nni_logged_ts = -1.0

        state.last_analysis_time = 0.0
        state.display_t = 0.0
        state.session_started_at = time.time()
        state.last_saved_summary_file = ""
        state.last_saved_raw_file = ""

        # IMU sync is reset per session
        state.imu_pc_t0 = None
        state.imu_arduino_t0_ms = None


def start_measurement(state: SharedState):
    reset_measurement_state(state)
    with state.lock:
        state.is_measuring = True
        state.status_message = "Measuring"
        print("START: is_measuring =", state.is_measuring)


def stop_measurement(state: SharedState):
    with state.lock:
        state.is_measuring = False
        state.status_message = "Stopped"
        print("STOP: is_measuring =", state.is_measuring)
        print("IMU log rows =", len(state.session_imu_log))


def save_current_session(state: SharedState, user_file_name: str):
    safe_name = sanitize_file_name(user_file_name)
    if not safe_name:
        return False, "", "", "ファイル名を入力して"

    with state.lock:
        print("SAVE IMU rows =", len(state.session_imu_log))
        if state.is_measuring:
            return False, "", "", "Stopしてから保存して"
        if not state.has_session_data():
            return False, "", "", "保存できるデータがまだない"

        date_prefix = datetime.now().strftime("%Y-%m-%d")
        base_name = f"{date_prefix}_{safe_name}"

        summary_path = os.path.join(SAVE_DIR, f"{base_name}_summary.csv")
        raw_path = os.path.join(SAVE_DIR, f"{base_name}_raw.csv")

        elapsed_sec = 0.0
        if state.session_started_at is not None:
            elapsed_sec = max(0.0, time.time() - state.session_started_at)

        summary_row = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "session_name": safe_name,
            "elapsed_sec": round(elapsed_sec, 1),
            "heart_rate_bpm": round(state.latest_hr, 2),
            "bitalino_rmssd_ms": round(state.latest_rmssd, 2),
            "polar_rmssd_ms": round(state.latest_polar_rmssd, 2),
            "agreement_percent": round(state.latest_rmssd_agreement, 2),
            "signal_quality_10s_percent": round(state.latest_signal_quality, 2),
            "rmssd_confidence_percent": round(state.latest_rmssd_confidence, 2),
            "rmssd_error_ms": round(state.latest_rmssd_error_ms, 2),
            "motion_index": round(state.latest_motion_index, 4),
            "status": state.status_message,
        }

        raw_rows = []

        for ts, val in state.session_ppg_raw_log:
            raw_rows.append({
                "stream": "ppg_raw",
                "timestamp": ts,
                "value": val,
                "extra": "",
            })

        for ts, val in state.session_ppg_filtered_log:
            raw_rows.append({
                "stream": "ppg_filtered",
                "timestamp": ts,
                "value": val,
                "extra": "",
            })

        for ts, val in state.session_polar_rr_log:
            raw_rows.append({
                "stream": "polar_rr_ms",
                "timestamp": ts,
                "value": val,
                "extra": "",
            })

        for ts, nni, trust in state.session_nni_log:
            raw_rows.append({
                "stream": "nni_ms",
                "timestamp": ts,
                "value": nni,
                "extra": f"trust={int(bool(trust))}",
            })

        for ts, ax, ay, az, gx, gy, gz, motion in state.session_imu_log:
            raw_rows.append({
                "stream": "imu_ax_g",
                "timestamp": ts,
                "value": ax,
                "extra": "",
            })
            raw_rows.append({
                "stream": "imu_ay_g",
                "timestamp": ts,
                "value": ay,
                "extra": "",
            })
            raw_rows.append({
                "stream": "imu_az_g",
                "timestamp": ts,
                "value": az,
                "extra": "",
            })
            raw_rows.append({
                "stream": "imu_gx_dps",
                "timestamp": ts,
                "value": gx,
                "extra": "",
            })
            raw_rows.append({
                "stream": "imu_gy_dps",
                "timestamp": ts,
                "value": gy,
                "extra": "",
            })
            raw_rows.append({
                "stream": "imu_gz_dps",
                "timestamp": ts,
                "value": gz,
                "extra": "",
            })
            raw_rows.append({
                "stream": "motion_index",
                "timestamp": ts,
                "value": motion,
                "extra": "",
            })

    os.makedirs(SAVE_DIR, exist_ok=True)

    # summary save
    with open(summary_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_row.keys()))
        writer.writeheader()
        writer.writerow(summary_row)

    # raw save
    with open(raw_path, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["stream", "timestamp", "value", "extra"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(raw_rows, key=lambda x: (x["timestamp"], x["stream"])):
            writer.writerow(row)

    with state.lock:
        state.last_saved_summary_file = summary_path
        state.last_saved_raw_file = raw_path
        state.status_message = "Saved"

    return True, summary_path, raw_path, "summary + raw を保存した"


def analyze_psmar(state: SharedState):
    with state.lock:
        if not state.is_measuring:
            return
        if len(state.raw_values) < int(PPG_SAMPLING_RATE * 5):
            return

        timestamps = np.array(state.raw_times, dtype=float)
        values = np.array(state.raw_values, dtype=float)

    try:
        (
            nni_timestamps,
            fixed_nnis,
            trust_mask,
            signal_quality,
            debug_info
        ) = get_nni_from_ppg_with_quality(timestamps, values)

        raw, wavelet_filtered, smooth, _, _, _ = get_filtered_and_raw_data(
            timestamps, values
        )

        hr = 0.0
        if len(fixed_nnis) > 0:
            mean_nni_ms = float(np.mean(fixed_nnis))
            if mean_nni_ms > 0:
                hr = 60000.0 / mean_nni_ms

        now_t = timestamps[-1]

        with state.lock:
            # NNI history for metrics
            last_hist_t = state.nni_history[-1][0] if state.nni_history else -1.0
            for ts, nni, trust in zip(nni_timestamps, fixed_nnis, trust_mask):
                ts_f = float(ts)
                if ts_f > last_hist_t:
                    state.nni_history.append((ts_f, float(nni), bool(trust)))

            # log NNI only once per session
            for ts, nni, trust in zip(nni_timestamps, fixed_nnis, trust_mask):
                ts_f = float(ts)
                if ts_f > state.last_nni_logged_ts:
                    state.session_nni_log.append((ts_f, float(nni), bool(trust)))
                    state.last_nni_logged_ts = ts_f

            cutoff = now_t - RMSSD_WINDOW_SEC
            while state.nni_history and state.nni_history[0][0] < cutoff:
                state.nni_history.popleft()

            nni_values = np.array([x[1] for x in state.nni_history], dtype=float)
            bitalino_rmssd = compute_rmssd(nni_values)

            elapsed = max(0.0, now_t - state.session_started_at) if state.session_started_at else 0.0
            window_fill_ratio = min(1.0, elapsed / RMSSD_WINDOW_SEC)

            trust_values = np.array([x[2] for x in state.nni_history], dtype=float)
            trust_ratio = float(np.mean(trust_values)) if len(trust_values) > 0 else 0.0
            rmssd_confidence = 100.0 * window_fill_ratio * trust_ratio

            # display wave + filtered log
            if smooth is not None and len(smooth) > 0:
                state.display_wave.clear()
                for i in range(0, len(smooth), DISPLAY_DECIMATION):
                    state.display_t += DISPLAY_DECIMATION / PPG_SAMPLING_RATE
                    val = float(smooth[i])
                    state.display_wave.append((state.display_t, val))

                    ts_f = float(timestamps[i])
                    if ts_f > state.last_filtered_logged_ts:
                        state.session_ppg_filtered_log.append((ts_f, val))
                        state.last_filtered_logged_ts = ts_f

            state.latest_hr = hr
            state.latest_rmssd = bitalino_rmssd
            state.latest_signal_quality = signal_quality
            state.latest_rmssd_confidence = rmssd_confidence

            polar_rr_values = np.array([x[1] for x in state.polar_rr_history], dtype=float)
            polar_rmssd = compute_rmssd(polar_rr_values)
            state.latest_polar_rmssd = polar_rmssd

            if polar_rmssd > 0:
                error_ms = abs(bitalino_rmssd - polar_rmssd)
                agreement = compute_agreement_percent(bitalino_rmssd, polar_rmssd)
            else:
                error_ms = 0.0
                agreement = 0.0

            state.latest_rmssd_error_ms = error_ms
            state.latest_rmssd_agreement = agreement

    except Exception as e:
        print(f"IMU serial error: {e}")
        with state.lock:
           state.connected_imu = False
        time.sleep(5)


def bitalino_reader(stop_event, state: SharedState):
    device = None
    try:
        print("Connecting to BITalino...")
        device = BITalino(BITALINO_MAC)
        device.start(PPG_SAMPLING_RATE, PPG_CHANNEL)
        print("BITalino connected")

        with state.lock:
            state.connected_bitalino = True

        while not stop_event.is_set():
            data = device.read(PPG_BUFFER_SIZE)
            ppg_values = data[:, -1]

            with state.lock:
                measuring = state.is_measuring

            if not measuring:
                continue

            ts_end = time.time()
            ts_start = ts_end - (len(ppg_values) / PPG_SAMPLING_RATE)
            timestamps = np.linspace(ts_start, ts_end, len(ppg_values), endpoint=True)

            with state.lock:
                for ts, val in zip(timestamps, ppg_values):
                    ts_f = float(ts)
                    val_f = float(val)
                    state.raw_times.append(ts_f)
                    state.raw_values.append(val_f)
                    state.session_ppg_raw_log.append((ts_f, val_f))

            now = time.time()
            with state.lock:
                should_analyze = (now - state.last_analysis_time) >= ANALYSIS_INTERVAL_SEC

            if should_analyze:
                analyze_psmar(state)
                with state.lock:
                    state.last_analysis_time = now

    except Exception as e:
        print(f"BITalino error: {e}")

    finally:
        with state.lock:
            state.connected_bitalino = False
        if device is not None:
            try:
                device.stop()
                device.close()
            except Exception:
                pass
        print("BITalino disconnected")


def polar_lsl_reader(stop_event, state: SharedState):
    while not stop_event.is_set():
        inlet = None
        try:
            print("Looking for Polar LSL stream...")
            streams = resolve_byprop("type", POLAR_LSL_TYPE, timeout=POLAR_RESOLVE_TIMEOUT)
            if not streams:
                with state.lock:
                    state.connected_polar = False
                time.sleep(2)
                continue

            inlet = StreamInlet(streams[0])
            print("Polar LSL connected")

            with state.lock:
                state.connected_polar = True

            while not stop_event.is_set():
                sample, timestamp = inlet.pull_sample(timeout=1.0)
                if not sample:
                    continue

                with state.lock:
                    measuring = state.is_measuring

                if not measuring:
                    continue

                rr_ms = float(sample[0])
                ts = float(timestamp)

                with state.lock:
                    state.polar_rr_history.append((ts, rr_ms))
                    state.session_polar_rr_log.append((ts, rr_ms))

                    cutoff = ts - RMSSD_WINDOW_SEC
                    while state.polar_rr_history and state.polar_rr_history[0][0] < cutoff:
                        state.polar_rr_history.popleft()

        except Exception as e:
            print(f"Polar LSL error: {e}")
            with state.lock:
                state.connected_polar = False
            time.sleep(2)

        finally:
            if inlet is not None:
                try:
                    inlet.close_stream()
                except Exception:
                    pass


def imu_serial_reader(stop_event, state: SharedState):
    ser = None

    while not stop_event.is_set():
        try:
            print(f"Connecting to IMU on {IMU_PORT} ...")
            ser = serial.Serial(IMU_PORT, IMU_BAUDRATE, timeout=IMU_SERIAL_TIMEOUT)
            time.sleep(2.0)
            ser.reset_input_buffer()
            print("IMU serial connected")

            with state.lock:
                state.connected_imu = True
                state.imu_pc_t0 = None
                state.imu_arduino_t0_ms = None

            imu_count = 0

            while not stop_event.is_set():
                line = ser.readline().decode("utf-8", errors="ignore").strip()
                if not line:
                    continue

                if (
                    line.startswith("time_ms")
                    or line.startswith("MPU")
                    or line.startswith("WHO_AM_I")
                    or line.startswith("read failed")
                ):
                    continue

                parts = line.split(",")
                print("IMU READ:", line)
                print("IMU PARTS:", len(parts))
                
                try:
                    # Arduino新コード：13列
                    if len(parts) == 13:
                        arduino_ms = float(parts[0])
                        ax_g = float(parts[7])
                        ay_g = float(parts[8])
                        az_g = float(parts[9])
                        gx_dps = float(parts[10])
                        gy_dps = float(parts[11])
                        gz_dps = float(parts[12])

                    # 念のため旧形式：7列
                    elif len(parts) == 7:
                        arduino_ms = float(parts[0])
                        ax_g = float(parts[1])
                        ay_g = float(parts[2])
                        az_g = float(parts[3])
                        gx_dps = float(parts[4])
                        gy_dps = float(parts[5])
                        gz_dps = float(parts[6])

                    else:
                        print("IMU skip len:", len(parts), line)
                        continue

                except ValueError:
                    print("IMU parse error:", line)
                    continue

                with state.lock:
                  if state.imu_pc_t0 is None or state.imu_arduino_t0_ms is None:
                      state.imu_pc_t0 = time.time()
                      state.imu_arduino_t0_ms = arduino_ms

                  imu_ts = state.imu_pc_t0 + (arduino_ms - state.imu_arduino_t0_ms) / 1000.0
                  motion = compute_motion_index(ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps)
                  state.latest_motion_index = motion

                  if state.is_measuring:
                      state.session_imu_log.append(
                         (imu_ts, ax_g, ay_g, az_g, gx_dps, gy_dps, gz_dps, motion)
                      )

                  if len(state.session_imu_log) % 20 == 0:
                      print("IMU saved rows:", len(state.session_imu_log))

        except Exception as e:
            print(f"IMU serial error: {e}")
            with state.lock:
                state.connected_imu = False
            time.sleep(5)

        finally:
            if ser is not None:
                try:
                    ser.close()
                except Exception:
                    pass
            with state.lock:
                state.connected_imu = False


def get_telemetry_payload(state: SharedState):
    with state.lock:
        payload = {
            "type": "telemetry",
            "is_measuring": state.is_measuring,
            "status_message": state.status_message,
            "connected_bitalino": state.connected_bitalino,
            "connected_polar": state.connected_polar,
            "connected_imu": state.connected_imu,
            "can_save": (not state.is_measuring) and state.has_session_data(),
            "heart_rate": state.latest_hr,
            "rmssd": state.latest_rmssd,
            "signal_quality": state.latest_signal_quality,
            "rmssd_confidence": state.latest_rmssd_confidence,
            "polar_rmssd": state.latest_polar_rmssd,
            "rmssd_agreement": state.latest_rmssd_agreement,
            "rmssd_error_ms": state.latest_rmssd_error_ms,
            "motion_index": state.latest_motion_index,
            "last_saved_summary_file": state.last_saved_summary_file,
            "last_saved_raw_file": state.last_saved_raw_file,
        }

        if state.display_wave:
            t, ppg = state.display_wave.popleft()
            payload["timestamp"] = t
            payload["ppg"] = ppg
        else:
            payload["timestamp"] = None
            payload["ppg"] = None

        return payload


async def handle_command(websocket, message: str, state: SharedState):
    print("RECEIVED MESSAGE:", message)

    try:
        data = json.loads(message)
    except Exception:
        await websocket.send(json.dumps({
            "type": "ack",
            "ok": False,
            "message": "JSONの読み取りに失敗した",
        }))
        return

    command = data.get("command")

    if command == "start":
        start_measurement(state)
        await websocket.send(json.dumps({
            "type": "ack",
            "command": "start",
            "ok": True,
            "message": "計測を開始した",
            "is_measuring": True,
        }))
        return

    if command == "stop":
        stop_measurement(state)
        await websocket.send(json.dumps({
            "type": "ack",
            "command": "stop",
            "ok": True,
            "message": "計測を停止した",
            "is_measuring": False,
        }))
        return

    if command == "save":
        file_name = (data.get("file_name") or "").strip()
        ok, summary_path, raw_path, msg = save_current_session(state, file_name)
        await websocket.send(json.dumps({
            "type": "ack",
            "command": "save",
            "ok": ok,
            "message": msg,
            "last_saved_summary_file": summary_path,
            "last_saved_raw_file": raw_path,
        }))
        return

    await websocket.send(json.dumps({
        "type": "ack",
        "ok": False,
        "message": f"未知のコマンド: {command}",
    }))


async def telemetry_sender(websocket, state: SharedState):
    while True:
        payload = get_telemetry_payload(state)
        await websocket.send(json.dumps(payload))
        if payload["ppg"] is None:
            await asyncio.sleep(0.20)
        else:
            await asyncio.sleep(0.01)


async def command_receiver(websocket, state: SharedState):
    async for message in websocket:
        await handle_command(websocket, message, state)


async def stream_data(websocket):
    print("Flutter connected")

    sender_task = asyncio.create_task(telemetry_sender(websocket, shared_state))
    receiver_task = asyncio.create_task(command_receiver(websocket, shared_state))

    done, pending = await asyncio.wait(
        [sender_task, receiver_task],
        return_when=asyncio.FIRST_EXCEPTION,
    )

    for task in pending:
        task.cancel()

    print("Flutter disconnected")


async def main():
    start_backend_threads_once()

    async with websockets.serve(stream_data, HOST, PORT):
        print(f"WebSocket server running at ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())