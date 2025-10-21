import threading
import time
import random
import csv
from pathlib import Path
import requests
from datetime import datetime


class HttpDeviceThread(threading.Thread):
    def __init__(self, device_id: str, sensor_files: dict, collector_url: str, fixed_interval: int | None = None):
        super().__init__(daemon=True)
        self.device_id = device_id
        self.sensor_files = sensor_files
        self.collector_url = collector_url
        self.fixed_interval = fixed_interval
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def join(self, timeout=None):
        self._stop_event.set()
        super().join(timeout)

    def _pick_random_reading(self):
        # Choose sensor type that exists and pick a random line
        sensor_type = random.choice(list(self.sensor_files.keys()))
        path = self.sensor_files[sensor_type]
        if not Path(path).exists():
            return None
        with open(path, "r", encoding="utf-8") as fh:
            reader = list(csv.reader(fh))
            if len(reader) <= 1:
                return None
            # skip header
            row = random.choice(reader[1:])
            # row: time, date, value
            if len(row) < 3:
                return None
            return {"time": row[0], "date": row[1], "sensor_type": sensor_type, "value": row[2]}

    def run(self):
        while not self._stop_event.is_set():
            reading = self._pick_random_reading()
            if reading:
                payload = {
                    "device_id": self.device_id,
                    "time": reading["time"],
                    "date": reading["date"],
                    "protocol": "HTTP",
                    "sensor_type": reading["sensor_type"],
                    "value": reading["value"],
                }
                try:
                    requests.post(self.collector_url, json=payload, timeout=2)
                except Exception as e:
                    # Just print and continue
                    print(f"[HTTP DEVICE {self.device_id}] Error sending: {e}")

            # Sleep interval
            if self.fixed_interval and self.fixed_interval > 0:
                sleep_for = self.fixed_interval
            else:
                sleep_for = random.randint(4, 10)
            # break sleep into small chunks to be responsive to stop
            for _ in range(int(sleep_for * 10)):
                if self._stop_event.is_set():
                    break
                time.sleep(0.1)


def start_http_device_thread(device_id: str, sensor_files: dict, collector_url: str, fixed_interval: int | None = None):
    t = HttpDeviceThread(device_id=device_id, sensor_files=sensor_files, collector_url=collector_url, fixed_interval=fixed_interval)
    t.start()
    return t
