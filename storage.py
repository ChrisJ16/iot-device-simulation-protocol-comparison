import csv
import threading
from pathlib import Path

_lock = threading.Lock()
_output = Path("http_recorded_data.csv")


def set_output_file(path: str):
    """Set the global output CSV file path used by save_to_csv.

    Use this at startup when switching protocol (e.g. to 'mqtt_recorded_data.csv').
    """
    global _output
    _output = Path(path)


def save_to_csv(record: dict, output_path: str | None = None):
    # Ensure keys: device_id, time, date, protocol, sensor_type, value
    header = ["device_id", "time", "date", "protocol", "sensor_type", "value"]
    target = Path(output_path) if output_path else _output
    with _lock:
        exists = target.exists()
        with target.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if not exists:
                writer.writerow(header)
            row = [
                record.get("device_id"),
                record.get("time"),
                record.get("date"),
                record.get("protocol"),
                record.get("sensor_type"),
                record.get("value"),
            ]
            writer.writerow(row)

def read_all():
    if not _output.exists():
        return []
    with _output.open("r", encoding="utf-8") as fh:
        return list(csv.reader(fh))
