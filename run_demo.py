import csv
import random
from pathlib import Path
import json
import threading
import time
import signal
import sys
from datetime import datetime

from collector.http_collector import create_app, HttpCollectorServer
from devices.http_device import start_http_device_thread
from collector.mqtt_collector import MqttCollector
from collector.local_broker import LocalBroker
from devices.mqtt_device import start_mqtt_device_thread
from storage import set_output_file

def initial_data_parser(path_to_file, how_many_rows_to_read):
    # enforce minimum
    if how_many_rows_to_read < 100:
        how_many_rows_to_read = 100

    out_files = {
        "humidity": Path("parsed_data_humidity_sensors.csv"),
        "light": Path("parsed_data_light_sensors.csv"),
        "temperature": Path("parsed_data_temperature_sensors.csv"),
    }

    # Open writers and write headers (overwrite existing files)
    writers = {}
    files = {}
    try:
        files["humidity"] = out_files["humidity"].open("w", newline="", encoding="utf-8")
        files["light"] = out_files["light"].open("w", newline="", encoding="utf-8")
        files["temperature"] = out_files["temperature"].open("w", newline="", encoding="utf-8")

        writers["humidity"] = csv.writer(files["humidity"])
        writers["light"] = csv.writer(files["light"])
        writers["temperature"] = csv.writer(files["temperature"])

        writers["humidity"].writerow(["time", "date", "humidity"])
        writers["light"].writerow(["time", "date", "light"])
        writers["temperature"].writerow(["time", "date", "temperature"])

        counts = {"humidity": 0, "light": 0, "temperature": 0}
        with open(path_to_file, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= how_many_rows_to_read:
                    break
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                # Expected: date, time, epoch, moteid, temperature, humidity, light, voltage
                if len(parts) < 8:
                    continue
                date, time = parts[0], parts[1]
                moteid = parts[3] # will not use it, progresses to slow (too many records :( )
                temperature = parts[4]
                humidity = parts[5]
                light = parts[6]

                chosen = random.choice(["humidity", "light", "temperature"])
                if chosen == "humidity":
                    writers["humidity"].writerow([time, date, humidity])
                elif chosen == "light":
                    writers["light"].writerow([time, date, light])
                else:
                    writers["temperature"].writerow([time, date, temperature])
                counts[chosen] += 1

        return counts

    finally:
        for fh in files.values():
            try:
                fh.close()
            except Exception:
                pass

def main():
    # Read configuration from config.json (in the current working directory)
    config_path = Path("config.json")
    if not config_path.exists():
        print("Configuration file 'config.json' not found.")
        return

    try:
        with config_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        print("Failed to read config.json:", e)
        return

    # Required fields
    path = cfg.get("path_to_data_file")
    if not path:
        print("'path_to_data_file' not set in config.json")
        return

    # rows_to_read (enforce integer and minimum inside parser too)
    try:
        rows = int(cfg.get("rows_to_read", 100))
    except (TypeError, ValueError):
        rows = 100

    # Read and keep other parameters (for now we only print/save them)
    num_devices_http = cfg.get("num_devices_http")
    num_devices_mqtt = cfg.get("num_devices_mqtt")
    message_interval_http = cfg.get("message_interval_http")
    message_interval_mqtt = cfg.get("message_interval_mqtt")
    protocol = cfg.get("protocol")
    mqtt_broker = cfg.get("mqtt_broker")
    mqtt_topic = cfg.get("mqtt_topic")
    http_server = cfg.get("http_server")

    config_summary = {
        "path_to_data_file": path,
        "rows_to_read": rows,
        "num_devices_http": num_devices_http,
        "num_devices_mqtt": num_devices_mqtt,
        "message_interval_http": message_interval_http,
        "message_interval_mqtt": message_interval_mqtt,
        "protocol": protocol,
        "mqtt_broker": mqtt_broker,
        "mqtt_topic": mqtt_topic,
        "http_server": http_server,
    }

    print("Loaded configuration:")
    for k, v in config_summary.items():
        print(f"  {k}: {v}")

    counts = initial_data_parser(path, rows)
    print("Parsing finished. Rows written per file:")
    for k, v in counts.items():
        print(f"  {k}: {v}")

    # Sensor files map
    sensor_files = {
        "humidity": Path("parsed_data_humidity_sensors.csv"),
        "light": Path("parsed_data_light_sensors.csv"),
        "temperature": Path("parsed_data_temperature_sensors.csv"),
    }

    devices = []

    if protocol and protocol.upper() == "MQTT":
        # switch storage output file
        set_output_file("mqtt_recorded_data.csv")
        # start local broker
        broker_host = mqtt_broker or "localhost"
        local_broker = LocalBroker(host=broker_host, port=1883)
        local_broker.start()
        print(f"Local MQTT broker started at {broker_host}:1883")
        # Start MQTT collector
        broker = mqtt_broker or "localhost"
        topic = mqtt_topic or "iot"
        mqtt_col = MqttCollector(broker_host=broker, topic=topic)
        mqtt_col.start()
        print(f"MQTT collector started and subscribed to topic '{topic}' on {broker}:1883")

        num_mqtt = int(num_devices_mqtt) if num_devices_mqtt else 0
        for i in range(1, num_mqtt + 1):
            device_id = "id_device" if i == 1 else f"id_device{i}"
            t = start_mqtt_device_thread(
                device_id=device_id,
                sensor_files=sensor_files,
                broker_host=broker,
                topic=topic,
                fixed_interval=(None if message_interval_mqtt == -1 else int(message_interval_mqtt)),
            )
            devices.append(t)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping MQTT devices and collector...")
            for t in devices:
                t.stop()
            for t in devices:
                t.join()
            mqtt_col.stop()
            local_broker.stop()
            print("Shutdown complete.")
    else:
        # Default: HTTP
        # Start HTTP collector server
        app = create_app()
        server = HttpCollectorServer(app, host="127.0.0.1", port=5000)
        server.start()
        print("HTTP collector server started at http://127.0.0.1:5000/collect")

        num_http = int(num_devices_http) if num_devices_http else 0
        # if message_interval_http == -1, use random interval per device between 4 and 10s
        for i in range(1, num_http + 1):
            device_id = "id_device" if i == 1 else f"id_device{i}"
            t = start_http_device_thread(
                device_id=device_id,
                sensor_files=sensor_files,
                collector_url="http://127.0.0.1:5000/collect",
                fixed_interval=(None if message_interval_http == -1 else int(message_interval_http)),
            )
            devices.append(t)

        # Run until Ctrl+C
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping device threads and HTTP server...")
            for t in devices:
                t.stop()
            for t in devices:
                t.join()
            server.stop()
            print("Shutdown complete.")

if __name__ == "__main__":
    main()