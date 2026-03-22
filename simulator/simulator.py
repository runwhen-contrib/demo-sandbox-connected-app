#!/usr/bin/env python3
"""
Connected Vehicle Telemetry Simulator.

Generates realistic vehicle telemetry and sends it to the demo API.
Injects anomalies randomly (~5% of data points) to trigger Lambda
anomaly detection and EventBridge events.

Usage:
    pip install requests
    python simulator.py                          # defaults
    python simulator.py --url https://api.aws.sandbox.runwhen.com --fleet 10 --interval 2
"""

import argparse
import json
import random
import string
import sys
import time
from datetime import datetime, timezone

import requests

VINS = [
    "1HGBH41JXMN109186", "2T1BURHE5JC123456", "3FADP4BJ7EM234567",
    "5YFBURHE9JP345678", "1N4AL3AP8JC456789", "JM1BK32F781567890",
    "WDBRF61J21F678901", "1FTFW1EF5EFC78901", "2HGFB2F59CH890123",
    "3VW2K7AJ9CM901234",
]

CITIES = [
    ("Seattle", 47.6062, -122.3321),
    ("Portland", 45.5155, -122.6789),
    ("San Francisco", 37.7749, -122.4194),
    ("Los Angeles", 34.0522, -118.2437),
    ("Denver", 39.7392, -104.9903),
    ("Phoenix", 33.4484, -112.0740),
    ("Austin", 30.2672, -97.7431),
    ("Chicago", 41.8781, -87.6298),
    ("New York", 40.7128, -74.0060),
    ("Miami", 25.7617, -80.1918),
]


def generate_telemetry(vin: str, city_idx: int, inject_anomaly: bool = False) -> dict:
    city_name, base_lat, base_lon = CITIES[city_idx % len(CITIES)]
    lat = base_lat + random.uniform(-0.05, 0.05)
    lon = base_lon + random.uniform(-0.05, 0.05)

    speed = random.uniform(25, 75)
    engine_temp = random.uniform(180, 210)
    fuel = random.uniform(30, 90)
    battery = random.uniform(12.8, 14.2)
    tire_base = random.uniform(31, 35)
    tire_pressure = {
        "fl": round(tire_base + random.uniform(-1, 1), 1),
        "fr": round(tire_base + random.uniform(-1, 1), 1),
        "rl": round(tire_base + random.uniform(-1.5, 0.5), 1),
        "rr": round(tire_base + random.uniform(-1.5, 0.5), 1),
    }
    odometer = random.randint(5000, 120000)

    if inject_anomaly:
        anomaly_type = random.choice(["overspeed", "high_temp", "low_fuel", "low_battery", "low_tire"])
        if anomaly_type == "overspeed":
            speed = random.uniform(105, 140)
        elif anomaly_type == "high_temp":
            engine_temp = random.uniform(245, 280)
        elif anomaly_type == "low_fuel":
            fuel = random.uniform(2, 9)
        elif anomaly_type == "low_battery":
            battery = random.uniform(9.5, 11.4)
        elif anomaly_type == "low_tire":
            flat_tire = random.choice(["fl", "fr", "rl", "rr"])
            tire_pressure[flat_tire] = random.uniform(15, 24)

    return {
        "vin": vin,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": {"lat": round(lat, 6), "lon": round(lon, 6)},
        "speed_mph": round(speed, 1),
        "engine_temp_f": round(engine_temp, 1),
        "fuel_pct": round(fuel, 1),
        "battery_v": round(battery, 2),
        "odometer_mi": odometer,
        "tire_pressure_psi": tire_pressure,
    }


def main():
    parser = argparse.ArgumentParser(description="Connected Vehicle Telemetry Simulator")
    parser.add_argument("--url", default="https://api.aws.sandbox.runwhen.com", help="API base URL")
    parser.add_argument("--fleet", type=int, default=10, help="Number of vehicles")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between batches")
    parser.add_argument("--batches", type=int, default=0, help="Number of batches (0 = infinite)")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Fraction of anomalous readings")
    args = parser.parse_args()

    endpoint = f"{args.url.rstrip('/')}/api/v1/telemetry"
    fleet_size = min(args.fleet, len(VINS))
    fleet = VINS[:fleet_size]

    print(f"Simulator started — {fleet_size} vehicles, target: {endpoint}")
    print(f"Anomaly rate: {args.anomaly_rate:.0%}, interval: {args.interval}s\n")

    success = 0
    failed = 0
    anomalies_sent = 0
    batch_num = 0

    try:
        while True:
            batch_num += 1
            if args.batches > 0 and batch_num > args.batches:
                break

            for i, vin in enumerate(fleet):
                inject = random.random() < args.anomaly_rate
                telemetry = generate_telemetry(vin, i, inject_anomaly=inject)

                try:
                    resp = requests.post(endpoint, json=telemetry, timeout=15)
                    if resp.status_code < 300:
                        success += 1
                    else:
                        failed += 1
                        print(f"  FAIL [{resp.status_code}] VIN={vin}: {resp.text[:200]}")
                except requests.RequestException as exc:
                    failed += 1
                    print(f"  ERROR VIN={vin}: {exc}")

                if inject:
                    anomalies_sent += 1

            total = success + failed
            print(
                f"[Batch {batch_num}] sent={total} ok={success} fail={failed} "
                f"anomalies_injected={anomalies_sent}"
            )

            if args.batches == 0 or batch_num < args.batches:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped.")

    print(f"\nSummary: {success + failed} total, {success} ok, {failed} failed, {anomalies_sent} anomalies injected")


if __name__ == "__main__":
    main()
