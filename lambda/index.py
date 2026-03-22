"""
Connected Vehicle Lambda — Anomaly Detection Processor.

Receives telemetry from trip-service (EKS), checks thresholds,
writes processed results to S3, publishes anomaly events to EventBridge.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger("anomaly-processor")
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("BUCKET_NAME", "sandbox-eks-data-west")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "sandbox")
EVENT_BUS = os.environ.get("EVENT_BUS", "default")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-west-2"))

s3 = boto3.client("s3", region_name=AWS_REGION)
events = boto3.client("events", region_name=AWS_REGION)

THRESHOLDS = {
    "overspeed": lambda d: d.get("speed_mph", 0) > 100,
    "high_temperature": lambda d: d.get("engine_temp_f", 0) > 240,
    "low_fuel": lambda d: d.get("fuel_pct", 100) < 10,
    "low_battery": lambda d: d.get("battery_v", 14) < 11.5,
    "low_tire_pressure": lambda d: any(
        v < 25 for v in (d.get("tire_pressure_psi") or {}).values()
    ),
}


def handler(event, context):
    ts = datetime.now(timezone.utc)
    vin = event.get("vin", "unknown")
    logger.info("Processing telemetry for VIN=%s", vin)

    anomalies = []
    for anomaly_type, check_fn in THRESHOLDS.items():
        try:
            if check_fn(event):
                anomalies.append({
                    "type": anomaly_type,
                    "vin": vin,
                    "timestamp": event.get("timestamp", ts.isoformat()),
                    "value": _extract_value(event, anomaly_type),
                })
        except Exception as exc:
            logger.warning("Threshold check %s failed: %s", anomaly_type, exc)

    result = {
        "vin": vin,
        "timestamp": event.get("timestamp", ts.isoformat()),
        "processed_at": ts.isoformat(),
        "environment": ENVIRONMENT,
        "anomalies_found": len(anomalies),
        "anomalies": anomalies,
    }

    processed_key = f"processed/{ts:%Y/%m/%d}/{vin}/{ts:%H%M%S}-{int(ts.timestamp()*1000)}.json"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=processed_key,
            Body=json.dumps(result),
            ContentType="application/json",
        )
        logger.info("Stored processed result at s3://%s/%s", S3_BUCKET, processed_key)
    except Exception as exc:
        logger.error("S3 write FAILED for processed result: %s", exc)
        result["s3_error"] = str(exc)

    if anomalies:
        try:
            events.put_events(Entries=[{
                "Source": "connected-vehicle-demo",
                "DetailType": "TelemetryAnomaly",
                "Detail": json.dumps({
                    "vin": vin,
                    "environment": ENVIRONMENT,
                    "anomalies": anomalies,
                    "timestamp": event.get("timestamp", ts.isoformat()),
                }),
                "EventBusName": EVENT_BUS,
            }])
            logger.info("Published %d anomaly event(s) to EventBridge for VIN=%s", len(anomalies), vin)
        except Exception as exc:
            logger.error("EventBridge publish FAILED: %s", exc)
            result["eventbridge_error"] = str(exc)

    return {
        "statusCode": 200,
        "body": result,
        **result,
    }


def _extract_value(data, anomaly_type):
    """Pull the relevant metric value for the anomaly report."""
    mapping = {
        "overspeed": ("speed_mph", data.get("speed_mph")),
        "high_temperature": ("engine_temp_f", data.get("engine_temp_f")),
        "low_fuel": ("fuel_pct", data.get("fuel_pct")),
        "low_battery": ("battery_v", data.get("battery_v")),
        "low_tire_pressure": ("tire_pressure_psi", data.get("tire_pressure_psi")),
    }
    metric, value = mapping.get(anomaly_type, ("unknown", None))
    return {"metric": metric, "value": value}
