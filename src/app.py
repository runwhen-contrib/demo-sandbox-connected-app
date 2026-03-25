"""
Connected Vehicle Demo — Telemetry Ingestion Pipeline.

Single container image, behavior driven by SERVICE_NAME env var.
Each service participates in the critical data path:

  Simulator → API Gateway → vehicle-api → telemetry-ingest (S3)
                                        → trip-service (Lambda → S3 + EventBridge)
  notification-service polls S3 + Lambda for pipeline health.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from pathlib import Path

import boto3
import httpx
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "sandbox-eks-data-west")
LAMBDA_FUNCTION = os.environ.get("LAMBDA_FUNCTION", "sandbox-eks-processor")
UPSTREAM_TELEMETRY = os.environ.get("UPSTREAM_TELEMETRY", "http://telemetry-ingest.connected-vehicle.svc:80")
UPSTREAM_TRIP = os.environ.get("UPSTREAM_TRIP", "http://trip-service.connected-vehicle.svc:80")
UPSTREAM_VEHICLE_API = os.environ.get("UPSTREAM_VEHICLE_API", "http://vehicle-api.connected-vehicle.svc:80")
UPSTREAM_NOTIFICATION = os.environ.get("UPSTREAM_NOTIFICATION", "http://notification-service.connected-vehicle.svc:80")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
STATIC_DIR = Path(__file__).parent / "static"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(SERVICE_NAME)

app = FastAPI(title=f"Connected Vehicle — {SERVICE_NAME}")

_s3 = None
_lambda_client = None
http_client = httpx.AsyncClient(timeout=10.0)


def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=AWS_REGION)
    return _s3


def get_lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    return _lambda_client

REQUIRED_FIELDS = {"vin", "timestamp", "location", "speed_mph", "engine_temp_f", "fuel_pct"}


def _validate_telemetry(data: dict) -> list[str]:
    """Return list of validation errors, empty if valid."""
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in data:
            errors.append(f"missing required field: {field}")
    if "location" in data:
        loc = data["location"]
        if not isinstance(loc, dict) or "lat" not in loc or "lon" not in loc:
            errors.append("location must have lat and lon")
    return errors


# ---------------------------------------------------------------------------
# Shared endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    if SERVICE_NAME == "dashboard":
        html = (STATIC_DIR / "dashboard.html").read_text()
        return HTMLResponse(content=html)
    return {"service": SERVICE_NAME, "status": "running", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/ready")
async def ready():
    checks = {}
    if SERVICE_NAME in ("vehicle-api", "dashboard"):
        checks["self"] = "ok"
        return {"status": "ready", "service": SERVICE_NAME, "checks": checks}
    try:
        loop = asyncio.get_event_loop()
        if SERVICE_NAME in ("telemetry-ingest", "notification-service"):
            await loop.run_in_executor(None, lambda: get_s3().head_bucket(Bucket=S3_BUCKET))
            checks["s3"] = "ok"
        if SERVICE_NAME in ("trip-service", "notification-service"):
            await loop.run_in_executor(None, lambda: get_lambda().get_function(FunctionName=LAMBDA_FUNCTION))
            checks["lambda"] = "ok"
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"]["Message"]
        logger.error("Readiness check failed: %s — %s", code, msg)
        return JSONResponse(status_code=503, content={"status": "degraded", "service": SERVICE_NAME, "checks": checks, "error": msg})
    return {"status": "ready", "service": SERVICE_NAME, "checks": checks}


# ---------------------------------------------------------------------------
# vehicle-api — gateway, fans out to telemetry-ingest + trip-service
# ---------------------------------------------------------------------------

@app.post("/api/v1/telemetry")
async def receive_telemetry(request: Request):
    if SERVICE_NAME != "vehicle-api":
        raise HTTPException(404)

    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"error": "invalid JSON"})

    errors = _validate_telemetry(data)
    if errors:
        return JSONResponse(status_code=422, content={"error": "validation failed", "details": errors})

    logger.info("Received telemetry for VIN=%s, forwarding to downstream services", data.get("vin"))

    async def _forward(name, url, path):
        try:
            resp = await http_client.post(f"{url}{path}", content=body, headers={"Content-Type": "application/json"})
            return name, resp.status_code, resp.json()
        except httpx.ConnectError:
            logger.error("Connection refused to %s at %s", name, url)
            return name, 502, {"error": f"{name} unreachable"}
        except Exception as exc:
            logger.exception("Error forwarding to %s", name)
            return name, 502, {"error": str(exc)}

    results = await asyncio.gather(
        _forward("telemetry-ingest", UPSTREAM_TELEMETRY, "/ingest"),
        _forward("trip-service", UPSTREAM_TRIP, "/process"),
    )

    response = {"vin": data.get("vin"), "results": {}}
    overall_status = 200
    for name, status, payload in results:
        response["results"][name] = {"status": status, "body": payload}
        if status >= 400:
            logger.error("%s returned %d: %s", name, status, json.dumps(payload)[:300])
            overall_status = 207  # multi-status: partial success

    return JSONResponse(status_code=overall_status, content=response)


@app.get("/api/v1/health")
async def aggregated_health():
    if SERVICE_NAME != "vehicle-api":
        raise HTTPException(404)

    results = {}
    for name, url in [("telemetry-ingest", UPSTREAM_TELEMETRY), ("trip-service", UPSTREAM_TRIP)]:
        try:
            resp = await http_client.get(f"{url}/ready", timeout=5.0)
            results[name] = resp.json()
        except Exception as exc:
            results[name] = {"status": "unreachable", "error": str(exc)}
    all_ok = all(r.get("status") == "ready" for r in results.values())
    return {"service": SERVICE_NAME, "status": "healthy" if all_ok else "degraded", "downstream": results}


@app.get("/api/v1/status")
async def pipeline_status():
    """Quick count of today's telemetry in S3."""
    if SERVICE_NAME != "vehicle-api":
        raise HTTPException(404)
    try:
        ts = datetime.now(timezone.utc)
        prefix = f"telemetry/{ts:%Y/%m/%d}/"
        resp = get_s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
        count = resp.get("KeyCount", 0)
        return {"date": f"{ts:%Y-%m-%d}", "telemetry_stored": count, "bucket": S3_BUCKET}
    except ClientError as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ---------------------------------------------------------------------------
# telemetry-ingest — writes raw telemetry to S3
# ---------------------------------------------------------------------------

@app.post("/ingest")
async def ingest_telemetry(request: Request):
    if SERVICE_NAME != "telemetry-ingest":
        raise HTTPException(404)

    body = await request.body()
    data = json.loads(body)
    vin = data.get("vin", "unknown")
    ts = datetime.now(timezone.utc)
    key = f"telemetry/{ts:%Y/%m/%d}/{vin}/{ts:%H%M%S}-{int(ts.timestamp()*1000)}.json"

    logger.info("Writing telemetry to s3://%s/%s (%d bytes)", S3_BUCKET, key, len(body))
    try:
        get_s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        logger.info("Stored s3://%s/%s", S3_BUCKET, key)
        return {"status": "stored", "bucket": S3_BUCKET, "key": key}
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"]["Message"]
        logger.error("S3 PutObject FAILED — %s: %s (bucket=%s, key=%s)", code, msg, S3_BUCKET, key)
        return JSONResponse(status_code=500, content={"error": f"S3 write failed: {code}", "detail": msg})


# ---------------------------------------------------------------------------
# trip-service — invokes Lambda for anomaly detection
# ---------------------------------------------------------------------------

@app.post("/process")
async def process_telemetry(request: Request):
    if SERVICE_NAME != "trip-service":
        raise HTTPException(404)

    body = await request.body()
    data = json.loads(body)
    logger.info("Invoking Lambda %s for VIN=%s", LAMBDA_FUNCTION, data.get("vin"))

    try:
        resp = get_lambda().invoke(
            FunctionName=LAMBDA_FUNCTION,
            InvocationType="RequestResponse",
            Payload=body,
        )
        status = resp["StatusCode"]
        payload = json.loads(resp["Payload"].read())

        if "FunctionError" in resp:
            logger.error("Lambda %s FunctionError: %s", LAMBDA_FUNCTION, payload)
            return JSONResponse(status_code=502, content={"error": "Lambda execution error", "detail": payload})

        logger.info("Lambda %s returned %d, anomalies=%s", LAMBDA_FUNCTION, status, payload.get("anomalies_found", 0))
        return {"status": "processed", "lambda_status": status, "result": payload}
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"]["Message"]
        logger.error("Lambda invoke FAILED — %s: %s (function=%s)", code, msg, LAMBDA_FUNCTION)
        return JSONResponse(status_code=500, content={"error": f"Lambda invoke failed: {code}", "detail": msg})


# ---------------------------------------------------------------------------
# notification-service — monitors pipeline health
# ---------------------------------------------------------------------------

@app.get("/notifications")
def list_recent_telemetry():
    if SERVICE_NAME != "notification-service":
        raise HTTPException(404)
    try:
        ts = datetime.now(timezone.utc)
        prefix = f"telemetry/{ts:%Y/%m/%d}/"
        resp = get_s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=20)
        objects = [{"key": o["Key"], "size": o["Size"], "modified": o["LastModified"].isoformat()} for o in resp.get("Contents", [])]
        return {"bucket": S3_BUCKET, "prefix": prefix, "count": len(objects), "objects": objects}
    except ClientError as exc:
        logger.error("S3 ListObjects FAILED — %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/anomalies")
def list_recent_anomalies():
    """Lists recent processed results that contain anomalies."""
    if SERVICE_NAME != "notification-service":
        raise HTTPException(404)
    try:
        ts = datetime.now(timezone.utc)
        prefix = f"processed/{ts:%Y/%m/%d}/"
        resp = get_s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=50)
        anomaly_objects = []
        for obj in resp.get("Contents", []):
            body = get_s3().get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            data = json.loads(body)
            if data.get("anomalies_found", 0) > 0:
                anomaly_objects.append({"key": obj["Key"], "anomalies": data.get("anomalies", []), "vin": data.get("vin")})
        return {"count": len(anomaly_objects), "anomalies": anomaly_objects}
    except ClientError as exc:
        logger.error("S3 read FAILED — %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/lambda-status")
def lambda_health():
    if SERVICE_NAME != "notification-service":
        raise HTTPException(404)
    try:
        resp = get_lambda().get_function(FunctionName=LAMBDA_FUNCTION)
        cfg = resp["Configuration"]
        return {"function": LAMBDA_FUNCTION, "state": cfg["State"], "runtime": cfg.get("Runtime"), "last_modified": cfg["LastModified"]}
    except ClientError as exc:
        logger.error("Lambda GetFunction FAILED — %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ---------------------------------------------------------------------------
# dashboard — proxies to other services so the browser only talks to one host
# ---------------------------------------------------------------------------

async def _proxy_get(upstream_url: str, path: str):
    try:
        resp = await http_client.get(f"{upstream_url}{path}", timeout=8.0)
        return JSONResponse(status_code=resp.status_code, content=resp.json())
    except Exception as exc:
        logger.error("Dashboard proxy error: %s%s — %s", upstream_url, path, exc)
        return JSONResponse(status_code=502, content={"error": str(exc)})


@app.get("/dash/health")
async def dash_health():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    return await _proxy_get(UPSTREAM_VEHICLE_API, "/api/v1/health")


@app.get("/dash/status")
async def dash_status():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    return await _proxy_get(UPSTREAM_VEHICLE_API, "/api/v1/status")


@app.get("/dash/telemetry")
async def dash_telemetry():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    return await _proxy_get(UPSTREAM_NOTIFICATION, "/notifications")


@app.get("/dash/anomalies")
async def dash_anomalies():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    return await _proxy_get(UPSTREAM_NOTIFICATION, "/anomalies")


@app.get("/dash/lambda")
async def dash_lambda():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    return await _proxy_get(UPSTREAM_NOTIFICATION, "/lambda-status")


# ---------------------------------------------------------------------------
# dashboard — embedded simulator (start/stop from the UI)
# ---------------------------------------------------------------------------

import random

VINS = [
    "1HGBH41JXMN109186", "2T1BURHE5JC123456", "3FADP4BJ7EM234567",
    "5YFBURHE9JP345678", "1N4AL3AP8JC456789", "JM1BK32F781567890",
    "WDBRF61J21F678901", "1FTFW1EF5EFC78901", "2HGFB2F59CH890123",
    "3VW2K7AJ9CM901234",
]
CITIES = [
    (47.6062, -122.3321), (45.5155, -122.6789), (37.7749, -122.4194),
    (34.0522, -118.2437), (39.7392, -104.9903), (33.4484, -112.0740),
    (30.2672, -97.7431), (41.8781, -87.6298), (40.7128, -74.0060),
    (25.7617, -80.1918),
]

_sim_task: asyncio.Task | None = None
_sim_stats = {"running": False, "sent": 0, "errors": 0, "anomalies_injected": 0}
_ambient_task: asyncio.Task | None = None
_ambient_stats = {"sent": 0, "errors": 0}
AMBIENT_FLEET = 2
AMBIENT_INTERVAL = 10.0
AMBIENT_ANOMALY_RATE = 0.02


def _gen_telemetry(vin: str, idx: int, inject: bool) -> dict:
    lat, lon = CITIES[idx % len(CITIES)]
    speed = random.uniform(25, 75)
    temp = random.uniform(180, 210)
    fuel = random.uniform(30, 90)
    batt = random.uniform(12.8, 14.2)
    tp = {k: round(random.uniform(31, 35), 1) for k in ("fl", "fr", "rl", "rr")}
    if inject:
        pick = random.choice(["speed", "temp", "fuel", "batt", "tire"])
        if pick == "speed":   speed = random.uniform(105, 140)
        elif pick == "temp":  temp = random.uniform(245, 280)
        elif pick == "fuel":  fuel = random.uniform(2, 9)
        elif pick == "batt":  batt = random.uniform(9.5, 11.4)
        elif pick == "tire":  tp[random.choice(list(tp))] = random.uniform(15, 24)
    return {
        "vin": vin,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": {"lat": round(lat + random.uniform(-0.05, 0.05), 6),
                      "lon": round(lon + random.uniform(-0.05, 0.05), 6)},
        "speed_mph": round(speed, 1),
        "engine_temp_f": round(temp, 1),
        "fuel_pct": round(fuel, 1),
        "battery_v": round(batt, 2),
        "odometer_mi": random.randint(5000, 120000),
        "tire_pressure_psi": tp,
    }


async def _sim_loop(fleet_size: int, interval: float, anomaly_rate: float):
    endpoint = f"{UPSTREAM_VEHICLE_API}/api/v1/telemetry"
    fleet = VINS[:fleet_size]
    logger.info("Simulator started — %d vehicles, interval=%.1fs, anomaly_rate=%.0f%%",
                fleet_size, interval, anomaly_rate * 100)
    while True:
        for i, vin in enumerate(fleet):
            inject = random.random() < anomaly_rate
            payload = _gen_telemetry(vin, i, inject)
            try:
                resp = await http_client.post(endpoint, json=payload, timeout=15.0)
                if resp.status_code < 300:
                    _sim_stats["sent"] += 1
                else:
                    _sim_stats["errors"] += 1
            except Exception:
                _sim_stats["errors"] += 1
            if inject:
                _sim_stats["anomalies_injected"] += 1
        await asyncio.sleep(interval)


async def _ambient_loop():
    """Always-on background trickle so the dashboard never looks empty."""
    endpoint = f"{UPSTREAM_VEHICLE_API}/api/v1/telemetry"
    fleet = VINS[:AMBIENT_FLEET]
    logger.info("Ambient trickle started — %d vehicles every %.0fs", AMBIENT_FLEET, AMBIENT_INTERVAL)
    while True:
        for i, vin in enumerate(fleet):
            inject = random.random() < AMBIENT_ANOMALY_RATE
            payload = _gen_telemetry(vin, i, inject)
            try:
                resp = await http_client.post(endpoint, json=payload, timeout=15.0)
                if resp.status_code < 300:
                    _ambient_stats["sent"] += 1
                else:
                    _ambient_stats["errors"] += 1
            except Exception:
                _ambient_stats["errors"] += 1
        await asyncio.sleep(AMBIENT_INTERVAL)


@app.on_event("startup")
async def _start_ambient():
    global _ambient_task
    if SERVICE_NAME == "dashboard":
        _ambient_task = asyncio.create_task(_ambient_loop())


@app.post("/dash/simulator/start")
async def sim_start(request: Request):
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    global _sim_task
    if _sim_task and not _sim_task.done():
        return {"status": "already_running", **_sim_stats}
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    fleet = min(int(body.get("fleet", 10)), len(VINS))
    interval = max(float(body.get("interval", 3)), 0.5)
    anomaly_rate = float(body.get("anomaly_rate", 0.05))
    _sim_stats.update({"running": True, "sent": 0, "errors": 0, "anomalies_injected": 0})
    _sim_task = asyncio.create_task(_sim_loop(fleet, interval, anomaly_rate))
    return {"status": "started", "fleet": fleet, "interval": interval, "anomaly_rate": anomaly_rate}


@app.post("/dash/simulator/stop")
async def sim_stop():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    global _sim_task
    if _sim_task and not _sim_task.done():
        _sim_task.cancel()
        _sim_task = None
        _sim_stats["running"] = False
        return {"status": "stopped", **_sim_stats}
    return {"status": "not_running"}


@app.get("/dash/simulator/status")
async def sim_status():
    if SERVICE_NAME != "dashboard":
        raise HTTPException(404)
    running = _sim_task is not None and not _sim_task.done()
    _sim_stats["running"] = running
    return _sim_stats
