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

import boto3
import httpx
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "sandbox-eks-data-west")
LAMBDA_FUNCTION = os.environ.get("LAMBDA_FUNCTION", "sandbox-eks-processor")
UPSTREAM_TELEMETRY = os.environ.get("UPSTREAM_TELEMETRY", "http://telemetry-ingest.connected-vehicle.svc:80")
UPSTREAM_TRIP = os.environ.get("UPSTREAM_TRIP", "http://trip-service.connected-vehicle.svc:80")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(SERVICE_NAME)

app = FastAPI(title=f"Connected Vehicle — {SERVICE_NAME}")

s3 = boto3.client("s3", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
http_client = httpx.AsyncClient(timeout=10.0)

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
    return {"service": SERVICE_NAME, "status": "running", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/ready")
def ready():
    checks = {}
    try:
        if SERVICE_NAME in ("telemetry-ingest", "notification-service"):
            s3.head_bucket(Bucket=S3_BUCKET)
            checks["s3"] = "ok"
        if SERVICE_NAME in ("trip-service", "notification-service"):
            lambda_client.get_function(FunctionName=LAMBDA_FUNCTION)
            checks["lambda"] = "ok"
        if SERVICE_NAME == "vehicle-api":
            checks["self"] = "ok"
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
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
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
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
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
        resp = lambda_client.invoke(
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
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=20)
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
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=50)
        anomaly_objects = []
        for obj in resp.get("Contents", []):
            body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
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
        resp = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION)
        cfg = resp["Configuration"]
        return {"function": LAMBDA_FUNCTION, "state": cfg["State"], "runtime": cfg.get("Runtime"), "last_modified": cfg["LastModified"]}
    except ClientError as exc:
        logger.error("Lambda GetFunction FAILED — %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
