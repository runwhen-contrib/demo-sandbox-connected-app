# Connected Vehicle Demo — Telemetry Ingestion Pipeline

Application code for the connected vehicle telemetry demo. Infrastructure
(Crossplane manifests, EKS, S3, Lambda config) lives in
[infra-flux-nonprod-sandbox](https://github.com/runwhen/infra-flux-nonprod-sandbox).

## Architecture

```
Simulator → API Gateway → EKS vehicle-api → telemetry-ingest → S3 (raw)
                                           → trip-service → Lambda → S3 (processed)
                                                                   → EventBridge (anomalies)
```

All four EKS services run the same container image, differentiated by the
`SERVICE_NAME` environment variable.

## Repository Structure

```
src/app.py              EKS microservices (FastAPI)
lambda/index.py         Lambda anomaly processor
lambda/deploy.sh        Package + upload Lambda zip to S3
simulator/simulator.py  Telemetry generator (runs locally)
scripts/verify.py       Transaction verification (queries S3, CW, EB)
Dockerfile              EKS app container
.github/workflows/      CI: auto-build + push to ECR on merge
```

## Quick Start

### Run the simulator

```bash
cd simulator
pip install -r requirements.txt
python simulator.py --url https://api.aws.sandbox.runwhen.com --fleet 10 --batches 5
```

### Verify transactions

```bash
cd scripts
pip install boto3
export AWS_PROFILE=sandbox-admins-connected-app-dev-account
python verify.py
```

### Deploy Lambda code

```bash
cd lambda
./deploy.sh
```

### Build and push the EKS image manually

```bash
aws ecr get-login-password --region us-west-2 --profile sandbox-admins-connected-app-dev-account \
  | podman login --username AWS --password-stdin 526247032735.dkr.ecr.us-west-2.amazonaws.com

podman build --platform linux/amd64 -t connected-vehicle-demo:latest .
podman tag localhost/connected-vehicle-demo:latest \
  526247032735.dkr.ecr.us-west-2.amazonaws.com/connected-vehicle-demo:latest
podman push 526247032735.dkr.ecr.us-west-2.amazonaws.com/connected-vehicle-demo:latest

kubectl --context sandbox-eks -n connected-vehicle rollout restart deployment
```

## CI/CD

Merges to `main` that touch `src/`, `Dockerfile`, or `requirements.txt`
trigger a GitHub Actions workflow that builds and pushes to ECR.

**Required repo secrets**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
(IAM user `github-actions-ecr-push` in account `526247032735`).

## Telemetry Data Format

```json
{
  "vin": "1HGBH41JXMN109186",
  "timestamp": "2026-03-19T12:00:00Z",
  "location": {"lat": 45.5155, "lon": -122.6789},
  "speed_mph": 65,
  "engine_temp_f": 195,
  "fuel_pct": 72,
  "battery_v": 13.8,
  "odometer_mi": 45230,
  "tire_pressure_psi": {"fl": 33, "fr": 33, "rl": 32, "rr": 32}
}
```

## Anomaly Thresholds

| Metric | Threshold | Type |
|--------|-----------|------|
| Speed | > 100 mph | `overspeed` |
| Engine temp | > 240 F | `high_temperature` |
| Fuel | < 10% | `low_fuel` |
| Battery | < 11.5V | `low_battery` |
| Tire pressure | < 25 PSI | `low_tire_pressure` |
