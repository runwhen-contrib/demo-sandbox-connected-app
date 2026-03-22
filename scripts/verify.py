#!/usr/bin/env python3
"""
Connected Vehicle — Transaction Verification Script.

Queries S3, CloudWatch, and EventBridge to show evidence of
successful/failed telemetry processing.

Usage:
    pip install boto3
    export AWS_PROFILE=sandbox-admins-connected-app-dev-account
    python verify.py
    python verify.py --date 2026-03-22
"""

import argparse
import json
from datetime import datetime, timezone

import boto3

S3_BUCKET = "sandbox-eks-data-west"
LAMBDA_FUNCTION = "sandbox-eks-processor"
LOG_GROUP = "/aws/events/connected-vehicle-anomaly"
LAMBDA_LOG_GROUP = f"/aws/lambda/{LAMBDA_FUNCTION}"
REGION = "us-west-2"

s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def count_s3_objects(prefix: str) -> int:
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        count += page.get("KeyCount", 0)
    return count


def get_lambda_metrics(start_time, end_time) -> dict:
    metrics = {}
    for metric_name in ["Invocations", "Errors", "Throttles", "Duration"]:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName=metric_name,
            Dimensions=[{"Name": "FunctionName", "Value": LAMBDA_FUNCTION}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=["Sum"] if metric_name != "Duration" else ["Average"],
        )
        datapoints = resp.get("Datapoints", [])
        if datapoints:
            stat = "Sum" if metric_name != "Duration" else "Average"
            metrics[metric_name.lower()] = datapoints[0].get(stat, 0)
        else:
            metrics[metric_name.lower()] = 0
    return metrics


def count_eventbridge_events(start_time) -> int:
    try:
        resp = logs.filter_log_events(
            logGroupName=LOG_GROUP,
            startTime=int(start_time.timestamp() * 1000),
            limit=1000,
        )
        return len(resp.get("events", []))
    except logs.exceptions.ResourceNotFoundException:
        return -1  # log group doesn't exist yet


def main():
    parser = argparse.ArgumentParser(description="Transaction Verification")
    parser.add_argument("--date", default=None, help="Date to check (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    if args.date:
        check_date = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        check_date = datetime.now(timezone.utc)

    date_str = check_date.strftime("%Y/%m/%d")
    start_of_day = check_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = check_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    print(f"Connected Vehicle Demo — Verification Report")
    print(f"Date: {check_date.strftime('%Y-%m-%d')}")
    print(f"{'=' * 55}")

    raw_count = count_s3_objects(f"telemetry/{date_str}/")
    processed_count = count_s3_objects(f"processed/{date_str}/")
    lambda_metrics = get_lambda_metrics(start_of_day, end_of_day)
    eb_events = count_eventbridge_events(start_of_day)

    print(f"\n{'Metric':<35} {'Value':>15}")
    print(f"{'-' * 55}")
    print(f"{'Raw telemetry stored (S3)':<35} {raw_count:>15,}")
    print(f"{'Processed results (S3)':<35} {processed_count:>15,}")
    print(f"{'Lambda invocations':<35} {int(lambda_metrics['invocations']):>15,}")
    print(f"{'Lambda errors':<35} {int(lambda_metrics['errors']):>15,}")
    print(f"{'Lambda throttles':<35} {int(lambda_metrics['throttles']):>15,}")
    print(f"{'Lambda avg duration (ms)':<35} {lambda_metrics['duration']:>15,.1f}")

    if eb_events >= 0:
        print(f"{'EventBridge anomaly events':<35} {eb_events:>15,}")
    else:
        print(f"{'EventBridge anomaly events':<35} {'(log group not found)':>15}")

    print(f"\n{'=' * 55}")

    if raw_count > 0 and processed_count > 0 and lambda_metrics["errors"] == 0:
        print("Pipeline status: HEALTHY")
    elif raw_count > 0 and lambda_metrics["errors"] > 0:
        print("Pipeline status: DEGRADED (Lambda errors detected)")
    elif raw_count == 0:
        print("Pipeline status: NO DATA (no telemetry received today)")
    else:
        print("Pipeline status: PARTIAL (check individual metrics)")


if __name__ == "__main__":
    main()
