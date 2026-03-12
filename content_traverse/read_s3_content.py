import json
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")

BUCKET_NAME  = "bucket-name"
PREFIX       = "banking/output/"
ALLOWED_KEYS = ["SBI", "IDFC", "ICICI", "AXIS", "HDFC"]


def lambda_handler(event, context):
    # Parse ?key=AXIS or return all if no key provided
    params  = event.get("queryStringParameters") or {}
    raw_key = params.get("key", "").upper().replace(".JSON", "").strip()

    # Return all four JSONs at once
    if not raw_key:
        return _get_all()

    # Return single key
    if raw_key not in ALLOWED_KEYS:
        return _response(400, {"error": f"Invalid key. Allowed: {ALLOWED_KEYS}"})

    return _get_one(raw_key)


def _get_all():
    """Fetch and return all JSON files directly from S3."""
    result = {}
    for key in ALLOWED_KEYS:
        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{PREFIX}{key}.json")
            result[key] = json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            result[key] = {"error": str(e)}

    return _response(200, result)


def _get_one(key):
    """Fetch and return a single JSON file directly from S3."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{PREFIX}{key}.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return _response(200, data)
    except ClientError as e:
        return _response(500, {"error": str(e)})


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }