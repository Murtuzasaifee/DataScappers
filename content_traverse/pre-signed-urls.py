import json
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")

BUCKET_NAME    = "bucket-name"
PREFIX         = "banking/output/"
EXPIRY_SECONDS = 21600 # 6 hours
ALLOWED_KEYS   = ["SBI", "ICICI", "AXIS", "HDFC","IDFC"]


def lambda_handler(event, context):
    try:
        urls = {
            key: s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET_NAME, "Key": f"{PREFIX}{key}.json"},
                ExpiresIn=EXPIRY_SECONDS,
            )
            for key in ALLOWED_KEYS
        }
        return _response(200, urls)

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