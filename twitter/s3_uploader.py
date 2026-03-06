"""
s3_uploader.py — Upload JSON payload to Amazon S3.

Uses boto3's S3 client (available in the Lambda runtime by default).
The Lambda execution role must have the ``s3:PutObject`` permission
on the target bucket.
"""

import json
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# Reuse a single boto3 client across warm Lambda invocations
_s3_client = boto3.client("s3")


def upload_to_s3(bucket: str, key: str, payload: dict) -> str:
    """
    Serialise ``payload`` to JSON and upload it to S3.

    Parameters
    ----------
    bucket : str
        Target S3 bucket name.
    key : str
        Full S3 object key (path inside the bucket), e.g.
        ``twitter/raw/HDFC_Bank/2026-03-05_10-30-00_UTC.json``.
    payload : dict
        Data to serialise and upload.

    Returns
    -------
    str
        S3 URI of the uploaded object, e.g.
        ``s3://my-bucket/twitter/raw/HDFC_Bank/2026-03-05_10-30-00_UTC.json``.

    Raises
    ------
    botocore.exceptions.ClientError
        If the upload fails (e.g. bucket not found, permissions error).
    """
    json_body = json.dumps(payload, ensure_ascii=False, indent=2)
    body_bytes = json_body.encode("utf-8")

    logger.info(
        "Uploading %d bytes to s3://%s/%s", len(body_bytes), bucket, key
    )

    try:
        _s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body_bytes,
            ContentType="application/json; charset=utf-8",
        )
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "S3 ClientError [%s] uploading to s3://%s/%s: %s",
            error_code, bucket, key, exc,
        )
        raise
    except BotoCoreError as exc:
        logger.error(
            "BotoCoreError uploading to s3://%s/%s: %s", bucket, key, exc
        )
        raise

    s3_uri = f"s3://{bucket}/{key}"
    logger.info("Successfully uploaded to %s", s3_uri)
    return s3_uri