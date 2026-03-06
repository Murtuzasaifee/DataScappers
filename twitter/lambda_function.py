"""
AWS Lambda Handler — Twitter/Social Media Scraper
Entry point for the Lambda function.

Expected event payload:
{
    "search_query": "HDFC Bank",
    "days_back": 1,                        # 0=today, 1=yesterday, 2=last two days, etc.
    "s3_bucket": "my-ews-bucket",
    "s3_prefix": "twitter/raw"             # optional, default: "raw"
}
"""

import json
import logging

from config import get_config
from apify_client import fetch_tweets
from transformer import transform_apify_response
from s3_uploader import upload_to_s3
from date_utils import build_date_range, build_s3_key

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point.

    Parameters
    ----------
    event : dict
        Trigger payload (see module docstring for schema).
    context : LambdaContext
        AWS Lambda context object (unused but required by signature).

    Returns
    -------
    dict
        HTTP-style response with statusCode and body.
    """
    logger.info("Lambda invoked with event: %s", json.dumps(event))

    try:
        # ------------------------------------------------------------------ #
        # 1. Validate & extract inputs
        # ------------------------------------------------------------------ #
        search_query: str = _require(event, "search_query")
        days_back: int    = int(_require(event, "days_back"))
        s3_bucket: str    = _require(event, "s3_bucket")
        s3_prefix: str    = event.get("s3_prefix", "raw")

        if days_back < 0:
            raise ValueError("days_back must be >= 0")

        # ------------------------------------------------------------------ #
        # 2. Load environment-driven config (API keys, etc.)
        # ------------------------------------------------------------------ #
        config = get_config()

        # ------------------------------------------------------------------ #
        # 3. Build since / until date strings
        # ------------------------------------------------------------------ #
        since_date, until_date = build_date_range(days_back)
        logger.info("Date range — since: %s  until: %s", since_date, until_date)

        # ------------------------------------------------------------------ #
        # 4. Fetch raw data from Apify
        # ------------------------------------------------------------------ #
        raw_items = fetch_tweets(
            api_token=config["apify_api_token"],
            search_query=search_query,
            since=since_date,
            until=until_date,
            max_items=config["max_items"],
        )
        logger.info("Fetched %d raw items from Apify", len(raw_items))

        # ------------------------------------------------------------------ #
        # 5. Transform to desired output schema
        # ------------------------------------------------------------------ #
        output_payload = transform_apify_response(raw_items)
        logger.info(
            "Transformed payload — tweets: %d",
            len(output_payload.get("twitter", [])),
        )

        # ------------------------------------------------------------------ #
        # 6. Build S3 key with timestamp and upload
        # ------------------------------------------------------------------ #
        s3_key = build_s3_key(prefix=s3_prefix, query=search_query)
        s3_uri = upload_to_s3(
            bucket=s3_bucket,
            key=s3_key,
            payload=output_payload,
        )
        logger.info("Uploaded output to %s", s3_uri)

        # ------------------------------------------------------------------ #
        # 7. Return success response
        # ------------------------------------------------------------------ #
        return _response(
            200,
            {
                "message": "Success",
                "s3_uri": s3_uri,
                "tweet_count": len(output_payload.get("twitter", [])),
                "since": since_date,
                "until": until_date,
            },
        )

    except (KeyError, ValueError) as exc:
        logger.error("Input validation error: %s", exc)
        return _response(400, {"error": str(exc)})

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Unhandled exception in lambda_handler")
        return _response(500, {"error": str(exc)})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require(event: dict, key: str) -> str:
    """Raise a descriptive KeyError when a required event key is missing."""
    if key not in event or event[key] is None:
        raise KeyError(f"Required input '{key}' is missing from event payload.")
    return event[key]


def _response(status_code: int, body: dict) -> dict:
    """Build a standard Lambda HTTP-style response envelope."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }