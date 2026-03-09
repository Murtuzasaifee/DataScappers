"""
lambda_function.py — Social Media Scraper (Twitter via Apify)

Triggered every 4 hours by EventBridge. Fetches tweets for multiple
bank entities IN PARALLEL and stores results in S3.

Environment Variables:
    APIFY_API_TOKEN : str  — Apify API token (required)
    MAX_ITEMS       : int  — Max tweets per query (default: 100)
    MAX_WORKERS     : int  — Parallel threads (default: 4)

EventBridge Input:
    {
        "days_back": 0,
        "s3_bucket": "s3-bucket-name",
        "queries": [
            {"search_query": "HDFC Bank",  "s3_prefix": "banking/HDFC/socialmedia"},
            {"search_query": "ICICI Bank", "s3_prefix": "banking/ICICI/socialmedia"},
            {"search_query": "SBI Bank",   "s3_prefix": "banking/SBI/socialmedia"},
            {"search_query": "Axis Bank",  "s3_prefix": "banking/AXIS/socialmedia"}
        ]
    }
"""

import json
import logging
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APIFY_URL = (
    "https://api.apify.com/v2/acts/altimis~scweet"
    "/run-sync-get-dataset-items"
)
APIFY_TIMEOUT = 300

s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    try:
        api_token   = _get_env("APIFY_API_TOKEN")
        max_items   = int(os.environ.get("MAX_ITEMS", "100"))
        max_workers = int(os.environ.get("MAX_WORKERS", "4"))
        days_back   = int(event["days_back"])
        s3_bucket   = event["s3_bucket"]
        queries     = event["queries"]
        since, until = _date_range(days_back)

        logger.info(
            "Run started — since: %s  until: %s  queries: %d  workers: %d",
            since, until, len(queries), max_workers,
        )

        # ------------------------------------------------------------------ #
        # Dispatch all queries in parallel
        # ------------------------------------------------------------------ #
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_query = {
                executor.submit(
                    _process_query,
                    q["search_query"],
                    q["s3_prefix"],
                    s3_bucket,
                    api_token,
                    since,
                    until,
                    max_items,
                ): q["search_query"]
                for q in queries
            }

            # Collect results as each future completes
            for future in as_completed(future_to_query):
                results.append(future.result())

        # Sort results to keep output order consistent
        results.sort(key=lambda r: r["query"])

        logger.info(
            "Run complete — success: %d  failed: %d",
            sum(1 for r in results if "error" not in r),
            sum(1 for r in results if "error" in r),
        )

        return _response(200, {"since": since, "until": until, "results": results})

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Fatal error in lambda_handler")
        return _response(500, {"error": str(exc)})


# ---------------------------------------------------------------------------
# Per-query worker (runs in thread)
# ---------------------------------------------------------------------------
def _process_query(
    search_query: str,
    s3_prefix: str,
    s3_bucket: str,
    api_token: str,
    since: str,
    until: str,
    max_items: int,
) -> dict:
    """
    Fetch → Transform → Upload for a single query.
    Runs inside a thread. Returns a result dict (never raises).
    """
    try:
        raw_items = _fetch_tweets(api_token, search_query, since, until, max_items)
        payload   = _transform(raw_items)
        s3_uri    = _upload(s3_bucket, s3_prefix, payload)
        logger.info("OK   %-20s  tweets: %3d  → %s", search_query, len(payload["twitter"]), s3_uri)
        return {"query": search_query, "tweet_count": len(payload["twitter"]), "s3_uri": s3_uri}

    except Exception as exc:  # pylint: disable=broad-except
        logger.error("FAIL %-20s  error: %s", search_query, exc)
        return {"query": search_query, "error": str(exc)}


# ---------------------------------------------------------------------------
# Apify
# ---------------------------------------------------------------------------
def _fetch_tweets(
    api_token: str,
    search_query: str,
    since: str,
    until: str,
    max_items: int,
) -> list:
    """Call Apify altimis/scweet actor and return raw dataset items."""
    payload = json.dumps({
        "search_query":       search_query,
        "since":              since,
        "until":              until,
        "max_items":          max_items,
        "search_sort":        "Latest",
        "source_mode":        "search",
        "tweet_type":         "all",
        "blue_verified_only": False,
        "has_hashtags":       False,
        "has_images":         False,
        "has_links":          False,
        "has_mentions":       False,
        "has_videos":         False,
        "verified_only":      False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{APIFY_URL}?token={api_token}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=APIFY_TIMEOUT) as resp:
        items = json.loads(resp.read())

    if not isinstance(items, list):
        raise ValueError(f"Expected list from Apify, got {type(items).__name__}")

    return items


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def _transform(raw_items: list) -> dict:
    """Map raw Apify items to the desired output schema."""
    tweets = []
    for item in raw_items:
        try:
            user     = item.get("user") or item.get("author") or {}
            entities = item.get("entities") or {}
            hashtags = [
                (t.get("text") or t) if isinstance(t, dict) else str(t)
                for t in (entities.get("hashtags") or [])
            ]
            tweets.append({
                "text":       item.get("full_text") or item.get("text") or "",
                "lang":       item.get("lang") or "und",
                "tweet_url":  item.get("url") or item.get("tweet_url") or "",
                "org_handle": user.get("screen_name") or user.get("username") or "",
                "engagement": {
                    "retweet_count": int(item.get("retweet_count") or 0),
                    "reply_count":   int(item.get("reply_count")   or 0),
                    "view_count":    int(item.get("views_count")   or 0),
                },
                "hashtags":   hashtags,
                "created_at": item.get("created_at") or "",
            })
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Skipping malformed item: %s", exc)

    return {"twitter": tweets, "reddit": {}}


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------
def _upload(bucket: str, prefix: str, payload: dict) -> str:
    """Serialise payload to JSON and upload to S3 under a day-level folder."""
    now       = datetime.now(tz=timezone.utc)
    date_str  = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    key       = f"{prefix.rstrip('/')}/{date_str}/{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _date_range(days_back: int) -> tuple:
    """Return (since, until) date strings in YYYY-MM-DD format."""
    today = datetime.now(tz=timezone.utc).date()
    since = today if days_back == 0 else today - timedelta(days=days_back)
    return since.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _get_env(key: str) -> str:
    """Read a required environment variable or raise."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return value


def _response(status_code: int, body: dict) -> dict:
    """Standard Lambda HTTP-style response envelope."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }