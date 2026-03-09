"""
lambda_function.py — Social Media Scraper (Twitter via Apify + Reddit via RapidAPI)

Triggered every 4 hours by EventBridge. Fetches tweets and/or Reddit posts for
multiple bank entities IN PARALLEL and stores results in S3.

Environment Variables:
    APIFY_API_TOKEN          : str  — Apify API token (required for Twitter)
    RAPIDAPI_KEY             : str  — RapidAPI key (required for Reddit)
    DATA_SOURCES             : str  — ENUM: "all" | "twitter" | "reddit" (default: "all")
    MAX_ITEMS                : int  — Max tweets per query (default: 100)
    MAX_REDDIT_POSTS         : int  — Max Reddit posts per query (default: 50)
    MAX_WORKERS              : int  — Parallel threads (default: 4)
    REDDIT_REQUEST_DELAY_SECS: float — Seconds to sleep between Reddit API calls to
                                       avoid 429 rate-limit errors when running parallel
                                       queries against the same RapidAPI key (default: 1.5)
    TWITTER_REQUEST_DELAY_SECS: float — Seconds to sleep between Twitter/Apify API calls
                                        for the same reason (default: 1.5)

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
import time
import threading
import urllib.parse
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

RAPIDAPI_REDDIT_URL = "https://reddit34.p.rapidapi.com/getSearchPosts"
RAPIDAPI_REDDIT_HOST = "reddit34.p.rapidapi.com"
RAPIDAPI_TIMEOUT = 30

# Valid values for the DATA_SOURCES environment variable
DATA_SOURCE_ALL     = "all"
DATA_SOURCE_TWITTER = "twitter"
DATA_SOURCE_REDDIT  = "reddit"
VALID_DATA_SOURCES  = {DATA_SOURCE_ALL, DATA_SOURCE_TWITTER, DATA_SOURCE_REDDIT}

s3 = boto3.client("s3")

# Per-source locks that serialise API calls across worker threads.
# Parallel threads sharing the same API key cause HTTP 429 bursts;
# each thread acquires the relevant lock, fires its request, then sleeps
# for the configured delay before releasing — spacing out consecutive calls.
_twitter_rate_limit_lock = threading.Lock()
_reddit_rate_limit_lock  = threading.Lock()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def lambda_handler(event: dict, context) -> dict:
    try:
        # -- Resolve which data sources to fetch -----------------------------
        data_sources = os.environ.get("DATA_SOURCES", DATA_SOURCE_ALL).strip().lower()
        if data_sources not in VALID_DATA_SOURCES:
            raise ValueError(
                f"Invalid DATA_SOURCES='{data_sources}'. "
                f"Must be one of: {sorted(VALID_DATA_SOURCES)}"
            )

        fetch_twitter = data_sources in (DATA_SOURCE_ALL, DATA_SOURCE_TWITTER)
        fetch_reddit  = data_sources in (DATA_SOURCE_ALL, DATA_SOURCE_REDDIT)

        # -- Credentials (only required when the source is enabled) ----------
        api_token   = _get_env("APIFY_API_TOKEN")  if fetch_twitter else None
        rapidapi_key = _get_env("RAPIDAPI_KEY")    if fetch_reddit  else None

        # -- Config ----------------------------------------------------------
        max_items              = int(os.environ.get("MAX_ITEMS",              "100"))
        max_reddit_posts       = int(os.environ.get("MAX_REDDIT_POSTS",       "50"))
        max_workers            = int(os.environ.get("MAX_WORKERS",            "4"))
        twitter_request_delay  = float(os.environ.get("TWITTER_REQUEST_DELAY_SECS", "3"))
        reddit_request_delay   = float(os.environ.get("REDDIT_REQUEST_DELAY_SECS",  "3"))

        days_back        = int(event["days_back"])
        s3_bucket        = event["s3_bucket"]
        queries          = event["queries"]
        since, until     = _date_range(days_back)

        logger.info(
            "Run started — sources: %s  since: %s  until: %s  queries: %d  workers: %d",
            data_sources, since, until, len(queries), max_workers,
        )

        # ------------------------------------------------------------------ #
        # Dispatch all queries in parallel
        # ------------------------------------------------------------------ #
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(
                    _process_query,
                    q["search_query"],
                    q["s3_prefix"],
                    s3_bucket,
                    api_token,
                    rapidapi_key,
                    since,
                    until,
                    max_items,
                    max_reddit_posts,
                    twitter_request_delay,
                    reddit_request_delay,
                    fetch_twitter,
                    fetch_reddit,
                ): q["search_query"]
                for q in queries
            }

            for future in as_completed(future_to_query):
                results.append(future.result())

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
    api_token: str | None,
    rapidapi_key: str | None,
    since: str,
    until: str,
    max_items: int,
    max_reddit_posts: int,
    twitter_request_delay: float,
    reddit_request_delay: float,
    fetch_twitter: bool,
    fetch_reddit: bool,
) -> dict:
    """
    Fetch → Transform → Upload for a single query.
    Calls Twitter and/or Reddit based on the enabled flags.
    Runs inside a thread. Returns a result dict (never raises).
    """
    try:
        # -- Twitter ---------------------------------------------------------
        twitter_payload = []
        if fetch_twitter:
            raw_tweets      = _fetch_tweets(
                api_token, search_query, since, until, max_items, twitter_request_delay,
            )
            twitter_payload = _transform_twitter(raw_tweets)

        # -- Reddit ----------------------------------------------------------
        reddit_payload = []
        if fetch_reddit:
            raw_posts    = _fetch_reddit_posts(
                rapidapi_key, search_query, max_reddit_posts, reddit_request_delay,
            )
            reddit_payload = _transform_reddit(raw_posts)

        # -- Merge & upload --------------------------------------------------
        payload = {
            "twitter": twitter_payload,
            "reddit":  reddit_payload,
        }
        s3_uri = _upload(s3_bucket, s3_prefix, payload)

        logger.info(
            "OK   %-20s  tweets: %3d  reddit_posts: %3d  → %s",
            search_query, len(twitter_payload), len(reddit_payload), s3_uri,
        )
        return {
            "query":        search_query,
            "tweet_count":  len(twitter_payload),
            "reddit_count": len(reddit_payload),
            "s3_uri":       s3_uri,
        }

    except Exception as exc:  # pylint: disable=broad-except
        logger.error("FAIL %-20s  error: %s", search_query, exc)
        return {"query": search_query, "error": str(exc)}


# ---------------------------------------------------------------------------
# Twitter — Apify
# ---------------------------------------------------------------------------
def _fetch_tweets(
    api_token: str,
    search_query: str,
    since: str,
    until: str,
    max_items: int,
    request_delay: float,
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
        f"https://api.apify.com/v2/acts/altimis~scweet"
        f"/run-sync-get-dataset-items?token={api_token}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with _twitter_rate_limit_lock:
        with urllib.request.urlopen(req, timeout=APIFY_TIMEOUT) as resp:
            items = json.loads(resp.read())
        time.sleep(request_delay)

    if not isinstance(items, list):
        raise ValueError(f"Expected list from Apify, got {type(items).__name__}")

    return items


def _transform_twitter(raw_items: list) -> list:
    """Map raw Apify tweet items to the desired output schema."""
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
            logger.warning("Skipping malformed tweet item: %s", exc)

    return tweets


# ---------------------------------------------------------------------------
# Reddit — RapidAPI (reddit34)
# ---------------------------------------------------------------------------
def _fetch_reddit_posts(
    rapidapi_key: str,
    search_query: str,
    max_posts: int,
    request_delay: float,
) -> list:
    """
    Call RapidAPI reddit34 /getSearchPosts endpoint with sort=new (hardcoded).

    Rate-limit handling
    -------------------
    Acquires ``_reddit_rate_limit_lock`` before each HTTP request so that
    parallel worker threads cannot fire simultaneous Reddit calls against the
    same RapidAPI key (which would trigger HTTP 429). After each successful
    request the lock is held for ``request_delay`` seconds before being
    released, spacing out consecutive calls.

    Pagination
    ----------
    Iterates via the ``cursor`` field returned in each response until
    ``max_posts`` items are collected or no further cursor is provided.
    """
    collected: list = []
    cursor: str | None = None

    while len(collected) < max_posts:
        params: dict = {
            "query": search_query,
            "sort":  "new",          # hardcoded — fetch most recent posts first
        }
        if cursor:
            params["cursor"] = cursor

        url = f"{RAPIDAPI_REDDIT_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "x-rapidapi-host": RAPIDAPI_REDDIT_HOST,
                "x-rapidapi-key":  rapidapi_key,
            },
            method="GET",
        )

        # Serialise all Reddit HTTP calls; sleep inside the lock so the next
        # thread waits until the delay has fully elapsed before proceeding.
        with _reddit_rate_limit_lock:
            with urllib.request.urlopen(req, timeout=RAPIDAPI_TIMEOUT) as resp:
                body = json.loads(resp.read())
            time.sleep(request_delay)

        # Validate top-level response shape
        if not body.get("success"):
            raise ValueError(f"Reddit API returned success=false: {body}")

        data  = body.get("data") or {}
        posts = data.get("posts") or []

        if not posts:
            break  # No more results

        # Each post is a Reddit listing item; extract the inner `data` dict
        for listing_item in posts:
            if len(collected) >= max_posts:
                break
            post_data = listing_item.get("data")
            if post_data:
                collected.append(post_data)

        # Advance cursor for pagination; stop if none provided
        cursor = data.get("cursor")
        if not cursor:
            break

    logger.debug("Fetched %d Reddit posts for query '%s'", len(collected), search_query)
    return collected


def _transform_reddit(raw_posts: list) -> list:
    """
    Map raw Reddit post data dicts to the desired output schema.
    URL is constructed as "https://www.reddit.com" + permalink.
    """
    posts = []
    for post in raw_posts:
        try:
            permalink = post.get("permalink") or ""
            posts.append({
                "subreddit":    post.get("subreddit") or "",
                "title":        post.get("title") or "",
                "text":         post.get("selftext") or "",
                "created_utc":  int(post.get("created_utc") or 0),
                "score":        int(post.get("score") or 0),
                "num_comments": int(post.get("num_comments") or 0),
                "upvote_ratio": float(post.get("upvote_ratio") or 0.0),
                "url":          f"https://www.reddit.com{permalink}",
            })
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Skipping malformed Reddit post: %s", exc)

    return posts


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