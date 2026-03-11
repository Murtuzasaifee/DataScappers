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
# Twitter — RapidAPI (twitter241)
RAPIDAPI_TWITTER_URL  = "https://twitter241.p.rapidapi.com/search-v3"
RAPIDAPI_TWITTER_HOST = "twitter241.p.rapidapi.com"

# Reddit — RapidAPI (reddit34)
RAPIDAPI_REDDIT_URL  = "https://reddit34.p.rapidapi.com/getSearchPosts"
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
        # Both Twitter and Reddit now share the same RapidAPI key
        rapidapi_key = _get_env("RAPIDAPI_KEY")

        # -- Config ----------------------------------------------------------
        max_items             = int(os.environ.get("MAX_ITEMS",              "100"))
        max_reddit_posts      = int(os.environ.get("MAX_REDDIT_POSTS",       "25"))
        max_workers           = int(os.environ.get("MAX_WORKERS",            "4"))
        twitter_request_delay = float(os.environ.get("TWITTER_REQUEST_DELAY_SECS", "3"))
        reddit_request_delay  = float(os.environ.get("REDDIT_REQUEST_DELAY_SECS",  "3"))

        days_back    = int(event["days_back"])
        s3_bucket    = event["s3_bucket"]
        queries      = event["queries"]
        since, until = _date_range(days_back)

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
            "Run complete — success: %d  partial: %d  failed: %d",
            sum(1 for r in results if "error" not in r and "source_errors" not in r),
            sum(1 for r in results if "source_errors" in r and "error" not in r),
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
    rapidapi_key: str,
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

    Fault isolation: each source is fetched independently inside its own
    try/except. A failure in one source does not prevent the other from being
    collected or uploaded. Per-source errors are recorded under ``source_errors``.
    The S3 upload only proceeds if at least one source returned data.

    Runs inside a thread. Returns a result dict (never raises).
    """
    source_errors: dict = {}

    # -- Twitter -------------------------------------------------------------
    twitter_payload: list = []
    if fetch_twitter:
        twitter_payload, twitter_err = _fetch_and_transform_source(
            source_name="twitter",
            fetch_fn=lambda: _fetch_tweets(
                rapidapi_key, search_query, max_items, twitter_request_delay,
            ),
            transform_fn=_transform_twitter,
            search_query=search_query,
        )
        if twitter_err:
            source_errors["twitter"] = twitter_err

    # -- Reddit --------------------------------------------------------------
    reddit_payload: list = []
    if fetch_reddit:
        reddit_payload, reddit_err = _fetch_and_transform_source(
            source_name="reddit",
            fetch_fn=lambda: _fetch_reddit_posts(
                rapidapi_key, search_query, max_reddit_posts, reddit_request_delay,
            ),
            transform_fn=_transform_reddit,
            search_query=search_query,
        )
        if reddit_err:
            source_errors["reddit"] = reddit_err

    # -- Abort only when every enabled source failed (nothing to upload) -----
    enabled_sources    = (["twitter"] if fetch_twitter else []) + (["reddit"] if fetch_reddit else [])
    all_sources_failed = set(source_errors.keys()) == set(enabled_sources)

    if all_sources_failed:
        logger.error(
            "FAIL %-20s  all enabled sources failed: %s",
            search_query, source_errors,
        )
        return {
            "query":         search_query,
            "error":         "all sources failed",
            "source_errors": source_errors,
        }

    # -- Merge & upload (partial results are acceptable) ---------------------
    payload = {
        "twitter": twitter_payload,
        "reddit":  reddit_payload,
    }

    try:
        s3_uri = _upload(s3_bucket, s3_prefix, payload)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("FAIL %-20s  S3 upload error: %s", search_query, exc)
        return {
            "query":         search_query,
            "error":         f"S3 upload failed: {exc}",
            "source_errors": source_errors,
        }

    status_tag = "PARTIAL" if source_errors else "OK     "
    logger.info(
        "%s %-20s  tweets: %3d  reddit_posts: %3d  → %s%s",
        status_tag,
        search_query,
        len(twitter_payload),
        len(reddit_payload),
        s3_uri,
        f"  skipped_sources: {list(source_errors.keys())}" if source_errors else "",
    )

    result: dict = {
        "query":        search_query,
        "tweet_count":  len(twitter_payload),
        "reddit_count": len(reddit_payload),
        "s3_uri":       s3_uri,
    }
    if source_errors:
        result["source_errors"] = source_errors

    return result


# ---------------------------------------------------------------------------
# Source-level fetch + transform wrapper
# ---------------------------------------------------------------------------
def _fetch_and_transform_source(
    source_name: str,
    fetch_fn,
    transform_fn,
    search_query: str,
) -> tuple[list, str | None]:
    """
    Execute ``fetch_fn`` followed by ``transform_fn`` for a single data source.

    Provides per-source fault isolation — exceptions are caught, logged, and
    returned as an error string rather than propagated.

    Returns
    -------
    (payload, error_message)
        ``payload``       — transformed records (empty list on failure)
        ``error_message`` — string description of the error, or ``None`` on success
    """
    try:
        raw     = fetch_fn()
        payload = transform_fn(raw)
        return payload, None
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "Source '%s' failed for query '%s': %s",
            source_name, search_query, exc,
        )
        return [], str(exc)


# ---------------------------------------------------------------------------
# Twitter — RapidAPI (twitter241)
# ---------------------------------------------------------------------------
def _fetch_tweets(
    rapidapi_key: str,
    search_query: str,
    max_items: int,
    request_delay: float,
) -> list:
    """
    Call RapidAPI twitter241 /search-v3 endpoint (type=Latest).

    Pagination
    ----------
    Iterates via the ``cursor`` field returned in each response until
    ``max_items`` tweets are collected or no further cursor is provided.

    Rate-limit handling
    -------------------
    Acquires ``_twitter_rate_limit_lock`` before each HTTP request and sleeps
    for ``request_delay`` seconds inside the lock before releasing, spacing
    out consecutive calls from parallel worker threads.
    """
    collected: list = []
    cursor: str | None = None

    while len(collected) < max_items:
        params: dict = {
            "type":  "Latest",
            "count": min(20, max_items - len(collected)),  # API max per page is 20
            "query": search_query,
        }
        if cursor:
            params["cursor"] = cursor

        url = f"{RAPIDAPI_TWITTER_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "x-rapidapi-host": RAPIDAPI_TWITTER_HOST,
                "x-rapidapi-key":  rapidapi_key,
            },
            method="GET",
        )

        with _twitter_rate_limit_lock:
            with urllib.request.urlopen(req, timeout=RAPIDAPI_TIMEOUT) as resp:
                body = json.loads(resp.read())
            time.sleep(request_delay)

        # ------------------------------------------------------------------ #
        # Parse response:
        # body.result.timeline_response.timeline.instructions[].entries[]
        #   .content.content.tweet_results.result  → Tweet object
        #
        # Iterate ALL instructions to find TimelineAddEntries — do NOT
        # assume entries are always in instructions[0]; other instruction
        # types (e.g. TimelinePinEntry, TimelineClearCache) can appear first.
        # ------------------------------------------------------------------ #
        instructions = (
            body.get("result", {})
                .get("timeline_response", {})
                .get("timeline", {})
                .get("instructions", [])
        )

        entries = []
        for instruction in instructions:
            if instruction.get("__typename") == "TimelineAddEntries":
                entries = instruction.get("entries", [])
                break

        if not entries:
            break

        for entry in entries:
            if len(collected) >= max_items:
                break
            tweet_result = (
                entry.get("content", {})
                     .get("content", {})
                     .get("tweet_results", {})
                     .get("result", {})
            )
            # Unwrap TweetWithVisibilityResults — the actual Tweet is nested
            # one level deeper under result.tweet in this variant.
            if tweet_result.get("__typename") == "TweetWithVisibilityResults":
                tweet_result = tweet_result.get("tweet") or {}
            # Only collect entries that carry an actual Tweet object
            if tweet_result.get("__typename") == "Tweet":
                collected.append(tweet_result)

        # Advance cursor — bottom cursor entry carries the next page token
        cursor = _extract_twitter_cursor(body)
        if not cursor:
            break

    logger.debug("Fetched %d tweets for query '%s'", len(collected), search_query)
    return collected


def _extract_twitter_cursor(body: dict) -> str | None:
    """
    Extract the bottom cursor value from a twitter241 search response.
    The cursor lives in the ``cursor`` field at the top of the response
    (sibling of ``result``).
    """
    bottom_cursor = body.get("cursor", {}).get("bottom")
    return bottom_cursor or None


def _transform_twitter(raw_items: list) -> list:
    """
    Map raw twitter241 Tweet result objects to the canonical output schema.

    Input shape (per item):
        result.details.full_text
        result.details.created_at_ms
        result.details.hashtag_entities[].text
        result.core.user_results.result.core.screen_name
        result.counts.{retweet_count, reply_count}
        result.views.count
        result.rest_id  (used to build tweet URL)
        result.mention_entities[].screen_name  (to detect org replies)
    """
    tweets = []
    for item in raw_items:
        try:
            details   = item.get("details") or {}
            counts    = item.get("counts")  or {}
            views     = item.get("views")   or {}
            user_core = (
                item.get("core", {})
                    .get("user_results", {})
                    .get("result", {})
                    .get("core", {})
            )

            screen_name = user_core.get("screen_name") or ""
            rest_id     = item.get("rest_id") or ""
            tweet_url   = f"https://twitter.com/{screen_name}/status/{rest_id}" if rest_id else ""

            hashtags = [
                h.get("text", "") if isinstance(h, dict) else str(h)
                for h in (details.get("hashtag_entities") or [])
            ]

            tweets.append({
                "text":       details.get("full_text") or "",
                "lang":       item.get("lang") or details.get("lang") or "und",
                "tweet_url":  tweet_url,
                "org_handle": screen_name,
                "engagement": {
                    "retweet_count": int(counts.get("retweet_count") or 0),
                    "reply_count":   int(counts.get("reply_count")   or 0),
                    "view_count":    int(views.get("count")          or 0),
                },
                "hashtags":   hashtags,
                "created_at": str(details.get("created_at_ms") or ""),
            })
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Skipping malformed tweet item: %s", exc)

    return tweets


# ---------------------------------------------------------------------------
# Reddit — RapidAPI (reddit34)  [UNCHANGED]
# ---------------------------------------------------------------------------
def _fetch_reddit_posts(
    rapidapi_key: str,
    search_query: str,
    max_posts: int,
    request_delay: float,
) -> list:
    """
    Call RapidAPI reddit34 /getSearchPosts endpoint with sort=new (hardcoded).

    Paginates via the ``cursor`` field until ``max_posts`` items are collected
    or no further cursor is provided.
    """
    collected: list = []
    cursor: str | None = None

    while len(collected) < max_posts:
        params: dict = {
            "query": search_query,
            "sort":  "new",
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

        with _reddit_rate_limit_lock:
            with urllib.request.urlopen(req, timeout=RAPIDAPI_TIMEOUT) as resp:
                body = json.loads(resp.read())
            time.sleep(request_delay)

        if not body.get("success"):
            raise ValueError(f"Reddit API returned success=false: {body}")

        data  = body.get("data") or {}
        posts = data.get("posts") or []

        if not posts:
            break

        for listing_item in posts:
            if len(collected) >= max_posts:
                break
            post_data = listing_item.get("data")
            if post_data:
                collected.append(post_data)

        cursor = data.get("cursor")
        if not cursor:
            break

    logger.debug("Fetched %d Reddit posts for query '%s'", len(collected), search_query)
    return collected


def _transform_reddit(raw_posts: list) -> list:
    """Map raw Reddit post data dicts to the canonical output schema."""
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
    key = f"{prefix.rstrip('/')}/socialmedia/{date_str}/{timestamp}.json"

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