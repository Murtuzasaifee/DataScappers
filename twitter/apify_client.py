"""
apify_client.py — Thin HTTP client for the Apify altimis/scweet actor.

Actor endpoint (synchronous run + dataset retrieval):
  POST https://api.apify.com/v2/acts/altimis~scweet/run-sync-get-dataset-items
      ?token=<APIFY_API_TOKEN>

The function blocks until Apify returns the full dataset (sync mode).
Lambda timeout should be set to at least 5 minutes to accommodate this.
"""

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_APIFY_BASE_URL = (
    "https://api.apify.com/v2/acts/altimis~scweet"
    "/run-sync-get-dataset-items"
)

# Fixed actor parameters (not user-configurable)
_STATIC_PAYLOAD = {
    "blue_verified_only": False,
    "has_hashtags":       False,
    "has_images":         False,
    "has_links":          False,
    "has_mentions":       False,
    "has_videos":         False,
    "search_sort":        "Latest",
    "source_mode":        "search",
    "tweet_type":         "originals_only",
    "verified_only":      False,
}

# Default HTTP timeout in seconds (sync Apify runs can take a while)
_REQUEST_TIMEOUT_SECONDS = 300


def fetch_tweets(
    api_token:    str,
    search_query: str,
    since:        str,
    until:        str,
    max_items:    int = 100,
) -> list[dict]:
    """
    Call the Apify altimis/scweet actor and return the raw dataset items.

    Parameters
    ----------
    api_token : str
        Apify API token (read from Lambda environment variable).
    search_query : str
        Twitter search query string.
    since : str
        Start date in "YYYY-MM-DD" format (inclusive).
    until : str
        End date in "YYYY-MM-DD" format (exclusive).
    max_items : int
        Maximum number of tweets to retrieve.

    Returns
    -------
    list[dict]
        Raw list of dataset item dicts returned by Apify.

    Raises
    ------
    urllib.error.HTTPError
        If Apify returns a non-2xx HTTP status.
    ValueError
        If the response body cannot be parsed as JSON.
    """
    url = f"{_APIFY_BASE_URL}?token={api_token}"

    payload = {
        **_STATIC_PAYLOAD,
        "search_query": search_query,
        "since":        since,
        "until":        until,
        "max_items":    max_items,
    }

    body_bytes = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body_bytes,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    logger.info(
        "Calling Apify actor — query: '%s'  since: %s  until: %s  max_items: %d",
        search_query, since, until, max_items,
    )

    try:
        with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:
            raw_body = resp.read()
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        logger.error(
            "Apify HTTP error %d: %s", exc.code, error_body
        )
        raise

    try:
        items = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse Apify response as JSON: %s", exc)
        raise ValueError(f"Apify response is not valid JSON: {exc}") from exc

    if not isinstance(items, list):
        raise ValueError(
            f"Expected a JSON array from Apify, got: {type(items).__name__}"
        )

    return items