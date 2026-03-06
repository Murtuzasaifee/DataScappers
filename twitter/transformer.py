"""
transformer.py — Map raw Apify dataset items to the desired output schema.

Apify altimis/scweet item fields (key ones used here):
  full_text / text     → tweet body
  lang                 → ISO-639 language code
  url                  → canonical tweet URL
  user.screen_name     → handle without '@'
  retweet_count        → int
  reply_count          → int
  views_count          → int  (may be null for older/private tweets)
  entities.hashtags[]  → list of { text: "..." } objects
  created_at           → ISO-8601 string

Any field absent in the raw item gracefully defaults to a safe value
rather than raising a KeyError.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def transform_apify_response(raw_items: list[dict]) -> dict:
    """
    Convert a list of raw Apify items into the standard output envelope.

    Parameters
    ----------
    raw_items : list[dict]
        Dataset items as returned by the Apify actor.

    Returns
    -------
    dict
        {
            "twitter": [ <transformed tweet>, ... ],
            "reddit":  {}
        }
    """
    transformed_tweets = []

    for idx, item in enumerate(raw_items):
        try:
            tweet = _transform_single_tweet(item)
            transformed_tweets.append(tweet)
        except Exception as exc:  # pylint: disable=broad-except
            # Log bad records but do not abort the entire run
            logger.warning(
                "Skipping item %d due to transformation error: %s | item: %s",
                idx, exc, str(item)[:200],
            )

    return {
        "twitter": transformed_tweets,
        "reddit":  {},
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _transform_single_tweet(item: dict) -> dict:
    """
    Map a single raw Apify item to the target tweet schema.

    Parameters
    ----------
    item : dict
        Single raw item from the Apify dataset.

    Returns
    -------
    dict
        Tweet in the desired output format.
    """
    return {
        "text":       _get_text(item),
        "lang":       item.get("lang") or "und",            # "und" = undetermined
        "tweet_url":  _get_tweet_url(item),
        "org_handle": _get_handle(item),
        "engagement": _get_engagement(item),
        "hashtags":   _get_hashtags(item),
        "created_at": _get_created_at(item),
    }


def _get_text(item: dict) -> str:
    """Return tweet body, preferring 'full_text' over 'text'."""
    return item.get("full_text") or item.get("text") or ""


def _get_tweet_url(item: dict) -> str:
    """Return the canonical tweet URL."""
    return item.get("url") or item.get("tweet_url") or ""


def _get_handle(item: dict) -> str:
    """Extract the screen_name (handle without '@') from nested user object."""
    user: dict[str, Any] = item.get("user") or item.get("author") or {}
    return (
        user.get("screen_name")
        or user.get("username")
        or user.get("handle")
        or ""
    )


def _get_engagement(item: dict) -> dict:
    """
    Build the engagement sub-object.

    views_count can be null for protected accounts or very old tweets;
    default to 0 in that case.
    """
    return {
        "retweet_count": int(item.get("retweet_count") or 0),
        "reply_count":   int(item.get("reply_count")   or 0),
        "view_count":    int(item.get("views_count")   or 0),
    }


def _get_hashtags(item: dict) -> list[str]:
    """
    Extract hashtag strings from the entities block.

    Handles two common shapes:
      - entities.hashtags = [{"text": "HDFC"}, ...]   (Twitter v1-style)
      - entities.hashtags = ["HDFC", ...]              (already plain strings)
    """
    entities: dict = item.get("entities") or {}
    raw_tags: list = entities.get("hashtags") or []

    result = []
    for tag in raw_tags:
        if isinstance(tag, dict):
            value = tag.get("text") or tag.get("tag") or ""
        else:
            value = str(tag)
        if value:
            result.append(value)

    return result


def _get_created_at(item: dict) -> str:
    """Return the ISO-8601 created_at string, defaulting to empty string."""
    return item.get("created_at") or ""