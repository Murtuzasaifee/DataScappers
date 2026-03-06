"""
date_utils.py — Date range and S3 key generation utilities.

Days-back semantics
-------------------
  0  → today only          (since=today,       until=today+1)
  1  → last 1 day          (since=yesterday,   until=today)
  2  → last 2 days         (since=2 days ago,  until=today)
  N  → last N days         (since=N days ago,  until=today)

All dates are in UTC to stay consistent with Twitter's timestamps.
"""

import re
from datetime import datetime, timedelta, timezone


# Date format expected by the Apify actor
_DATE_FMT = "%Y-%m-%d"


def build_date_range(days_back: int) -> tuple[str, str]:
    """
    Compute the ``since`` / ``until`` date strings for the Apify query.

    Parameters
    ----------
    days_back : int
        0  → today only
        N  → last N days

    Returns
    -------
    tuple[str, str]
        (since_date, until_date) both formatted as "YYYY-MM-DD".
    """
    today_utc = datetime.now(tz=timezone.utc).date()

    if days_back == 0:
        since = today_utc
        until = today_utc          # today only
    else:
        since = today_utc - timedelta(days=days_back)
        until = today_utc          # up to and including today

    return since.strftime(_DATE_FMT), until.strftime(_DATE_FMT)


def build_s3_key(prefix: str, query: str) -> str:
    """
    Build a timestamped S3 object key.

    Pattern: ``<prefix>/<sanitised_query>/<YYYY-MM-DD_HH-MM-SS_UTC>.json``

    Parameters
    ----------
    prefix : str
        Top-level S3 folder / prefix (e.g. "twitter/raw").
    query : str
        The search query string (will be sanitised for use in a filename).

    Returns
    -------
    str
        Full S3 key, e.g.
        ``twitter/raw/2026-03-05_10-30-00.json``
    """
    now_utc   = datetime.now(tz=timezone.utc)
    timestamp = now_utc.strftime("%Y-%m-%d_%H-%M-%S")

    clean_prefix = prefix.rstrip("/")

    return f"{clean_prefix}/{timestamp}.json"