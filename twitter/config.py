"""
config.py — Environment-driven configuration loader.

All sensitive keys (API tokens, etc.) are read exclusively from Lambda
environment variables so they never appear in source code.

Required environment variables
-------------------------------
APIFY_API_TOKEN : str
    Apify API token for the altimis/scweet actor.

Optional environment variables
-------------------------------
MAX_ITEMS : int
    Maximum number of tweets to fetch per run (default: 100).
"""

import os


# ---------------------------------------------------------------------------
# Required environment variable names
# ---------------------------------------------------------------------------
_ENV_APIFY_TOKEN = "APIFY_API_TOKEN"
_ENV_MAX_ITEMS   = "MAX_ITEMS"

# Defaults
_DEFAULT_MAX_ITEMS = 100


def get_config() -> dict:
    """
    Read and validate all environment-driven configuration.

    Returns
    -------
    dict
        {
            "apify_api_token": str,
            "max_items":       int,
        }

    Raises
    ------
    EnvironmentError
        If a required environment variable is not set.
    """
    apify_token = os.environ.get(_ENV_APIFY_TOKEN)
    if not apify_token:
        raise EnvironmentError(
            f"Required environment variable '{_ENV_APIFY_TOKEN}' is not set. "
            "Configure it in the Lambda function's environment variables."
        )

    max_items_raw = os.environ.get(_ENV_MAX_ITEMS, str(_DEFAULT_MAX_ITEMS))
    try:
        max_items = int(max_items_raw)
        if max_items <= 0:
            raise ValueError
    except ValueError as exc:
        raise EnvironmentError(
            f"Environment variable '{_ENV_MAX_ITEMS}' must be a positive integer, "
            f"got: '{max_items_raw}'"
        ) from exc

    return {
        "apify_api_token": apify_token,
        "max_items":       max_items,
    }