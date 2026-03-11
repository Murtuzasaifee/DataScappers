"""
Lambda: Fetch news for multiple queries in parallel → save to S3.

EventBridge Input:
{
  "s3_bucket": "s3-bucket-name",
  "queries": [
    {"search_query": "HDFC Bank",  "s3_prefix": "banking/HDFC/news"},
    {"search_query": "ICICI Bank", "s3_prefix": "banking/ICICI/news"},
    {"search_query": "SBI Bank",   "s3_prefix": "banking/SBI/news"},
    {"search_query": "Axis Bank",  "s3_prefix": "banking/AXIS/news"}
  ]
}

Lambda Environment Variables:
  NEWSDATA_API_KEY    — newsdata.io API key (required)
  MINIMUM_ARTICLES    — articles to fetch per query (default: 50)
  QUERY_STAGGER_SECS  — delay in seconds between each query start (default: 2)
  API_RETRY_WAIT_SECS — wait in seconds before retrying a rate-limited request (default: 5)
  API_MAX_RETRIES     — max retries on rate limit error (default: 3)

Lambda Layer:
  requests, trafilatura, lxml
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
import requests
import trafilatura

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_KEY = os.environ["NEWSDATA_API_KEY"]
MINIMUM_ARTICLES = int(os.environ.get("MINIMUM_ARTICLES", "50"))
QUERY_STAGGER_SECS = int(os.environ.get("QUERY_STAGGER_SECS", "5"))
API_RETRY_WAIT_SECS = int(os.environ.get("API_RETRY_WAIT_SECS", "5"))
API_MAX_RETRIES = int(os.environ.get("API_MAX_RETRIES", "3"))
BLOCKED_SOURCES = {"Hello", "hello"}
BASE_URL = "https://newsdata.io/api/1/latest"

s3 = boto3.client("s3")


def scrape_article(url, retries=2):
    """Fetch full article text from a URL."""
    for attempt in range(retries + 1):
        try:
            response = requests.get(
                url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10
            )
            if response.status_code == 200:
                content = trafilatura.extract(response.text)
                if content:
                    return content.strip()
        except Exception:
            pass
        time.sleep(1)
    return ""


def fetch_page(params, retry_wait=API_RETRY_WAIT_SECS, max_retries=API_MAX_RETRIES):
    """
    Fetch a single page from newsdata.io with retry on rate limit.
    Returns parsed JSON or raises on persistent failure.
    """
    for attempt in range(max_retries + 1):
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()

        if data.get("status") == "success":
            return data

        # Rate limit or transient error — wait and retry
        error_code = data.get("results", {})
        logger.warning(
            "API error (attempt %d/%d) for query '%s': %s",
            attempt + 1,
            max_retries + 1,
            params.get("q"),
            data,
        )

        if attempt < max_retries:
            wait = retry_wait * (attempt + 1)  # incremental back-off: 5s, 10s, 15s
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)
        else:
            raise Exception(f"API failed after {max_retries + 1} attempts: {data}")

    return {}


def fetch_news(query, stagger_secs=0, minimum_articles=MINIMUM_ARTICLES):
    """
    Fetch article metadata from newsdata.io, then scrape full text in parallel.
    stagger_secs: initial delay before first API call to avoid simultaneous requests.
    """
    if stagger_secs:
        logger.info("Staggering query '%s' by %ds", query, stagger_secs)
        time.sleep(stagger_secs)

    collected_articles = []
    next_page = None

    while len(collected_articles) < minimum_articles:
        params = {
            "apikey": API_KEY,
            "country": "in",
            "removeduplicate": "1",
            "q": query,
            "category": "business,breaking,politics",
        }
        if next_page:
            params["page"] = next_page

        data = fetch_page(params)

        for article in data.get("results", []):
            source = article.get("source_name")
            if source in BLOCKED_SOURCES:
                continue
            collected_articles.append(
                {
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "pubDate": article.get("pubDate"),
                    "link": article.get("link"),
                    "source": source,
                    "category": article.get("category"),
                }
            )
            if len(collected_articles) >= minimum_articles:
                break

        next_page = data.get("nextPage")
        if not next_page:
            break

    collected_articles = collected_articles[:minimum_articles]

    # Scrape full text in parallel
    def process_article(article):
        link = article.get("link")
        article["content"] = scrape_article(link) if link else ""
        return article

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_article, article) for article in collected_articles
        ]
        for future in as_completed(futures):
            results.append(future.result())

    return results


def save_to_s3(data, bucket, prefix, ts):
    """Upload articles as JSON to S3 and return the S3 URI."""
    key = f"{prefix.strip('/')}/news/{ts.strftime('%Y-%m-%d')}/{ts.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def process_query(query_cfg, bucket, ts, stagger_secs=0):
    """Fetch → scrape → save pipeline for a single query."""
    q = query_cfg["search_query"]
    try:
        articles = fetch_news(q, stagger_secs=stagger_secs)
        uri = save_to_s3(articles, bucket, query_cfg["s3_prefix"], ts)
        logger.info("OK %s -> %s (%d articles)", q, uri, len(articles))
        return {"query": q, "uri": uri, "count": len(articles), "status": "success"}
    except Exception as e:
        logger.error("FAIL %s: %s", q, e)
        return {"query": q, "uri": None, "count": 0, "status": "error", "error": str(e)}


def lambda_handler(event, context):
    bucket = event["s3_bucket"]
    queries = event["queries"]
    ts = datetime.now(tz=timezone.utc)

    # Stagger query starts: query 0 starts immediately, query 1 waits 2s, query 2 waits 4s, etc.
    # They still run in parallel — just their first API call is offset to avoid rate limits.
    results = []
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        futures = [
            executor.submit(
                process_query, q, bucket, ts, stagger_secs=i * QUERY_STAGGER_SECS
            )
            for i, q in enumerate(queries)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    return {"statusCode": 200, "run_timestamp": ts.isoformat(), "results": results}
