import requests
import time
import trafilatura
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

API_KEY = "XXXXX"
BASE_URL = "https://newsdata.io/api/1/latest"

# Sources to ignore
BLOCKED_SOURCES = (
    "Hello",
    "hello"
)


def scrape_article(url, retries=2):
    for attempt in range(retries + 1):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if response.status_code == 200:
                content = trafilatura.extract(response.text)
                if content:
                    return content.strip()
        except Exception:
            pass
        time.sleep(1)
    return ""


def fetch_news(query="hdfc", minimum_articles=50):

    collected_articles = []
    next_page = None
    while len(collected_articles) < minimum_articles:
        params = {
            "apikey": API_KEY,
            "country": "in",
            "removeduplicate": "1",
            "q": query,
            "category": "business,breaking,politics"
        }
        if next_page:
            params["page"] = next_page

        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()
        articles = data.get("results", [])
        for article in articles:
            source = article.get("source_name")
            # Skip blocked sources
            if source in BLOCKED_SOURCES:
                continue

            collected_articles.append({
                "title": article.get("title"),
                "description": article.get("description"),
                "pubDate": article.get("pubDate"),
                "link": article.get("link"),
                "source": source,
                "category": article.get("category"),
            })

            if len(collected_articles) >= minimum_articles:
                break
        next_page = data.get("nextPage")
        if not next_page:
            break

    collected_articles = collected_articles[:minimum_articles]
    def process_article(article):
        link = article.get("link")
        content = ""
        if link:
            content = scrape_article(link)
        article["content"] = content
        return article
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_article, article) for article in collected_articles]
        for future in as_completed(futures):
            results.append(future.result())
    return results



# Example usage
news_data = fetch_news("hdfc", 50)
with open("news_data.json", "w", encoding="utf-8") as f:
    json.dump(news_data, f, ensure_ascii=False, indent=2)

print(len(news_data))
print(news_data[:1])