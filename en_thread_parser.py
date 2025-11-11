import json, time, os, re, sys
from parsel import Selector
from nested_lookup import nested_lookup
import jmespath
from playwright.sync_api import sync_playwright
import pandas as pd
from urllib.parse import quote
from glob import glob
import logging

ROOT_SAVE_DIR = "data/english/"
LIMIT_PER_KEYWORD = 5000
SLEEP = 2
LOCALE = ("en-US", "en-US,en;q=0.9")
os.makedirs(ROOT_SAVE_DIR, exist_ok=True)

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enThreadParser.log", mode='a', encoding='utf-8')
    ]
)

# SAD = [
#     "sad", "sadness", "lonely", "depression", "anxiety",
#     "crying", "broken heart", "miss you", "tired",
#     "pain", "hopeless", "heartbreak", "hurt", "bad day", "depressed", "depression", "suicide"
# ]

SAD = [
    "suicide"
]
NEUTRAL = [
    "life", "work", "school", "friends", "weather", "family",
    "travel", "morning", "routine", "food", "study",
    "weekend", "day", "evening", "city"
]
HAPPY = [
    "happy", "happiness", "joy", "smile", "love", "success",
    "motivation", "good vibes", "grateful", "blessed",
    "proud", "amazing day", "sunshine"
]

ALL_KEYWORDS = [("sad", k) for k in SAD] + [("neutral", k) for k in NEUTRAL] + [("happy", k) for k in HAPPY]

def load_existing_ids(keyword):
    """Avoid re-scraping posts that already exist."""
    existing = set()
    logging.debug(f"Looking for existing CSV files for keyword '{keyword}' in {ROOT_SAVE_DIR}")
    for file in glob(f"{ROOT_SAVE_DIR}/threads_{keyword}_*.csv"):
        logging.debug(f"Checking file: {file}")
        try:
            df = pd.read_csv(file, usecols=["id"])
            ids_found = set(df["id"].dropna().astype(str))
            existing.update(ids_found)
            logging.debug(f"Loaded {len(ids_found)} ids from {file}")
        except Exception as e:
            logging.warning(f"Could not read IDs from {file}: {e}")
            continue
    logging.info(f"Total existing IDs for '{keyword}': {len(existing)}")
    return existing

def parse_thread(data):
    """Extract main post data from JSON."""
    result = jmespath.search(
        """{
            text: post.caption.text,
            published_on: post.taken_at,
            id: post.id,
            code: post.code,
            username: post.user.username,
            like_count: post.like_count,
            reply_count: view_replies_cta_string,
            image_count: post.carousel_media_count,
            videos: post.video_versions[].url
        }""",
        data,
    )
    if not result:
        logging.debug("No result from jmespath for thread data")
        return None
    if result.get("reply_count") and not isinstance(result["reply_count"], int):
        try:
            first = str(result["reply_count"]).split(" ")[0]
            result["reply_count"] = int(first) if first.isdigit() else 0
        except Exception as e:
            logging.warning(f"Could not parse reply_count: {result['reply_count']}, error: {e}")
            result["reply_count"] = 0
    result["url"] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    result["repost_count"] = 0
    logging.debug(f"Parsed thread: id={result.get('id')} url={result['url']}")
    return result

def scrape_thread_page(page_source):
    """Extract all posts from the Threads page source."""
    selector = Selector(text=page_source)
    datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
    posts, seen_ids = [], set()
    logging.debug(f"Found {len(datasets)} candidate JSON script blocks in page source.")

    for raw in datasets:
        if '"ScheduledServerJS"' not in raw or "thread_items" not in raw:
            continue
        try:
            data = json.loads(raw)
        except Exception as e:
            logging.warning(f"Could not load JSON: {e}")
            continue
        thread_items = nested_lookup("thread_items", data)
        for group in thread_items:
            for t in group:
                parsed = parse_thread(t)
                if not parsed:
                    continue
                pid = str(parsed.get("id"))
                if pid in seen_ids:
                    logging.debug(f"Duplicate post id {pid}. Skipping.")
                    continue
                seen_ids.add(pid)
                posts.append(parsed)
    logging.info(f"Extracted {len(posts)} posts from thread page.")
    return posts

def save_results(keyword, posts):
    """Save results as CSV."""
    df = pd.DataFrame(posts)
    fname = f"{ROOT_SAVE_DIR}/threads_{keyword}_en_{int(time.time())}.csv"
    df.to_csv(fname, index=False)
    logging.info(f"💾 Saved {len(df)} posts → {fname}")
    print(f"💾 Saved {len(df)} posts → {fname}")

def scrape_english_data():
    locale, accept = LOCALE
    logging.info("Starting scrape_english_data()")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        logging.info("Launched Chromium browser (headless mode).")
        context = browser.new_context(
            locale=locale,
            extra_http_headers={"Accept-Language": accept},
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )
        logging.info(f"Browser context created with locale={locale}")

        for emotion, keyword in ALL_KEYWORDS:
            results, seen_ids = [], set()
            logging.info(f"Begin scrape for [{emotion}] → #{keyword}")
            existing_ids = load_existing_ids(keyword)
            encoded_keyword = quote(keyword)
            search_url = f"https://www.threads.net/tag/{encoded_keyword}"
            logging.info(f"Search URL: {search_url}")

            page = context.new_page()
            try:
                logging.debug(f"Going to {search_url}")
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                logging.info(f"Loaded page for keyword: {keyword}")
            except Exception as e:
                logging.error(f"⚠️ Failed to open {search_url}: {e}")
                page.close()
                continue

            last_height = 0
            while len(results) < LIMIT_PER_KEYWORD:
                links = page.locator('a[href*="/post/"]').all()
                logging.debug(f"Found {len(links)} post links on current page scroll for #{keyword}")
                for link in links:
                    href = link.get_attribute("href")
                    if not href or "post" not in href:
                        continue
                    if href.startswith("/"):
                        href = "https://www.threads.net" + href
                    post_code = href.split("/")[-1]
                    if post_code in seen_ids or post_code in existing_ids:
                        logging.debug(f"Already seen or existing: {post_code}")
                        continue
                    seen_ids.add(post_code)
                    logging.info(f"Scraping post {post_code} from {href}")

                    try:
                        p2 = context.new_page()
                        p2.goto(href, wait_until="domcontentloaded", timeout=20000)
                        logging.debug("Post page loaded.")
                        p2.wait_for_selector("[data-pressable-container=true]", timeout=8000)
                        posts = scrape_thread_page(p2.content())
                        p2.close()
                        logging.debug(f"Extracted {len(posts)} posts from this page.")
                        for p in posts:
                            pid = str(p.get("id"))
                            if pid in existing_ids:
                                logging.debug(f"Post id {pid} already in existing ids. Skipping.")
                                continue
                            p["keyword"] = keyword
                            p["emotion"] = emotion
                            p["language_context"] = "english"
                            results.append(p)
                            logging.info(f"Post id {pid} appended. Total results for #{keyword}: {len(results)}")
                    except Exception as e:
                        logging.error(f"⚠️ Post error ({href}): {e}")
                    if len(results) >= LIMIT_PER_KEYWORD:
                        logging.info(f"Limit for {keyword} reached ({LIMIT_PER_KEYWORD}). Breaking loop.")
                        break

                page.mouse.wheel(0, 4000)
                time.sleep(SLEEP)
                try:
                    new_height = page.evaluate("document.body.scrollHeight")
                    logging.debug(f"Current scroll height: {new_height}, Last: {last_height}")
                except Exception as e:
                    logging.warning(f"Could not evaluate scroll height: {e}")
                    break
                if new_height == last_height:
                    logging.info("No further page scroll possible, breaking out.")
                    break
                last_height = new_height

            save_results(keyword, results)
            page.close()
            logging.info(f"Finished keyword: {keyword}")

        browser.close()
        logging.info("Closed browser.")
    print("All English keywords scraped.")
    logging.info("All English keywords scraped.")

if __name__ == "__main__":
    logging.info("Script started.")
    scrape_english_data()
    logging.info("Script finished.")
