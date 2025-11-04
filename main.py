import json
import re
import time
import argparse
from typing import Dict, List, Optional

import jmespath
from parsel import Selector
from nested_lookup import nested_lookup
from playwright.sync_api import sync_playwright


def parse_thread(data: Dict) -> Dict:
    """Parse Twitter tweet JSON dataset for the most important fields"""
    result = jmespath.search(
        """{
        text: post.caption.text,
        published_on: post.taken_at,
        id: post.id,
        pk: post.pk,
        code: post.code,
        username: post.user.username,
        user_pic: post.user.profile_pic_url,
        user_verified: post.user.is_verified,
        user_pk: post.user.pk,
        user_id: post.user.id,
        has_audio: post.has_audio,
        reply_count: view_replies_cta_string,
        like_count: post.like_count,
        images: post.carousel_media[].image_versions2.candidates[1].url,
        image_count: post.carousel_media_count,
        videos: post.video_versions[].url
    }""",
        data,
    )
    result["videos"] = list(set(result["videos"] or []))
    rc = result.get("reply_count")
    if isinstance(rc, int):
        pass
    elif isinstance(rc, str):
        m = re.search(r"\d+", rc)
        result["reply_count"] = int(m.group(0)) if m else 0
    else:
        result["reply_count"] = int(rc or 0)
    result[
        "url"
    ] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    return result


def scrape_thread(url: str) -> dict:
    """Scrape Threads post and replies from a given URL"""
    with sync_playwright() as pw:
        # start Playwright browser
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        # go to url and wait for the page to load
        page.goto(url)
        # wait for page to finish loading
        page.wait_for_selector("[data-pressable-container=true]")
        # find all hidden datasets
        selector = Selector(page.content())
        hidden_datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
        # find datasets that contain threads data
        for hidden_dataset in hidden_datasets:
            # skip loading datasets that clearly don't contain threads data
            if '"ScheduledServerJS"' not in hidden_dataset:
                continue
            if "thread_items" not in hidden_dataset:
                continue
            data = json.loads(hidden_dataset)
            # datasets are heavily nested, use nested_lookup to find 
            # the thread_items key for thread data
            thread_items = nested_lookup("thread_items", data)
            if not thread_items:
                continue
            # use our jmespath parser to reduce the dataset to the most important fields
            threads = [parse_thread(t) for thread in thread_items for t in thread]
            return {
                # the first parsed thread is the main post:
                "thread": threads[0],
                # other threads are replies:
                "replies": threads[1:],
            }
        raise ValueError("could not find thread data in page")


def scrape_search(query: str, max_posts: Optional[int] = None, scroll_pause_s: float = 0.6, stable_rounds: int = 3) -> List[str]:
    """Scrape Threads search results for a given query and return all post URLs.

    Args:
        query: search phrase
        max_posts: optional hard cap on number of URLs; None = all found
        scroll_pause_s: delay between scroll checks
        stable_rounds: stop after this many rounds with no new URLs
    """
    search_url = f"https://www.threads.net/search?q={query}&serp_type=default"
    urls: set[str] = set()
    rounds_without_growth = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()
        page.goto(search_url)
        page.wait_for_selector("[data-pressable-container=true]")

        last_height = 0
        while True:
            selector = Selector(page.content())
            hidden = selector.css('script[type="application/json"][data-sjs]::text').getall()
            for ds in hidden:
                if '"ScheduledServerJS"' not in ds or "thread_items" not in ds:
                    continue
                try:
                    data = json.loads(ds)
                except Exception:
                    continue
                for items in nested_lookup("thread_items", data):
                    for thread in items:
                        try:
                            t = parse_thread(thread)
                            url = t.get("url")
                            if url:
                                urls.add(url)
                                if max_posts and len(urls) >= max_posts:
                                    return list(urls)
                        except Exception:
                            continue

            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                break
            time.sleep(scroll_pause_s)
            try:
                new_height = page.evaluate("document.body.scrollHeight")
            except Exception:
                break
            if new_height == last_height:
                rounds_without_growth += 1
            else:
                rounds_without_growth = 0
                last_height = new_height
            if rounds_without_growth >= stable_rounds:
                break

    return list(urls)


def scrape_query_with_replies(query: str, max_posts: Optional[int] = None, per_post_delay_s: float = 0.25) -> List[Dict]:
    """For a search query, collect all posts and fetch each full thread with replies.

    Returns a list of objects: { url, thread, replies }.
    """
    urls = scrape_search(query, max_posts=max_posts)
    results: List[Dict] = []
    for url in urls:
        try:
            data = scrape_thread(url)
            results.append({"url": url, **data})
        except Exception:
            continue
        if per_post_delay_s:
            time.sleep(per_post_delay_s)
    return results



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Threads query scraper (posts + replies)")
    parser.add_argument("--query", required=True, help="Search query string")
    parser.add_argument("--max-posts", type=int, default=None, help="Optional cap on number of posts to fetch")
    parser.add_argument("--out", default="data/output.json", help="Output JSON path")
    args = parser.parse_args()

    data = scrape_query_with_replies(args.query, max_posts=args.max_posts)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} threads to {args.out}")



# https://www.threads.net/search?q=sad&serp_type=default