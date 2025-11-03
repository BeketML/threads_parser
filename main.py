import json
import re
from typing import Dict

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


def scrape_search(query: str) -> dict:
    """Scrape Threads search results for a given query"""
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()
        page.goto(f"https://www.threads.com/search?q={query}&serp_type=default")
        page.wait_for_selector("[data-pressable-container=true]")
        selector = Selector(page.content())

#####################################################################################################

# def detect_lang(text: str) -> str:
#     """Heuristic language detection for 'kaz', 'rus', or 'mixed'."""
#     if not text:
#         return "rus"
#     t = text.lower()
#     kaz_letters = set("әғқңөұүһі")
#     has_cyr = any("\u0400" <= ch <= "\u04FF" for ch in t)
#     has_kaz = any(ch in kaz_letters for ch in t)
#     has_latin = any("a" <= ch <= "z" for ch in t)
#     if has_kaz and has_cyr and has_latin:
#         return "mixed"
#     if has_kaz and has_cyr:
#         return "kaz"
#     if has_cyr:
#         return "rus"
#     return "rus"


# def parse_search_features(keyword: str) -> list:
#     """Parse Threads search results and return desired fields per item.

#     Fields: nickname, text, likes, repost, comments, lang, label
#     """
#     url = f"https://www.threads.com/search?q={keyword}&serp_type=default"
#     results = []
#     with sync_playwright() as pw:
#         browser = pw.chromium.launch()
#         context = browser.new_context(viewport={"width": 1920, "height": 1080})
#         page = context.new_page()
#         page.goto(url)
#         page.wait_for_selector("[data-pressable-container=true]")
#         selector = Selector(page.content())
#         hidden = selector.css('script[type="application/json"][data-sjs]::text').getall()
#         for ds in hidden:
#             if '"ScheduledServerJS"' not in ds or "thread_items" not in ds:
#                 continue
#             data = json.loads(ds)
#             thread_items = nested_lookup("thread_items", data)
#             if not thread_items:
#                 continue
#             threads = [parse_thread(t) for thread in thread_items for t in thread]
#             for th in threads:
#                 text = (th.get("text") or "").strip()
#                 likes = th.get("like_count") or 0
#                 comments = th.get("reply_count") or 0
#                 try:
#                     likes = int(likes)
#                 except Exception:
#                     m = re.search(r"\d+", str(likes))
#                     likes = int(m.group(0)) if m else 0
#                 try:
#                     comments = int(comments)
#                 except Exception:
#                     m = re.search(r"\d+", str(comments))
#                     comments = int(m.group(0)) if m else 0
#                 results.append(
#                     {
#                         "nickname": th.get("username", ""),
#                         "text": text,
#                         "likes": likes,
#                         "repost": 0,
#                         "comments": comments,
#                         "lang": detect_lang(text),
#                         "label": keyword,
#                     }
#                 )
#     return results

# def save_results_json(path: str, rows: list) -> None:
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(rows, f, ensure_ascii=False, indent=2)


# def save_results_csv(path: str, rows: list) -> None:
#     import csv

#     fieldnames = [
#         "nickname",
#         "text",
#         "likes",
#         "repost",
#         "comments",
#         "lang",
#         "label",
#     ]
#     with open(path, "w", encoding="utf-8", newline="") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         for r in rows:
#             writer.writerow({k: r.get(k) for k in fieldnames})

if __name__ == "__main__":
    print(scrape_thread("https://www.threads.com/search?q=sad&serp_type=default"))