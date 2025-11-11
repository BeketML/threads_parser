import json, time, os, re, random
from parsel import Selector
from nested_lookup import nested_lookup
import jmespath
from playwright.sync_api import sync_playwright
import pandas as pd
from urllib.parse import quote
from langdetect import detect, DetectorFactory
from glob import glob

TARGET_TOTAL = 15000
PER_KEYWORD_LIMIT = 120
SCROLL_SLEEP = (1.2, 2.5)
POST_PAGE_TIMEOUT = 20000
ROOT_SAVE_DIR = "data/file/"
SHARD_SIZE = 1000

DetectorFactory.seed = 0

def detect_language_safe(text):
    try:
        return detect(text) if text else "unknown"
    except:
        return "unknown"

# SAD = [
#     "грустный","грусть","печаль","тоска","депрессия","одиночество","одиноко","слёзы","плачу","боль",
#     "страдания","скучаю","тревога","усталость","разбитое сердце",
#     "мұң","мұңайу","қайғы","жалғыздық","жылау","ауыр өмір","жаман күн","жүрегім ауырды","мен шаршадым","өмір қиын","сағыныш",
#     "sad","sadness","lonely","depression","anxiety","crying","broken heart","miss you","tired","pain","hopeless","heartbreak","hurt","bad day"
# ]
# SAD = [
#     "грустный","грусть","печаль","тоска","депрессия","одиночество","одиноко","слёзы","плачу","боль",
#     "страдания","скучаю","тревога","усталость","разбитое сердце",
#     "мұң","мұңайу","қайғы","жалғыздық","жылау","ауыр өмір","жаман күн","жүрегім ауырды","мен шаршадым","өмір қиын","сағыныш",
#     "sad","sadness","lonely","depression","anxiety","crying","broken heart","miss you","tired","pain","hopeless","heartbreak","hurt","bad day"
# ]
SAD = [
    "суицид"
]
NEUTRAL = [
    "новости","семья","день","утро","жизнь","работа","отдых","путешествия","еда","выходные","погода","друзья","день рождения","хобби","книги",
    "өмір","күнделікті өмір","сабақ","оқу","достар","отбасы","демалыс","тамақ","жұмыс","ауа райы","саяхат","кітап","бос уақыт","таң","кеш",
    "life","work","school","friends","weather","family","travel","morning","routine","food","study","weekend","day","evening","city"
]
HAPPY = [
    "счастье","радость","улыбка","любовь","мотивация","успех","вдохновение","достижения","прекрасный день","я счастлив","благодарность",
    "қуаныш","бақыт","шабыт","махаббат","жетістік","керемет күн","рахмет","ризашылық","мен бақыттымын","өмір тамаша",
    "happy","happiness","joy","smile","love","success","motivation","good vibes","grateful","blessed","proud","amazing day","sunshine"
]
# ALL_KEYWORDS = [("sad", k) for k in SAD] + [("neutral", k) for k in NEUTRAL] + [("happy", k) for k in HAPPY]
# ALL_KEYWORDS = [("sad", k) for k in SAD] + [("neutral", k) for k in NEUTRAL] + [("happy", k) for k in HAPPY]
ALL_KEYWORDS = [("sad", k) for k in SAD]

LOCALES = [
    ("ru-RU", "ru-RU,ru;q=0.9,en;q=0.8"),
    ("kk-KZ", "kk-KZ,kk;q=0.9,ru;q=0.8,en;q=0.7"),
    ("en-US", "en-US,en;q=0.9")
]

def load_existing_ids_all():
    existing = set()
    os.makedirs(ROOT_SAVE_DIR, exist_ok=True)
    for file in glob(f"{ROOT_SAVE_DIR}/threads_*.csv"):
        try:
            df = pd.read_csv(file, usecols=["id"])
            existing.update(df["id"].dropna().astype(str))
        except Exception:
            continue
    return existing

def parse_thread(data):
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
        return None

    if result.get("reply_count") and not isinstance(result["reply_count"], int):
        try:
            first = str(result["reply_count"]).split(" ")[0]
            result["reply_count"] = int(first) if first.isdigit() else 0
        except:
            result["reply_count"] = 0

    try:
        result["url"] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    except:
        result["url"] = ""
    result["repost_count"] = 0
    return result

def scrape_thread_page(page_source):
    selector = Selector(text=page_source)
    datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
    posts, seen_ids = [], set()

    for raw in datasets:
        if '"ScheduledServerJS"' not in raw or "thread_items" not in raw:
            continue
        try:
            data = json.loads(raw)
        except:
            continue

        thread_items = nested_lookup("thread_items", data)
        for group in thread_items:
            for t in group:
                parsed = parse_thread(t)
                if not parsed:
                    continue
                pid = str(parsed.get("id"))
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)

                repost_count = 0
                for txt in selector.css("span::text").getall():
                    if "repost" in txt.lower():
                        digits = "".join(ch for ch in txt if ch.isdigit())
                        if digits.isdigit():
                            repost_count = int(digits)
                            break
                parsed["repost_count"] = repost_count
                posts.append(parsed)
    return posts

def keyword_in_text(text, keyword):
    return keyword.lower() in text.lower() if text and keyword else False

def save_shard(rows, shard_idx):
    if not rows:
        return
    df = pd.DataFrame(rows)
    fname = f"{ROOT_SAVE_DIR}/threads_autoscrape_shard{shard_idx}_{int(time.time())}.csv"
    df.to_csv(fname, index=False)
    print(f"Saved shard #{shard_idx}: {len(df)} rows -> {fname}")

def run_autoscrape():
    os.makedirs(ROOT_SAVE_DIR, exist_ok=True)
    global_cache_ids = load_existing_ids_all()
    print(f"Found {len(global_cache_ids)} existing IDs in {ROOT_SAVE_DIR}/")

    collected, shard_idx, total = [], 1, 0
    random.shuffle(ALL_KEYWORDS)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)

        while total < TARGET_TOTAL:
            progress = 0
            for emotion, keyword in ALL_KEYWORDS:
                if total >= TARGET_TOTAL:
                    break
                for locale, accept in LOCALES:
                    if total >= TARGET_TOTAL:
                        break

                    context = browser.new_context(
                        locale=locale,
                        extra_http_headers={"Accept-Language": accept},
                        viewport={"width": 1920, "height": 1080},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                    )
                    page = context.new_page()
                    search_url = f"https://www.threads.net/tag/{quote(keyword)}"
                    print(f"[{emotion}/{locale}] -> #{keyword}")

                    try:
                        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                    except Exception as e:
                        print("Goto failed:", e)
                        context.close()
                        continue

                    seen_code, got_kw, stagnant, last_height = set(), 0, 0, 0

                    while got_kw < PER_KEYWORD_LIMIT and total < TARGET_TOTAL:
                        links = page.locator('a[href*="/post/"]').all()
                        for link in links:
                            href = link.get_attribute("href")
                            if not href or "post" not in href:
                                continue
                            if href.startswith("/"):
                                href = "https://www.threads.net" + href
                            post_code = href.split("/")[-1]
                            if not post_code or post_code in seen_code:
                                continue
                            seen_code.add(post_code)

                            try:
                                p2 = context.new_page()
                                p2.goto(href, wait_until="domcontentloaded", timeout=POST_PAGE_TIMEOUT)
                                p2.wait_for_selector("[data-pressable-container=true]", timeout=8000)
                                posts = scrape_thread_page(p2.content())
                                p2.close()
                            except Exception as e:
                                print("Post load error:", e)
                                continue

                            for p in posts:
                                pid, txt = str(p.get("id") or ""), p.get("text") or ""
                                if not pid or pid in global_cache_ids:
                                    continue
                                if not keyword_in_text(txt, keyword):
                                    continue

                                p["keyword"], p["emotion"], p["locale_context"], p["lang_detect"] = (
                                    keyword, emotion, locale, detect_language_safe(txt)
                                )

                                collected.append(p)
                                global_cache_ids.add(pid)
                                got_kw += 1
                                total += 1
                                progress += 1

                                if len(collected) >= SHARD_SIZE:
                                    save_shard(collected, shard_idx)
                                    collected.clear()
                                    shard_idx += 1

                                if total >= TARGET_TOTAL or got_kw >= PER_KEYWORD_LIMIT:
                                    break

                        if total >= TARGET_TOTAL or got_kw >= PER_KEYWORD_LIMIT:
                            break
                        page.mouse.wheel(0, random.randint(2200, 3000))
                        time.sleep(random.uniform(*SCROLL_SLEEP))
                        try:
                            new_height = page.evaluate("document.body.scrollHeight")
                        except:
                            new_height = last_height
                        stagnant = stagnant + 1 if new_height == last_height else 0
                        last_height = new_height
                        if stagnant >= 3:
                            break

                    context.close()

            if progress == 0:
                print("No progress this rotation — stopping.")
                break

        if collected:
            save_shard(collected, shard_idx)

    print(f"Done. Collected total {total} posts (including shards).")

if __name__ == "__main__":
    run_autoscrape()
