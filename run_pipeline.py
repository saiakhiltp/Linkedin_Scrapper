# run_pipeline.py
import os
import json
import time
from typing import List, Set
from urllib.parse import quote_plus

import requests
import pandas as pd 

# local module imports (make sure files are in same directory or install as package)
from scraper import fetch_html
from parse_linkedin_post import parse_linkedin_html

# ---------------- CONFIG ----------------
BING_API_KEY = os.getenv("BING_API_KEY")  # set env var or paste your key here (not recommended)
TOP_N_PER_KEYWORD = 10
HTML_TEMP_FOLDER = "html_temp"
PARSED_JSON_FOLDER = "parsed_jsons"
MASTER_EXCEL = "linkedin_posts_master.xlsx"
COMBINED_JSON = "all_posts_combined.json"
BING_DELAY_SEC = 1.0
SCRAPINGBEE_DELAY_SEC = 0.8
# ---------------------------------------

os.makedirs(HTML_TEMP_FOLDER, exist_ok=True)
os.makedirs(PARSED_JSON_FOLDER, exist_ok=True)


def serpapi_search(query: str, top: int = 10):
    import requests, os
    key = os.getenv("SERPAPI_KEY") or "paste_your_serpapi_key_here"
    endpoint = "https://serpapi.com/search.json"
    params = {"engine": "google", "q": query, "num": top, "api_key": key}
    r = requests.get(endpoint, params=params)
    r.raise_for_status()
    data = r.json()
    urls = []
    for item in data.get("organic_results", []):
        url = item.get("link")
        if url:
            urls.append(url)
    return urls

def is_linkedin_post_url(url: str) -> bool:
    if "linkedin.com" not in url:
        return False
    patterns = ["/posts/", "/feed/update/", "/activity:", "/activity/"]
    return any(p in url for p in patterns)

def save_json(parsed: dict, folder: str, filename_base: str):
    os.makedirs(folder, exist_ok=True)
    fname = f"{filename_base}.json"
    path = os.path.join(folder, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=4)
    return path

def load_master_df(path: str):
    if os.path.exists(path):
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def upsert_master(df: pd.DataFrame, parsed: dict) -> pd.DataFrame:
    row = {
        "url": parsed.get("url"),
        "title": parsed.get("title"),
        "author": parsed.get("author"),
        "content": parsed.get("content"),
        "likes": parsed.get("likes"),
        "comments": parsed.get("comments"),
        "date_published": parsed.get("date_published"),
        "images": json.dumps(parsed.get("images", []), ensure_ascii=False),
        "shared_url": parsed.get("shared_url"),
        "description": parsed.get("description"),
        "source_file": parsed.get("source_file")
    }
    if df is None or df.empty:
        return pd.DataFrame([row])
    if "url" in df.columns:
        idxs = df.index[df["url"] == row["url"]].tolist()
        if idxs:
            idx = idxs[0]
            for k, v in row.items():
                df.at[idx, k] = v
            return df
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df

def run_keywords(keywords: List[str], top_n_per_keyword: int = TOP_N_PER_KEYWORD):
    all_urls: Set[str] = set()
    # 1) discover urls via bing
    for kw in keywords:
        q = f'site:linkedin.com/posts "{kw}"'
        try:
            urls = serpapi_search(q, top=top_n_per_keyword)

        except Exception as e:
            print("Bing search error:", e)
            urls = []
        time.sleep(BING_DELAY_SEC)
        if len(urls) < top_n_per_keyword:
            q2 = f'site:linkedin.com/feed/update "{kw}"'
            try:
                more = bing_search(q2, top=(top_n_per_keyword - len(urls)))
                urls += more
            except Exception:
                pass
            time.sleep(BING_DELAY_SEC)
        for u in urls:
            if is_linkedin_post_url(u):
                all_urls.add(u)

    print(f"Found {len(all_urls)} LinkedIn candidate URLs from keywords: {keywords}")

    master_df = load_master_df(MASTER_EXCEL)
    processed = []
    for url in sorted(all_urls):
        try:
            print("Fetching:", url)
            html = fetch_html(url, render_js=True, save_path=None)  # you can save html by setting save_path
            time.sleep(SCRAPINGBEE_DELAY_SEC)
        except Exception as e:
            print("Fetch failed:", e)
            continue

        safe_base = url.replace("https://", "").replace("http://", "").replace("/", "_")
        html_save = os.path.join(HTML_TEMP_FOLDER, f"{safe_base}.html")
        with open(html_save, "w", encoding="utf-8") as f:
            f.write(html)

        parsed = parse_linkedin_html(html, source_filename=html_save)
        # ensure parsed.url is filled (fallback to url)
        if not parsed.get("url"):
            parsed["url"] = url

        json_path = save_json(parsed, PARSED_JSON_FOLDER, safe_base)
        print("Parsed JSON saved:", json_path)

        master_df = upsert_master(master_df, parsed)
        processed.append(parsed)

    # save outputs
    if master_df is not None:
        master_df.to_excel(MASTER_EXCEL, index=False, engine="openpyxl")
        print("Master Excel updated:", MASTER_EXCEL)

    with open(COMBINED_JSON, "w", encoding="utf-8") as f:
        json.dump(processed, f, ensure_ascii=False, indent=4)
    print("Combined JSON saved:", COMBINED_JSON)
    print("Done. Processed:", len(processed))

if __name__ == "__main__":
    # example: use CLI or change keywords here
    import argparse
    parser = argparse.ArgumentParser(description="Find LinkedIn posts by keywords, fetch and parse them")
    parser.add_argument("--keywords", "-k", nargs="+", required=False, help="Keywords to search (e.g. 'mothers day')", default=["mothers day"])
    parser.add_argument("--top", "-n", type=int, help="Top N results per keyword", default=TOP_N_PER_KEYWORD)
    args = parser.parse_args()

    run_keywords(args.keywords, top_n_per_keyword=args.top)
