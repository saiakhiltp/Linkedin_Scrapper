# app_streamlit.py (UPDATED)
import os
import time
import json
import re
from typing import List, Set, Dict
from datetime import datetime, date

import streamlit as st
import pandas as pd
import requests

# Try to import your existing modules (preferred)
try:
    from scrapper import fetch_html
except Exception:
    fetch_html = None

try:
    from parse_linkedin_post import parse_linkedin_html
except Exception:
    parse_linkedin_html = None

# -------------------- Utility: company matching --------------------
# ---------- Auto-detect company slug helper using SerpAPI ----------
import urllib.parse

def extract_linkedin_company_slugs_from_url(url: str):
    """
    Given a linkedin company URL (or any linkedin url), return potential slugs.
    Example: https://www.linkedin.com/company/teleperformance-india/ -> teleperformance-india
    """
    try:
        parsed = urllib.parse.urlparse(url)
        path = parsed.path  # e.g. /company/teleperformance-india/
        parts = [p for p in path.split("/") if p]
        # find 'company' then next segment is slug
        if "company" in parts:
            idx = parts.index("company")
            if idx + 1 < len(parts):
                return [parts[idx + 1]]
        # fallback: return last non-empty segment
        if parts:
            return [parts[-1]]
    except Exception:
        pass
    return []

def serpapi_find_company_slugs(company_name: str, serpapi_key: str = None, top: int = 6) -> List[str]:
    """
    Use SerpAPI to search for candidate LinkedIn company URLs for a given company name.
    Returns list of possible slugs (de-duplicated).
    """
    key = serpapi_key or os.getenv("SERPAPI_KEY")
    if not key:
        return []
    q = f'site:linkedin.com/company "{company_name}"'
    endpoint = "https://serpapi.com/search.json"
    params = {"engine": "google", "q": q, "num": top, "api_key": key}
    try:
        r = requests.get(endpoint, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    slugs = []
    for item in data.get("organic_results", []):
        link = item.get("link") or item.get("formattedUrl") or ""
        if "linkedin.com/company" in link:
            found = extract_linkedin_company_slugs_from_url(link)
            for s in found:
                if s and s not in slugs:
                    slugs.append(s)
    return slugs

def normalize(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip().lower()

def company_matches_parsed(parsed: Dict, company_names: List[str], company_slugs: List[str]=None) -> bool:
    """
    Return True if the parsed post looks like it was posted by one of the company_names/slugs.
    """
    company_slugs = company_slugs or []
    names_norm = [normalize(x) for x in company_names if x]
    slugs_norm = [normalize(x) for x in company_slugs if x]

    # 1) parsed author name
    author = normalize(parsed.get("author") or parsed.get("creator") or "")
    for n in names_norm:
        if n and n in author:
            return True

    # 2) raw_jsonld author/url fields
    raw = parsed.get("raw_jsonld") or []
    for obj in raw:
        try:
            auth = obj.get("author") or obj.get("creator") or obj.get("publisher")
            if isinstance(auth, dict):
                aname = normalize(auth.get("name"))
                aurl = normalize(auth.get("url") or auth.get("sameAs") or "")
                for n in names_norm:
                    if n and n in aname:
                        return True
                for s in slugs_norm:
                    if s and s in aurl:
                        return True
            elif isinstance(auth, str):
                for n in names_norm:
                    if n and n in normalize(auth):
                        return True
        except Exception:
            pass

    # 3) parsed url for company slug
    post_url = normalize(parsed.get("url") or parsed.get("fetched_url") or "")
    for s in slugs_norm:
        if s and s in post_url:
            return True

    # 4) check content/description for the company name (fallback)
    content = normalize(parsed.get("content") or parsed.get("description") or "")
    for n in names_norm:
        if n and content.startswith(n):
            return True

    return False

# -------------------- SerpAPI & fetch helpers --------------------
def serpapi_search(query: str, top: int = 10, serpapi_key: str = None) -> List[str]:
    key = serpapi_key or os.getenv("SERPAPI_KEY")
    if not key:
        raise RuntimeError("SerpAPI key not provided. Set SERPAPI_KEY env var or provide key in app.")
    endpoint = "https://serpapi.com/search.json"
    params = {"engine": "google", "q": query, "num": top, "api_key": key}
    r = requests.get(endpoint, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    urls = []
    for item in data.get("organic_results", []):
        url = item.get("link")
        if url and "linkedin.com" in url:
            urls.append(url)
    return urls

def local_fetch_html(url: str, scrapingbee_key: str = None, render_js=True, save_path=None, timeout=60) -> str:
    if fetch_html:
        return fetch_html(url, render_js=render_js, save_path=save_path, timeout=timeout)
    key = scrapingbee_key or os.getenv("SCRAPINGBEE_KEY")
    if not key:
        raise RuntimeError("No ScrapingBee key found. Set SCRAPINGBEE_KEY env var or provide key in app.")
    endpoint = "https://app.scrapingbee.com/api/v1/"
    params = {"api_key": key, "url": url, "render_js": "true" if render_js else "false"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    html = r.text
    if save_path:
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
    return html

def local_parse_html(html: str, source_filename: str = None) -> Dict:
    if parse_linkedin_html:
        return parse_linkedin_html(html, source_filename=source_filename)
    # minimal fallback
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    out = {"url": None, "title": None, "content": None, "likes": None,
           "comments": None, "reposts": None, "author": None, "date_published": None,
           "images": [], "raw_jsonld": []}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            out["raw_jsonld"].append(data)
            if isinstance(data, dict):
                if data.get("articleBody"):
                    out["content"] = data.get("articleBody")
                    out["title"] = data.get("headline") or out["title"]
                    out["date_published"] = data.get("datePublished") or out["date_published"]
                stats = data.get("interactionStatistic")
                if isinstance(stats, dict):
                    stats = [stats]
                if isinstance(stats, list):
                    for s in stats:
                        it = str(s.get("interactionType", "")).lower()
                        cnt = s.get("userInteractionCount")
                        if "like" in it:
                            out["likes"] = int(cnt or 0)
                        if "comment" in it:
                            out["comments"] = int(cnt or 0)
                        if "share" in it or "resha" in it:
                            out["reposts"] = int(cnt or 0)
        except Exception:
            continue
    can = soup.find("link", rel="canonical")
    if can and can.get("href"):
        out["url"] = can["href"]
    return out

# -------------------- Streamlit UI --------------------
st.set_page_config(layout="wide", page_title="LinkedIn Scraper for Marketing Teams")
st.title("LinkedIn Scraper — Search, Parse, Filter (SerpAPI + ScrapingBee)")

with st.sidebar:
    st.header("API Keys (optional)")
    serp_key_input = st.text_input("SerpAPI Key", value=os.getenv("SERPAPI_KEY") or "", type="password")
    sb_key_input = st.text_input("ScrapingBee Key", value=os.getenv("SCRAPINGBEE_KEY") or "", type="password")
    st.markdown("---")
    st.header("Pipeline options")
    top_n = st.number_input("Top N results per query", min_value=1, max_value=50, value=8, step=1)
    render_js = st.checkbox("Render JS when fetching pages (recommended)", value=True)
    delay_between_fetch = st.number_input("Delay between fetches (seconds)", min_value=0.0, max_value=5.0, value=0.8, step=0.1)
    st.markdown("---")
    st.header("Output")
    master_excel_name = st.text_input("Master Excel filename", value="linkedin_posts_master.xlsx")
    combined_json_name = st.text_input("Combined JSON filename", value="all_posts_combined.json")

mode = st.selectbox("Select mode", ["Event search", "Company search", "Specific post URLs"])

if mode == "Event search":
    st.subheader("Event search")
    event_keywords = st.text_area("Event keywords (one per line). Examples: children's day, #ChildrensDay, mothers day", height=120)
    companies_filter = st.text_area("Optional: restrict to companies (one per line)", height=80)
elif mode == "Company search":
    st.subheader("Company search")
    companies_input = st.text_area("Company names (one per line)", height=200)
    event_filter = st.text_area("Optional: event keywords to filter company posts (one per line)", height=80)
else:
    st.subheader("Specific post URLs")
    urls_input = st.text_area("Paste LinkedIn post URLs (one per line)", height=200)

st.sidebar.header("Company-only filter (optional)")
only_company = st.sidebar.checkbox("Only posts by specified company", value=False)
company_input = st.sidebar.text_input("Company name(s) (comma-separated)", value="")
slug_input = st.sidebar.text_input("Company slug(s) (comma-separated)", value="")

# Date range
st.sidebar.header("Date range filter (optional)")
col1, col2 = st.sidebar.columns(2)
start_date = col1.date_input("Start date (inclusive)", value=None)
end_date = col2.date_input("End date (inclusive)", value=None)

run = st.button("Run pipeline")
results_placeholder = st.empty()

def build_queries_from_inputs(company_slugs_for_queries: List[str] = None):
    """
    If company_slugs_for_queries is provided, we will include site:linkedin.com/company/<slug> queries
    to bias results toward official company posts.
    """
    queries = []
    if mode == "Event search":
        events = [e.strip() for e in event_keywords.splitlines() if e.strip()]
        companies = [c.strip() for c in companies_filter.splitlines() if c.strip()]
        for e in events:
            # general queries
            queries.append(f'site:linkedin.com/posts "{e}"')
            queries.append(f'site:linkedin.com/feed/update "{e}"')
            # if company slugs available, add strict company queries
            if company_slugs_for_queries:
                for slug in company_slugs_for_queries:
                    queries.append(f'site:linkedin.com/company/{slug} "{e}"')
                    queries.append(f'site:linkedin.com/posts "linkedin.com/company/{slug}" "{e}"')
            for c in companies:
                queries.append(f'site:linkedin.com/posts "{e}" "{c}"')
                queries.append(f'site:linkedin.com/feed/update "{e}" "{c}"')

    elif mode == "Company search":
        companies = [c.strip() for c in companies_input.splitlines() if c.strip()]
        events = [e.strip() for e in event_filter.splitlines() if e.strip()]
        for c in companies:
            # prefer company-page queries when we have slugs
            if company_slugs_for_queries:
                for slug in company_slugs_for_queries:
                    queries.append(f'site:linkedin.com/company/{slug}')
                    queries.append(f'site:linkedin.com/company/{slug} "{c}"')
            # fallback
            queries.append(f'site:linkedin.com/posts "{c}"')
            queries.append(f'site:linkedin.com/feed/update "{c}"')
            for e in events:
                if company_slugs_for_queries:
                    for slug in company_slugs_for_queries:
                        queries.append(f'site:linkedin.com/company/{slug} "{e}"')
                        queries.append(f'site:linkedin.com/posts "linkedin.com/company/{slug}" "{e}"')
                queries.append(f'site:linkedin.com/posts "{e}" "{c}"')
                queries.append(f'site:linkedin.com/feed/update "{e}" "{c}"')

    return list(dict.fromkeys(queries))

def within_date_range(parsed_obj, start_date_obj, end_date_obj):
    dp = parsed_obj.get("date_published")
    if not dp:
        return True
    try:
        dt = datetime.fromisoformat(dp.replace("Z", "+00:00"))
        d = dt.date()
        if start_date_obj and d < start_date_obj:
            return False
        if end_date_obj and d > end_date_obj:
            return False
        return True
    except Exception:
        return True

if run:
    serp_key = serp_key_input.strip() or os.getenv("SERPAPI_KEY")
    sb_key = sb_key_input.strip() or os.getenv("SCRAPINGBEE_KEY")

    candidate_urls: Set[str] = set()
    if mode in ("Event search", "Company search"):
        # --- Resolve company names & slugs for stricter/company-only queries ---
        company_names = [c.strip() for c in company_input.split(",") if c.strip()]
        company_slugs_ui = [s.strip() for s in slug_input.split(",") if s.strip()]

        resolved_slugs = []
        # if company-only filter enabled, try to auto-detect slugs
        if only_company and company_names:
            # prefer user-provided slugs first
            resolved_slugs.extend(company_slugs_ui)
            # auto-detect remaining slugs via SerpAPI
            for cn in company_names:
                # skip if a provided slug already contains the company name
                if any(cn.lower() in s.lower() for s in resolved_slugs):
                    continue
                try:
                    found = serpapi_find_company_slugs(cn, serpapi_key=serp_key, top=6)
                except Exception:
                    found = []
                for s in found:
                    if s not in resolved_slugs:
                        resolved_slugs.append(s)
            if resolved_slugs:
                st.info(f"Auto-detected company slug(s): {resolved_slugs}")
                # optional: show suggested slugs in sidebar so user can edit before run
                try:
                    st.sidebar.write("Suggested slug(s): " + ", ".join(resolved_slugs))
                except Exception:
                    pass

        # build queries; pass resolved_slugs so queries include site:linkedin.com/company/<slug> variants
        try:
            queries = build_queries_from_inputs(company_slugs_for_queries=resolved_slugs)
        except TypeError:
            # fallback if your build_queries_from_inputs doesn't accept param yet
            queries = build_queries_from_inputs()

        if not queries:
            st.error("No queries were built — provide keywords or companies.")
        else:
            st.info(f"Built {len(queries)} queries. Running SerpAPI (top {top_n} per query)...")
            progress = st.progress(0)
            total = len(queries)
            for i, q in enumerate(queries):
                try:
                    urls = serpapi_search(q, top=top_n, serpapi_key=serp_key)
                    for u in urls:
                        if "linkedin.com" in u:
                            candidate_urls.add(u)
                except Exception as e:
                    st.warning(f"Search failed for query: {q} — {e}")
                time.sleep(0.5)
                progress.progress(int((i+1)/total * 100))
            st.success(f"Discovered {len(candidate_urls)} candidate LinkedIn URLs.")
    else:
        urls = [u.strip() for u in urls_input.splitlines() if u.strip()]
        candidate_urls.update(urls)
        st.info(f"Added {len(urls)} specific URLs.")

    if not candidate_urls:
        st.warning("No LinkedIn URLs found — nothing to fetch.")
    else:
        st.info(f"Fetching and parsing {len(candidate_urls)} pages (this may take a while).")
        parsed_results = []
        pbar = st.progress(0)
        total_count = len(candidate_urls)
        # ensure company names/slugs are available for per-post filtering too
        company_names = [c.strip() for c in company_input.split(",") if c.strip()]
        company_slugs = [s.strip() for s in slug_input.split(",") if s.strip()]
        # if we auto-detected slugs and the user didn't provide any, use resolved_slugs for filtering
        try:
            if only_company and not company_slugs and resolved_slugs:
                company_slugs = resolved_slugs
        except NameError:
            # resolved_slugs not defined (shouldn't happen), ignore
            pass

        for idx, url in enumerate(sorted(candidate_urls)):
            try:
                st.write(f"Fetching: {url}")
                html = local_fetch_html(url, scrapingbee_key=sb_key, render_js=render_js, save_path=None, timeout=60)
                parsed = local_parse_html(html, source_filename=None)
                if not parsed.get("url"):
                    parsed["url"] = url
                parsed["fetched_url"] = url

                # Ensure numeric fields exist
                for k in ("likes", "comments", "reposts"):
                    if k not in parsed or parsed.get(k) is None:
                        parsed[k] = 0

                # company-only filter if enabled
                if only_company and company_names:
                    if not company_matches_parsed(parsed, company_names, company_slugs):
                        st.write(f"Skipping (not company-author): {url}")
                        time.sleep(delay_between_fetch)
                        pbar.progress(int((idx+1)/total_count * 100))
                        continue

                # date filter
                if not within_date_range(parsed, start_date, end_date):
                    st.write(f"Skipping (out of date range): {url}")
                    time.sleep(delay_between_fetch)
                    pbar.progress(int((idx+1)/total_count * 100))
                    continue

                parsed_results.append(parsed)
                st.write("Parsed:", parsed.get("title") or (parsed.get("content") or "")[:120])
            except Exception as e:
                st.error(f"Failed to fetch/parse {url}: {e}")
            time.sleep(delay_between_fetch)
            pbar.progress(int((idx+1)/total_count * 100))

        # Display results and KPIs
        if parsed_results:
            df = pd.DataFrame(parsed_results)
            # normalize lists
            if "images" in df.columns:
                df["images"] = df["images"].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v)
            # ensure numeric types
            for col in ("likes", "comments", "reposts"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                else:
                    df[col] = 0
            df["engagement"] = df["likes"] + df["comments"] + df["reposts"]

           
        
            # date parsing
        try:
            df["date_published"] = pd.to_datetime(df["date_published"], utc=True)
        except Exception:
            pass

        # Excel cannot handle timezone-aware datetimes → convert to naive datetime
        if "date_published" in df.columns:
            try:
               df["date_published"] = df["date_published"].dt.tz_localize(None)
            except:
                pass

            # KPIs at top
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Posts", len(df))
            col2.metric("Total Likes", int(df["likes"].sum()))
            col3.metric("Total Comments", int(df["comments"].sum()))
            col4.metric("Total Reposts", int(df["reposts"].sum()))

            # Averages
            col5, col6, col7 = st.columns(3)
            col5.metric("Avg Likes / Post", round(df["likes"].mean(), 1))
            col6.metric("Avg Comments / Post", round(df["comments"].mean(), 1))
            col7.metric("Avg Engagement / Post", round(df["engagement"].mean(), 1))

            results_placeholder.dataframe(df[["url", "title", "author", "date_published", "likes", "comments", "reposts", "engagement"]], use_container_width=True)

            # Save outputs
            combined_json_path = combined_json_name
            with open(combined_json_path, "w", encoding="utf-8") as f:
                json.dump(parsed_results, f, ensure_ascii=False, indent=4)
            st.success(f"Combined JSON saved to {combined_json_path}")

            try:
                if os.path.exists(master_excel_name):
                    master_df = pd.read_excel(master_excel_name, engine="openpyxl")
                else:
                    master_df = pd.DataFrame()
                for parsed in parsed_results:
                    url_val = parsed.get("url")
                    row = {
                        "url": url_val,
                        "title": parsed.get("title"),
                        "author": parsed.get("author"),
                        "content": parsed.get("content"),
                        "likes": parsed.get("likes"),
                        "comments": parsed.get("comments"),
                        "reposts": parsed.get("reposts"),
                        "date_published": parsed.get("date_published"),
                        "images": json.dumps(parsed.get("images", []), ensure_ascii=False),
                        "fetched_url": parsed.get("fetched_url")
                    }
                    if master_df is None or master_df.empty:
                        master_df = pd.DataFrame([row])
                    else:
                        exist = master_df.index[master_df["url"] == url_val].tolist()
                        if exist:
                            idx0 = exist[0]
                            for k, v in row.items():
                                master_df.at[idx0, k] = v
                        else:
                            master_df = pd.concat([master_df, pd.DataFrame([row])], ignore_index=True)
                master_df.to_excel(master_excel_name, index=False, engine="openpyxl")
                st.success(f"Master Excel updated: {master_excel_name}")
            except Exception as e:
                st.error(f"Failed to update master excel: {e}")

            # Download buttons
            json_bytes = json.dumps(parsed_results, ensure_ascii=False, indent=4).encode("utf-8")
            st.download_button("Download combined JSON", data=json_bytes, file_name=combined_json_name, mime="application/json")

            try:
                import io
                tosave = io.BytesIO()
                # Save the current df view to excel for download
                df_to_save = df[["url", "title", "author", "date_published", "likes", "comments", "reposts", "engagement"]]
                df_to_save.to_excel(tosave, index=False, engine="openpyxl")
                tosave.seek(0)
                st.download_button("Download results as Excel", data=tosave, file_name="linkedin_results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.warning("Excel download not available: " + str(e))

        else:
            st.warning("No parsed results after fetching (maybe filtered by company/date).")
