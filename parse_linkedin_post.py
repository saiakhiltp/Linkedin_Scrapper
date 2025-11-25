"""
parse_linkedin_post.py

This module contains the HTML parsing logic to extract structured data from LinkedIn post HTML pages.
It provides the function `parse_linkedin_html` which takes raw HTML content and returns a dictionary of extracted post information.

This centralized parsing logic is used by various scripts in the project to parse LinkedIn posts HTML files consistently.

The parsing extracts metadata such as post URL, title, content, author, likes, comments, date published, images, shared URLs, descriptions, and raw JSON-LD data.
"""

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from bs4 import BeautifulSoup


def parse_short_number(s: Union[str, int, None]) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    s = str(s).strip().replace(",", "").replace("\u00A0", "")
    if s == "":
        return None
    m = re.match(r"^([\d.]+)([KkMm]?)$", s)
    if m:
        num = float(m.group(1))
        suf = m.group(2).upper()
        if suf == "K":
            return int(num * 1_000)
        if suf == "M":
            return int(num * 1_000_000)
        return int(num)
    m2 = re.search(r"(\d[\d,]*)", s)
    if m2:
        return int(m2.group(1).replace(",", ""))
    return None


def extract_jsonld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    results = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        txt = script.string or script.get_text() or ""
        txt = txt.strip()
        if not txt:
            continue
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                results.extend(parsed)
            else:
                results.append(parsed)
        except Exception:
            # Try to split concatenated JSON objects naively
            parts = re.split(r"\}\s*\{", txt)
            if len(parts) > 1:
                for i, p in enumerate(parts):
                    if i == 0:
                        candidate = p + "}"
                    elif i == len(parts) - 1:
                        candidate = "{" + p
                    else:
                        candidate = "{" + p + "}"
                    try:
                        results.append(json.loads(candidate))
                    except Exception:
                        continue
    return results


def find_meta(soup: BeautifulSoup, name: str, prop: str = "property") -> Optional[str]:
    tag = soup.find("meta", attrs={prop: name})
    if tag and tag.get("content"):
        return tag["content"]
    tag = soup.find("meta", attrs={"name": name})
    if tag and tag.get("content"):
        return tag["content"]
    return None


def parse_linkedin_html(html: str, source_filename: Optional[str] = None) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    out = {
        "url": None,
        "title": None,
        "description": None,
        "content": None,
        "likes": None,
        "comments": None,
        "author": None,
        "date_published": None,
        "images": [],
        "shared_url": None,
        "raw_jsonld": [],
        "source_file": source_filename
    }

    jsonlds = extract_jsonld(soup)
    out["raw_jsonld"] = jsonlds

    posting = None
    for obj in jsonlds:
        if not isinstance(obj, dict):
            continue
        typ = obj.get("@type") or obj.get("type") or ""
        if isinstance(typ, list):
            typ = " ".join(typ)
        if "SocialMediaPosting" in str(typ) or "VideoObject" in str(typ) or "SocialMediaPosting" == typ:
            posting = obj
            break
        if "articleBody" in obj or "interactionStatistic" in obj:
            posting = obj
            break

    if posting:
        # article body / content
        out["content"] = posting.get("articleBody") or posting.get("headline") or out["content"]
        # title / name
        out["title"] = posting.get("headline") or posting.get("name") or out["title"]
        # description
        out["description"] = posting.get("description") or out["description"]
        # author/creator
        auth = posting.get("author") or posting.get("creator") or posting.get("publisher")
        if isinstance(auth, dict):
            out["author"] = auth.get("name") or auth.get("url")
        elif isinstance(auth, str):
            out["author"] = auth
        elif isinstance(auth, list) and auth:
            a0 = auth[0]
            out["author"] = a0.get("name") if isinstance(a0, dict) else str(a0)
        # dates
        out["date_published"] = posting.get("datePublished") or posting.get("uploadDate") or out["date_published"]
        # images
        # many postings contain thumbnailUrl, image, sharedContent.image, etc.
        for key in ("thumbnailUrl", "image", "thumbnail", "thumbnailUrl", "thumbnailImage"):
            v = posting.get(key)
            if isinstance(v, str) and v not in out["images"]:
                out["images"].append(v)
            elif isinstance(v, dict):
                url = v.get("url")
                if url and url not in out["images"]:
                    out["images"].append(url)
        # sharedContent.url
        shared = posting.get("sharedContent")
        if isinstance(shared, dict):
            out["shared_url"] = shared.get("url") or out["shared_url"]
            # shared image
            si = shared.get("image") or shared.get("thumbnail")
            if isinstance(si, dict):
                url = si.get("url") 
                if url and url not in out["images"]:
                    out["images"].append(url)
        # interactionStatistic
        stats = posting.get("interactionStatistic") or posting.get("interactionStatistics")
        if isinstance(stats, dict):
            stats = [stats]
        if isinstance(stats, list):
            for s in stats:
                itype = s.get("interactionType", "")
                count = s.get("userInteractionCount") or s.get("interactionCount")
                num = parse_short_number(count)
                itype_lower = str(itype).lower()
                if "like" in itype_lower:
                    out["likes"] = num
                elif "comment" in itype_lower:
                    out["comments"] = num

    # Meta tag fallback
    if not out["title"]:
        out["title"] = find_meta(soup, "og:title", prop="property") or (soup.title.string if soup.title else None)
    if not out["description"]:
        out["description"] = find_meta(soup, "og:description", prop="property") or find_meta(soup, "description", prop="name")
    if not out["url"]:
        out["url"] = find_meta(soup, "og:url", prop="property") or (soup.find("link", rel="canonical") and soup.find("link", rel="canonical").get("href"))
    # og:image fallback
    og_img = find_meta(soup, "og:image", prop="property")
    if og_img and og_img not in out["images"]:
        out["images"].append(og_img)
    # collect good image src attributes (filter out tiny icons by length)
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if src and len(src) > 20 and src not in out["images"]:
            out["images"].append(src)

    # textual fallback for likes/comments
    page_text = soup.get_text(" ", strip=True)
    likes_match = re.search(r"([\d,.]+(?:[KMkm]?))\s+likes?\b", page_text)
    comments_match = re.search(r"([\d,.]+(?:[KMkm]?))\s+comments?\b", page_text)
    if likes_match and out["likes"] is None:
        out["likes"] = parse_short_number(likes_match.group(1))
    if comments_match and out["comments"] is None:
        out["comments"] = parse_short_number(comments_match.group(1))

    # Normalize date string if possible (leave as-is otherwise)
    dp = out.get("date_published")
    if dp:
        # Some dates end with Z, change to ISO offset
        try:
            # Try a few common formats
            if dp.endswith("Z"):
                dt = datetime.fromisoformat(dp.replace("Z", "+00:00"))
                out["date_published"] = dt.isoformat()
            else:
                # try parse directly
                dt = datetime.fromisoformat(dp)
                out["date_published"] = dt.isoformat()
        except Exception:
            out["date_published"] = dp

    # ensure numeric types
    for k in ("likes", "comments"):
        if out.get(k) is not None:
            try:
                out[k] = int(out[k])
            except Exception:
                out[k] = parse_short_number(out[k])

    return out
