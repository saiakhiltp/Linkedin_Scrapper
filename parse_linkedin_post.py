# parse_linkedin_post.py
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

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

def _extract_jsonld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
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

    jsonlds = _extract_jsonld(soup)
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
        out["content"] = posting.get("articleBody") or posting.get("headline") or out["content"]
        out["title"] = posting.get("headline") or posting.get("name") or out["title"]
        out["description"] = posting.get("description") or out["description"]

        auth = posting.get("author") or posting.get("creator") or posting.get("publisher")
        if isinstance(auth, dict):
            out["author"] = auth.get("name") or auth.get("url")
        elif isinstance(auth, str):
            out["author"] = auth
        elif isinstance(auth, list) and auth:
            a0 = auth[0]
            if isinstance(a0, dict):
                out["author"] = a0.get("name")

        out["date_published"] = posting.get("datePublished") or posting.get("uploadDate") or out["date_published"]

        for key in ("thumbnailUrl", "image", "thumbnail", "thumbnailUrl", "thumbnailImage", "thumbnailUrl"):
            v = posting.get(key)
            if isinstance(v, str) and v not in out["images"]:
                out["images"].append(v)
            elif isinstance(v, dict):
                url = v.get("url")
                if url and url not in out["images"]:
                    out["images"].append(url)

        shared = posting.get("sharedContent")
        if isinstance(shared, dict):
            out["shared_url"] = shared.get("url") or out["shared_url"]
            si = shared.get("image") or shared.get("thumbnail")
            if isinstance(si, dict):
                url = si.get("url")
                if url and url not in out["images"]:
                    out["images"].append(url)

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

    # meta fallback
    if not out["title"]:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            out["title"] = og["content"]
        elif soup.title:
            out["title"] = soup.title.string

    if not out["description"]:
        ogd = soup.find("meta", property="og:description")
        if ogd and ogd.get("content"):
            out["description"] = ogd["content"]

    if not out["url"]:
        ogu = soup.find("meta", property="og:url")
        if ogu and ogu.get("content"):
            out["url"] = ogu["content"]
        else:
            canonical = soup.find("link", rel="canonical")
            if canonical and canonical.get("href"):
                out["url"] = canonical["href"]

    og_img = None
    og_img_tag = soup.find("meta", property="og:image")
    if og_img_tag and og_img_tag.get("content"):
        og_img = og_img_tag["content"]
        if og_img not in out["images"]:
            out["images"].append(og_img)

    for img in soup.find_all("img", src=True):
        src = img["src"]
        if src and len(src) > 20 and src not in out["images"]:
            out["images"].append(src)

    page_text = soup.get_text(" ", strip=True)
    likes_match = re.search(r"([\d,.]+(?:[KMkm]?))\s+likes?\b", page_text)
    comments_match = re.search(r"([\d,.]+(?:[KMkm]?))\s+comments?\b", page_text)
    if likes_match and out["likes"] is None:
        out["likes"] = parse_short_number(likes_match.group(1))
    if comments_match and out["comments"] is None:
        out["comments"] = parse_short_number(comments_match.group(1))

    # normalize date
    dp = out.get("date_published")
    if dp:
        try:
            if dp.endswith("Z"):
                dt = datetime.fromisoformat(dp.replace("Z", "+00:00"))
                out["date_published"] = dt.isoformat()
            else:
                dt = datetime.fromisoformat(dp)
                out["date_published"] = dt.isoformat()
        except Exception:
            out["date_published"] = dp

    for k in ("likes", "comments"):
        if out.get(k) is not None:
            try:
                out[k] = int(out[k])
            except Exception:
                out[k] = parse_short_number(out[k])

    return out

# quick test block
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python parse_linkedin_post.py <html_file>")
        sys.exit(1)
    html_path = sys.argv[1]
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    parsed = parse_linkedin_html(html, source_filename=html_path)
    print(json.dumps(parsed, indent=2, ensure_ascii=False))
