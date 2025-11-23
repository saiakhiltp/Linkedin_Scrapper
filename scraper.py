# scrapper.py
import os
import requests
from typing import Optional

# You can keep your existing API key here, but it's safer to set environment variable SCRAPINGBEE_KEY.
# If you already have the key hard-coded, you can leave it here. Otherwise set env var SCRAPINGBEE_KEY.
API_KEY = os.getenv("SCRAPINGBEE_KEY") or "PZIZ4DXE3FEIVXEHECZJ9JN58KY4RSY6E4U9WORNZ8T0RKTDHRYC79P4QZJ40ZKFWVDTBIQ1R0Q0FB80"
ENDPOINT = "https://app.scrapingbee.com/api/v1/"

def fetch_html(url: str, render_js: bool = True, save_path: Optional[str] = None, timeout: int = 60) -> str:
    """
    Fetch page HTML from ScrapingBee.
    - render_js: set True to render JS (recommended for LinkedIn pages)
    - save_path: optional path to save the HTML
    Returns HTML text (string) or raises on error.
    """
    if not API_KEY:
        raise RuntimeError("No ScrapingBee API key found. Set SCRAPINGBEE_KEY env var or add API_KEY in scrapper.py")

    params = {
        "api_key": API_KEY,
        "url": url,
        "render_js": "true" if render_js else "false",
        # you can add other params like premium_proxy, block_ads, etc.
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(ENDPOINT, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    html = r.text

    if save_path:
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)

    return html

# If you prefer to run as script for quick test:
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python scrapper.py <url> [save_path]")
        sys.exit(1)
    url = sys.argv[1]
    save = sys.argv[2] if len(sys.argv) >= 3 else None
    print("Fetching:", url)
    html = fetch_html(url, save_path=save)
    print("Fetched length:", len(html))
