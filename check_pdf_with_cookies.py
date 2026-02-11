import json
import httpx

PDF_URL = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"

def load_cookies_from_storage(path: str):
    data = json.load(open(path, "r", encoding="utf-8"))
    # Playwright storage_state format
    cookies = {}
    for c in data.get("cookies", []):
        cookies[c["name"]] = c["value"]
    return cookies

cookies = load_cookies_from_storage("justice_storage.json")

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "*/*",
    "Range": "bytes=0-1",
}

with httpx.Client(headers=headers, cookies=cookies, follow_redirects=True, timeout=30.0) as client:
    r = client.get(PDF_URL)
    print("Final URL:", r.url)
    print("Status:", r.status_code)
    print("Content-Type:", r.headers.get("content-type"))
