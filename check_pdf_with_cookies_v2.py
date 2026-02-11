import json
import httpx

PDF_URL = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"

def load_cookies_from_storage(path: str):
    data = json.load(open(path, "r", encoding="utf-8"))
    return {c["name"]: c["value"] for c in data.get("cookies", [])}

cookies = load_cookies_from_storage("justice_storage.json")

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "*/*",
    "Range": "bytes=0-1",
}

with httpx.Client(headers=headers, cookies=cookies, follow_redirects=False, timeout=30.0) as client:
    r = client.get(PDF_URL)
    print("Status:", r.status_code)
    print("Location:", r.headers.get("location"))
    print("Content-Type:", r.headers.get("content-type"))
