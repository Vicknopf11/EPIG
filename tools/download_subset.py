import os
import json
import csv
import asyncio
from pathlib import Path
from typing import Dict, Optional

import httpx

BASE = "https://www.justice.gov/epstein/files/DataSet%201"
OUTDIR = Path("downloads_dataset1")
OUTDIR.mkdir(exist_ok=True)

def efta_url(n: int) -> str:
    return f"{BASE}/EFTA{n:08d}.pdf"

def load_cookies_from_storage(path: str) -> Dict[str, str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Cookie file not found: {p.resolve()}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return {c["name"]: c["value"] for c in data.get("cookies", [])}


def is_pdf(r: httpx.Response) -> bool:
    ct = (r.headers.get("content-type") or "").lower()
    return "application/pdf" in ct or "pdf" in ct

async def fetch_one(client: httpx.AsyncClient, n: int) -> Dict[str, str]:
    url = efta_url(n)
    fname = f"EFTA{n:08d}.pdf"
    target = OUTDIR / fname

    if target.exists() and target.stat().st_size > 0:
        return {"id": str(n), "file": fname, "status": "SKIP_EXISTS", "http": ""}

    try:
        # prima prova: range leggero per capire se esiste davvero
        r = await client.get(url, headers={"Range": "bytes=0-1"}, follow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308):
            # con cookie giusti non dovrebbe succedere; logghiamo
            return {"id": str(n), "file": fname, "status": "REDIRECT", "http": str(r.status_code)}

        if r.status_code == 404:
            return {"id": str(n), "file": fname, "status": "NOT_FOUND", "http": "404"}

        if r.status_code not in (200, 206) or not is_pdf(r):
            return {"id": str(n), "file": fname, "status": "NOT_PDF", "http": str(r.status_code)}

        # scarica completo
        r2 = await client.get(url, follow_redirects=True)
        if r2.status_code == 200 and is_pdf(r2):
            target.write_bytes(r2.content)
            return {"id": str(n), "file": fname, "status": "DOWNLOADED", "http": "200"}
        else:
            return {"id": str(n), "file": fname, "status": "DOWNLOAD_FAIL", "http": str(r2.status_code)}

    except Exception as e:
        return {"id": str(n), "file": fname, "status": "ERROR", "http": repr(e)}

async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True, help="Start EFTA id (e.g., 1)")
    ap.add_argument("--end", type=int, required=True, help="End EFTA id inclusive (e.g., 500)")
    ap.add_argument("--concurrency", type=int, default=3, help="Parallel downloads (keep low)")
    ap.add_argument("--delay", type=float, default=0.5, help="Delay between task scheduling (seconds)")
    ap.add_argument("--cookies", default="justice_storage.json", help="Playwright storage_state JSON")
    args = ap.parse_args()

    cookies = load_cookies_from_storage(args.cookies)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    sem = asyncio.Semaphore(args.concurrency)
    results = []

    async with httpx.AsyncClient(headers=headers, cookies=cookies, timeout=60.0) as client:

        async def bound_fetch(n: int):
            async with sem:
                return await fetch_one(client, n)

        tasks = []
        for n in range(args.start, args.end + 1):
            tasks.append(asyncio.create_task(bound_fetch(n)))
            await asyncio.sleep(args.delay)

        for t in asyncio.as_completed(tasks):
            res = await t
            results.append(res)
            if res["status"] in ("DOWNLOADED", "NOT_FOUND"):
                print(res["status"], res["file"])

    # log CSV
    log_name = f"download_log_dataset1_{args.start:08d}_{args.end:08d}.csv"
    with open(log_name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "file", "status", "http"])
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved log: {log_name}")
    print("Downloaded files in:", OUTDIR.resolve())

if __name__ == "__main__":
    asyncio.run(main())
