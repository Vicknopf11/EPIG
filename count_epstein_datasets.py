import asyncio
import csv
import re
import time
from dataclasses import dataclass, field
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

BASE = "https://www.justice.gov"
DATASET_URL_TMPL = BASE + "/epstein/doj-disclosures/data-set-{n}-files"

# robots.txt indica Crawl-delay: 10 -> default 10s tra richieste
DEFAULT_DELAY_S = 10.0

FILE_EXT_RE = re.compile(r"\.([a-zA-Z0-9]{1,8})(?:\?|$)")

@dataclass
class PageResult:
    file_links: List[str] = field(default_factory=list)
    status_code: int = 0
    url: str = ""

def extract_file_links(html: str) -> List[str]:
    """
    Estrae href che sembrano puntare a file (pdf/zip/txt/csv/jpg/png/mp3/mp4 ecc.)
    Evita link interni non-file.
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        # normalizza relative -> absolute
        if href.startswith("/"):
            full = BASE + href
        elif href.startswith("http://") or href.startswith("https://"):
            full = href
        else:
            # ignore altre forme
            continue

        # Heuristica: consideriamo "file" se contiene un'estensione e non è una pagina HTML normale
        # (molti file sono /files/.../*.pdf, *.zip, ecc.)
        m = FILE_EXT_RE.search(full.lower())
        if not m:
            continue

        ext = m.group(1)
        # whitelist soft (lascia estensioni comuni, ma non blocca altre)
        if ext in {"pdf", "zip", "txt", "csv", "json", "xml", "jpg", "jpeg", "png", "gif", "tif", "tiff", "mp3", "mp4", "wav"}:
            links.append(full)
        else:
            # tieni comunque estensioni sconosciute: potrebbero esserci formati strani
            links.append(full)

    # dedup preservando ordine
    seen = set()
    deduped = []
    for u in links:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped

class RateLimiter:
    """
    Rate limiter globale: garantisce almeno `delay_s` tra richieste
    anche con concurrency.
    """
    def __init__(self, delay_s: float):
        self.delay_s = delay_s
        self._lock = asyncio.Lock()
        self._last_ts = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_ts
            if elapsed < self.delay_s:
                await asyncio.sleep(self.delay_s - elapsed)
            self._last_ts = time.monotonic()

async def fetch_page(client: httpx.AsyncClient, rl: RateLimiter, url: str, retries: int = 4) -> PageResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; dataset-counter/1.0; +https://www.justice.gov/)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    backoff = 2.0
    for attempt in range(retries + 1):
        await rl.wait()
        try:
            r = await client.get(url, headers=headers, follow_redirects=True, timeout=30.0)
            status = r.status_code

            if status == 200:
                return PageResult(
                    file_links=extract_file_links(r.text),
                    status_code=status,
                    url=str(r.url),
                )

            # 403/429/5xx: retry con backoff
            if status in {403, 429} or 500 <= status <= 599:
                if attempt < retries:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

            return PageResult(file_links=[], status_code=status, url=str(r.url))

        except (httpx.TimeoutException, httpx.TransportError):
            if attempt < retries:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            return PageResult(file_links=[], status_code=0, url=url)

async def count_dataset(n: int, rl: RateLimiter, max_pages: int = 200000) -> Tuple[int, Counter, Dict[int, int]]:
    """
    Scorre data-set-n-files?page=k finché:
    - la pagina non contiene nuovi file link
    - o ritorna 404
    """
    url0 = DATASET_URL_TMPL.format(n=n)
    ext_counter = Counter()
    total_links = 0
    status_by_page: Dict[int, int] = {}

    async with httpx.AsyncClient() as client:
        seen_files = set()
        empty_streak = 0

        for page in range(max_pages):
            url = url0 if page == 0 else f"{url0}?page={page}"
            res = await fetch_page(client, rl, url)

            status_by_page[page] = res.status_code

            # stop hard su 404 (pagina non esiste)
            if res.status_code == 404:
                break

            # se 403 persistente: fermati (meglio evitare martellamento)
            if res.status_code == 403 and page == 0:
                # dataset accessibile ma bloccato; esci
                break

            # conta solo nuovi file (dedup globale dataset)
            new_links = 0
            for link in res.file_links:
                if link in seen_files:
                    continue
                seen_files.add(link)
                new_links += 1

                m = FILE_EXT_RE.search(link.lower())
                ext = m.group(1) if m else "unknown"
                ext_counter[ext] += 1

            total_links = len(seen_files)

            # Heuristica di fine paginazione:
            # se 2 pagine consecutive non aggiungono nuovi link, stop
            if new_links == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0

        return total_links, ext_counter, status_by_page

def write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

async def main():
    import argparse

    ap = argparse.ArgumentParser(description="Count DOJ Epstein files per dataset by scraping dataset listing pages.")
    ap.add_argument("--datasets", default="1-12", help='Range "1-12" or comma list "1,2,3"')
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY_S, help="Seconds between HTTP requests (default 10).")
    ap.add_argument("--out", default="dataset_counts.csv", help="Output CSV path.")
    ap.add_argument("--ext-out", default="dataset_ext_counts.csv", help="Output CSV path for extension breakdown.")
    args = ap.parse_args()

    # parse datasets arg
    ds: List[int] = []
    if "-" in args.datasets:
        a, b = args.datasets.split("-", 1)
        ds = list(range(int(a), int(b) + 1))
    else:
        ds = [int(x.strip()) for x in args.datasets.split(",") if x.strip()]

    rl = RateLimiter(args.delay)

    summary_rows = []
    ext_rows = []

    for n in ds:
        total, ext_counter, statuses = await count_dataset(n, rl)

        # stato: usa la prima pagina come indicatore
        first_status = statuses.get(0, 0)

        summary_rows.append({
            "dataset": str(n),
            "total_files_links": str(total),
            "first_page_status": str(first_status),
            "pages_checked": str(len(statuses)),
        })

        for ext, cnt in ext_counter.most_common():
            ext_rows.append({
                "dataset": str(n),
                "ext": ext,
                "count": str(cnt),
            })

        print(f"Data Set {n}: {total} file-link (status page0={first_status}, pages={len(statuses)})")

    write_csv(args.out, summary_rows)
    write_csv(args.ext_out, ext_rows)

    print(f"\nWrote: {args.out}")
    print(f"Wrote: {args.ext_out}")

if __name__ == "__main__":
    asyncio.run(main())
