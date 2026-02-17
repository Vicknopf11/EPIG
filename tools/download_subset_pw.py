import asyncio
import csv
import io
from pathlib import Path
from typing import Dict, Any, Optional

from playwright.async_api import async_playwright
from pypdf import PdfReader

BASE = "https://www.justice.gov/epstein/files/DataSet%201"
OUTDIR = Path("downloads_dataset1")
OUTDIR.mkdir(parents=True, exist_ok=True)

def efta_url(n: int) -> str:
    return f"{BASE}/EFTA{n:08d}.pdf"

def extract_pdf_metadata_from_bytes(pdf_bytes: bytes) -> Dict[str, Any]:
    md: Dict[str, Any] = {}
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        md["pages"] = len(reader.pages)
        info = reader.metadata
        if info:
            md["title"] = getattr(info, "title", None)
            md["author"] = getattr(info, "author", None)
            md["subject"] = getattr(info, "subject", None)
            md["creator"] = getattr(info, "creator", None)
            md["producer"] = getattr(info, "producer", None)
            md["creation_date"] = getattr(info, "creation_date", None)
            md["mod_date"] = getattr(info, "modification_date", None)
    except Exception as e:
        md["meta_error"] = repr(e)
    return md

async def fetch_and_save(request, n: int) -> Dict[str, Any]:
    url = efta_url(n)
    fname = f"EFTA{n:08d}.pdf"
    target = OUTDIR / fname

    if target.exists() and target.stat().st_size > 0:
        # metadata da file già scaricato (opzionale: qui lo rifacciamo su bytes = più costoso)
        return {"id": n, "file": fname, "status": "SKIP_EXISTS", "http": ""}

    try:
        resp = await request.get(url)
        status = resp.status
        ct = (resp.headers.get("content-type") or "").lower()

        if status == 404:
            return {"id": n, "file": fname, "status": "NOT_FOUND", "http": "404"}
        if status in (401, 403):
            body = await resp.text()
            return {"id": n, "file": fname, "status": "UNAUTHORIZED", "http": str(status), "note": body[:120].replace("\n", " ")}
        if status != 200:
            return {"id": n, "file": fname, "status": "HTTP_FAIL", "http": str(status), "ct": ct}

        b = await resp.body()
        if not b.startswith(b"%PDF"):
            # tipicamente HTML di gate/consenso
            return {"id": n, "file": fname, "status": "NOT_PDF", "http": "200", "ct": ct, "note": repr(b[:120])}

        target.write_bytes(b)
        md = extract_pdf_metadata_from_bytes(b)
        return {"id": n, "file": fname, "status": "DOWNLOADED", "http": "200", **md}

    except Exception as e:
        return {"id": n, "file": fname, "status": "ERROR", "http": repr(e)}

async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--storage", default="justice_storage.json", help="Playwright storage_state JSON")
    ap.add_argument("--auth", action="store_true", help="Force interactive DOJ session refresh")
    ap.add_argument("--headless", action="store_true", help="Run browser headless (not recommended for auth)")
    args = ap.parse_args()

    storage_path = Path(args.storage)

    log_name = f"download_log_dataset1_{args.start:08d}_{args.end:08d}.csv"
    log_path = Path(log_name)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)

        # 1) Auth step (se richiesto o storage assente)
        if args.auth or not storage_path.exists():
            ctx = await browser.new_context()
            page = await ctx.new_page()
            await page.goto("https://www.justice.gov/epstein", wait_until="domcontentloaded")

            print("\n➡️ Completa manualmente eventuali consensi/login nel browser.")
            print("➡️ Poi prova ad aprire un PDF di DataSet 1 in una tab (se possibile).")
            input("\nPremi INVIO per salvare la sessione DOJ... ")

            await ctx.storage_state(path=str(storage_path))
            await ctx.close()

        # 2) Download context con storage_state
        ctx = await browser.new_context(storage_state=str(storage_path)) if storage_path.exists() else await browser.new_context()
        request = ctx.request

        sem = asyncio.Semaphore(args.concurrency)
        results = []

        async def bound(n: int):
            async with sem:
                return await fetch_and_save(request, n)

        tasks = [asyncio.create_task(bound(n)) for n in range(args.start, args.end + 1)]

        unauthorized_hits = 0
        for t in asyncio.as_completed(tasks):
            res = await t
            results.append(res)

            if res["status"] in ("DOWNLOADED", "NOT_FOUND", "SKIP_EXISTS"):
                print(res["status"], res["file"])

            if res["status"] == "UNAUTHORIZED":
                unauthorized_hits += 1
                if unauthorized_hits >= 5:
                    print("\n❌ Troppi UNAUTHORIZED: sessione non valida. Rilancia con --auth.")
                    break

        # cancella eventuali task rimasti
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await ctx.close()
        await browser.close()

    # 3) Log CSV
    fieldnames = [
        "id","file","status","http","ct","note",
        "pages","title","author","subject","creator","producer","creation_date","mod_date","meta_error"
    ]
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"\nSaved log: {log_path}")
    print("Downloaded files in:", OUTDIR.resolve())

if __name__ == "__main__":
    asyncio.run(main())
