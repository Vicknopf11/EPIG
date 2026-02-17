import asyncio
import csv
import os
from pathlib import Path
from typing import Dict, Any, Optional

from playwright.async_api import async_playwright
from pypdf import PdfReader

BASE = "https://www.justice.gov/epstein/files/DataSet%201"

def efta_url(n: int) -> str:
    return f"{BASE}/EFTA{n:08d}.pdf"

def default_drive_dir() -> Path:
    """
    Prova alcuni path tipici di Google Drive su macOS.
    Tu ne scegli uno funzionante via --outdir se serve.
    """
    home = Path.home()
    candidates = [
        home / "Google Drive" / "My Drive" / "EPICOSO" / "dataset1",
        home / "Google Drive" / "Il mio Drive" / "EPICOSO" / "dataset1",
        home / "Library" / "CloudStorage" / "GoogleDrive-"  # base, spesso con suffisso account
    ]
    for c in candidates:
        if c.exists():
            # se è la base CloudStorage, non è una cartella finale: l'utente deve specificare outdir
            if c.name.startswith("GoogleDrive-"):
                return c
            return c
    # fallback: cartella locale
    return Path("downloads_dataset1_drive")

def extract_pdf_metadata(pdf_path: Path) -> Dict[str, Any]:
    """
    Metadata base: info dictionary + numero pagine.
    Nota: molti PDF avranno metadata vuoti o sporchi.
    """
    md: Dict[str, Any] = {}
    try:
        reader = PdfReader(str(pdf_path))
        md["pages"] = len(reader.pages)

        info = reader.metadata  # può essere None
        if info:
            # Normalizza chiavi comuni
            md["title"] = getattr(info, "title", None)
            md["author"] = getattr(info, "author", None)
            md["subject"] = getattr(info, "subject", None)
            md["creator"] = getattr(info, "creator", None)
            md["producer"] = getattr(info, "producer", None)
            md["creation_date"] = getattr(info, "creation_date", None)
            md["mod_date"] = getattr(info, "modification_date", None)
        return md
    except Exception as e:
        return {"meta_error": repr(e)}

async def download_one(request, outdir: Path, n: int) -> Dict[str, Any]:
    url = efta_url(n)
    fname = f"EFTA{n:08d}.pdf"
    target = outdir / fname

    if target.exists() and target.stat().st_size > 0:
        md = extract_pdf_metadata(target)
        return {"id": n, "file": fname, "status": "SKIP_EXISTS", "http": "", **md}

    try:
        resp = await request.get(url)
        status = resp.status
        ct = (resp.headers.get("content-type") or "").lower()

        if status == 404:
            return {"id": n, "file": fname, "status": "NOT_FOUND", "http": "404"}
        if status in (401, 403):
            # qui è il punto: se non sei autenticato, ti fermo subito.
            body = await resp.text()
            snippet = body[:120].replace("\n", " ")
            return {"id": n, "file": fname, "status": "UNAUTHORIZED", "http": str(status), "note": snippet}
        if status != 200:
            return {"id": n, "file": fname, "status": "HTTP_FAIL", "http": str(status), "ct": ct}

        # Controllo firma PDF
        content = await resp.body()
        if not content.startswith(b"%PDF"):
            # spesso è HTML di gate/consenso
            snippet = content[:120]
            return {"id": n, "file": fname, "status": "NOT_PDF", "http": "200", "ct": ct, "note": repr(snippet)}

        target.write_bytes(content)
        md = extract_pdf_metadata(target)
        return {"id": n, "file": fname, "status": "DOWNLOADED", "http": "200", **md}

    except Exception as e:
        return {"id": n, "file": fname, "status": "ERROR", "http": repr(e)}

async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--outdir", type=str, default=None, help="Google Drive synced folder (recommended)")
    ap.add_argument("--storage", type=str, default="justice_storage.json", help="playwright storage_state path")
    ap.add_argument("--headless", action="store_true", help="Run browser headless")
    ap.add_argument("--auth", action="store_true",
                    help="Force interactive auth step before downloading (updates storage_state)")
    args = ap.parse_args()

    outdir = Path(args.outdir) if args.outdir else default_drive_dir()
    # Se outdir è una base CloudStorage GoogleDrive-..., obbligo l'utente a scegliere
    if outdir.name.startswith("GoogleDrive-") and outdir.is_dir() and outdir.parent.name == "CloudStorage":
        raise SystemExit(
            f"Outdir sembra base CloudStorage ({outdir}). Specifica una cartella reale con --outdir "
            f"(es: '{outdir}/My Drive/EPICOSO/dataset1')."
        )

    outdir.mkdir(parents=True, exist_ok=True)

    log_name = f"download_log_dataset1_{args.start:08d}_{args.end:08d}.csv"
    log_path = outdir / log_name

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        context = None

        storage_path = Path(args.storage)
        if args.auth or not storage_path.exists():
            # Auth interattiva: apri una pagina "gateway", poi l'utente naviga
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://www.justice.gov/epstein", wait_until="domcontentloaded")

            print("\n➡️  Fai manualmente eventuali consensi/login nel browser.")
            print("➡️  IMPORTANTISSIMO: prova ad aprire un PDF di DataSet 1 in una tab (se possibile).")
            input("\nPremi INVIO quando sei pronto a salvare sessione/cookie... ")

            await context.storage_state(path=str(storage_path))
            await context.close()

        # Contesto per download: usa storage_state (se esiste)
        if storage_path.exists():
            context = await browser.new_context(storage_state=str(storage_path))
        else:
            # senza storage: quasi certamente fallirà con 401/403, ma non mentiamo
            context = await browser.new_context()

        request = context.request

        sem = asyncio.Semaphore(args.concurrency)
        results = []

        async def bound(n: int):
            async with sem:
                return await download_one(request, outdir, n)

        tasks = [asyncio.create_task(bound(n)) for n in range(args.start, args.end + 1)]

        unauthorized_hits = 0
        for t in asyncio.as_completed(tasks):
            res = await t
            results.append(res)

            if res["status"] in ("DOWNLOADED", "NOT_FOUND", "SKIP_EXISTS"):
                print(res["status"], res["file"])

            if res["status"] == "UNAUTHORIZED":
                unauthorized_hits += 1
                # Se ti sta respingendo in massa, fermati presto: non sprecare 200 richieste
                if unauthorized_hits >= 5:
                    print("\n❌ Troppi UNAUTHORIZED: sessione non valida. Rilancia con --auth (interattivo).")
                    break

        # Cancella task rimasti se abbiamo interrotto presto
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await context.close()
        await browser.close()

    # Scrivi log CSV con colonne stabili
    fieldnames = [
        "id", "file", "status", "http",
        "pages", "title", "author", "subject", "creator", "producer", "creation_date", "mod_date",
        "ct", "note", "meta_error"
    ]
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"\nSaved log: {log_path}")
    print("Files in:", outdir.resolve())

if __name__ == "__main__":
    asyncio.run(main())
