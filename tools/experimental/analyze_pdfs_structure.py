import fitz  # PyMuPDF
from pathlib import Path
import csv

PDF_DIR = Path("downloads_dataset1")
OUT_CSV = Path("analysis_200.csv")

def analyze_pdf(path: Path) -> dict:
    doc = fitz.open(path)
    pages = len(doc)

    total_images = 0
    total_text_chars = 0

    for page in doc:
        # immagini embedded nella pagina
        total_images += len(page.get_images(full=True))

        # testo estratto (se è scansione pura, spesso sarà ~0)
        txt = page.get_text("text") or ""
        total_text_chars += len(txt.strip())

    doc.close()

    return {
        "file": path.name,
        "pages": pages,
        "images": total_images,
        "text_chars": total_text_chars,
        "size_kb": round(path.stat().st_size / 1024, 1),
    }

def main():
    if not PDF_DIR.exists():
        raise SystemExit(f"Directory non trovata: {PDF_DIR.resolve()}")

    pdfs = sorted(PDF_DIR.glob("EFTA*.pdf"))
    if not pdfs:
        raise SystemExit(f"Nessun PDF trovato in: {PDF_DIR.resolve()}")

    rows = [analyze_pdf(p) for p in pdfs]

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    # mini-sommario
    only_images = sum(1 for r in rows if r["images"] > 0 and r["text_chars"] == 0)
    has_text = sum(1 for r in rows if r["text_chars"] > 0)
    no_images = sum(1 for r in rows if r["images"] == 0)

    print("Saved:", OUT_CSV.resolve())
    print(f"Totale file analizzati: {len(rows)}")
    print(f"PDF con immagini e zero testo: {only_images}")
    print(f"PDF con testo (text_chars>0): {has_text}")
    print(f"PDF senza immagini (images=0): {no_images}")

if __name__ == "__main__":
    main()
