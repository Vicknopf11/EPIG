from pathlib import Path
import datetime as dt
import pandas as pd
import fitz            # PyMuPDF
import pikepdf         # PDF metadata / XMP
import hashlib
import argparse

def ts_from_stat(epoch: float) -> str:
    try:
        return dt.datetime.fromtimestamp(epoch).isoformat(timespec="seconds")
    except Exception:
        return ""

def safe_str(x):
    if x is None:
        return ""
    return str(x)

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def extract_docinfo(pdf_path: Path):
    info = {}
    try:
        with pikepdf.open(pdf_path) as pdf:
            docinfo = pdf.docinfo
            for k, v in docinfo.items():
                info[safe_str(k)] = safe_str(v)

            # XMP (se presente)
            try:
                xmp = pdf.open_metadata()
                for key in [
                    "dc:title", "dc:creator",
                    "xmp:CreateDate", "xmp:ModifyDate",
                    "pdf:Producer",
                    "xmpMM:DocumentID", "xmpMM:InstanceID"
                ]:
                    if key in xmp:
                        info[f"XMP_{key}"] = safe_str(xmp[key])
            except Exception:
                pass
    except Exception as e:
        info["ERROR_docinfo"] = repr(e)
    return info

def extract_structure(pdf_path: Path):
    s = {}
    try:
        doc = fitz.open(pdf_path)
        s["pages"] = len(doc)

        # dimensioni prima pagina
        p0 = doc[0]
        rect = p0.rect
        s["page0_width_pt"] = round(rect.width, 2)
        s["page0_height_pt"] = round(rect.height, 2)

        text_chars = 0
        img_count = 0

        for page in doc:
            text = (page.get_text("text") or "").strip()
            text_chars += len(text)
            img_count += len(page.get_images(full=True))

        s["text_chars_total"] = text_chars
        s["images_total"] = img_count

        # feature utile: PDF tipicamente "solo immagine"
        s["is_image_only_pdf"] = int(text_chars == 0 and img_count > 0)

        doc.close()
    except Exception as e:
        s["ERROR_structure"] = repr(e)
    return s

def build_row(p: Path, compute_hash: bool):
    st = p.stat()
    row = {
        "file": p.name,
        "size_bytes": st.st_size,
        "mtime": ts_from_stat(st.st_mtime),
        "stat_ctime": ts_from_stat(st.st_ctime),  # su macOS NON è createtime affidabile
    }

    if compute_hash:
        try:
            row["sha256"] = sha256_file(p)
        except Exception as e:
            row["sha256_error"] = repr(e)

    info = extract_docinfo(p)

    row["PDF_Title"] = info.get("/Title", "")
    row["PDF_Author"] = info.get("/Author", "")
    row["PDF_Creator"] = info.get("/Creator", "")
    row["PDF_Producer"] = info.get("/Producer", "")
    row["PDF_CreationDate"] = info.get("/CreationDate", "")
    row["PDF_ModDate"] = info.get("/ModDate", "")

    row["XMP_CreateDate"] = info.get("XMP_xmp:CreateDate", "")
    row["XMP_ModifyDate"] = info.get("XMP_xmp:ModifyDate", "")
    row["XMP_DocumentID"] = info.get("XMP_xmpMM:DocumentID", "")
    row["XMP_InstanceID"] = info.get("XMP_xmpMM:InstanceID", "")

    row.update(extract_structure(p))
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", default="downloads_dataset1", help="Directory con i PDF EFTA*.pdf")
    ap.add_argument("--out", default=None, help="Output CSV (default: pdf_metadata_<dir>.csv)")
    ap.add_argument("--start", type=int, default=None, help="Start ID (es: 1)")
    ap.add_argument("--end", type=int, default=None, help="End ID inclusive (es: 3158)")
    ap.add_argument("--limit", type=int, default=None, help="Processa solo i primi N file (debug)")
    ap.add_argument("--hash", action="store_true", help="Calcola SHA256 (più lento)")
    args = ap.parse_args()

    pdf_dir = Path(args.pdf_dir)
    if not pdf_dir.exists():
        raise SystemExit(f"Directory non trovata: {pdf_dir.resolve()}")

    pdfs = sorted(pdf_dir.glob("EFTA*.pdf"))
    if not pdfs:
        raise SystemExit(f"Nessun PDF trovato in {pdf_dir.resolve()}")

    # filtro per range ID
    def id_from_name(name: str) -> int:
        # EFTA00000123.pdf -> 123
        return int(name.replace("EFTA", "").replace(".pdf", ""))

    if args.start is not None:
        pdfs = [p for p in pdfs if id_from_name(p.name) >= args.start]
    if args.end is not None:
        pdfs = [p for p in pdfs if id_from_name(p.name) <= args.end]

    if args.limit is not None:
        pdfs = pdfs[:args.limit]

    if args.out:
        out = Path(args.out)
    else:
        out = Path(f"pdf_metadata_{pdf_dir.name}.csv")

    rows = []
    for i, p in enumerate(pdfs, 1):
        rows.append(build_row(p, compute_hash=args.hash))
        if i % 100 == 0:
            print(f"Processed {i}/{len(pdfs)}")

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    print("Saved:", out.resolve())

    # mini-sommario utile
    print("\n=== SUMMARY ===")
    print("Files:", len(df))
    if "PDF_Producer" in df.columns:
        print("Unique PDF_Producer:", df["PDF_Producer"].nunique(dropna=True))
    if "PDF_CreationDate" in df.columns:
        print("PDF_CreationDate non-empty:", (df["PDF_CreationDate"].astype(str).str.len() > 0).sum())
    if "XMP_CreateDate" in df.columns:
        print("XMP_CreateDate non-empty:", (df["XMP_CreateDate"].astype(str).str.len() > 0).sum())
    if "images_total" in df.columns:
        print("Images_total > 0:", (df["images_total"].fillna(0) > 0).sum())
    if "is_image_only_pdf" in df.columns:
        print("Image-only PDFs:", int(df["is_image_only_pdf"].fillna(0).sum()))

if __name__ == "__main__":
    main()
