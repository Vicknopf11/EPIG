import argparse
import hashlib
import os
import re
from pathlib import Path

import duckdb
import fitz  # PyMuPDF
import numpy as np
from PIL import Image
import imagehash
import pytesseract
import cv2
from tqdm import tqdm

# ---------- Helpers ----------
EFTA_RE = re.compile(r"EFTA(\d{8})", re.IGNORECASE)
ROOM_RE = re.compile(r"^[A-Z]{1,2}$")

SLATE_KEYWORDS = ["DATE", "CASE ID", "PHOTOGRAPHER", "LOCATION", "FBI"]

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def parse_efta_id(path: Path) -> int:
    m = EFTA_RE.search(path.name)
    if not m:
        raise ValueError(f"Filename without EFTA########: {path.name}")
    return int(m.group(1))

def render_first_page(pdf_path: Path, zoom: float = 2.0) -> np.ndarray:
    doc = fitz.open(str(pdf_path))
    page = doc[0]
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3)
    doc.close()
    return img

def compute_features(img_bgr: np.ndarray):
    # pHash on RGB PIL
    img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(img_rgb)
    ph = str(imagehash.phash(pil))

    # mean luma
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    mean_luma = float(np.mean(gray))

    # blur score: Laplacian variance
    blur = float(cv2.Laplacian(gray, cv2.CV_64F).var())

    h, w = img_bgr.shape[:2]
    return ph, mean_luma, blur, w, h

def ocr_text(img_bgr: np.ndarray, roi: str, psm: int, whitelist: str | None = None) -> str:
    h, w = img_bgr.shape[:2]

    if roi == "top":
        crop = img_bgr[0:int(h * 0.55), 0:w]
    elif roi == "center":
        y0 = int(h * 0.15)
        y1 = int(h * 0.75)
        x0 = int(w * 0.10)
        x1 = int(w * 0.90)
        crop = img_bgr[y0:y1, x0:x1]
    else:
        crop = img_bgr

    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

    config = f"--psm {psm}"
    if whitelist:
        config += f" -c tessedit_char_whitelist={whitelist}"

    txt = pytesseract.image_to_string(gray, config=config)
    return txt

def detect_slate(img_bgr: np.ndarray):
    txt = ocr_text(img_bgr, roi="top", psm=6, whitelist=None)
    up = txt.upper()
    found = [k for k in SLATE_KEYWORDS if k in up]
    strong = all(k in up for k in ["DATE", "CASE ID", "LOCATION"])
    is_slate = (len(found) >= 3) or strong
    return is_slate, txt, found

def detect_room_marker(img_bgr: np.ndarray):
    # OCR mirato: solo lettere, psm "single word"
    txt = ocr_text(img_bgr, roi="center", psm=8, whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    # pulizia aggressiva
    cand = re.sub(r"[^A-Z]", "", txt.upper()).strip()

    # se l’OCR “incolla” troppo, prova a prendere token brevi
    if not ROOM_RE.match(cand):
        # fallback: prova a estrarre tutte le sequenze di 1-2 lettere dalla stringa
        tokens = re.findall(r"[A-Z]{1,2}", txt.upper())
        # elimina token troppo comuni/rumore: qui lasciamo semplice
        tokens = [t for t in tokens if ROOM_RE.match(t)]
        cand = tokens[0] if tokens else ""

    if ROOM_RE.match(cand):
        return True, cand, txt
    return False, None, txt

def seed_location_shoot(efta_id: int):
    # NY shoot1: 1–836 ; NY shoot2: 837–1499 ; LSJ from 1500+
    if 1 <= efta_id <= 836:
        return "NY_9E71st", 1, "range_seed", 0.95
    if 837 <= efta_id <= 1499:
        return "NY_9E71st", 2, "range_seed", 0.95
    if efta_id >= 1500:
        return "LSJ", None, "range_seed", 0.80
    return None, None, "range_seed", 0.50

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Cartella con i PDF del dataset1")
    ap.add_argument("--db", required=True, help="Path file DuckDB, es: epig_dataset1.duckdb")
    ap.add_argument("--images_dir", required=True, help="Cartella dove salvare JPG estratti")
    args = ap.parse_args()

    in_dir = Path(args.input)
    db_path = Path(args.db)
    images_dir = Path(args.images_dir)
    images_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    # Schema minimo
    con.execute("""
    CREATE TABLE IF NOT EXISTS files (
      file_id VARCHAR PRIMARY KEY,
      path VARCHAR,
      efta_id INTEGER,
      bytes BIGINT,
      sha256 VARCHAR,
      pages INTEGER
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS images (
      file_id VARCHAR,
      jpg_path VARCHAR,
      width INTEGER,
      height INTEGER,
      phash VARCHAR,
      mean_luma DOUBLE,
      blur_score DOUBLE
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS assignments (
      file_id VARCHAR,
      location_label VARCHAR,
      shoot_index INTEGER,
      method VARCHAR,
      confidence DOUBLE
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS slates (
      file_id VARCHAR,
      is_slate BOOLEAN,
      keywords_found VARCHAR,
      ocr_text VARCHAR
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS room_markers (
      file_id VARCHAR,
      has_marker BOOLEAN,
      marker_text VARCHAR,
      ocr_text VARCHAR
    );
    """)

    pdfs = sorted(in_dir.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"Nessun PDF trovato in: {in_dir}")

    for pdf in tqdm(pdfs, desc="Ingest PDF"):
        efta_id = parse_efta_id(pdf)
        file_id = f"EFTA{efta_id:08d}"
        size = pdf.stat().st_size
        sha = sha256_file(pdf)

        # Pages
        doc = fitz.open(str(pdf))
        pages = doc.page_count
        doc.close()

        # Render + features
        img = render_first_page(pdf, zoom=2.0)
        ph, mean_luma, blur, w, h = compute_features(img)

        jpg_path = images_dir / f"{file_id}.jpg"
        # salva JPG (BGR->RGB)
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        Image.fromarray(img_rgb).save(jpg_path, quality=92)

        # Slate + marker detection
        is_slate, slate_txt, slate_kw = detect_slate(img)
        has_marker, marker, marker_txt = detect_room_marker(img)

        # Seed assignment
        loc, shoot, method, conf = seed_location_shoot(efta_id)

        # Write DB (upsert semplice: delete+insert)
        con.execute("DELETE FROM files WHERE file_id = ?", [file_id])
        con.execute("INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)",
                    [file_id, str(pdf), efta_id, size, sha, pages])

        con.execute("DELETE FROM images WHERE file_id = ?", [file_id])
        con.execute("INSERT INTO images VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [file_id, str(jpg_path), w, h, ph, mean_luma, blur])

        con.execute("DELETE FROM assignments WHERE file_id = ?", [file_id])
        con.execute("INSERT INTO assignments VALUES (?, ?, ?, ?, ?)",
                    [file_id, loc, shoot, method, conf])

        con.execute("DELETE FROM slates WHERE file_id = ?", [file_id])
        con.execute("INSERT INTO slates VALUES (?, ?, ?, ?)",
                    [file_id, is_slate, ",".join(slate_kw), slate_txt])

        con.execute("DELETE FROM room_markers WHERE file_id = ?", [file_id])
        con.execute("INSERT INTO room_markers VALUES (?, ?, ?, ?)",
                    [file_id, bool(has_marker), marker, marker_txt])

    # Quick report
    print("\n--- SUMMARY ---")
    print(con.execute("""
      SELECT
        location_label,
        shoot_index,
        COUNT(*) AS n_files
      FROM assignments
      GROUP BY 1,2
      ORDER BY 1,2
    """).fetchdf())

    print("\nTop room markers:")
    print(con.execute("""
      SELECT marker_text, COUNT(*) AS n
      FROM room_markers
      WHERE has_marker
      GROUP BY 1
      ORDER BY n DESC
      LIMIT 20
    """).fetchdf())

    print("\nSlate detected:")
    print(con.execute("""
      SELECT COUNT(*) AS n_slates
      FROM slates
      WHERE is_slate
    """).fetchdf())

    con.close()
    print(f"\nDB scritto in: {db_path}")
    print(f"JPG estratti in: {images_dir}")

if __name__ == "__main__":
    main()
