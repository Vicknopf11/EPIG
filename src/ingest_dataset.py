#!/usr/bin/env python3
import argparse
import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import fitz  # PyMuPDF
import numpy as np
from PIL import Image
import imagehash
import pytesseract
import cv2
from tqdm import tqdm
import yaml


# ---------- Helpers ----------
def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def resolve_path(cfg_dir: Path, p: str) -> Path:
    """Resolve a path from YAML relative to YAML directory unless absolute."""
    pp = Path(p)
    return pp if pp.is_absolute() else (cfg_dir / pp).resolve()


def load_yaml_config(config_path: Path) -> Tuple[Dict[str, Any], Path]:
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError("Invalid YAML: root must be a mapping/dict.")
    return cfg, config_path.parent.resolve()


def cfg_get(cfg: Dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = cfg
    for k in dotted.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def render_first_page(pdf_path: Path, zoom: float = 2.0) -> np.ndarray:
    doc = fitz.open(str(pdf_path))
    page = doc[0]
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3)
    doc.close()
    return img


def compute_features(img_bgr: np.ndarray) -> Tuple[str, float, float, int, int]:
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


def crop_roi(img_bgr: np.ndarray, roi: str) -> np.ndarray:
    h, w = img_bgr.shape[:2]

    if roi == "top":
        return img_bgr[0:int(h * 0.55), 0:w]

    if roi == "slate":
        return img_bgr[0:int(h * 0.80), 0:w]

    if roi == "center":
        y0 = int(h * 0.15)
        y1 = int(h * 0.75)
        x0 = int(w * 0.10)
        x1 = int(w * 0.90)
        return img_bgr[y0:y1, x0:x1]

    # --- marker-focused ROIs (for scattered room-letter sheets) ---
    if roi == "full":
        return img_bgr

    if roi == "top_band":
        return img_bgr[0:int(h * 0.45), 0:w]

    if roi == "bottom_band":
        return img_bgr[int(h * 0.45):h, 0:w]

    if roi == "left_band":
        y0 = int(h * 0.05)
        y1 = int(h * 0.90)
        x0 = 0
        x1 = int(w * 0.55)
        return img_bgr[y0:y1, x0:x1]

    if roi == "right_band":
        y0 = int(h * 0.05)
        y1 = int(h * 0.90)
        x0 = int(w * 0.45)
        x1 = w
        return img_bgr[y0:y1, x0:x1]

    if roi == "center_small":
        y0 = int(h * 0.20)
        y1 = int(h * 0.70)
        x0 = int(w * 0.25)
        x1 = int(w * 0.75)
        return img_bgr[y0:y1, x0:x1]

    return img_bgr



def ocr_text(img_bgr: np.ndarray, roi: str, psm: int, whitelist: Optional[str] = None) -> str:
    crop = crop_roi(img_bgr, roi)
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

    config = f"--psm {psm}"
    if whitelist:
        config += f" -c tessedit_char_whitelist={whitelist}"

    return pytesseract.image_to_string(gray, config=config)


def detect_slate(
    img_bgr: np.ndarray,
    roi: str,
    psm: int,
    keywords: List[str],
    min_keywords: int,
    strong_keywords: List[str],
) -> Tuple[bool, str, List[str]]:
    """
    Robust slate detection:
    - matches both raw OCR text and a compact normalized version
    - tolerates OCR glitches like "CASE ID" -> "CASEID" -> "CASEIN"
    """
    txt = ocr_text(img_bgr, roi=roi, psm=psm, whitelist=None)
    up = txt.upper()

    # compact normalization: keep only A-Z0-9
    compact = re.sub(r"[^A-Z0-9]", "", up)

    keywords_compact = [re.sub(r"[^A-Z0-9]", "", k.upper()) for k in keywords]

    found: List[str] = []
    for k_raw, k_cmp in zip(keywords, keywords_compact):
        if (k_raw.upper() in up) or (k_cmp and k_cmp in compact):
            found.append(k_raw)

    strong_compact = [re.sub(r"[^A-Z0-9]", "", k.upper()) for k in strong_keywords]
    strong = all(
        ((k.upper() in up) or (kc and kc in compact))
        for k, kc in zip(strong_keywords, strong_compact)
    ) if strong_keywords else False

    is_slate = (len(found) >= int(min_keywords)) or strong
    return is_slate, txt, found


def detect_room_marker(
    img_bgr: np.ndarray,
    roi: str,  # kept for backward compatibility; ignored if roi == "multi"
    psm: int,
    whitelist: str,
    room_re: re.Pattern,
) -> Tuple[bool, Optional[str], str]:
    """
    Robust marker detection for scattered room-letter sheets.
    Uses multiple ROIs and selects the best valid candidate.
    """

    def clean_letters(s: str) -> str:
        return re.sub(r"[^A-Z]", "", s.upper()).strip()

    def best_candidate_from_text(txt: str) -> Optional[str]:
        cand = clean_letters(txt)
        if room_re.match(cand):
            return cand
        tokens = re.findall(r"[A-Z]{1,2}", txt.upper())
        tokens = [t for t in tokens if room_re.match(t)]
        return tokens[0] if tokens else None

    # If config passes a single ROI, still allow it.
    if roi != "multi":
        txt = ocr_text(img_bgr, roi=roi, psm=psm, whitelist=whitelist)
        cand = best_candidate_from_text(txt)
        if cand:
            return True, cand, txt
        # fallback: single-char OCR
        txt2 = ocr_text(img_bgr, roi=roi, psm=10, whitelist=whitelist)
        cand2 = best_candidate_from_text(txt2)
        if cand2:
            return True, cand2, txt2
        return False, None, txt

    # Multi-ROI mode
    roi_list = ["left_band", "right_band", "top_band", "bottom_band", "center_small", "full"]

    best = None  # (score, marker, ocr_text, roi_name, psm_used)
    for r in roi_list:
        # First try single-character OCR
        txt = ocr_text(img_bgr, roi=r, psm=10, whitelist=whitelist)
        cand = best_candidate_from_text(txt)
        if cand:
            score = 10 + (2 if len(cand) == 2 else 1)  # prefer double letters slightly
            # small bonus if OCR output is short (less noise)
            score += max(0, 5 - len(clean_letters(txt)))
            if best is None or score > best[0]:
                best = (score, cand, txt, r, 10)

        # Then try psm=8 as fallback
        txt = ocr_text(img_bgr, roi=r, psm=8, whitelist=whitelist)
        cand = best_candidate_from_text(txt)
        if cand:
            score = 8 + (2 if len(cand) == 2 else 1)
            score += max(0, 5 - len(clean_letters(txt)))
            if best is None or score > best[0]:
                best = (score, cand, txt, r, 8)

    if best:
        _, cand, txt, r, p = best
        # annotate OCR text with provenance for debugging
        dbg = f"[roi={r} psm={p}] {txt}"
        return True, cand, dbg

    return False, None, ""



def seed_from_config(
    efta_id: int,
    seed_assignments: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[int], str, float]:
    for entry in seed_assignments:
        loc = entry.get("location")
        for sh in (entry.get("shoots") or []):
            start = int(sh["efta_start"])
            end = int(sh["efta_end"])
            if start <= efta_id <= end:
                shoot_index = sh.get("shoot_index", None)  # YAML null -> None
                conf = float(sh.get("confidence", 0.9))
                return loc, shoot_index, "range_seed", conf
    return None, None, "range_seed", 0.50


def init_db(con: duckdb.DuckDBPyConnection) -> None:
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


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Ingest PDF-photo dataset into DuckDB + extracted JPGs (config-driven).")
    ap.add_argument("--config", required=True, help="Path YAML config, es: configs/dataset1.yaml")

    # Optional overrides (useful on other machines)
    ap.add_argument("--input", default=None, help="Override paths.input_dir from config")
    ap.add_argument("--db", default=None, help="Override paths.db_path from config")
    ap.add_argument("--images_dir", default=None, help="Override paths.images_dir from config")

    # Fast targeting
    ap.add_argument("--limit", type=int, default=None, help="Process only first N PDFs (sorted).")
    ap.add_argument("--start_efta", type=int, default=None, help="Only process files with efta_id >= this.")
    ap.add_argument("--end_efta", type=int, default=None, help="Only process files with efta_id <= this.")
    args = ap.parse_args()

    cfg_path = Path(args.config).resolve()
    cfg, cfg_dir = load_yaml_config(cfg_path)

    # Paths from config
    input_dir_cfg = cfg_get(cfg, "paths.input_dir")
    db_path_cfg = cfg_get(cfg, "paths.db_path")
    images_dir_cfg = cfg_get(cfg, "paths.images_dir")
    if not input_dir_cfg or not db_path_cfg or not images_dir_cfg:
        raise ValueError("Config must contain: paths.input_dir, paths.db_path, paths.images_dir")

    in_dir = resolve_path(cfg_dir, args.input or input_dir_cfg)
    db_path = resolve_path(cfg_dir, args.db or db_path_cfg)
    images_dir = resolve_path(cfg_dir, args.images_dir or images_dir_cfg)
    images_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Naming
    efta_regex = cfg_get(cfg, "naming.efta_regex", r"EFTA(\d{8})")
    efta_re = re.compile(str(efta_regex), re.IGNORECASE)

    def parse_efta_id(path: Path) -> int:
        m = efta_re.search(path.name)
        if not m:
            raise ValueError(f"Filename without EFTA########: {path.name}")
        return int(m.group(1))

    # OCR + render config
    zoom = float(cfg_get(cfg, "render.zoom", 2.0))
    jpg_quality = int(cfg_get(cfg, "render.jpg_quality", 92))

    ocr_enabled = bool(cfg_get(cfg, "ocr.enabled", True))

    slate_roi = str(cfg_get(cfg, "ocr.slate.roi", "top"))
    slate_psm = int(cfg_get(cfg, "ocr.slate.psm", 6))
    slate_keywords = list(cfg_get(cfg, "ocr.slate.keywords", ["DATE", "CASE ID", "PHOTOGRAPHER", "LOCATION", "FBI"]))
    slate_min_keywords = int(cfg_get(cfg, "ocr.slate.min_keywords", 3))
    slate_strong = list(cfg_get(cfg, "ocr.slate.strong_keywords", ["DATE", "CASE ID", "LOCATION"]))

    room_roi = str(cfg_get(cfg, "ocr.room_marker.roi", "center"))
    room_psm = int(cfg_get(cfg, "ocr.room_marker.psm", 8))
    room_whitelist = str(cfg_get(cfg, "ocr.room_marker.whitelist", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    room_regex = str(cfg_get(cfg, "ocr.room_marker.regex", r"^[A-Z]{1,2}$"))
    room_re = re.compile(room_regex)

    seed_assignments = list(cfg_get(cfg, "seed_assignments", []))

    # Collect PDFs
    pdfs = sorted(in_dir.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"Nessun PDF trovato in: {in_dir}")

    # Apply range filter (cheap and effective)
    if args.start_efta is not None or args.end_efta is not None:
        start = args.start_efta if args.start_efta is not None else -1
        end = args.end_efta if args.end_efta is not None else 10**12
        filtered = []
        for p in pdfs:
            m = efta_re.search(p.name)
            if not m:
                continue
            eid = int(m.group(1))
            if start <= eid <= end:
                filtered.append(p)
        pdfs = filtered
        if not pdfs:
            raise SystemExit(f"Nessun PDF nel range richiesto: {args.start_efta}–{args.end_efta}")

    # Apply limit last
    if args.limit is not None:
        pdfs = pdfs[: max(0, int(args.limit))]

    con = duckdb.connect(str(db_path))
    init_db(con)

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
        img = render_first_page(pdf, zoom=zoom)
        ph, mean_luma, blur, w, h = compute_features(img)

        jpg_path = images_dir / f"{file_id}.jpg"
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        Image.fromarray(img_rgb).save(jpg_path, quality=jpg_quality)

        # Slate + marker detection
        if ocr_enabled:
            is_slate, slate_txt, slate_kw = detect_slate(
                img_bgr=img,
                roi=slate_roi,
                psm=slate_psm,
                keywords=slate_keywords,
                min_keywords=slate_min_keywords,
                strong_keywords=slate_strong,
            )
            has_marker, marker, marker_txt = detect_room_marker(
                img_bgr=img,
                roi=room_roi,
                psm=room_psm,
                whitelist=room_whitelist,
                room_re=room_re,
            )
        else:
            is_slate, slate_txt, slate_kw = False, "", []
            has_marker, marker, marker_txt = False, None, ""

        # Seed assignment
        loc, shoot, method, conf = seed_from_config(efta_id, seed_assignments)

        # Upsert: delete + insert
        con.execute("DELETE FROM files WHERE file_id = ?", [file_id])
        con.execute(
            "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)",
            [file_id, str(pdf), efta_id, size, sha, pages],
        )

        con.execute("DELETE FROM images WHERE file_id = ?", [file_id])
        con.execute(
            "INSERT INTO images VALUES (?, ?, ?, ?, ?, ?, ?)",
            [file_id, str(jpg_path), w, h, ph, mean_luma, blur],
        )

        con.execute("DELETE FROM assignments WHERE file_id = ?", [file_id])
        con.execute(
            "INSERT INTO assignments VALUES (?, ?, ?, ?, ?)",
            [file_id, loc, shoot, method, conf],
        )

        con.execute("DELETE FROM slates WHERE file_id = ?", [file_id])
        con.execute(
            "INSERT INTO slates VALUES (?, ?, ?, ?)",
            [file_id, is_slate, ",".join(slate_kw), slate_txt],
        )

        con.execute("DELETE FROM room_markers WHERE file_id = ?", [file_id])
        con.execute(
            "INSERT INTO room_markers VALUES (?, ?, ?, ?)",
            [file_id, bool(has_marker), marker, marker_txt],
        )

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
