import re
from pathlib import Path
from collections import Counter

import pandas as pd
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
import cv2
import numpy as np

DATA_DIR = Path("downloads_dataset1")

LETTER_RE = re.compile(r"^[A-Z]{1,3}$")  # A..Z, AA..ZZ, etc (fino a 3)

def pdf_firstpage_to_np(pdf_path: Path, dpi: int = 200) -> np.ndarray:
    img = convert_from_path(str(pdf_path), first_page=1, last_page=1, dpi=dpi)[0]
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def preprocess_for_ocr(bgr: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    # aumenta contrasto in modo semplice
    gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
    # threshold adattivo: utile perché il cartello è bianco ma lo sfondo varia
    th = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                               cv2.THRESH_BINARY, 51, 7)
    return th

def extract_marker_letter(pdf_path: Path) -> str | None:
    bgr = pdf_firstpage_to_np(pdf_path, dpi=200)
    th = preprocess_for_ocr(bgr)

    # OCR "a parole" con box e confidenza
    # whitelist solo lettere (riduce rumore)
    cfg = r'--oem 1 --psm 11 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    data = pytesseract.image_to_data(th, config=cfg, output_type=pytesseract.Output.DICT)

    best = None
    best_area = 0

    for text, conf, x, y, w, h in zip(
        data["text"], data["conf"], data["left"], data["top"], data["width"], data["height"]
    ):
        if not text:
            continue

        t = re.sub(r"[^A-Z]", "", text.upper())

        # Tesseract a volte spezza "AA" in "A" + "A": gestiremo anche con fallback dopo
        if not LETTER_RE.match(t):
            continue

        try:
            c = float(conf)
        except Exception:
            continue

        # scarta conf basse: marker grande tende ad avere conf decente
        if c < 40:
            continue

        area = w * h
        # il marker è grande: scegliamo la box più grande
        if area > best_area:
            best_area = area
            best = t

    # fallback: se non ha trovato box "pulite", prova OCR a stringa e prendi token breve
    if best is None:
        cfg2 = r'--oem 1 --psm 6 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        txt = pytesseract.image_to_string(th, config=cfg2).upper()
        tokens = re.findall(r"[A-Z]{1,3}", txt)
        # prendiamo il token più frequente (spesso la lettera appare più volte nel cartello)
        if tokens:
            cand = Counter(tokens).most_common(1)[0][0]
            if LETTER_RE.match(cand):
                return cand
        return None

    return best

def main():
    pdf_files = sorted(DATA_DIR.glob("*.pdf"))

    current = None
    rows = []

    for i, pdf in enumerate(pdf_files, 1):
        letter = extract_marker_letter(pdf)

        if letter:
            current = letter
            print(f"[{i}/{len(pdf_files)}] MARKER {pdf.name} -> {letter}")

        rows.append({"file": pdf.name, "marker": letter or "", "segment": current or ""})

    df = pd.DataFrame(rows)
    df.to_csv("segmentation_result.csv", index=False)
    print("\nSaved: segmentation_result.csv")

if __name__ == "__main__":
    main()
