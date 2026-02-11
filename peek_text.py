import fitz
from pathlib import Path

PDF_DIR = Path("downloads_dataset1")

for name in ["EFTA00000001.pdf", "EFTA00000002.pdf", "EFTA00000003.pdf"]:
    p = PDF_DIR / name
    doc = fitz.open(p)
    txt = doc[0].get_text("text")
    doc.close()

    print("\n====", name, "====")
    print(txt[:800].replace("\n\n", "\n"))
