#!/usr/bin/env python3
import argparse
from pathlib import Path
import duckdb


def main():
    ap = argparse.ArgumentParser(description="Export positions of room markers and slates for manual verification.")
    ap.add_argument("--db", required=True, help="Path to DuckDB")
    ap.add_argument("--out_dir", default="outputs/csv", help="Output directory for CSVs")
    ap.add_argument("--only_location", default=None, help="Filter by location_label (e.g. NY_9E71st, LSJ)")
    ap.add_argument("--only_shoot", type=int, default=None, help="Filter by shoot_index (e.g. 1 or 2)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.db)

    filters = []
    if args.only_location:
        filters.append(f"a.location_label = '{args.only_location}'")
    if args.only_shoot is not None:
        filters.append(f"a.shoot_index = {args.only_shoot}")

    where_extra = ""
    if filters:
        where_extra = " AND " + " AND ".join(filters)

    # 1) Marker occurrences (one row per file with marker)
    markers_csv = out_dir / "markers_occurrences.csv"
    con.execute(f"""
      COPY (
        SELECT
          f.efta_id,
          f.file_id,
          a.location_label,
          a.shoot_index,
          rm.marker_text,
          rm.ocr_text AS marker_ocr,
          f.path AS pdf_path,
          i.jpg_path
        FROM room_markers rm
        JOIN files f USING(file_id)
        LEFT JOIN assignments a USING(file_id)
        LEFT JOIN images i USING(file_id)
        WHERE rm.has_marker = TRUE
        {where_extra}
        ORDER BY rm.marker_text, f.efta_id
      )
      TO '{markers_csv.as_posix()}'
      (HEADER, DELIMITER ',');
    """)

    # 2) Slate occurrences
    slates_csv = out_dir / "slates_occurrences.csv"
    con.execute(f"""
      COPY (
        SELECT
          f.efta_id,
          f.file_id,
          a.location_label,
          a.shoot_index,
          s.is_slate,
          s.keywords_found,
          s.ocr_text AS slate_ocr,
          f.path AS pdf_path,
          i.jpg_path
        FROM slates s
        JOIN files f USING(file_id)
        LEFT JOIN assignments a USING(file_id)
        LEFT JOIN images i USING(file_id)
        WHERE s.is_slate = TRUE
        {where_extra}
        ORDER BY f.efta_id
      )
      TO '{slates_csv.as_posix()}'
      (HEADER, DELIMITER ',');
    """)

    # 3) Marker summary (counts + first/last occurrences)
    summary_csv = out_dir / "markers_summary.csv"
    con.execute(f"""
      COPY (
        SELECT
          rm.marker_text,
          COUNT(*) AS n,
          MIN(f.efta_id) AS first_efta,
          MAX(f.efta_id) AS last_efta
        FROM room_markers rm
        JOIN files f USING(file_id)
        LEFT JOIN assignments a USING(file_id)
        WHERE rm.has_marker = TRUE
        {where_extra}
        GROUP BY 1
        ORDER BY n DESC, rm.marker_text
      )
      TO '{summary_csv.as_posix()}'
      (HEADER, DELIMITER ',');
    """)

    con.close()

    print("Wrote:")
    print(" -", markers_csv)
    print(" -", slates_csv)
    print(" -", summary_csv)


if __name__ == "__main__":
    main()
