#!/usr/bin/env python3
import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser(description="Build room segments from room_markers + assignments.")
    ap.add_argument("--db", required=True, help="Path to DuckDB (epig_dataset1.duckdb)")
    ap.add_argument("--out_table", default="room_segments", help="Output table name")
    ap.add_argument("--min_run", type=int, default=2, help="Minimum consecutive files after a marker to keep segment")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    # Ensure output table exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {args.out_table} (
      segment_id BIGINT,
      location_label VARCHAR,
      shoot_index INTEGER,
      marker_text VARCHAR,
      start_efta_id INTEGER,
      end_efta_id INTEGER,
      start_file_id VARCHAR,
      end_file_id VARCHAR,
      n_files INTEGER,
      confidence DOUBLE
    );
    """)

    # Pull the ordered sequence with marker + assignment
    rows = con.execute("""
      SELECT
        f.efta_id,
        f.file_id,
        a.location_label,
        a.shoot_index,
        rm.has_marker,
        rm.marker_text
      FROM files f
      LEFT JOIN assignments a USING(file_id)
      LEFT JOIN room_markers rm USING(file_id)
      ORDER BY f.efta_id ASC
    """).fetchall()

    # Helper: marker validity
    def is_valid_marker(m):
        if m is None:
            return False
        if len(m) == 1 and m.isalpha():
            return True
        if len(m) == 2 and m[0] == m[1] and m.isalpha():
            return True
        return False

    segments = []
    cur = None  # dict
    seg_id = 0

    for efta_id, file_id, loc, shoot, has_marker, marker in rows:
        marker = marker if has_marker else None
        new_marker = marker if is_valid_marker(marker) else None

        if new_marker is not None:
            # Start a new segment if marker changes
            if cur is None:
                seg_id += 1
                cur = {
                    "segment_id": seg_id,
                    "location_label": loc,
                    "shoot_index": shoot,
                    "marker_text": new_marker,
                    "start_efta_id": efta_id,
                    "start_file_id": file_id,
                    "end_efta_id": efta_id,
                    "end_file_id": file_id,
                    "n_files": 1,
                }
            else:
                if new_marker != cur["marker_text"]:
                    # close current segment at previous file
                    segments.append(cur)
                    seg_id += 1
                    cur = {
                        "segment_id": seg_id,
                        "location_label": loc,
                        "shoot_index": shoot,
                        "marker_text": new_marker,
                        "start_efta_id": efta_id,
                        "start_file_id": file_id,
                        "end_efta_id": efta_id,
                        "end_file_id": file_id,
                        "n_files": 1,
                    }
                else:
                    # Same marker repeats: treat as continuing current segment
                    cur["end_efta_id"] = efta_id
                    cur["end_file_id"] = file_id
                    cur["n_files"] += 1
        else:
            # No new marker: extend current segment if exists
            if cur is not None:
                cur["end_efta_id"] = efta_id
                cur["end_file_id"] = file_id
                cur["n_files"] += 1

    if cur is not None:
        segments.append(cur)

    # Filter tiny segments (optional)
    kept = []
    for s in segments:
        if s["n_files"] >= args.min_run:
            kept.append(s)

    # Confidence heuristic
    for s in kept:
        if s["n_files"] >= 8:
            conf = 0.90
        elif s["n_files"] >= 3:
            conf = 0.80
        else:
            conf = 0.60
        s["confidence"] = conf

    # Write to DB: replace table contents
    con.execute(f"DELETE FROM {args.out_table};")
    for s in kept:
        con.execute(
            f"INSERT INTO {args.out_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                s["segment_id"],
                s["location_label"],
                s["shoot_index"],
                s["marker_text"],
                s["start_efta_id"],
                s["end_efta_id"],
                s["start_file_id"],
                s["end_file_id"],
                s["n_files"],
                s["confidence"],
            ],
        )

    # Print summary
    print(f"Wrote {len(kept)} segments to table '{args.out_table}'.")
    print(con.execute(f"""
      SELECT location_label, shoot_index, marker_text, COUNT(*) AS n_segments, SUM(n_files) AS sum_files
      FROM {args.out_table}
      GROUP BY 1,2,3
      ORDER BY location_label, shoot_index, n_segments DESC
      LIMIT 50
    """).fetchdf())

    # Show first 30 segments
    print(con.execute(f"""
      SELECT segment_id, location_label, shoot_index, marker_text, start_efta_id, end_efta_id, n_files, confidence
      FROM {args.out_table}
      ORDER BY segment_id
      LIMIT 30
    """).fetchdf())

    con.close()


if __name__ == "__main__":
    main()
