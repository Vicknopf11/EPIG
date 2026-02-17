#!/usr/bin/env python3
import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser(description="Build room segments with hard resets (location/shoot/slate).")
    ap.add_argument("--db", required=True, help="Path to DuckDB (epig_dataset1.duckdb)")
    ap.add_argument("--out_table", default="room_segments_v2", help="Output table name")
    ap.add_argument("--min_run", type=int, default=2, help="Minimum files per segment to keep")
    ap.add_argument("--keep_unmarked", action="store_true", help="Keep UNMARKED segments (recommended for debug)")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    # Drop+create to avoid schema mismatch with previous versions
    con.execute(f"DROP TABLE IF EXISTS {args.out_table};")
    con.execute(f"""
    CREATE TABLE {args.out_table} (
      segment_id BIGINT,
      location_label VARCHAR,
      shoot_index INTEGER,
      marker_text VARCHAR,
      start_efta_id INTEGER,
      end_efta_id INTEGER,
      start_file_id VARCHAR,
      end_file_id VARCHAR,
      n_files INTEGER,
      confidence DOUBLE,
      reset_reason VARCHAR
    );
    """)

    rows = con.execute("""
      SELECT
        f.efta_id,
        f.file_id,
        a.location_label,
        a.shoot_index,
        COALESCE(s.is_slate, FALSE) AS is_slate,
        rm.has_marker,
        rm.marker_text
      FROM files f
      LEFT JOIN assignments a USING(file_id)
      LEFT JOIN slates s USING(file_id)
      LEFT JOIN room_markers rm USING(file_id)
      ORDER BY f.efta_id ASC
    """).fetchall()

    def is_valid_marker(m):
        if m is None:
            return False
        if len(m) == 1 and m.isalpha():
            return True
        if len(m) == 2 and m[0] == m[1] and m.isalpha():
            return True
        return False

    def confidence_for(n):
        if n >= 20:
            return 0.92
        if n >= 8:
            return 0.90
        if n >= 3:
            return 0.80
        return 0.60

    segments = []
    seg_id = 0
    cur = None  # dict

    def close_cur(reason: str):
        nonlocal cur
        if cur is None:
            return
        cur["reset_reason"] = reason
        segments.append(cur)
        cur = None

    def start_segment(loc, shoot, marker_text, efta_id, file_id):
        nonlocal seg_id, cur
        seg_id += 1
        cur = {
            "segment_id": seg_id,
            "location_label": loc,
            "shoot_index": shoot,
            "marker_text": marker_text,
            "start_efta_id": efta_id,
            "start_file_id": file_id,
            "end_efta_id": efta_id,
            "end_file_id": file_id,
            "n_files": 1,
            "confidence": 0.0,
            "reset_reason": "",
        }

    prev_loc = None
    prev_shoot = None

    for efta_id, file_id, loc, shoot, is_slate, has_marker, marker in rows:
        loc = loc if loc is not None else "UNKNOWN"

        # hard resets
        if prev_loc is None:
            prev_loc, prev_shoot = loc, shoot
        else:
            if loc != prev_loc:
                close_cur("location_change")
                prev_loc, prev_shoot = loc, shoot
            elif shoot != prev_shoot:
                close_cur("shoot_change")
                prev_loc, prev_shoot = loc, shoot

        if is_slate:
            close_cur("slate")
            continue

        new_marker = marker if has_marker and is_valid_marker(marker) else None

        if new_marker is not None:
            if cur is None:
                start_segment(loc, shoot, new_marker, efta_id, file_id)
            else:
                if new_marker != cur["marker_text"]:
                    close_cur("marker_change")
                    start_segment(loc, shoot, new_marker, efta_id, file_id)
                else:
                    cur["end_efta_id"] = efta_id
                    cur["end_file_id"] = file_id
                    cur["n_files"] += 1
        else:
            if cur is not None:
                cur["end_efta_id"] = efta_id
                cur["end_file_id"] = file_id
                cur["n_files"] += 1
            else:
                if args.keep_unmarked:
                    start_segment(loc, shoot, "UNMARKED", efta_id, file_id)

    close_cur("eof")

    kept = []
    for s in segments:
        s["confidence"] = confidence_for(s["n_files"])
        if s["marker_text"] == "UNMARKED" and not args.keep_unmarked:
            continue
        if s["n_files"] >= args.min_run:
            kept.append(s)

    for s in kept:
        con.execute(
            f"INSERT INTO {args.out_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                s["reset_reason"],
            ],
        )

    print(f"Wrote {len(kept)} segments to table '{args.out_table}'.")

    print(con.execute(f"""
      SELECT location_label, shoot_index, marker_text,
             COUNT(*) AS n_segments,
             SUM(n_files) AS sum_files,
             MAX(n_files) AS max_segment_files
      FROM {args.out_table}
      GROUP BY 1,2,3
      ORDER BY location_label, shoot_index, max_segment_files DESC
      LIMIT 60
    """).fetchdf())

    print("\nFirst 30 segments:")
    print(con.execute(f"""
      SELECT segment_id, location_label, shoot_index, marker_text,
             start_efta_id, end_efta_id, n_files, confidence, reset_reason
      FROM {args.out_table}
      ORDER BY segment_id
      LIMIT 30
    """).fetchdf())

    print("\nLargest 20 segments:")
    print(con.execute(f"""
      SELECT segment_id, location_label, shoot_index, marker_text,
             start_efta_id, end_efta_id, n_files, confidence, reset_reason
      FROM {args.out_table}
      ORDER BY n_files DESC
      LIMIT 20
    """).fetchdf())

    con.close()


if __name__ == "__main__":
    main()
