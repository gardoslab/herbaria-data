#!/usr/bin/env python3
"""
One-time builder for the image-download status database (download_status.db).

What it does
------------
1. Creates the SQLite schema (see download_db.py).
2. Reads multimedia.txt once and loads every (gbifID, image URL) pair into the
   `images` table and every gbifID into `gbif_ids`. After this, runs of
   image_install_db.py no longer need to re-read and re-group the 59M-row
   multimedia.txt -- the work queue lives in the database.
3. Imports processed_ids.txt: for each already-finished gbifID it locates the
   downloaded file, renames legacy `<id>.jpg` to `<id>-00.jpg` so the dataset
   uses one consistent naming scheme, and marks image index 0 as 'success'.
   gbifIDs with more than one image are left 'partial' so the multi-image
   downloader goes back and fetches their remaining images.

   NOTE: the old one-image-per-ID downloader shuffled candidate URLs, so for a
   multi-image gbifID we cannot know which URL the existing file came from. It
   is recorded against img_index 0 with error_type 'legacy_unverified_index'.
   ~87% of gbifIDs have only one image, where this assignment is exact.

failed_ids.txt is intentionally NOT imported: those IDs stay 'pending' and get
a fresh, fully-tracked retry.

This script is destructive-ish (it renames files and can drop an existing DB
with --reset). It does not download anything. Run it once before the first
run of image_install_db.py.

Usage
-----
    python init_download_db.py                 # build DB + import legacy
    python init_download_db.py --skip-legacy   # build DB only
    python init_download_db.py --reset         # rebuild from scratch
"""

import os
import sys
import time
import sqlite3
import argparse

import pandas as pd

import download_db as ddb

GBIF_MULTIMEDIA_DATA = "/projectnb/herbdl/data/GBIF-F25/multimedia.txt"
INSTALL_PATH = "/projectnb/herbdl/data/GBIF-F25h"
PROCESSED_FILE = "processed_ids.txt"

INSERT_BATCH = 200_000
LEGACY_BATCH = 50_000


def hierarchical_path(base_dir, gbif_id, suffix=""):
    """Mirror image_install_db.get_hierarchical_path (without makedirs)."""
    stem = str(gbif_id)
    prefix1 = stem[:3] if len(stem) >= 3 else stem
    prefix2 = stem[3:6] if len(stem) >= 6 else "000"
    return os.path.join(base_dir, prefix1, prefix2, f"{stem}{suffix}.jpg")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=ddb.DEFAULT_DB_PATH,
                   help=f"Path to the SQLite database (default: {ddb.DEFAULT_DB_PATH})")
    p.add_argument("--multimedia", default=GBIF_MULTIMEDIA_DATA,
                   help="GBIF multimedia.txt to ingest")
    p.add_argument("--install-path", default=INSTALL_PATH,
                   help="Root directory where images are stored")
    p.add_argument("--processed-file", default=PROCESSED_FILE,
                   help="processed_ids.txt to import as already-done gbifIDs")
    p.add_argument("--skip-legacy", action="store_true",
                   help="Do not import processed_ids.txt")
    p.add_argument("--reset", action="store_true",
                   help="Delete an existing database before building")
    return p.parse_args()


def ingest_multimedia(conn, multimedia_path):
    """Load every image URL from multimedia.txt into images + gbif_ids."""
    print(f"Reading {multimedia_path} ...")
    df = pd.read_csv(
        multimedia_path,
        delimiter="\t",
        usecols=lambda c: c in ("gbifID", "identifier"),
        on_bad_lines="skip",
    )
    df = df.dropna(subset=["gbifID", "identifier"])
    df["gbifID"] = df["gbifID"].astype("int64")
    df["identifier"] = df["identifier"].astype("string")
    print(f"  {len(df):,} (gbifID, URL) rows")

    # Sort so each gbifID's rows are contiguous, then number them 0,1,2,...
    df = df.sort_values("gbifID", kind="stable").reset_index(drop=True)
    df["img_index"] = df.groupby("gbifID").cumcount()
    df["host"] = (
        df["identifier"].str.extract(r"^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:]+)",
                                     expand=False)
        .fillna("")
    )

    print("  Inserting image rows ...")
    inserted = 0
    for start in range(0, len(df), INSERT_BATCH):
        sub = df.iloc[start:start + INSERT_BATCH]
        rows = list(zip(
            sub["gbifID"].tolist(),
            sub["img_index"].tolist(),
            sub["identifier"].tolist(),
            sub["host"].tolist(),
        ))
        conn.executemany(
            "INSERT OR IGNORE INTO images(gbif_id, img_index, url, host) "
            "VALUES(?,?,?,?)",
            rows,
        )
        conn.commit()
        inserted += len(rows)
        print(f"    {inserted:,}/{len(df):,} image rows", end="\r")
    print(f"    {inserted:,} image rows inserted        ")

    print("  Inserting gbifID rows ...")
    sizes = df.groupby("gbifID").size()
    gid_rows = list(zip(sizes.index.tolist(), sizes.tolist()))
    for start in range(0, len(gid_rows), INSERT_BATCH):
        conn.executemany(
            "INSERT OR IGNORE INTO gbif_ids(gbif_id, n_images) VALUES(?,?)",
            gid_rows[start:start + INSERT_BATCH],
        )
        conn.commit()
    print(f"    {len(gid_rows):,} gbifIDs inserted")


def import_legacy(conn, processed_file, install_path):
    """Mark gbifIDs from processed_ids.txt as already having their first image."""
    if not os.path.exists(processed_file):
        print(f"  {processed_file} not found -- skipping legacy import.")
        return

    print(f"Importing already-processed gbifIDs from {processed_file} ...")
    renamed = relabeled = missing = 0
    updates = []

    def flush(batch):
        if batch:
            conn.executemany(
                "UPDATE images SET status='success', "
                "  error_type=?, file_path=?, file_size=?, "
                "  last_attempt_at=datetime('now') "
                "WHERE gbif_id=? AND img_index=0",
                batch,
            )
            conn.commit()

    with open(processed_file) as fh:
        for line in fh:
            gid = line.strip()
            if not gid or not gid.isdigit():
                continue

            new_path = hierarchical_path(install_path, gid, "-00")
            old_path = hierarchical_path(install_path, gid, "")

            if os.path.exists(new_path):
                path = new_path
                relabeled += 1
            elif os.path.exists(old_path):
                try:
                    os.rename(old_path, new_path)
                except OSError:
                    missing += 1
                    continue
                path = new_path
                renamed += 1
            else:
                missing += 1
                continue

            try:
                size = os.path.getsize(path)
            except OSError:
                missing += 1
                continue

            updates.append((ddb.ERR_LEGACY, path, size, int(gid)))
            if len(updates) >= LEGACY_BATCH:
                flush(updates)
                updates = []
                print(f"    renamed={renamed:,} relabeled={relabeled:,} "
                      f"missing={missing:,}", end="\r")
    flush(updates)
    print(f"    renamed={renamed:,}  already-suffixed={relabeled:,}  "
          f"file-missing={missing:,}")

    # Roll the per-image success flags up into gbif_ids statuses in one pass.
    print("  Recomputing gbifID statuses ...")
    conn.execute(
        "UPDATE gbif_ids SET "
        "  n_success=(SELECT COUNT(*) FROM images i "
        "             WHERE i.gbif_id=gbif_ids.gbif_id AND i.status='success'), "
        "  status=CASE "
        "    WHEN n_images>0 AND n_images=(SELECT COUNT(*) FROM images i "
        "         WHERE i.gbif_id=gbif_ids.gbif_id AND i.status='success') "
        "      THEN 'done' "
        "    WHEN (SELECT COUNT(*) FROM images i "
        "         WHERE i.gbif_id=gbif_ids.gbif_id AND i.status='success')>0 "
        "      THEN 'partial' "
        "    ELSE 'pending' END "
        "WHERE gbif_id IN (SELECT DISTINCT gbif_id FROM images "
        "                  WHERE status='success')"
    )
    conn.execute(
        "UPDATE gbif_ids SET completed_at=datetime('now') "
        "WHERE status='done' AND completed_at IS NULL"
    )
    conn.commit()


def main():
    args = parse_args()
    start = time.time()

    if os.path.exists(args.db):
        if args.reset:
            print(f"Removing existing database {args.db}")
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(args.db + suffix)
                except FileNotFoundError:
                    pass
        else:
            sys.exit(f"Database already exists: {args.db}\n"
                     f"Pass --reset to rebuild it from scratch.")

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    conn = sqlite3.connect(args.db)
    # Fast bulk-load settings; the DB is fully rebuildable, so durability during
    # ingest is not needed. image_install_db.py switches it to WAL later.
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-200000")  # ~200 MB page cache

    print("Creating schema ...")
    ddb.create_tables(conn)

    ingest_multimedia(conn, args.multimedia)

    print("Building indexes (this takes a few minutes) ...")
    ddb.create_indexes(conn)

    if not args.skip_legacy:
        import_legacy(conn, args.processed_file, args.install_path)

    print("\nFinal gbifID status counts:")
    for status, count in conn.execute(
        "SELECT status, COUNT(*) FROM gbif_ids GROUP BY status ORDER BY status"
    ):
        print(f"  {status:10s} {count:,}")

    conn.close()
    print(f"\nDone in {time.time() - start:.0f}s. Database: {os.path.abspath(args.db)}")


if __name__ == "__main__":
    main()
