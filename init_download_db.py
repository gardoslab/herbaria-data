#!/usr/bin/env python3
"""
One-time builder for the image-download status database (download_status.db).

What it does
------------
1. Creates the SQLite schema (see download_db.py).
2. Reads multimedia.txt once and groups its rows into *distinct images*: a IIIF
   manifest plus the resolution variants of one specimen collapse to a single
   image (see download_db.canonical_image_key). Each distinct image becomes one
   row in `images`, carrying all of its candidate URLs; `gbif_ids` gets one row
   per gbifID. After this, runs of image_install_db.py never re-read the 59M-row
   multimedia.txt -- the work queue lives in the database.
3. Imports processed_ids.txt: for each already-finished gbifID it locates the
   downloaded file, renames legacy `<id>.jpg` to `<id>-00.jpg` so the dataset
   uses one consistent naming scheme, and marks image 0 as 'success'. gbifIDs
   with more than one distinct image are left 'partial' so the downloader goes
   back and fetches their remaining images.

   NOTE: the old one-image-per-ID downloader shuffled candidate URLs, so for a
   multi-image gbifID we cannot know which image the existing file is. It is
   recorded against image 0 with error_type 'legacy_unverified_index'.

failed_ids.txt is intentionally NOT imported: those IDs stay 'pending' and get
a fresh, fully-tracked retry.

This script is destructive-ish (it renames files and can drop an existing DB
with --reset). It does not download anything.

The legacy import is idempotent and resumable: if it is interrupted, re-run
with --legacy-only to finish it without redoing the multimedia ingest.

Usage
-----
    python init_download_db.py                 # build DB + import legacy
    python init_download_db.py --skip-legacy   # build DB only
    python init_download_db.py --legacy-only   # (re-)run only the legacy import
    python init_download_db.py --reset         # rebuild from scratch
"""

import os
import sys
import gc
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


def progress(msg):
    """
    Print a progress line, flushed immediately so a batch job's .o log updates
    live instead of only at exit. Overwrites in place on an interactive
    terminal; writes one line per update when redirected to a log file.
    """
    if sys.stdout.isatty():
        print(f"\r{msg}", end="", flush=True)
    else:
        print(msg, flush=True)


def hierarchical_path(base_dir, gbif_id, suffix=""):
    """Path for a stored image (mirrors image_install_db.image_path, no mkdir)."""
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
    p.add_argument("--legacy-only", action="store_true",
                   help="Skip the ingest; only (re-)run the legacy import on an "
                        "existing database (use this to finish an interrupted import)")
    p.add_argument("--reset", action="store_true",
                   help="Delete an existing database before building")
    return p.parse_args()


def ingest_multimedia(conn, multimedia_path):
    """Group multimedia.txt rows into distinct images and load images + gbif_ids."""
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

    # Stable sort -> each gbifID's rows keep their multimedia.txt order.
    df = df.sort_values("gbifID", kind="stable").reset_index(drop=True)

    print("  Computing canonical image keys ...")
    df["image_key"] = df["identifier"].map(ddb.canonical_image_key).astype("string")

    # image_no: dense rank of image_key within each gbifID, by first appearance.
    df["is_new"] = ~df.duplicated(["gbifID", "image_key"])
    df["image_no"] = df.groupby("gbifID")["is_new"].cumsum().astype("int32") - 1

    # Order a distinct image's candidate URLs: highest resolution first, IIIF
    # manifests last (they are only a fallback -- expanded at download time).
    df["is_manifest"] = df["identifier"].str.contains(
        "/manifest", case=False, na=False).astype("int8")
    size = pd.to_numeric(
        df["identifier"].str.extract(r"/full/(\d+),", expand=False),
        errors="coerce")
    big = df["identifier"].str.contains(
        r"/full/(?:max|full)/", case=False, na=False, regex=True)
    size = size.where(~big, 100000)
    size = size.mask(df["is_manifest"] == 1, 1600)   # manifest expands to ~1600
    df["eff_size"] = size.fillna(0).astype("int32")

    df["host"] = (
        df["identifier"].str.extract(r"^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:]+)",
                                     expand=False)
        .fillna("").astype("string")
    )

    df = df.sort_values(
        ["gbifID", "image_no", "eff_size", "is_manifest"],
        ascending=[True, True, False, True], kind="stable")

    print("  Streaming distinct images into the database ...")
    # Single-pass streaming groupby. The previous pandas
    # .agg('\n'.join, ...) over ~52M groups was the bottleneck (did not
    # finish inside a 12 h job). Pull the sorted columns into native
    # Python lists and emit one INSERT batch per chunk of distinct images.
    gid_l  = df["gbifID"].tolist();     del df["gbifID"]
    no_l   = df["image_no"].tolist();   del df["image_no"]
    url_l  = df["identifier"].tolist(); del df["identifier"]
    key_l  = df["image_key"].tolist();  del df["image_key"]
    host_l = df["host"].tolist();       del df
    gc.collect()
    n_rows = len(gid_l)

    img_batch = []
    distinct = 0
    cur_gid = cur_no = None
    cur_key = cur_host = None
    cur_urls = []

    def flush():
        if not img_batch:
            return
        conn.executemany(
            "INSERT OR IGNORE INTO images"
            "(gbif_id, image_no, image_key, urls, host) VALUES(?,?,?,?,?)",
            img_batch)
        conn.commit()
        img_batch.clear()

    for i in range(n_rows):
        g = gid_l[i]
        n = no_l[i]
        if g != cur_gid or n != cur_no:
            if cur_gid is not None:
                img_batch.append((cur_gid, cur_no, cur_key,
                                  "\n".join(cur_urls), cur_host))
                distinct += 1
                if len(img_batch) >= INSERT_BATCH:
                    flush()
                    progress(f"    {distinct:,} images inserted "
                             f"({100 * i / n_rows:.1f}% through input)")
            cur_gid, cur_no = g, n
            cur_key, cur_host = key_l[i], host_l[i]
            cur_urls = [url_l[i]]
        else:
            cur_urls.append(url_l[i])
    if cur_gid is not None:
        img_batch.append((cur_gid, cur_no, cur_key,
                          "\n".join(cur_urls), cur_host))
        distinct += 1
    flush()
    print(f"    {distinct:,} distinct images inserted        ")

    print("  Populating gbif_ids from images ...")
    conn.execute(
        "INSERT INTO gbif_ids(gbif_id, n_images) "
        "SELECT gbif_id, COUNT(*) FROM images GROUP BY gbif_id")
    conn.commit()
    n_gbif = conn.execute("SELECT COUNT(*) FROM gbif_ids").fetchone()[0]
    print(f"    {n_gbif:,} gbifIDs inserted")


def import_legacy(conn, processed_file, install_path):
    """Mark gbifIDs from processed_ids.txt as already having their first image."""
    if not os.path.exists(processed_file):
        print(f"  {processed_file} not found -- skipping legacy import.")
        return

    print(f"Importing already-processed gbifIDs from {processed_file} ...")
    renamed = relabeled = missing = 0
    updates = []

    def flush(batch):
        if not batch:
            return
        # WAL mode + the 120 s busy timeout make a lock here very unlikely, but
        # retry rather than throw away a long-running import if one occurs.
        for attempt in range(1, 4):
            try:
                conn.executemany(
                    "UPDATE images SET status='success', "
                    "  error_type=?, file_path=?, file_size=?, "
                    "  last_attempt_at=datetime('now') "
                    "WHERE gbif_id=? AND image_no=0",
                    batch,
                )
                conn.commit()
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < 3:
                    print(f"\n  database locked; retry {attempt}/3 "
                          f"in {10 * attempt}s ...")
                    time.sleep(10 * attempt)
                    continue
                raise

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
                progress(f"    renamed={renamed:,} relabeled={relabeled:,} "
                         f"missing={missing:,}")
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


def report_status(conn):
    """Print the gbifID status breakdown."""
    print("\nFinal gbifID status counts:")
    for status, count in conn.execute(
        "SELECT status, COUNT(*) FROM gbif_ids GROUP BY status ORDER BY status"
    ):
        print(f"  {status:10s} {count:,}")


def connect(db_path, bulk_load):
    """
    Open the database with a 120 s busy timeout, so a momentary lock from a
    concurrent reader (e.g. status_report.py) makes the write wait rather than
    abort the run.

    bulk_load=True  -> fastest, no durability (for the rebuildable ingest).
    bulk_load=False -> WAL + synchronous=NORMAL: durable, and readers never
                       block the writer (used for the legacy import).
    """
    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute("PRAGMA cache_size=-200000")  # ~200 MB page cache
    if bulk_load:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
    else:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def main():
    # Line-buffer stdout so progress appears in a batch job's .o log live,
    # not only when the job finishes.
    sys.stdout.reconfigure(line_buffering=True)

    args = parse_args()
    start = time.time()

    # --legacy-only: skip the ingest and just (re-)run the legacy import. Use
    # this to finish an interrupted import without rebuilding the database.
    if args.legacy_only:
        if not os.path.exists(args.db):
            sys.exit(f"--legacy-only needs an existing database, but none was "
                     f"found at: {args.db}\nRun the full build first.")
        print(f"--legacy-only: (re-)running the legacy import on {args.db}")
        conn = connect(args.db, bulk_load=False)
        import_legacy(conn, args.processed_file, args.install_path)
        report_status(conn)
        conn.close()
        print(f"\nDone in {time.time() - start:.0f}s.")
        return

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
                     f"  --reset        rebuild it from scratch\n"
                     f"  --legacy-only  (re-)run just the legacy import on it")

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    # Fast bulk-load settings; the DB is fully rebuildable, so durability during
    # the ingest is not needed.
    conn = connect(args.db, bulk_load=True)

    print("Creating schema ...")
    ddb.create_tables(conn)

    ingest_multimedia(conn, args.multimedia)

    print("Building indexes (this takes a few minutes) ...")
    ddb.create_indexes(conn)

    if not args.skip_legacy:
        # Switch to a durable, reader-tolerant mode for the legacy import: it
        # renames files on disk, so a crash here is costlier than during ingest.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        import_legacy(conn, args.processed_file, args.install_path)

    report_status(conn)
    conn.close()
    print(f"\nDone in {time.time() - start:.0f}s. Database: {os.path.abspath(args.db)}")


if __name__ == "__main__":
    main()
