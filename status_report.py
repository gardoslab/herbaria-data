#!/usr/bin/env python3
"""
Report image-download progress straight from the SQLite status database.

This replaces analyze_image_progress.py: instead of loading ~180 MB of text
checkpoint files and re-grouping the 59M-row multimedia.txt with pandas, every
number here is a single indexed SQL query, so the report returns in seconds.

Counts are over *distinct images* (a IIIF manifest plus its resolution variants
count once -- see download_db.canonical_image_key).

The same numbers are available ad hoc -- a few useful queries:

    -- how many of each kind of failure?
    SELECT error_type, COUNT(*) FROM images
    WHERE status LIKE 'failed%' GROUP BY error_type ORDER BY 2 DESC;

    -- every image still worth retrying
    SELECT gbif_id, image_no, urls FROM images WHERE status='failed_transient';

    -- URLs that returned an HTML/text page, with the captured message
    SELECT host, error_detail FROM images
    WHERE error_type='invalid_content_type' GROUP BY host;

    -- raw files (DNG etc.) kept for a later conversion pass
    SELECT gbif_id, image_no, file_path FROM images
    WHERE error_type='raw_unprocessed';

Usage:
    python status_report.py [--db PATH] [--output-dir DIR]
"""

import os
import sqlite3
import argparse
from datetime import datetime

import download_db as ddb


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=ddb.DEFAULT_DB_PATH,
                   help=f"Status database path (default: {ddb.DEFAULT_DB_PATH})")
    p.add_argument("--output-dir", default=os.getcwd(),
                   help="Directory for the summary_YYYYMMDDHHMM.txt file")
    return p.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"Status database not found: {args.db}")

    # Read-only with a generous busy timeout so a brief WAL contention
    # window from a running downloader can't surface as "disk I/O error".
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    run_time = datetime.now()
    output_file = os.path.join(
        args.output_dir, f"summary_{run_time:%Y%m%d%H%M}.txt")
    os.makedirs(args.output_dir, exist_ok=True)

    with open(output_file, "w") as out:
        def write(msg=""):
            out.write(msg + "\n")
            print(msg)

        def section(title):
            write()
            write("=" * 70)
            write(title)
            write("=" * 70)

        write(f"Run date: {run_time:%Y-%m-%d %H:%M:%S}")
        write(f"Database: {os.path.abspath(args.db)}")

        # -- gbifID progress --------------------------------------------------
        section("GBIFID PROGRESS")
        gbif_counts = dict(conn.execute(
            "SELECT status, COUNT(*) FROM gbif_ids GROUP BY status").fetchall())
        total_ids = sum(gbif_counts.values())
        write(f"Total gbifIDs:                  {total_ids:,}")
        for status in (ddb.G_DONE, ddb.G_PARTIAL, ddb.G_PENDING, ddb.G_FAILED):
            count = gbif_counts.get(status, 0)
            pct = (count / total_ids * 100) if total_ids else 0.0
            write(f"  {status:10s}                  {count:>14,}  ({pct:5.2f}%)")
        remaining = gbif_counts.get(ddb.G_PENDING, 0) + gbif_counts.get(ddb.G_PARTIAL, 0)
        write(f"Still in the work queue:        {remaining:,}")

        # -- per-image progress ----------------------------------------------
        section("DISTINCT-IMAGE PROGRESS")
        img_counts = dict(conn.execute(
            "SELECT status, COUNT(*) FROM images GROUP BY status").fetchall())
        total_imgs = sum(img_counts.values())
        write(f"Total distinct images:          {total_imgs:,}")
        for status in (ddb.ST_SUCCESS, ddb.ST_PENDING,
                       ddb.ST_FAILED_TRANSIENT, ddb.ST_FAILED_PERMANENT):
            count = img_counts.get(status, 0)
            pct = (count / total_imgs * 100) if total_imgs else 0.0
            write(f"  {status:18s}          {count:>14,}  ({pct:5.2f}%)")
        raw_kept = conn.execute(
            "SELECT COUNT(*) FROM images WHERE error_type=?",
            (ddb.ERR_RAW_UNPROCESSED,)).fetchone()[0]
        write(f"  (of 'success', kept raw -- DNG etc., need conversion: "
              f"{raw_kept:,})")

        # -- failure breakdown ------------------------------------------------
        section("FAILURES BY TYPE")
        write(f"{'error_type':24s} {'count':>14s}  {'verdict':s}")
        write("-" * 60)
        rows = conn.execute(
            "SELECT error_type, COUNT(*) FROM images "
            "WHERE status LIKE 'failed%' AND error_type IS NOT NULL "
            "GROUP BY error_type ORDER BY 2 DESC").fetchall()
        for error_type, count in rows:
            verdict = "permanent" if ddb.is_permanent(error_type) else "retryable"
            write(f"{error_type:24s} {count:>14,}  {verdict}")
        if not rows:
            write("(no failures recorded yet)")

        # -- retry attempt distribution --------------------------------------
        section("RETRY ATTEMPTS (failed_transient images)")
        rows = conn.execute(
            "SELECT attempts, COUNT(*) FROM images "
            "WHERE status='failed_transient' GROUP BY attempts ORDER BY attempts"
        ).fetchall()
        for attempts, count in rows:
            note = "  <- retry budget exhausted" if attempts >= ddb.MAX_ATTEMPTS else ""
            write(f"  {attempts} attempt(s): {count:,}{note}")
        if not rows:
            write("(none)")

        # -- non-image (HTML/text) responses ---------------------------------
        section("NON-IMAGE RESPONSES BY HOST (for follow-up)")
        write("Hosts whose URLs returned an HTML/text page instead of an image")
        write("(e.g. 'direct download no longer supported'). Sample message shown.")
        write("-" * 70)
        rows = conn.execute(
            "SELECT host, COUNT(*) AS n, MIN(error_detail) "
            "FROM images WHERE error_type=? "
            "GROUP BY host ORDER BY n DESC LIMIT 20",
            (ddb.ERR_INVALID_CONTENT,)).fetchall()
        for host, count, sample in rows:
            write(f"{(host or '?')[:45]:45s} {count:>10,}")
            if sample:
                write(f"    {sample[:200].strip()}")
        if not rows:
            write("(none recorded yet)")

        # -- worst hosts ------------------------------------------------------
        section("TOP 20 HOSTS BY FAILED IMAGES")
        write(f"{'host':40s} {'failed':>10s} {'success':>10s}")
        write("-" * 64)
        rows = conn.execute(
            "SELECT host, "
            "  SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) AS failed, "
            "  SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS ok "
            "FROM images WHERE host IS NOT NULL AND host != '' "
            "GROUP BY host ORDER BY failed DESC LIMIT 20").fetchall()
        for host, failed, ok in rows:
            write(f"{host[:40]:40s} {failed or 0:>10,} {ok or 0:>10,}")

        # -- circuit-breaker state -------------------------------------------
        section("CIRCUIT BREAKER / COOLDOWNS")
        broken = conn.execute(
            "SELECT COUNT(*) FROM hosts WHERE error_count >= 500").fetchone()[0]
        blocked = conn.execute(
            "SELECT COUNT(*) FROM hosts "
            "WHERE blocked_until IS NOT NULL "
            "AND blocked_until > strftime('%s','now')").fetchone()[0]
        write(f"Hosts past the circuit-breaker threshold (500 errors): {broken:,}")
        write(f"Hosts currently in cooldown:                          {blocked:,}")

        section("NOTES")
        write("- Counts are over distinct images: a IIIF manifest plus its")
        write("  resolution variants count as one image.")
        write("- 'done'    = every distinct image for the gbifID succeeded.")
        write("- 'partial' = still has retryable work; stays in the queue.")
        write("- 'failed'  = all images terminal, not all succeeded; no retries left.")
        write("- failed_transient images are retried until "
              f"{ddb.MAX_ATTEMPTS} attempts, then count toward 'failed'.")
        write("- 'raw_unprocessed' images ARE downloaded (status success); they")
        write("  are raw files (DNG etc.) awaiting a separate conversion pass.")
        write()
        write(f"Summary written to: {os.path.abspath(output_file)}")

    conn.close()


if __name__ == "__main__":
    main()
