#!/usr/bin/env python
"""
One-time maintenance: release hosts that were quarantined under the old absolute
error-count rule (error_count >= 500) but are actually healthy under the failure-
RATE rule now used everywhere (see download_db.QUARANTINE_FAILURE_RATE).

A big source such as nmnh accrued 500 errors against ~1.94M successes -- a ~0.03%
failure rate -- yet the old rule quarantined it, pushing its gbifIDs to the back
of the queue. This script walks every quarantined host, recomputes its failure
rate from the images table, and clears the quarantine (quarantined=0,
quarantined_at=NULL) for any host whose rate is at or below the threshold.

Dry-run by default: it only prints the hosts it WOULD release. Pass --apply to
write the change to the database.

    python unquarantine_healthy.py                 # preview only
    python unquarantine_healthy.py --apply         # actually release them
"""

import argparse
import sqlite3

import download_db as ddb


def quarantined_host_stats(conn):
    """
    Return [(host, error_count, n_success, rate)] for every quarantined host,
    ordered worst-rate first. n_success is the host's succeeded-image count;
    rate uses the same float formula as quarantine_if_unhealthy / the backfill.
    """
    hosts = conn.execute(
        "SELECT host, error_count FROM hosts WHERE quarantined=1"
    ).fetchall()
    stats = []
    for host, error_count in hosts:
        n_success = conn.execute(
            "SELECT COUNT(*) FROM images WHERE host=? AND status=?",
            (host, ddb.ST_SUCCESS),
        ).fetchone()[0]
        total = error_count + n_success
        rate = error_count / total if total else 0.0
        stats.append((host, error_count, n_success, rate))
    stats.sort(key=lambda r: r[3], reverse=True)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=ddb.DEFAULT_DB_PATH,
                        help=f"Status database path (default: {ddb.DEFAULT_DB_PATH})")
    parser.add_argument("--apply", action="store_true",
                        help="Actually clear the quarantine flag; without this the "
                             "script only prints what it would do.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=120)
    try:
        stats = quarantined_host_stats(conn)
        healthy = [r for r in stats if r[3] <= ddb.QUARANTINE_FAILURE_RATE]

        print(f"Quarantined hosts: {len(stats)}. "
              f"Failure-rate threshold: {ddb.QUARANTINE_FAILURE_RATE:.3f}")
        print(f"{'host':<40} {'errors':>10} {'success':>12} {'rate':>8}  action")
        for host, error_count, n_success, rate in stats:
            action = "RELEASE" if rate <= ddb.QUARANTINE_FAILURE_RATE else "keep"
            print(f"{host:<40} {error_count:>10} {n_success:>12} "
                  f"{rate:>8.3f}  {action}")

        if not healthy:
            print("\nNo quarantined host qualifies for release. Nothing to do.")
            return

        if not args.apply:
            print(f"\nDRY RUN: would release {len(healthy)} host(s). "
                  f"Re-run with --apply to write the change.")
            return

        conn.executemany(
            "UPDATE hosts SET quarantined=0, quarantined_at=NULL WHERE host=?",
            [(r[0],) for r in healthy],
        )
        conn.commit()
        print(f"\nReleased {len(healthy)} host(s) from quarantine.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
