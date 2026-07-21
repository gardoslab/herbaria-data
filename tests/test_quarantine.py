"""
Unit tests for host quarantine: schema migration, the quarantine helpers, and
the quarantine-aware work-queue ordering in download_db.

Run from the repo root:
    python -m unittest tests.test_quarantine
"""

import os
import sys
import sqlite3
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import download_db as ddb
from download_db import DownloadDB


class QuarantineSchemaTest(unittest.TestCase):
    """The quarantine columns exist on fresh DBs and are added to old ones."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "status.db")

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except OSError:
                pass
        try:
            os.rmdir(self.dir)
        except OSError:
            pass

    def _host_columns(self):
        conn = sqlite3.connect(self.path)
        try:
            return {row[1] for row in conn.execute("PRAGMA table_info(hosts)")}
        finally:
            conn.close()

    def test_fresh_db_has_quarantine_columns(self):
        db = DownloadDB(self.path)
        db.close()
        cols = self._host_columns()
        self.assertIn("quarantined", cols)
        self.assertIn("quarantined_at", cols)

    def test_migration_adds_columns_to_old_schema(self):
        # Simulate the pre-quarantine production schema (no quarantine columns).
        conn = sqlite3.connect(self.path)
        conn.execute(
            "CREATE TABLE hosts (host TEXT PRIMARY KEY, "
            "error_count INTEGER NOT NULL DEFAULT 0, blocked_until REAL)")
        conn.execute("INSERT INTO hosts(host, error_count) VALUES('legacy', 7)")
        conn.commit()
        conn.close()

        self.assertNotIn("quarantined", self._host_columns())

        # Opening with DownloadDB runs apply_schema -> migrate_schema.
        db = DownloadDB(self.path)
        db.close()

        cols = self._host_columns()
        self.assertIn("quarantined", cols)
        self.assertIn("quarantined_at", cols)

        # Existing row is preserved and defaulted to not-quarantined.
        conn = sqlite3.connect(self.path)
        row = conn.execute(
            "SELECT error_count, quarantined FROM hosts WHERE host='legacy'"
        ).fetchone()
        conn.close()
        self.assertEqual(row, (7, 0))

    def _old_hosts_db(self, rows):
        """Build a pre-quarantine DB with hosts (host, error_count) `rows`."""
        conn = sqlite3.connect(self.path)
        conn.execute(
            "CREATE TABLE hosts (host TEXT PRIMARY KEY, "
            "error_count INTEGER NOT NULL DEFAULT 0, blocked_until REAL)")
        conn.executemany(
            "INSERT INTO hosts(host, error_count) VALUES(?, ?)", rows)
        conn.commit()
        conn.close()

    def test_migration_backfills_hosts_past_threshold(self):
        thr = ddb.QUARANTINE_ERROR_THRESHOLD
        self._old_hosts_db([
            ("dead.example", thr + 100),   # past threshold -> quarantined
            ("edge.example", thr),         # exactly at threshold -> quarantined
            ("ok.example", thr - 1),       # below threshold -> left alone
        ])

        # Opening with DownloadDB runs the one-time backfill during migration.
        db = DownloadDB(self.path)
        try:
            self.assertEqual(db.quarantined_host_set(),
                             {"dead.example", "edge.example"})
            # Backfilled hosts get a quarantined_at timestamp.
            for host, _count, quarantined_at in db.get_quarantined_hosts():
                self.assertIsNotNone(quarantined_at, host)
        finally:
            db.close()

    def test_backfill_respects_failure_rate(self):
        thr = ddb.QUARANTINE_ERROR_THRESHOLD
        # Old (pre-quarantine) schema: hosts with error counts, plus an images
        # table so the failure-rate backfill can count per-host successes.
        conn = sqlite3.connect(self.path)
        conn.execute(
            "CREATE TABLE hosts (host TEXT PRIMARY KEY, "
            "error_count INTEGER NOT NULL DEFAULT 0, blocked_until REAL)")
        conn.executemany(
            "INSERT INTO hosts(host, error_count) VALUES(?, ?)",
            [("nmnh.example", thr), ("oxalis.example", thr)])
        # Real images schema so create_indexes (which indexes error_type/host/
        # status) succeeds when DownloadDB opens this old-schema DB.
        conn.execute(
            "CREATE TABLE images (gbif_id INTEGER NOT NULL, "
            "image_no INTEGER NOT NULL, image_key TEXT NOT NULL, "
            "urls TEXT NOT NULL, host TEXT, "
            "status TEXT NOT NULL DEFAULT 'pending', http_status INTEGER, "
            "error_type TEXT, error_detail TEXT, file_path TEXT, "
            "file_size INTEGER, attempts INTEGER NOT NULL DEFAULT 0, "
            "last_attempt_at TEXT, PRIMARY KEY (gbif_id, image_no))")
        # nmnh: thr errors but overwhelmingly successful -> healthy, keep it.
        # (1000 successes vs 500 errors = 0.33 rate, well under the threshold.)
        conn.executemany(
            "INSERT INTO images(gbif_id, image_no, image_key, urls, host, status) "
            "VALUES(?,?,?,?,?,?)",
            [(i, 0, f"k{i}", "u", "nmnh.example", ddb.ST_SUCCESS)
             for i in range(1000)])
        # oxalis: thr errors, zero successes -> dead source, quarantine it.
        conn.commit()
        conn.close()

        # Opening with DownloadDB runs the one-time failure-rate backfill.
        db = DownloadDB(self.path)
        try:
            self.assertEqual(db.quarantined_host_set(), {"oxalis.example"})
        finally:
            db.close()

    def test_backfill_runs_once_and_does_not_requarantine(self):
        thr = ddb.QUARANTINE_ERROR_THRESHOLD
        self._old_hosts_db([("dead.example", thr + 100)])

        # First open: backfill quarantines the host.
        db = DownloadDB(self.path)
        self.assertEqual(db.quarantined_host_set(), {"dead.example"})
        db.close()

        # Operator deliberately clears the quarantine.
        conn = sqlite3.connect(self.path)
        conn.execute("UPDATE hosts SET quarantined=0 WHERE host='dead.example'")
        conn.commit()
        conn.close()

        # Re-opening re-runs migrate_schema, but the columns already exist so the
        # backfill must NOT fire again -- the cleared host stays cleared.
        db = DownloadDB(self.path)
        try:
            self.assertEqual(db.quarantined_host_set(), set())
        finally:
            db.close()


class QuarantineHelpersTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "status.db")
        self.db = DownloadDB(self.path)

    def tearDown(self):
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except OSError:
                pass
        try:
            os.rmdir(self.dir)
        except OSError:
            pass

    def test_mark_and_query_quarantined(self):
        self.db.mark_host_quarantined("dead.example", error_count=500)

        self.assertEqual(self.db.quarantined_host_set(), {"dead.example"})
        rows = self.db.get_quarantined_hosts()
        self.assertEqual(len(rows), 1)
        host, error_count, quarantined_at = rows[0]
        self.assertEqual(host, "dead.example")
        self.assertEqual(error_count, 500)
        self.assertIsNotNone(quarantined_at)

    def test_mark_is_idempotent_and_keeps_first_timestamp(self):
        self.db.mark_host_quarantined("dead.example", error_count=500)
        first_at = self.db.get_quarantined_hosts()[0][2]

        # Re-mark with a higher count: timestamp stays, error_count rises, and
        # there is still exactly one row.
        self.db.mark_host_quarantined("dead.example", error_count=800)
        rows = self.db.get_quarantined_hosts()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 800)
        self.assertEqual(rows[0][2], first_at)

    def test_mark_upserts_over_existing_host_row(self):
        # A host that already has circuit-breaker state but is not quarantined.
        self.db.save_host_state({"dead.example": 300}, {})
        self.db.mark_host_quarantined("dead.example", error_count=500)
        self.assertEqual(self.db.quarantined_host_set(), {"dead.example"})

    def _add_success_images(self, host, n):
        """Record `n` succeeded images for `host` (its n_success signal)."""
        with self.db.lock:
            self.db.conn.executemany(
                "INSERT INTO images(gbif_id, image_no, image_key, urls, host, "
                "status) VALUES(?,?,?,?,?,?)",
                [(i, 0, f"k{i}", "u", host, ddb.ST_SUCCESS) for i in range(n)])
            self.db.conn.commit()

    def test_quarantine_if_unhealthy_by_failure_rate(self):
        thr = ddb.QUARANTINE_ERROR_THRESHOLD
        # Healthy source: thr errors but far more successes -> spared (False).
        self._add_success_images("nmnh.example", 1000)
        self.assertFalse(self.db.quarantine_if_unhealthy("nmnh.example", thr))
        self.assertNotIn("nmnh.example", self.db.quarantined_host_set())

        # Pure-failure source: thr errors, zero successes -> quarantined (True).
        self.assertTrue(self.db.quarantine_if_unhealthy("oxalis.example", thr))
        self.assertIn("oxalis.example", self.db.quarantined_host_set())


class WorkQueueOrderingTest(unittest.TestCase):
    """gbifIDs whose images all live on quarantined hosts sort to the back."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "status.db")
        self.db = DownloadDB(self.path)
        self._seed()

    def tearDown(self):
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.path + suffix)
            except OSError:
                pass
        try:
            os.rmdir(self.dir)
        except OSError:
            pass

    def _add_gbif(self, gbif_id, status, images):
        """images: list of (image_no, host)."""
        with self.db.lock:
            self.db.conn.execute(
                "INSERT INTO gbif_ids(gbif_id, n_images, status) VALUES(?,?,?)",
                (gbif_id, len(images), status))
            for image_no, host in images:
                self.db.conn.execute(
                    "INSERT INTO images(gbif_id, image_no, image_key, urls, "
                    "host, status) VALUES(?,?,?,?,?,?)",
                    (gbif_id, image_no, f"key-{gbif_id}-{image_no}",
                     f"http://{host or 'none'}/img", host, ddb.ST_PENDING))
            self.db.conn.commit()

    def _seed(self):
        GOOD, DEAD = "good.example", "dead.example"
        self._add_gbif(100, ddb.G_PENDING, [(0, GOOD)])            # working
        self._add_gbif(200, ddb.G_PARTIAL, [(0, GOOD), (1, DEAD)])  # mixed -> working
        self._add_gbif(300, ddb.G_PENDING, [(0, DEAD), (1, DEAD)])  # all dead -> back
        self._add_gbif(400, ddb.G_PENDING, [(0, None)])            # unknown host -> working
        self._add_gbif(500, ddb.G_DONE, [(0, GOOD)])               # done -> excluded

    def test_ordering_without_quarantine_is_ascending(self):
        # No quarantine yet: original behaviour, ascending gbif_id, done excluded.
        self.assertEqual(self.db.get_work_gbif_ids(), [100, 200, 300, 400])

    def test_all_quarantined_gbifids_pushed_to_back(self):
        self.db.mark_host_quarantined("dead.example", error_count=500)
        work = self.db.get_work_gbif_ids()

        # done (500) is excluded; the all-dead gbifID (300) is last; working
        # sources keep ascending order among themselves.
        self.assertEqual(work, [100, 200, 400, 300])
        self.assertEqual(work[-1], 300)
        self.assertLess(work.index(200), work.index(300))  # mixed beats all-dead


if __name__ == "__main__":
    unittest.main()
