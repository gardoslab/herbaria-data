"""
SQLite-backed download-status tracking for image_install_db.py.

Replaces the flat processed_ids.txt / failed_ids.txt checkpoint files with a
queryable database that records, for every image URL, whether it succeeded or
failed and *why*. That makes it possible to:
  * resume a run without re-reading and re-grouping the 59M-row multimedia.txt,
  * retry only transient failures (timeouts, rate limits, 5xx, dropped
    connections) while leaving permanent ones (404/410/etc.) alone,
  * answer questions like "how many 404s?" or "which hosts fail most?" with a
    single SQL query (see status_report.py).

Tables
------
images    one row per source image URL (a GBIF "identifier").
gbif_ids  one row per gbifID; doubles as the resumable work queue.
hosts     per-host error tally + cooldown timestamp, so circuit-breaker and
          rate-limit state survive a job restart.

A gbifID is 'done' only when every one of its images has status 'success'.
"""

import os
import time
import sqlite3
import threading

DEFAULT_DB_PATH = "/projectnb/herbdl/data/GBIF-F25h/download_status.db"

# Retry budget: a transient failure is retried until this many attempts.
MAX_ATTEMPTS = 4

# ---- images.status -----------------------------------------------------------
ST_PENDING = "pending"            # never attempted
ST_SUCCESS = "success"            # downloaded (and resized) OK
ST_FAILED_PERMANENT = "failed_permanent"   # retrying will not help
ST_FAILED_TRANSIENT = "failed_transient"   # may succeed on a later run

# ---- gbif_ids.status ---------------------------------------------------------
G_PENDING = "pending"             # no image attempted yet
G_PARTIAL = "partial"             # some work still possible (in the work queue)
G_DONE = "done"                   # every image succeeded
G_FAILED = "failed"               # all images terminal, not all succeeded

# ---- error_type values -------------------------------------------------------
ERR_RATE_LIMITED = "rate_limited"          # HTTP 429
ERR_TIMEOUT = "timeout"                    # connect/read timeout, HTTP 408
ERR_SERVER = "server_error"                # HTTP 5xx
ERR_CONNECTION = "connection_broken"       # dropped connection / IncompleteRead
ERR_TRUNCATED = "truncated"                # download shorter than Content-Length
ERR_MANIFEST = "manifest_error"            # IIIF manifest could not be parsed
ERR_INVALID_CONTENT = "invalid_content_type"   # server returned HTML/XML/text
ERR_NOT_IMAGE = "not_an_image"             # bytes downloaded but not decodable
ERR_NO_URL = "no_url"                      # no usable URL for this identifier
ERR_OTHER = "other"                        # anything uncategorised
ERR_LEGACY = "legacy_unverified_index"     # marker on imported processed_ids.txt

# Everything not in this set is treated as permanent (e.g. any "http_4xx").
TRANSIENT_ERRORS = {
    ERR_RATE_LIMITED, ERR_TIMEOUT, ERR_SERVER,
    ERR_CONNECTION, ERR_TRUNCATED, ERR_MANIFEST, ERR_OTHER,
}


def http_error_type(code):
    """Map an HTTP status code to an error_type string."""
    if code == 429:
        return ERR_RATE_LIMITED
    if code == 408:
        return ERR_TIMEOUT
    if 500 <= code <= 599:
        return ERR_SERVER
    return f"http_{code}"


def is_permanent(error_type):
    """True if a failure of this type is not worth retrying."""
    return error_type not in TRANSIENT_ERRORS


def status_for_error(error_type):
    """Pick the images.status value implied by an error_type."""
    return ST_FAILED_PERMANENT if is_permanent(error_type) else ST_FAILED_TRANSIENT


# ---- schema ------------------------------------------------------------------

_TABLES = [
    """CREATE TABLE IF NOT EXISTS images (
        gbif_id         INTEGER NOT NULL,
        img_index       INTEGER NOT NULL,   -- position in this ID's URL list
        url             TEXT    NOT NULL,
        host            TEXT,
        status          TEXT    NOT NULL DEFAULT 'pending',
        http_status     INTEGER,
        error_type      TEXT,
        error_detail    TEXT,               -- truncated message, for debugging
        file_path       TEXT,
        file_size       INTEGER,            -- bytes on disk after resize
        attempts        INTEGER NOT NULL DEFAULT 0,
        last_attempt_at TEXT,
        PRIMARY KEY (gbif_id, img_index)
    )""",
    """CREATE TABLE IF NOT EXISTS gbif_ids (
        gbif_id      INTEGER PRIMARY KEY,
        n_images     INTEGER NOT NULL DEFAULT 0,
        n_success    INTEGER NOT NULL DEFAULT 0,
        status       TEXT    NOT NULL DEFAULT 'pending',
        completed_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS hosts (
        host          TEXT PRIMARY KEY,
        error_count   INTEGER NOT NULL DEFAULT 0,
        blocked_until REAL                  -- epoch seconds; NULL when not blocked
    )""",
]

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_images_status ON images(status)",
    "CREATE INDEX IF NOT EXISTS idx_images_host   ON images(host)",
    "CREATE INDEX IF NOT EXISTS idx_images_error  ON images(error_type) "
    "WHERE error_type IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_gbif_status   ON gbif_ids(status)",
]


def create_tables(conn):
    for sql in _TABLES:
        conn.execute(sql)
    conn.commit()


def create_indexes(conn):
    for sql in _INDEXES:
        conn.execute(sql)
    conn.commit()


def apply_schema(conn):
    """Create tables and indexes if they do not already exist."""
    create_tables(conn)
    create_indexes(conn)


# ---- runtime handle ----------------------------------------------------------

class DownloadDB:
    """
    Thread-safe handle used by image_install_db.py during a run.

    One SQLite connection is shared by all worker threads and guarded by a
    single lock. The downloads themselves take seconds each, so lock contention
    on these short statements is negligible. WAL mode keeps writes durable
    without blocking the occasional reader.
    """

    def __init__(self, db_path=DEFAULT_DB_PATH, max_attempts=MAX_ATTEMPTS):
        self.path = db_path
        self.max_attempts = max_attempts
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=120)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=120000")
        apply_schema(self.conn)
        self.lock = threading.Lock()

    def close(self):
        with self.lock:
            self.conn.commit()
            self.conn.close()

    # -- work queue ------------------------------------------------------------

    def get_work_gbif_ids(self):
        """Return every gbifID that still has work to do, in ascending order."""
        with self.lock:
            cur = self.conn.execute(
                "SELECT gbif_id FROM gbif_ids WHERE status IN (?, ?) ORDER BY gbif_id",
                (G_PENDING, G_PARTIAL),
            )
            return [row[0] for row in cur.fetchall()]

    def get_images_for(self, gbif_id):
        """Return (img_index, url, host, status, attempts) rows for one gbifID."""
        with self.lock:
            cur = self.conn.execute(
                "SELECT img_index, url, host, status, attempts "
                "FROM images WHERE gbif_id=? ORDER BY img_index",
                (gbif_id,),
            )
            return cur.fetchall()

    # -- recording results -----------------------------------------------------

    def record_image_result(self, gbif_id, img_index, status, *, host=None,
                             http_status=None, error_type=None, error_detail=None,
                             file_path=None, file_size=None,
                             increment_attempts=True):
        """Write the outcome of one image attempt into the images table."""
        detail = (error_detail or "")[:500] or None
        delta = 1 if increment_attempts else 0
        with self.lock:
            self.conn.execute(
                "UPDATE images SET "
                "  status=?, host=COALESCE(?, host), http_status=?, "
                "  error_type=?, error_detail=?, file_path=?, file_size=?, "
                "  attempts=attempts+?, last_attempt_at=datetime('now') "
                "WHERE gbif_id=? AND img_index=?",
                (status, host, http_status, error_type, detail, file_path,
                 file_size, delta, gbif_id, img_index),
            )
            self.conn.commit()

    def finalize_gbif_id(self, gbif_id):
        """
        Recompute and store a gbifID's rolled-up status from its image rows.
        Returns the new status string.
        """
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, attempts FROM images WHERE gbif_id=?",
                (gbif_id,),
            ).fetchall()
            if not rows:
                return None

            n_success = sum(1 for s, _ in rows if s == ST_SUCCESS)

            def retryable(status, attempts):
                if status == ST_PENDING:
                    return True
                if status == ST_FAILED_TRANSIENT and attempts < self.max_attempts:
                    return True
                return False

            if n_success == len(rows):
                status = G_DONE
            elif any(retryable(s, a) for s, a in rows):
                status = G_PARTIAL
            else:
                status = G_FAILED

            self.conn.execute(
                "UPDATE gbif_ids SET n_success=?, status=?, "
                "completed_at=CASE WHEN ? IN (?, ?) THEN datetime('now') "
                "                  ELSE completed_at END "
                "WHERE gbif_id=?",
                (n_success, status, status, G_DONE, G_FAILED, gbif_id),
            )
            self.conn.commit()
            return status

    # -- host circuit-breaker state -------------------------------------------

    def load_host_state(self):
        """Return (error_counts, blocked_until) dicts to seed the in-memory state."""
        now = time.time()
        with self.lock:
            cur = self.conn.execute(
                "SELECT host, error_count, blocked_until FROM hosts"
            )
            error_counts, blocked_until = {}, {}
            for host, count, until in cur.fetchall():
                if count:
                    error_counts[host] = count
                if until and until > now:
                    blocked_until[host] = until
            return error_counts, blocked_until

    def save_host_state(self, error_counts, blocked_until):
        """Persist the in-memory circuit-breaker dicts so they survive a restart."""
        hosts = set(error_counts) | set(blocked_until)
        rows = [(h, error_counts.get(h, 0), blocked_until.get(h)) for h in hosts]
        if not rows:
            return
        with self.lock:
            self.conn.executemany(
                "INSERT INTO hosts(host, error_count, blocked_until) VALUES(?,?,?) "
                "ON CONFLICT(host) DO UPDATE SET "
                "  error_count=excluded.error_count, "
                "  blocked_until=excluded.blocked_until",
                rows,
            )
            self.conn.commit()

    # -- reporting helpers -----------------------------------------------------

    def gbif_status_counts(self):
        """Return {status: count} over the gbif_ids table."""
        with self.lock:
            return dict(self.conn.execute(
                "SELECT status, COUNT(*) FROM gbif_ids GROUP BY status"
            ).fetchall())
