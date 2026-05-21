# Deployment Guide — Image Downloader with SQLite Status Tracking

This guide covers deploying the GBIF herbarium image downloader after its switch
from flat-file checkpoints (`processed_ids.txt` / `failed_ids.txt`) to a
queryable SQLite status database.

There are two phases: **build the database once**, then **run (and re-run) the
downloader**. All commands assume the SCC and the `spring-2026-pyt` conda
environment.

---

## What changed

| Before | After |
|---|---|
| Progress in `processed_ids.txt` / `failed_ids.txt` (ID only, no reason) | Progress in `download_status.db` — every URL's outcome and *why* it failed |
| `multimedia.txt` re-read and re-grouped with pandas every run | Ingested into the DB once; later runs query the work queue |
| Failed IDs all retried blindly (or skipped) | Only transient failures retried (timeout/rate-limit/5xx/dropped connection), capped at 4 attempts |
| `analyze_image_progress.py` (slow, loads ~180 MB of text) | `status_report.py` (instant SQL queries) |
| ~1.4 GB run logs, ~134 MB warning spam | `WARNING`-level log only; warning spam suppressed |

The database lives **outside this git repo**, in the data directory, so it is
never committed:

- `download_status.db` (+ `-wal`, `-shm` companions) at
  `/projectnb/herbdl/data/GBIF-F25h/download_status.db`
- Estimated size after ingest: **~10–15 GB**

---

## Files

| File | Role |
|---|---|
| `init_download_db.py` / `init_download_db.sh` | One-time database builder (+ qsub wrapper) |
| `image_install_db.py` / `image_install_db.sh` | The downloader (+ qsub wrapper) |
| `status_report.py` | Progress reporting |
| `download_db.py` | Shared schema + DB helpers (imported, not run) |

> The original flat-file downloader is preserved as `image_install_parallel.py`
> (run via `image_install.sh`). It is independent of the database workflow
> described here and is kept only for reference / fallback.

---

## Phase 1 — Build the status database (once)

This step ingests `multimedia.txt`, imports already-completed downloads from
`processed_ids.txt`, and renames legacy `<id>.jpg` files to `<id>-00.jpg`.

It is heavy — it reads the ~59M-row `multimedia.txt` with pandas and renames up
to ~13.5M files. **Run it as a batch job, not on a login node.**

```bash
qsub -N init_download_db -l h_rt=12:00:00 -pe omp 16 -P herbdl \
     -m beas -M your_email@bu.edu init_download_db.sh
```

`init_download_db.sh` runs:

```bash
python init_download_db.py \
    --processed-file /projectnb/herbdl/workspaces/ljhao/herbdl/utils/processed_ids.txt
```

> **Important:** the production `processed_ids.txt` (~13.5M IDs) lives in
> ljhao's working directory, not in this repo. The wrapper already points there.
> If you build the DB by hand, pass that `--processed-file` path explicitly, or
> the legacy progress will not be imported.

**Options:**

| Command | Effect |
|---|---|
| `python init_download_db.py` | Build DB + import legacy progress |
| `python init_download_db.py --skip-legacy` | Build DB only (everything starts `pending`) |
| `python init_download_db.py --legacy-only` | Skip the ingest; only (re-)run the legacy import on an existing DB |
| `python init_download_db.py --reset` | Delete an existing DB and rebuild from scratch |

**Expected output** — a status breakdown, e.g.:

```
Final gbifID status counts:
  done       13,200,000
  partial       320,000
  pending    36,900,000
```

- `done` — every image for the gbifID is present
- `partial` — has an image already (legacy first image) but more to fetch
- `pending` — never attempted

Re-running is safe: file renames and database updates are idempotent
(already-renamed files are detected and reused). If the **ingest** fails partway,
re-run with `--reset`. If only the **legacy import** fails partway (e.g. it was
interrupted), re-run with `--legacy-only` — that finishes the import without
redoing the hour-long ingest.

---

## Phase 2 — Run the downloader

The downloader has no separate "resume" mode — every run reads the work queue
(`pending` + `partial` gbifIDs) from the database. Submit it as many times as
needed; each run continues where the last left off.

```bash
qsub -N image_install_db -l h_rt=48:00:00 -pe omp 16 -P herbdl \
     -m beas -M your_email@bu.edu image_install_db.sh
```

If the job hits its `h_rt` wall-clock limit, just submit it again — progress is
committed to the database continuously, and host cooldown / circuit-breaker
state is persisted between runs.

When the work queue is empty the script prints
`Nothing to download` and exits.

To point at a non-default database, pass `--db PATH` (edit `image_install_db.sh`).

---

## Phase 3 — Monitor progress

Run any time — it is read-only and returns in seconds:

```bash
python status_report.py
```

It prints (and writes `summary_YYYYMMDDHHMM.txt`): gbifID and per-image
progress, failures broken down by type, retry-attempt distribution, the worst
hosts, and circuit-breaker state.

The run log (`WARNING` and above) is at
`/projectnb/herbdl/logs/image_install_<timestamp>.log`.

Ad hoc queries:

```bash
sqlite3 /projectnb/herbdl/data/GBIF-F25h/download_status.db
```
```sql
-- count each kind of failure
SELECT error_type, COUNT(*) FROM images
WHERE status LIKE 'failed%' GROUP BY error_type ORDER BY 2 DESC;

-- URLs still worth retrying
SELECT gbif_id, url FROM images WHERE status='failed_transient' LIMIT 50;

-- hosts currently in cooldown
SELECT host, datetime(blocked_until,'unixepoch') FROM hosts
WHERE blocked_until > strftime('%s','now');
```

---

## How retries work

Each failure is classified into an `error_type`:

- **Permanent** — `http_404`, `http_401`, `http_403`, `http_410`,
  `invalid_content_type`, `not_an_image`, … → never retried.
- **Transient** — `timeout`, `rate_limited`, `server_error`,
  `connection_broken`, `truncated`, `manifest_error` → retried on later runs,
  up to **4 attempts** (`MAX_ATTEMPTS` in `download_db.py`), then they count
  toward the gbifID's `failed` status.

A gbifID leaves the work queue only when it is `done` (all images succeeded) or
`failed` (all images terminal, no retries left). To re-open exhausted transient
failures for another pass, raise `MAX_ATTEMPTS` or reset rows manually, e.g.:

```sql
UPDATE images SET status='pending', attempts=0
WHERE status='failed_transient';
UPDATE gbif_ids SET status='partial' WHERE status='failed';
```

---

## Caveats

- **Legacy first-image index is approximate.** For gbifIDs imported from
  `processed_ids.txt` that have more than one image, the existing file is
  assumed to be image index 0 and marked `error_type='legacy_unverified_index'`.
  The old downloader shuffled URLs, so the exact source URL is unknown. This is
  exact for the ~87% of gbifIDs that have only one image; for the rest it
  affects only metadata, not the image files.
- **Database size.** Expect ~10–15 GB. It sits in the data directory, not the
  repo. Ensure the `herbdl` project has the space.
- **Single job at a time.** SQLite (WAL mode) is fine for one job with 5 worker
  threads. Do not run multiple `image_install_db.sh` jobs against the same
  database concurrently.

---

## Rollback

The previous flat-file downloader still exists in
`/projectnb/herbdl/workspaces/ljhao/herbdl/utils/` and is unaffected by this
work. To revert this repo, use git (`git log` / `git revert`). The status
database is independent — deleting `download_status.db*` simply means Phase 1
must be re-run.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Status database not found` | Run Phase 1 first (`init_download_db.sh`). |
| `Database already exists` from the builder | Intended guard — `--reset` to rebuild, or `--legacy-only` to just (re-)run the legacy import. |
| `database is locked` | The builder now uses WAL mode (readers do not block the writer) and a 120 s busy timeout, so this should not recur. If the legacy import was interrupted by it, finish it with `init_download_db.py --legacy-only`. Still avoid running two writers against one DB. |
| Legacy import interrupted partway | Re-run `init_download_db.py --legacy-only` — it is idempotent and skips the hour-long ingest. |
| Builder runs out of memory | `multimedia.txt` is large; request more memory (e.g. a larger `-pe omp` slot count). |
| Legacy progress not imported | `--processed-file` was not pointed at ljhao's `processed_ids.txt`. |
