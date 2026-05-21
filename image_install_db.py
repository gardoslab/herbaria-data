"""
Image install script: download herbarium specimen images from a GBIF
multimedia.txt file.

Downloads ALL images for each gbifID. Each source URL (a GBIF "identifier") is
saved as one file with an index suffix: <gbifID>-00.jpg, <gbifID>-01.jpg, ...
A gbifID is marked 'done' only once every one of its images has succeeded.

Status tracking
---------------
Per-image and per-gbifID status lives in a SQLite database (download_status.db,
see download_db.py) instead of the old processed_ids.txt / failed_ids.txt flat
files. Build the database once with init_download_db.py before the first run.

The database lets the script:
  * resume without re-reading the 59M-row multimedia.txt every run,
  * retry only transient failures (timeout / rate-limit / 5xx / dropped
    connection), capped at MAX_ATTEMPTS, and never re-hammer permanent 404s,
  * record *why* each download failed so failures are queryable afterwards
    (see status_report.py).

Accurate as of May 2026.
"""

import os
import time
import random
import logging
import threading
import datetime as dt
from argparse import ArgumentParser
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
import requests as req
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PIL import UnidentifiedImageError

from notifications import send_notification
from image_utils import get_file_size_in_mb, resize_with_aspect_ratio
import download_db as ddb
from download_db import DownloadDB

# verify=False is needed because many herbarium hosts have broken TLS certs.
# Suppress the resulting per-request warning so it does not flood the .e log
# (it previously produced ~134 MB of InsecureRequestWarning spam per run).
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---- configuration -----------------------------------------------------------

INSTALL_PATH = "/projectnb/herbdl/data/GBIF-F25h"
LOG_DIR = "/projectnb/herbdl/logs"

MAX_WORKERS = 5
WORK_CHUNK = 20_000          # gbifIDs submitted to the pool at a time
MIN_IMAGE_MB = 0.01          # files smaller than this are treated as invalid

HOST_COOLDOWN_DEFAULT = 30 * 60
HOST_COOLDOWN_TIMEOUT = 60 * 60
HOST_ERROR_THRESHOLD = 500   # circuit breaker: skip a host after this many errors

# ---- in-memory host circuit-breaker state (seeded from / saved to the DB) ----

host_block_until = {}
host_error_counts = {}
host_lock = threading.Lock()
circuit_breaker_lock = threading.Lock()
counter_lock = threading.Lock()

n_installed = 0

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

session = req.Session()
retry_strategy = Retry(
    total=2,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

logger = logging.getLogger(__name__)


# ---- paths -------------------------------------------------------------------

def get_hierarchical_path(base_dir, gbif_id, suffix, ext=".jpg"):
    """
    Build a hierarchical storage path to avoid millions of files in one dir.
    suffix: image index suffix, e.g. '-00', '-01'.
    Example: gbifID=1057161997, suffix='-00' -> <base>/105/716/1057161997-00.jpg
    """
    stem = str(gbif_id)
    prefix1 = stem[:3] if len(stem) >= 3 else stem
    prefix2 = stem[3:6] if len(stem) >= 6 else "000"
    dest_dir = os.path.join(base_dir, prefix1, prefix2)
    os.makedirs(dest_dir, exist_ok=True)
    return os.path.join(dest_dir, f"{stem}{suffix}{ext}")


# ---- host circuit breaker / cooldown -----------------------------------------

def _host_from_url(url):
    return urlparse(url).netloc.split(":")[0]


def is_host_blocked(url):
    host = _host_from_url(url)
    now = time.time()
    with host_lock:
        until = host_block_until.get(host)
        if until and now < until:
            return True
        if until and now >= until:
            del host_block_until[host]
    return False


def is_host_circuit_broken(url):
    host = _host_from_url(url)
    with circuit_breaker_lock:
        return host_error_counts.get(host, 0) >= HOST_ERROR_THRESHOLD


def increment_host_errors(url, is_rate_limit=False):
    # Rate limiting is handled by a timed cooldown, not the permanent breaker.
    if is_rate_limit:
        return
    host = _host_from_url(url)
    with circuit_breaker_lock:
        host_error_counts[host] = host_error_counts.get(host, 0) + 1
        count = host_error_counts[host]
        if count == HOST_ERROR_THRESHOLD:
            logger.error(f"CIRCUIT BREAKER: host '{host}' reached "
                         f"{HOST_ERROR_THRESHOLD} errors; skipping it from now on.")


def block_host(url, retry_after=None, timeout_issue=False):
    host = _host_from_url(url)
    now = time.time()
    seconds = HOST_COOLDOWN_TIMEOUT if timeout_issue else HOST_COOLDOWN_DEFAULT
    if retry_after and not timeout_issue:
        try:
            seconds = int(retry_after)
        except (TypeError, ValueError):
            try:
                from email.utils import parsedate_to_datetime
                dt_retry = parsedate_to_datetime(retry_after)
                seconds = max(0, (dt_retry - dt.datetime.now(dt.timezone.utc))
                              .total_seconds())
            except Exception:
                seconds = HOST_COOLDOWN_DEFAULT
    with host_lock:
        host_block_until[host] = now + seconds
    reason = "timeout issues" if timeout_issue else "rate limiting"
    logger.warning(f"Blocking host '{host}' due to {reason} for ~{int(seconds)}s.")


# ---- IIIF manifests ----------------------------------------------------------

def extract_image_from_iiif_manifest(manifest_url, gbif_id):
    """
    Fetch a IIIF manifest and return (image_urls, error_type).

    image_urls is an ordered list of direct image URLs (highest resolution
    first). On failure image_urls is empty and error_type explains why, so the
    caller can decide whether the manifest is worth retrying.
    """
    try:
        response = session.get(
            manifest_url,
            headers={"User-Agent": random.choice(user_agents),
                     "Accept": "application/json"},
            timeout=120,
        )
        if response.status_code != 200:
            logger.warning(f"IIIF manifest {gbif_id}: HTTP {response.status_code}")
            return [], ddb.http_error_type(response.status_code)

        manifest = response.json()
        image_urls = []
        for item in manifest.get("items", []):
            if item.get("type") != "Canvas":
                continue
            for anno_page in item.get("items", []):
                if anno_page.get("type") != "AnnotationPage":
                    continue
                for anno in anno_page.get("items", []):
                    body = anno.get("body")
                    if not isinstance(body, dict):
                        continue
                    for service in body.get("service", []):
                        base_url = service.get("id")
                        if base_url:
                            # Highest resolution first; caller stops at the
                            # first that succeeds, so only one file is saved.
                            image_urls.append(f"{base_url}/full/1600,/0/default.jpg")
                            image_urls.append(f"{base_url}/full/1200,/0/default.jpg")
                            image_urls.append(f"{base_url}/full/800,/0/default.jpg")

        if not image_urls:
            return [], ddb.ERR_MANIFEST
        return image_urls, None

    except (ConnectTimeout, ReadTimeout, Timeout) as e:
        logger.warning(f"IIIF manifest {gbif_id}: timeout {e}")
        return [], ddb.ERR_TIMEOUT
    except Exception as e:
        logger.warning(f"IIIF manifest {gbif_id}: parse error {e}")
        return [], ddb.ERR_MANIFEST


# ---- downloading -------------------------------------------------------------

def _rm(path):
    try:
        os.remove(path)
    except OSError:
        pass


def download_one_url(gbif_id, image_url, local_path):
    """
    Download a single URL to local_path, atomically.

    Bytes are streamed to a .tmp file, length-checked against Content-Length,
    then renamed into place -- so a dropped connection never leaves a corrupt
    file behind. Returns a result dict with keys: ok, size, http_status,
    error_type, error_detail, host.
    """
    host = _host_from_url(image_url)
    tmp_path = local_path + ".tmp"

    def fail(error_type, detail, http_status=None):
        return {"ok": False, "size": None, "http_status": http_status,
                "error_type": error_type, "error_detail": detail, "host": host}

    try:
        time.sleep(random.uniform(0.2, 0.8))
        with session.get(
            image_url,
            stream=True,
            verify=False,
            headers={
                "User-Agent": random.choice(user_agents),
                "Connection": "keep-alive",
                "Referer": "https://scc-ondemand1.bu.edu/",
            },
            timeout=180,
        ) as resp:
            status = resp.status_code

            if status == 429:
                increment_host_errors(image_url, is_rate_limit=True)
                block_host(image_url, resp.headers.get("Retry-After"))
                return fail(ddb.ERR_RATE_LIMITED, "HTTP 429", status)

            if status != 200:
                increment_host_errors(image_url)
                return fail(ddb.http_error_type(status), f"HTTP {status}", status)

            ctype = (resp.headers.get("Content-Type") or "").lower()
            if ctype and any(bad in ctype for bad in
                             ("text/html", "text/plain", "application/xml")):
                increment_host_errors(image_url)
                return fail(ddb.ERR_INVALID_CONTENT, f"Content-Type: {ctype}", status)

            expected = resp.headers.get("Content-Length")
            written = 0
            with open(tmp_path, "wb") as out:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        out.write(chunk)
                        written += len(chunk)

            if expected is not None:
                try:
                    if int(expected) != written:
                        _rm(tmp_path)
                        return fail(ddb.ERR_TRUNCATED,
                                    f"expected {expected} bytes, got {written}",
                                    status)
                except ValueError:
                    pass

            if written < 1024:
                _rm(tmp_path)
                return fail(ddb.ERR_TRUNCATED, f"only {written} bytes", status)

            os.replace(tmp_path, local_path)
            return {"ok": True, "size": written, "http_status": status,
                    "error_type": None, "error_detail": None, "host": host}

    except (ConnectTimeout, ReadTimeout, Timeout) as e:
        _rm(tmp_path)
        block_host(image_url, timeout_issue=True)
        return fail(ddb.ERR_TIMEOUT, str(e))
    except (ConnectionError, req.exceptions.ChunkedEncodingError) as e:
        # ChunkedEncodingError covers IncompleteRead -- a connection dropped
        # mid-download, which leaves only a partial .tmp file.
        _rm(tmp_path)
        increment_host_errors(image_url)
        return fail(ddb.ERR_CONNECTION, str(e))
    except Exception as e:
        _rm(tmp_path)
        increment_host_errors(image_url)
        return fail(ddb.ERR_OTHER, str(e))


def resize_image(gbif_id, local_path):
    changed, new_size = resize_with_aspect_ratio(
        local_path, local_path, max_size=1024, format="JPEG", quality=85)
    if changed:
        logger.info(f"Resized {gbif_id} to {new_size} at {local_path}")


def resolve_and_download(gbif_id, identifier_url, local_path):
    """
    Download the image for one source identifier (one img_index) and save it as
    exactly one file at local_path.

    For a plain URL there is one candidate. For a IIIF manifest the manifest is
    expanded into resolution variants and tried highest-first; the first success
    wins, so still only one file is saved per identifier.

    Returns a result dict with keys: outcome ('success' | 'failed' |
    'deferred'), db_status, http_status, error_type, error_detail, host,
    file_size. 'deferred' means every candidate host was blocked/circuit-broken,
    so the image was not really attempted and should stay 'pending'.
    """
    if "/manifest" in identifier_url or identifier_url.endswith(".json"):
        candidates, manifest_err = extract_image_from_iiif_manifest(
            identifier_url, gbif_id)
        if not candidates:
            return {"outcome": "failed",
                    "db_status": ddb.status_for_error(manifest_err),
                    "http_status": None, "error_type": manifest_err,
                    "error_detail": "IIIF manifest yielded no image URLs",
                    "host": _host_from_url(identifier_url), "file_size": None}
    else:
        candidates = [identifier_url]

    # Deduplicate while preserving the highest-resolution-first order.
    seen, ordered = set(), []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            ordered.append(url)

    failures = []
    attempted_any = False
    for url in ordered:
        if is_host_circuit_broken(url) or is_host_blocked(url):
            continue
        attempted_any = True
        result = download_one_url(gbif_id, url, local_path)
        if result["ok"]:
            try:
                resize_image(gbif_id, local_path)
            except (OSError, UnidentifiedImageError) as e:
                _rm(local_path)
                return {"outcome": "failed", "db_status": ddb.ST_FAILED_PERMANENT,
                        "http_status": result["http_status"],
                        "error_type": ddb.ERR_NOT_IMAGE,
                        "error_detail": str(e), "host": result["host"],
                        "file_size": None}
            try:
                size = os.path.getsize(local_path)
            except OSError:
                size = result["size"]
            return {"outcome": "success", "db_status": ddb.ST_SUCCESS,
                    "http_status": 200, "error_type": None,
                    "error_detail": None, "host": result["host"],
                    "file_size": size}
        failures.append(result)

    if not attempted_any:
        # Every candidate's host was blocked -- leave the image 'pending'.
        return {"outcome": "deferred"}

    # Prefer a transient failure as the recorded reason: if any candidate could
    # still succeed later, the whole identifier is worth retrying.
    transient = [f for f in failures if not ddb.is_permanent(f["error_type"])]
    chosen = transient[0] if transient else failures[0]
    db_status = ddb.ST_FAILED_TRANSIENT if transient else ddb.ST_FAILED_PERMANENT
    return {"outcome": "failed", "db_status": db_status,
            "http_status": chosen["http_status"],
            "error_type": chosen["error_type"],
            "error_detail": chosen["error_detail"],
            "host": chosen["host"], "file_size": None}


# ---- per-gbifID processing ---------------------------------------------------

def process_id(db, gbif_id, total_to_install):
    """Download every not-yet-done image for one gbifID and update the DB."""
    global n_installed
    images = db.get_images_for(gbif_id)

    for img_index, url, _host, status, attempts in images:
        # Skip images that are already finished or have exhausted their retries.
        if status == ddb.ST_SUCCESS:
            continue
        if status == ddb.ST_FAILED_PERMANENT:
            continue
        if status == ddb.ST_FAILED_TRANSIENT and attempts >= db.max_attempts:
            continue

        suffix = f"-{img_index:02d}"
        local_path = get_hierarchical_path(INSTALL_PATH, gbif_id, suffix)

        # If a valid file is already on disk, record it without downloading.
        if os.path.exists(local_path):
            try:
                size_mb = get_file_size_in_mb(local_path)
            except OSError:
                size_mb = 0.0
            if size_mb >= MIN_IMAGE_MB:
                db.record_image_result(
                    gbif_id, img_index, ddb.ST_SUCCESS,
                    host=_host_from_url(url), http_status=200,
                    file_path=local_path, file_size=int(size_mb * 1024 * 1024),
                    increment_attempts=False)
                continue

        result = resolve_and_download(gbif_id, url, local_path)
        if result["outcome"] == "deferred":
            continue  # host blocked; leave 'pending' for a later run

        db.record_image_result(
            gbif_id, img_index, result["db_status"],
            host=result.get("host"), http_status=result.get("http_status"),
            error_type=result.get("error_type"),
            error_detail=result.get("error_detail"),
            file_path=local_path if result["outcome"] == "success" else None,
            file_size=result.get("file_size"))

        if result["outcome"] == "success":
            with counter_lock:
                n_installed += 1
                current = n_installed
            if current % 50000 == 0:
                send_notification(
                    "Image Installation",
                    f"Installed {current} images this run "
                    f"(work queue: {total_to_install} gbifIDs).")
                logger.warning(f"Installed {current} images this run.")

    db.finalize_gbif_id(gbif_id)


# ---- main --------------------------------------------------------------------

def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("-c", "--country", dest="country",
                        help="(Unsupported) country filter -- multimedia.txt "
                             "has no countryCode column; ignored.")
    parser.add_argument("--db", default=ddb.DEFAULT_DB_PATH,
                        help=f"Status database path (default: {ddb.DEFAULT_DB_PATH})")
    args = parser.parse_args()

    if args.country:
        print("WARNING: -c/--country is ignored; the work queue comes from the "
              "database and multimedia.txt has no countryCode column.")

    today = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    os.makedirs(LOG_DIR, exist_ok=True)
    # File log captures WARNING and above only. Routine per-image successes are
    # recorded in the database, not the log -- this keeps the log from growing
    # to the ~1.4 GB seen with INFO-level logging.
    logging.basicConfig(filename=f"{LOG_DIR}/image_install_{today}.log",
                        level=logging.WARNING, filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    if not os.path.exists(args.db):
        raise SystemExit(
            f"Status database not found: {args.db}\n"
            f"Build it once first:  python init_download_db.py")

    db = DownloadDB(args.db)

    # Seed the in-memory circuit breaker from the last run's host stats.
    saved_errors, saved_blocks = db.load_host_state()
    host_error_counts.update(saved_errors)
    host_block_until.update(saved_blocks)
    print(f"Loaded host state: {len(saved_errors)} hosts with errors, "
          f"{len(saved_blocks)} currently blocked.")

    work = db.get_work_gbif_ids()
    total_to_install = len(work)
    print(f"gbifIDs with work to do: {total_to_install}")
    if total_to_install == 0:
        print("Nothing to download. All gbifIDs are 'done' or terminally 'failed'.")
        db.close()
        return

    send_notification("Image Installation",
                      f"Starting run: {total_to_install} gbifIDs to process.")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for start in range(0, total_to_install, WORK_CHUNK):
                chunk = work[start:start + WORK_CHUNK]
                futures = [executor.submit(process_id, db, gid, total_to_install)
                           for gid in chunk]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker error: {e}")
                # Persist circuit-breaker state periodically so a killed job
                # (e.g. qsub h_rt limit) does not lose it.
                db.save_host_state(host_error_counts, host_block_until)
                print(f"  processed {min(start + WORK_CHUNK, total_to_install)}"
                      f"/{total_to_install} gbifIDs", end="\r")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user; saving state and exiting.")
    finally:
        db.save_host_state(host_error_counts, host_block_until)
        counts = db.gbif_status_counts()
        broken = sum(1 for c in host_error_counts.values()
                     if c >= HOST_ERROR_THRESHOLD)
        logger.warning(f"Run finished. Images installed this run: {n_installed}. "
                       f"gbifID status: {counts}. Circuit-broken hosts: {broken}.")
        for host, count in sorted(host_error_counts.items(),
                                  key=lambda x: x[1], reverse=True)[:10]:
            logger.warning(f"  host errors: {host}: {count}")
        db.close()

    print(f"\nDone. Images installed this run: {n_installed}")
    print(f"gbifID status: {counts}")


if __name__ == "__main__":
    main()
