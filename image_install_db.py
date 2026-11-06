"""
Image install script: download herbarium specimen images from a GBIF
multimedia.txt file.

Downloads one file per *distinct image* of each gbifID. A IIIF manifest and the
resolution variants of one specimen photo are treated as a single image (see
download_db.canonical_image_key) -- so a record listed in multimedia.txt as
"manifest + 300px + 1600px" produces ONE file, not three. Files are named
<gbifID>-00.jpg, <gbifID>-01.jpg, ... A gbifID is 'done' only once every one of
its distinct images has succeeded.

Status tracking
---------------
Per-image and per-gbifID status lives in a SQLite database (download_status.db,
see download_db.py). Build it once with init_download_db.py before the first run.

The database lets the script:
  * resume without re-reading the 59M-row multimedia.txt every run,
  * retry only transient failures (timeout / rate-limit / 5xx / dropped
    connection), capped at MAX_ATTEMPTS, and never re-hammer permanent 404s,
  * record *why* each download failed so failures are queryable afterwards.

Non-JPEG handling
-----------------
TIFF/PNG/etc. are decoded by Pillow and saved as resized JPEG like everything
else. A file Pillow cannot decode but that is a real image format (camera-raw
DNG) is kept as-is (<gbifID>-NN.dng) and flagged 'raw_unprocessed' for a later
conversion pass -- it is not discarded. A URL that returns an HTML/text page
(e.g. "direct download no longer supported") is recorded as
'invalid_content_type' with the page text captured for follow-up.

Accurate as of May 2026.
"""

import os
import re
import sys
import time
import random
import logging
import threading
import datetime as dt
from argparse import ArgumentParser
from urllib.parse import urlparse, urlsplit, urlunsplit
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
import requests as req
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
from requests.adapters import HTTPAdapter
from urllib3.exceptions import NameResolutionError, NewConnectionError, ProtocolError
from urllib3.util.retry import Retry
from PIL import UnidentifiedImageError

from notifications import send_notification
from image_utils import get_file_size_in_mb, resize_with_aspect_ratio
import download_db as ddb
from download_db import DownloadDB

# verify=False is needed because many herbarium hosts have broken TLS certs.
# Suppress the resulting per-request warning so it does not flood the .e log.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---- configuration -----------------------------------------------------------

INSTALL_PATH = "/projectnb/herbdl/data/GBIF-F25h"
LOG_DIR = "/projectnb/herbdl/logs"

MAX_WORKERS = 5
WORK_CHUNK = 20_000          # gbifIDs submitted to the pool at a time
MIN_IMAGE_MB = 0.01          # files smaller than this are treated as invalid

HOST_COOLDOWN_DEFAULT = 30 * 60
HOST_COOLDOWN_TIMEOUT = 60 * 60
HOST_ERROR_THRESHOLD = 500   # circuit breaker: min errors before rate is judged
# 500 is the minimum-sample floor, not an automatic skip: past it, both the
# in-memory breaker (is_host_circuit_broken) and the persistent quarantine
# (download_db.quarantine_if_unhealthy) judge a host by its failure RATE, so a
# big-but-healthy source is spared and a released host is NOT re-skipped just for
# carrying its old ~500 error_count. The two paths share this floor -- and the
# QUARANTINE_FAILURE_RATE below -- so they cannot drift apart; keep the assert.
QUARANTINE_THRESHOLD = ddb.QUARANTINE_ERROR_THRESHOLD
assert QUARANTINE_THRESHOLD == HOST_ERROR_THRESHOLD

# Past the threshold, a host is judged again every this many further errors
# (600, 700, ...), not only once at 500. Otherwise a host that crossed 500 while
# healthy -- or was released from quarantine still carrying its old >500
# error_count -- is never re-checked if it goes bad. The spacing keeps the
# quarantine check (a DB read, plus a write when it quarantines) rare.
HOST_ERROR_RECHECK_EVERY = 100

# Request timeouts, passed to requests as a (connect, read) tuple. A host that
# has not accepted the connection within CONNECT_TIMEOUT is treated as
# unreachable; the read timeouts (longest wait for data once connected) stay
# long so a slow but working host is not cut off.
CONNECT_TIMEOUT = 20
IMAGE_READ_TIMEOUT = 180
MANIFEST_READ_TIMEOUT = 120

# Extensions under which an undecodable-but-real image is kept for later.
RAW_EXTS = (".dng", ".nef", ".cr2", ".cr3", ".arw", ".raf", ".orf", ".rw2",
            ".tif", ".raw")

# ---- in-memory host circuit-breaker state (seeded from / saved to the DB) ----

host_block_until = {}
host_error_counts = {}
host_success_counts = {}   # per-host succeeded downloads, for the failure-rate breaker
host_lock = threading.Lock()
circuit_breaker_lock = threading.Lock()
counter_lock = threading.Lock()

# DownloadDB handle, set in main(). increment_host_errors() uses it to persist a
# quarantine flag when a host trips the threshold; None outside a real run.
quarantine_db = None

n_installed = 0

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

session = req.Session()
retry_strategy = Retry(
    total=2,
    # Never retry a failed connection. Within one CONNECT_TIMEOUT the kernel has
    # already re-sent the SYN several times, so a host that stayed silent is
    # down, not flaky -- and each retry cost another full timeout (it was 3 x
    # 180 s + backoff, ~9 min per URL). A dead host now costs one
    # CONNECT_TIMEOUT per URL, ~40 s for an http:// URL including its https://
    # fallback (per address: urllib3 tries each of a host's IPs in turn).
    # Read errors and 5xx responses are still retried under `total`.
    connect=0,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

logger = logging.getLogger(__name__)


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


# ---- paths -------------------------------------------------------------------

def image_path(gbif_id, image_no, ext):
    """
    Storage path for one image: <base>/<p1>/<p2>/<gbifID>-NN<ext> (no mkdir).
    p1 = first 3 digits of the gbifID, p2 = digits 4-6.
    """
    stem = str(gbif_id)
    prefix1 = stem[:3] if len(stem) >= 3 else stem
    prefix2 = stem[3:6] if len(stem) >= 6 else "000"
    return os.path.join(INSTALL_PATH, prefix1, prefix2,
                        f"{stem}-{image_no:02d}{ext}")


def _rm(path):
    try:
        os.remove(path)
    except OSError:
        pass


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


def _breaker_trips(errors, successes):
    """
    The circuit-breaker rule, shared by is_host_circuit_broken and the CIRCUIT
    BREAKER log: past the HOST_ERROR_THRESHOLD sample floor AND failing at a
    rate over QUARANTINE_FAILURE_RATE.
    """
    if errors < HOST_ERROR_THRESHOLD:
        return False
    total = errors + successes
    return total > 0 and errors / total > ddb.QUARANTINE_FAILURE_RATE


def is_host_circuit_broken(url):
    """
    Skip a host only when it is both past the minimum-sample floor AND failing
    at a bad RATE. A host under HOST_ERROR_THRESHOLD errors is never broken (too
    few samples to judge); past it, a big-but-healthy source (500 errors against
    millions of successes) stays under QUARANTINE_FAILURE_RATE and is spared,
    while a dead one (500 errors, no successes) trips.
    """
    host = _host_from_url(url)
    with circuit_breaker_lock:
        return _breaker_trips(host_error_counts.get(host, 0),
                              host_success_counts.get(host, 0))


def increment_host_errors(url, is_rate_limit=False):
    # Rate limiting is handled by a timed cooldown, not the permanent breaker.
    if is_rate_limit:
        return
    host = _host_from_url(url)
    with circuit_breaker_lock:
        host_error_counts[host] = host_error_counts.get(host, 0) + 1
        count = host_error_counts[host]
        # Judge the host at the threshold and every HOST_ERROR_RECHECK_EVERY
        # errors after it. Counts rise by one under this lock, so exactly one
        # thread sees each checkpoint -- including for a host seeded from the DB
        # already past the threshold (seeded at 501 -> first re-check at 600).
        past = count - HOST_ERROR_THRESHOLD
        checkpoint = past >= 0 and past % HOST_ERROR_RECHECK_EVERY == 0
        successes = host_success_counts.get(host, 0)
        if checkpoint and _breaker_trips(count, successes):
            logger.error(f"CIRCUIT BREAKER: host '{host}' at {count} errors vs "
                         f"{successes} successes (failure rate "
                         f"{count / (count + successes):.2f}); skipping it.")

    # Persistently quarantine a host at the same checkpoints, so its gbifIDs are
    # pushed to the back of the work queue on later runs. Done outside
    # circuit_breaker_lock so the DB work does not block other workers' error
    # accounting -- and quarantine_if_unhealthy takes the DB lock itself, so it
    # must not be called with that held either (downloads never hold it).
    #
    # quarantine_if_unhealthy checks the failure RATE, not just the raw count, so
    # a big-but-healthy source (nmnh) is spared while a dead one (oxalis) is
    # caught. Only log QUARANTINED when it actually quarantined the host.
    if checkpoint and quarantine_db is not None:
        try:
            if quarantine_db.quarantine_if_unhealthy(host, error_count=count):
                logger.error(f"QUARANTINED host '{host}' after {count} errors; its "
                             f"gbifIDs will be deprioritised on future runs.")
        except Exception as e:
            logger.error(f"Failed to quarantine host '{host}': {e}")


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


# ---- http:// -> https:// fallback --------------------------------------------
#
# Some hosts have closed port 80 but serve the same paths over HTTPS (e.g.
# sweetgum.nybg.org, ipt.nature.ca), while their GBIF records still give
# http:// URLs. So an http:// URL that fails to connect is retried once as
# https://.

# Hosts whose https:// fallback has worked this run. Their later http:// URLs go
# straight to https:// -- otherwise every image on such a host would first wait
# out CONNECT_TIMEOUT on the closed port. In memory only, so each run
# re-discovers them at the cost of one timeout per host. Guarded by host_lock.
https_upgrade_hosts = set()


def https_fallback_url(url):
    """
    The https:// twin of an http:// URL, or None when there is none to try: the
    URL is not http://, or names an explicit port (https:// on that same port
    would be a different service).
    """
    parts = urlsplit(url)
    if parts.scheme.lower() != "http":
        return None
    try:
        if parts.port is not None:
            return None
    except ValueError:      # malformed port; the request itself will fail
        return None
    return urlunsplit(parts._replace(scheme="https"))


def _failed_to_connect(exc):
    """
    True if a request exception means the server never answered: a connect
    timeout, a refused or unreachable connection, or the connection dropped
    before any reply. False for a DNS failure (https:// looks up the same name),
    a read timeout (the host is up, just slow), and errors after a response
    began, e.g. a manifest body stalling mid-download, which requests also
    raises as ConnectionError.
    """
    if isinstance(exc, ConnectTimeout):
        return True
    if not isinstance(exc, ConnectionError):
        return False
    reason = exc.args[0] if exc.args else None
    reason = getattr(reason, "reason", reason)   # unwrap urllib3's MaxRetryError
    if isinstance(reason, NameResolutionError):
        return False
    return isinstance(reason, (NewConnectionError, ProtocolError))


def get_with_https_fallback(url, **kwargs):
    """
    session.get(url, **kwargs), retrying an http:// URL once as https:// if it
    fails to connect. Returns (response, via_https).

    Whatever comes back is the caller's to classify, so the call is counted as
    one attempt however many requests it made. If the https:// retry fails too,
    one exception carries both errors: a ConnectTimeout if either attempt timed
    out (so the caller still applies the timeout cooldown -- each cost
    CONNECT_TIMEOUT), else the original http:// error's type.
    """
    https_url = https_fallback_url(url)
    if https_url is None:
        return session.get(url, **kwargs), False
    with host_lock:
        upgraded = _host_from_url(url) in https_upgrade_hosts
    if upgraded:
        return session.get(https_url, **kwargs), True

    try:
        return session.get(url, **kwargs), False
    except ConnectionError as e:          # ConnectTimeout is a ConnectionError
        if not _failed_to_connect(e):
            raise
        http_error = e

    try:
        return session.get(https_url, **kwargs), True
    except ConnectionError as e:
        timed_out = isinstance(http_error, ConnectTimeout) or isinstance(e, ConnectTimeout)
        error_cls = ConnectTimeout if timed_out else type(http_error)
        raise error_cls(f"{http_error} [https:// fallback also failed: {e}]",
                        request=http_error.request) from e


def _note_https_fallback_worked(url):
    """
    Record that an http:// URL's https:// fallback gave a good result, so the
    host's later http:// URLs go straight to https:// this run. Logged once per
    host rather than per image, to keep the log small.
    """
    host = _host_from_url(url)
    with host_lock:
        if host in https_upgrade_hosts:
            return
        https_upgrade_hosts.add(host)
    logger.warning(f"HTTPS fallback: '{host}' does not accept http:// connections "
                   f"but https:// works; fetching its http:// URLs over https:// "
                   f"for the rest of this run.")


# ---- IIIF manifests ----------------------------------------------------------

def is_manifest_url(url):
    low = url.lower()
    return "/manifest" in low or low.endswith(".json")


def extract_image_from_iiif_manifest(manifest_url, gbif_id):
    """
    Fetch a IIIF manifest and return (image_urls, error_type).

    image_urls is an ordered list of direct image URLs (highest resolution
    first). On failure image_urls is empty and error_type explains why.
    """
    try:
        # Manifests are generated on the fly (e.g. oxalis / Meise Botanic
        # Garden), so they are heavier for the host than a static image.
        # Throttle these requests a bit more than image downloads to
        # avoid overwhelming the server and getting connections dropped.
        time.sleep(random.uniform(0.5, 1.5))
        response, via_https = get_with_https_fallback(
            manifest_url,
            headers={"User-Agent": random.choice(user_agents),
                     "Accept": "application/json"},
            timeout=(CONNECT_TIMEOUT, MANIFEST_READ_TIMEOUT),
        )
        if response.status_code != 200:
            logger.warning(f"IIIF manifest {gbif_id}: HTTP {response.status_code}")
            # Count the failure against the host so a manifest host that fails
            # everything (e.g. oxalis) eventually trips the breaker / quarantine
            # -- previously only download_one_url incremented, so manifest hosts
            # never got there. 429 is a cooldown, not a permanent-breaker error.
            if response.status_code == 429:
                increment_host_errors(manifest_url, is_rate_limit=True)
                block_host(manifest_url, response.headers.get("Retry-After"))
            else:
                increment_host_errors(manifest_url)
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
            increment_host_errors(manifest_url)
            return [], ddb.ERR_MANIFEST
        if via_https:
            _note_https_fallback_worked(manifest_url)
        return image_urls, None

    except (ConnectTimeout, ReadTimeout, Timeout) as e:
        logger.warning(f"IIIF manifest {gbif_id}: timeout {e}")
        increment_host_errors(manifest_url)
        block_host(manifest_url, timeout_issue=True)
        return [], ddb.ERR_TIMEOUT
    except Exception as e:
        # A dropped connection under load (oxalis) lands here as well as a real
        # parse error; both count against the host.
        logger.warning(f"IIIF manifest {gbif_id}: parse error {e}")
        increment_host_errors(manifest_url)
        return [], ddb.ERR_MANIFEST


# ---- non-image response detection -------------------------------------------

_NON_IMAGE_CTYPES = ("text/html", "text/plain", "text/xml",
                     "application/xml", "application/json", "ld+json")


def _is_non_image_ctype(ctype):
    return bool(ctype) and any(t in ctype for t in _NON_IMAGE_CTYPES)


def _looks_like_text(data):
    """Heuristic: do the first bytes look like an HTML / XML / JSON document?"""
    if not data:
        return False
    head = data.lstrip()[:64].lower()
    return head.startswith((b"<!doctype", b"<html", b"<head", b"<body",
                            b"<?xml", b"{", b"["))


def _read_bounded(resp, limit=16384):
    """Read at most `limit` bytes of a streamed response body."""
    raw = b""
    for chunk in resp.iter_content(chunk_size=8192):
        raw += chunk
        if len(raw) >= limit:
            break
    return raw[:limit]


def _join_bounded(stream, limit=16384):
    """Drain at most `limit` bytes from an iter_content generator."""
    raw = b""
    for chunk in stream:
        raw += chunk
        if len(raw) >= limit:
            break
    return raw[:limit]


def _html_to_text(raw):
    """Strip an HTML/text body down to readable text for capture in the DB."""
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        text = str(raw)
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1800]


def raw_keep_extension(content_type, url):
    """
    Extension under which to keep an image Pillow could not decode, or None to
    discard it. Camera-raw DNG is the main case -- kept for a later conversion
    pass rather than lost.
    """
    ct = (content_type or "").lower()
    low = (url or "").lower().split("?")[0]
    if "dng" in ct or low.endswith(".dng"):
        return ".dng"
    for ext in (".nef", ".cr2", ".cr3", ".arw", ".raf", ".orf", ".rw2"):
        if low.endswith(ext):
            return ext
    if "tiff" in ct or "tif" in ct or low.endswith((".tif", ".tiff")):
        return ".tif"
    if ct.startswith("image/"):
        return ".raw"        # an image/* type Pillow cannot read -- keep it anyway
    return None


# ---- downloading -------------------------------------------------------------

def download_one_url(gbif_id, image_url, tmp_path):
    """
    Download one URL to tmp_path. Returns a dict:
      success -> {ok: True, host, http_status, content_type, size}
      failure -> {ok: False, host, http_status, content_type, size,
                  error_type, error_detail}

    Detects HTML/text responses -- including ones disguised with an image
    Content-Type -- and captures the page text into error_detail.

    An http:// URL that fails to connect is retried once as https:// (see
    get_with_https_fallback); either way the outcome is counted once.
    """
    host = _host_from_url(image_url)
    via_https = False   # set once a response is in; fail() reads the final value

    def fail(error_type, detail, http_status=None, content_type=None):
        if via_https:
            detail = f"{detail} [via https:// fallback]"
        return {"ok": False, "host": host, "http_status": http_status,
                "content_type": content_type, "size": None,
                "error_type": error_type, "error_detail": detail}

    try:
        time.sleep(random.uniform(0.2, 0.8))
        resp, via_https = get_with_https_fallback(
            image_url,
            stream=True,
            verify=False,
            headers={
                "User-Agent": random.choice(user_agents),
                "Connection": "keep-alive",
                "Referer": "https://scc-ondemand1.bu.edu/",
            },
            timeout=(CONNECT_TIMEOUT, IMAGE_READ_TIMEOUT),
        )
        with resp:
            status = resp.status_code

            if status == 429:
                increment_host_errors(image_url, is_rate_limit=True)
                block_host(image_url, resp.headers.get("Retry-After"))
                return fail(ddb.ERR_RATE_LIMITED, "HTTP 429", status)

            if status != 200:
                increment_host_errors(image_url)
                return fail(ddb.http_error_type(status), f"HTTP {status}", status)

            ctype = (resp.headers.get("Content-Type") or "").lower()

            # Content-Type clearly says this is not an image -> capture the text.
            if _is_non_image_ctype(ctype):
                increment_host_errors(image_url)
                snippet = _html_to_text(_read_bounded(resp))
                return fail(ddb.ERR_INVALID_CONTENT,
                            f"[{ctype}] {snippet}", status, ctype)

            # Sniff the first chunk: some hosts serve an HTML notice ("direct
            # download no longer supported ...") with an image/* Content-Type.
            stream = resp.iter_content(chunk_size=65536)
            first = b""
            for chunk in stream:
                if chunk:
                    first = chunk
                    break
            if _looks_like_text(first):
                increment_host_errors(image_url)
                snippet = _html_to_text(first + _join_bounded(stream))
                return fail(ddb.ERR_INVALID_CONTENT,
                            f"[non-image body, {ctype or 'no Content-Type'}] "
                            f"{snippet}", status, ctype)

            expected = resp.headers.get("Content-Length")
            written = 0
            with open(tmp_path, "wb") as out:
                if first:
                    out.write(first)
                    written += len(first)
                for chunk in stream:
                    if chunk:
                        out.write(chunk)
                        written += len(chunk)

            if expected is not None:
                try:
                    if int(expected) != written:
                        _rm(tmp_path)
                        return fail(ddb.ERR_TRUNCATED,
                                    f"expected {expected} bytes, got {written}",
                                    status, ctype)
                except ValueError:
                    pass

            if written < 1024:
                _rm(tmp_path)
                return fail(ddb.ERR_TRUNCATED, f"only {written} bytes",
                            status, ctype)

            if via_https:
                _note_https_fallback_worked(image_url)
            return {"ok": True, "host": host, "http_status": status,
                    "content_type": ctype, "size": written}

    except ConnectTimeout as e:
        # No connection within CONNECT_TIMEOUT (for an http:// URL, over https://
        # either). Count it, so a host that only ever times out still reaches
        # the circuit breaker / quarantine, and cool the host down.
        _rm(tmp_path)
        increment_host_errors(image_url)
        block_host(image_url, timeout_issue=True)
        return fail(ddb.ERR_TIMEOUT, str(e))
    except Timeout as e:
        # Read timeout: the host is up but slow. Cool it down, but do not count
        # it -- a slow but working source is not a broken one.
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


def _finalize_download(gbif_id, image_no, image_url, res, tmp_path):
    """Turn a downloaded temp file into the final image, or keep it raw."""
    try:
        # Decode + resize as a normal image (JPEG/TIFF/PNG/...), in place.
        resize_with_aspect_ratio(tmp_path, tmp_path, max_size=1024,
                                 format="JPEG", quality=85)
    except (OSError, UnidentifiedImageError) as e:
        # Pillow cannot decode it. If it is a real image format (DNG etc.),
        # keep the raw file for a later conversion pass; otherwise discard.
        ext = raw_keep_extension(res["content_type"], image_url)
        if ext:
            raw_path = image_path(gbif_id, image_no, ext)
            os.replace(tmp_path, raw_path)
            try:
                size = os.path.getsize(raw_path)
            except OSError:
                size = res["size"]
            logger.warning(f"Kept raw image {gbif_id} #{image_no} "
                           f"({res['content_type']}) at {raw_path}")
            return {"outcome": "success", "db_status": ddb.ST_SUCCESS,
                    "http_status": 200, "error_type": ddb.ERR_RAW_UNPROCESSED,
                    "error_detail": f"kept raw: {res['content_type'] or 'unknown'}",
                    "host": res["host"], "file_path": raw_path, "file_size": size}
        _rm(tmp_path)
        return {"outcome": "failed", "db_status": ddb.ST_FAILED_PERMANENT,
                "http_status": 200, "error_type": ddb.ERR_NOT_IMAGE,
                "error_detail": str(e), "host": res["host"],
                "file_path": None, "file_size": None}

    jpg_path = image_path(gbif_id, image_no, ".jpg")
    os.replace(tmp_path, jpg_path)
    try:
        size = os.path.getsize(jpg_path)
    except OSError:
        size = res["size"]
    return {"outcome": "success", "db_status": ddb.ST_SUCCESS, "http_status": 200,
            "error_type": None, "error_detail": None, "host": res["host"],
            "file_path": jpg_path, "file_size": size}


def resolve_and_download(gbif_id, image_no, candidate_urls):
    """
    Fetch one distinct image and save it as exactly one file.

    candidate_urls are this image's URLs from the database, best-resolution
    first. IIIF manifests among them are expanded into image URLs. The first
    URL that yields a usable image wins. Returns an outcome dict; outcome is
    'success', 'failed', or 'deferred' (every candidate host was blocked, so
    the image was not really attempted and should stay 'pending').
    """
    resolved, manifest_err = [], None
    for url in candidate_urls:
        if is_manifest_url(url):
            # Respect the circuit breaker / cooldown BEFORE fetching the
            # manifest -- otherwise a dead IIIF host (e.g. oxalis) is hit
            # on every manifest even after it has tripped the breaker,
            # which stalls the whole run on connection timeouts.
            if is_host_circuit_broken(url) or is_host_blocked(url):
                continue
            extracted, err = extract_image_from_iiif_manifest(url, gbif_id)
            if extracted:
                resolved.extend(extracted)
            elif err:
                manifest_err = err
        else:
            resolved.append(url)

    seen, ordered = set(), []
    for url in resolved:
        if url not in seen:
            seen.add(url)
            ordered.append(url)

    if not ordered:
        et = manifest_err or ddb.ERR_NO_URL
        return {"outcome": "failed", "db_status": ddb.status_for_error(et),
                "http_status": None, "error_type": et,
                "error_detail": "no downloadable image URL for this image",
                "host": None, "file_path": None, "file_size": None}

    tmp_path = image_path(gbif_id, image_no, ".tmp")
    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)

    failures, attempted = [], False
    for url in ordered:
        if is_host_circuit_broken(url) or is_host_blocked(url):
            continue
        attempted = True
        res = download_one_url(gbif_id, url, tmp_path)
        if res["ok"]:
            return _finalize_download(gbif_id, image_no, url, res, tmp_path)
        failures.append(res)

    if not attempted:
        # Every candidate's host was blocked -- leave the image 'pending'.
        return {"outcome": "deferred"}

    # Prefer a transient failure as the recorded reason: if any candidate could
    # still succeed later, the whole image is worth retrying.
    transient = [f for f in failures if not ddb.is_permanent(f["error_type"])]
    chosen = transient[0] if transient else failures[0]
    db_status = ddb.ST_FAILED_TRANSIENT if transient else ddb.ST_FAILED_PERMANENT
    return {"outcome": "failed", "db_status": db_status,
            "http_status": chosen["http_status"],
            "error_type": chosen["error_type"],
            "error_detail": chosen["error_detail"],
            "host": chosen["host"], "file_path": None, "file_size": None}


# ---- per-gbifID processing ---------------------------------------------------

def _existing_file(gbif_id, image_no):
    """Return (path, is_raw) if a valid file is already on disk, else None."""
    for ext in (".jpg",) + RAW_EXTS:
        path = image_path(gbif_id, image_no, ext)
        if os.path.exists(path):
            try:
                if get_file_size_in_mb(path) >= MIN_IMAGE_MB:
                    return path, (ext != ".jpg")
            except OSError:
                pass
    return None


def process_id(db, gbif_id, total_to_install):
    """Download every not-yet-done distinct image for one gbifID."""
    global n_installed
    images = db.get_images_for(gbif_id)

    for image_no, image_key, urls_str, host, status, attempts in images:
        # Skip images that are already finished or have exhausted their retries.
        if status == ddb.ST_SUCCESS:
            continue
        if status == ddb.ST_FAILED_PERMANENT:
            continue
        if status == ddb.ST_FAILED_TRANSIENT and attempts >= db.max_attempts:
            continue

        # If a valid file is already on disk (a previous run, or the legacy
        # import), record it without downloading.
        existing = _existing_file(gbif_id, image_no)
        if existing:
            path, is_raw = existing
            try:
                size = os.path.getsize(path)
            except OSError:
                size = None
            db.record_image_result(
                gbif_id, image_no, ddb.ST_SUCCESS, host=host, http_status=200,
                error_type=ddb.ERR_RAW_UNPROCESSED if is_raw else None,
                file_path=path, file_size=size, increment_attempts=False)
            continue

        candidate_urls = [u for u in urls_str.split("\n") if u]
        result = resolve_and_download(gbif_id, image_no, candidate_urls)
        if result["outcome"] == "deferred":
            continue  # host blocked; leave 'pending' for a later run

        db.record_image_result(
            gbif_id, image_no, result["db_status"],
            host=result.get("host") or host,
            http_status=result.get("http_status"),
            error_type=result.get("error_type"),
            error_detail=result.get("error_detail"),
            file_path=result.get("file_path"),
            file_size=result.get("file_size"))

        if result["outcome"] == "success":
            # Feed the failure-rate circuit breaker: every real download success
            # counts once here (both JPEG and kept-raw outcomes flow through this
            # branch). host may be None if the successful URL had no netloc.
            success_host = result.get("host")
            if success_host:
                with circuit_breaker_lock:
                    host_success_counts[success_host] = (
                        host_success_counts.get(success_host, 0) + 1)
            with counter_lock:
                n_installed += 1
                current = n_installed
            if current % 10000 == 0:
                send_notification(
                    "Image Installation",
                    f"Installed {current} images this run "
                    f"(work queue: {total_to_install} gbifIDs).")
                logger.warning(f"Installed {current} images this run.")

    db.finalize_gbif_id(gbif_id)


# ---- main --------------------------------------------------------------------

def main():
    # Line-buffer stdout so progress appears in a batch job's .o log live,
    # not only when the job finishes.
    sys.stdout.reconfigure(line_buffering=True)

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

    # Let increment_host_errors() persist quarantine flags on this run's DB.
    global quarantine_db
    quarantine_db = db

    # Seed the in-memory circuit breaker from the last run's host stats. Success
    # counts are seeded too so the breaker can judge a host by failure RATE from
    # the first request, instead of skipping a released host on its old errors.
    saved_errors, saved_successes, saved_blocks = db.load_host_state()
    host_error_counts.update(saved_errors)
    host_success_counts.update(saved_successes)
    host_block_until.update(saved_blocks)
    print(f"Loaded host state: {len(saved_errors)} hosts with errors, "
          f"{len(saved_successes)} with successes, "
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

    counts = {}
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
                progress(f"  processed "
                         f"{min(start + WORK_CHUNK, total_to_install)}"
                         f"/{total_to_install} gbifIDs")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user; saving state and exiting.")
    finally:
        db.save_host_state(host_error_counts, host_block_until)
        counts = db.gbif_status_counts()
        broken = sum(1 for host, errors in host_error_counts.items()
                     if _breaker_trips(errors, host_success_counts.get(host, 0)))
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
