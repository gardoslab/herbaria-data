"""
Tests for how image_install_db handles unreachable hosts:

  * a connect timeout counts as a host error (and cools the host down); a read
    timeout only cools it down,
  * an http:// URL that fails to connect is retried once as https://, and the
    pair counts as one attempt,
  * the (connect, read) timeouts bound the wait on a dead host,
  * hosts already past the error threshold are re-judged every 100 errors.

No real network: requests is mocked at session.get or, for the tests that need
urllib3's real retry logic, at urllib3's socket-level create_connection.

Run from the repo root:
    python -m pytest -q tests/test_unreachable_hosts.py
"""

import io
import json
import os
import shutil
import socket
import sys
import tempfile
import unittest
from unittest import mock

import requests
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from requests.structures import CaseInsensitiveDict
from urllib3.exceptions import (ConnectTimeoutError, MaxRetryError,
                                NewConnectionError, ReadTimeoutError)
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import download_db as ddb
from download_db import DownloadDB
import image_install_db as iidb


HOST = "host.example"


def http_url(n=1):
    return f"http://{HOST}/images/{n}.jpg"


def https_url(n=1):
    return f"https://{HOST}/images/{n}.jpg"


# ---- failures, shaped like the exceptions requests raises --------------------

def connect_timeout(url):
    return ConnectTimeout(MaxRetryError(None, url, ConnectTimeoutError(
        None, f"Connection to {HOST} timed out. (connect timeout=20)")))


def connection_refused(url):
    return ConnectionError(MaxRetryError(None, url, NewConnectionError(
        None, "Failed to establish a new connection: [Errno 111] Connection refused")))


def read_timeout(url):
    return ReadTimeout(MaxRetryError(None, url, ReadTimeoutError(
        None, url, "Read timed out. (read timeout=180)")))


# ---- responses ---------------------------------------------------------------

def _jpeg_bytes():
    buf = io.BytesIO()
    Image.effect_noise((128, 128), 64).convert("RGB").save(buf, "JPEG")
    return buf.getvalue()


JPEG = _jpeg_bytes()

MANIFEST = {"items": [{"type": "Canvas", "items": [{
    "type": "AnnotationPage",
    "items": [{"body": {"service": [{"id": "https://images.example/iiif/E1"}]}}],
}]}]}


def _response(url, status, body, content_type):
    resp = requests.Response()
    resp.status_code = status
    resp.url = url
    resp.headers = CaseInsensitiveDict({"Content-Type": content_type,
                                        "Content-Length": str(len(body))})
    resp.raw = io.BytesIO(body)
    return resp


def ok_image(url):
    return _response(url, 200, JPEG, "image/jpeg")


def not_found(url):
    return _response(url, 404, b"", "text/plain")


def ok_manifest(url):
    return _response(url, 200, json.dumps(MANIFEST).encode(), "application/json")


def fake_get(outcomes):
    """
    A session.get stand-in. `outcomes` maps each URL the test expects to be
    requested to a factory returning the exception to raise or the response.
    """
    def get(url, **kwargs):
        outcome = outcomes[url](url)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome
    return get


class HostStateTestCase(unittest.TestCase):
    """
    Isolates image_install_db's module-level host state, turns its polite random
    pre-request sleeps into sleep(0), and gives each test a scratch directory.
    """

    def setUp(self):
        state = (iidb.host_error_counts, iidb.host_success_counts,
                 iidb.host_block_until, iidb.https_upgrade_hosts)
        saved = [s.copy() for s in state]
        saved_db, saved_installed = iidb.quarantine_db, iidb.n_installed

        def restore():
            for s, old in zip(state, saved):
                s.clear()
                s.update(old)
            iidb.quarantine_db, iidb.n_installed = saved_db, saved_installed

        self.addCleanup(restore)
        for s in state:
            s.clear()
        iidb.quarantine_db = None
        iidb.n_installed = 0

        uniform = mock.patch.object(iidb.random, "uniform", return_value=0.0)
        uniform.start()
        self.addCleanup(uniform.stop)

        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.tmp_path = os.path.join(self.dir, "img.tmp")

    def patch_get(self, outcomes):
        patcher = mock.patch.object(iidb.session, "get",
                                    side_effect=fake_get(outcomes))
        get = patcher.start()
        self.addCleanup(patcher.stop)
        return get

    @staticmethod
    def requested(get):
        return [c.args[0] for c in get.call_args_list]

    def reset_host(self):
        for s in (iidb.host_error_counts, iidb.host_block_until,
                  iidb.https_upgrade_hosts):
            s.clear()


class TimeoutCountingTest(HostStateTestCase):
    """Connect timeouts count against the host; read timeouts only cool it down."""

    def test_connect_timeout_counts_a_host_error(self):
        get = self.patch_get({https_url(): connect_timeout})
        res = iidb.download_one_url(1, https_url(), self.tmp_path)

        self.assertEqual(res["error_type"], ddb.ERR_TIMEOUT)
        self.assertEqual(iidb.host_error_counts.get(HOST), 1)
        self.assertTrue(iidb.is_host_blocked(https_url()))        # cooldown kept
        self.assertEqual(self.requested(get), [https_url()])      # no fallback for https://
        self.assertEqual(get.call_args.kwargs["timeout"],
                         (iidb.CONNECT_TIMEOUT, iidb.IMAGE_READ_TIMEOUT))
        self.assertEqual(iidb.CONNECT_TIMEOUT, 20)

    def test_read_timeout_cools_down_without_counting(self):
        for url in (https_url(), http_url()):
            with self.subTest(url=url):
                self.reset_host()
                get = self.patch_get({url: read_timeout})
                res = iidb.download_one_url(1, url, self.tmp_path)

                self.assertEqual(res["error_type"], ddb.ERR_TIMEOUT)
                self.assertNotIn(HOST, iidb.host_error_counts)
                self.assertTrue(iidb.is_host_blocked(url))
                # The host did answer, so an http:// URL is not retried as https://.
                self.assertEqual(self.requested(get), [url])


class HttpsFallbackTest(HostStateTestCase):
    """An http:// URL that cannot connect is retried once as https://."""

    def _db_with_pending_image(self, gbif_id, url):
        db = DownloadDB(os.path.join(self.dir, "status.db"))
        self.addCleanup(db.close)
        with db.lock:
            db.conn.execute(
                "INSERT INTO gbif_ids(gbif_id, n_images, status) VALUES(?, 1, ?)",
                (gbif_id, ddb.G_PENDING))
            db.conn.execute(
                "INSERT INTO images(gbif_id, image_no, image_key, urls, host, status) "
                "VALUES(?, 0, ?, ?, ?, ?)",
                (gbif_id, url, url, HOST, ddb.ST_PENDING))
            db.conn.commit()
        return db

    def test_http_connect_failure_then_https_success_counts_as_success(self):
        gbif_id = 1234567
        db = self._db_with_pending_image(gbif_id, http_url())
        get = self.patch_get({http_url(): connect_timeout, https_url(): ok_image})

        with mock.patch.object(iidb, "INSTALL_PATH", self.dir), \
                self.assertLogs(iidb.logger, "WARNING") as logs:
            iidb.process_id(db, gbif_id, total_to_install=1)

        self.assertEqual(self.requested(get), [http_url(), https_url()])
        status, error_type, file_path = db.conn.execute(
            "SELECT status, error_type, file_path FROM images WHERE gbif_id=?",
            (gbif_id,)).fetchone()
        self.assertEqual((status, error_type), (ddb.ST_SUCCESS, None))
        self.assertTrue(os.path.exists(file_path))
        self.assertEqual(db.conn.execute(
            "SELECT status FROM gbif_ids WHERE gbif_id=?", (gbif_id,)).fetchone()[0],
            ddb.G_DONE)
        # Fed to the breaker as a success, not an error, and no cooldown.
        self.assertEqual(iidb.host_success_counts.get(HOST), 1)
        self.assertNotIn(HOST, iidb.host_error_counts)
        self.assertFalse(iidb.is_host_blocked(http_url()))
        self.assertTrue(any("HTTPS fallback" in m and HOST in m for m in logs.output))

    def test_refused_connection_also_falls_back(self):
        get = self.patch_get({http_url(): connection_refused, https_url(): ok_image})
        res = iidb.download_one_url(1, http_url(), self.tmp_path)

        self.assertTrue(res["ok"])
        self.assertEqual(self.requested(get), [http_url(), https_url()])
        self.assertNotIn(HOST, iidb.host_error_counts)

    def test_both_failing_counts_one_error(self):
        cases = [
            # http:// failure, https:// failure, recorded error_type, cooled down?
            (connect_timeout, connect_timeout, ddb.ERR_TIMEOUT, True),
            (connect_timeout, connection_refused, ddb.ERR_TIMEOUT, True),
            (connection_refused, connect_timeout, ddb.ERR_TIMEOUT, True),
            (connection_refused, connection_refused, ddb.ERR_CONNECTION, False),
        ]
        for http_fail, https_fail, error_type, cooled_down in cases:
            with self.subTest(http=http_fail.__name__, https=https_fail.__name__):
                self.reset_host()
                get = self.patch_get({http_url(): http_fail, https_url(): https_fail})
                res = iidb.download_one_url(1, http_url(), self.tmp_path)

                self.assertEqual(self.requested(get), [http_url(), https_url()])
                self.assertEqual(iidb.host_error_counts.get(HOST), 1)   # not 2
                self.assertEqual(res["error_type"], error_type)
                self.assertEqual(iidb.is_host_blocked(http_url()), cooled_down)
                self.assertIn("https:// fallback also failed", res["error_detail"])
                self.assertNotIn(HOST, iidb.https_upgrade_hosts)

    def test_host_with_working_fallback_skips_http_for_later_urls(self):
        get = self.patch_get({http_url(1): connect_timeout, https_url(1): ok_image,
                              https_url(2): ok_image, https_url(3): not_found})

        self.assertTrue(iidb.download_one_url(1, http_url(1), self.tmp_path)["ok"])
        self.assertTrue(iidb.download_one_url(2, http_url(2), self.tmp_path)["ok"])
        res = iidb.download_one_url(3, http_url(3), self.tmp_path)

        # Only the first image waited on the dead http:// port.
        self.assertEqual(self.requested(get), [http_url(1), https_url(1),
                                               https_url(2), https_url(3)])
        # A failure over https:// says so in the recorded detail.
        self.assertEqual(res["error_type"], "http_404")
        self.assertIn("via https:// fallback", res["error_detail"])

    def test_https_fallback_url(self):
        self.assertEqual(
            iidb.https_fallback_url("http://sweetgum.nybg.org/images3/3817/721/04266510.jpg"),
            "https://sweetgum.nybg.org/images3/3817/721/04266510.jpg")
        self.assertEqual(iidb.https_fallback_url("HTTP://h.example/a?b=1#c"),
                         "https://h.example/a?b=1#c")
        self.assertIsNone(iidb.https_fallback_url("https://h.example/a"))
        self.assertIsNone(iidb.https_fallback_url("http://h.example:8080/a"))

    def test_manifest_fetch_falls_back_to_https(self):
        http_manifest = f"http://{HOST}/iiif/E1/manifest"
        https_manifest = f"https://{HOST}/iiif/E1/manifest"
        get = self.patch_get({http_manifest: connect_timeout, https_manifest: ok_manifest})

        image_urls, err = iidb.extract_image_from_iiif_manifest(http_manifest, 1)

        self.assertIsNone(err)
        self.assertEqual(image_urls[0],
                         "https://images.example/iiif/E1/full/1600,/0/default.jpg")
        self.assertEqual(self.requested(get), [http_manifest, https_manifest])
        self.assertEqual(get.call_args.kwargs["timeout"],
                         (iidb.CONNECT_TIMEOUT, iidb.MANIFEST_READ_TIMEOUT))
        self.assertNotIn(HOST, iidb.host_error_counts)
        self.assertIn(HOST, iidb.https_upgrade_hosts)


class DeadHostBudgetTest(HostStateTestCase):
    """
    Through the real requests/urllib3 stack, with only the socket connect faked:
    how long can one URL on a dead host hold a worker thread?
    """

    def _download(self, url, connect_error):
        attempts, sleeps = [], []

        def create_connection(address, timeout=None, *args, **kwargs):
            attempts.append((address[1], timeout))
            raise connect_error

        with mock.patch("urllib3.util.connection.create_connection",
                        side_effect=create_connection), \
                mock.patch("time.sleep", side_effect=sleeps.append):
            res = iidb.download_one_url(1, url, self.tmp_path)
        return res, attempts, [s for s in sleeps if s]

    def test_dead_host_worst_case_is_under_a_minute(self):
        for url, ports in ((http_url(), [80, 443]), (https_url(), [443])):
            with self.subTest(url=url):
                self.reset_host()
                res, attempts, backoff = self._download(url, socket.timeout("timed out"))

                self.assertEqual(res["error_type"], ddb.ERR_TIMEOUT)
                # One connect per scheme (no urllib3 connect retries), each
                # capped at CONNECT_TIMEOUT, with no backoff sleep between.
                self.assertEqual([port for port, _ in attempts], ports)
                self.assertTrue(all(t == iidb.CONNECT_TIMEOUT for _, t in attempts))
                self.assertEqual(backoff, [])
                self.assertLessEqual(sum(t for _, t in attempts), 60)
                self.assertEqual(iidb.host_error_counts.get(HOST), 1)

    def test_fallback_only_when_https_could_help(self):
        cases = [
            # socket error on connect, ports tried, recorded error_type
            (ConnectionRefusedError(111, "Connection refused"), [80, 443],
             ddb.ERR_CONNECTION),
            # DNS failure: https:// would look up the same name, so no retry.
            (socket.gaierror(-2, "Name or service not known"), [80],
             ddb.ERR_CONNECTION),
        ]
        for error, ports, error_type in cases:
            with self.subTest(error=type(error).__name__):
                self.reset_host()
                res, attempts, _ = self._download(http_url(), error)

                self.assertEqual([port for port, _ in attempts], ports)
                self.assertEqual(res["error_type"], error_type)
                self.assertEqual(iidb.host_error_counts.get(HOST), 1)


class QuarantineRecheckTest(HostStateTestCase):
    """Past the threshold, a host is re-judged every 100 errors (600, 700, ...)."""

    def setUp(self):
        super().setUp()
        self.db = DownloadDB(os.path.join(self.dir, "status.db"))
        self.addCleanup(self.db.close)
        iidb.quarantine_db = self.db
        spy = mock.patch.object(self.db, "quarantine_if_unhealthy",
                                wraps=self.db.quarantine_if_unhealthy)
        self.spy = spy.start()
        self.addCleanup(spy.stop)

    def add_errors(self, n, host=HOST):
        for _ in range(n):
            iidb.increment_host_errors(f"https://{host}/img.jpg")

    def checked_at(self):
        return [c.kwargs["error_count"] for c in self.spy.call_args_list]

    def test_checkpoints_are_500_600_700(self):
        self.add_errors(750)
        self.assertEqual(self.checked_at(), [500, 600, 700])

    def test_host_seeded_at_501_is_rechecked_at_600(self):
        # Seeded from the DB like a host released from quarantine: already past
        # 500, so a check that fires only at exactly 500 never sees it again.
        iidb.host_error_counts[HOST] = 501

        self.add_errors(98)                                   # 502 .. 599
        self.assertEqual(self.checked_at(), [])
        self.assertNotIn(HOST, self.db.quarantined_host_set())

        with self.assertLogs(iidb.logger, "ERROR") as logs:
            self.add_errors(1)                                # 600
        self.assertEqual(self.checked_at(), [600])
        self.assertIn(HOST, self.db.quarantined_host_set())
        self.assertTrue(any("CIRCUIT BREAKER" in m for m in logs.output))
        self.assertTrue(any("QUARANTINED" in m for m in logs.output))

        self.add_errors(100)                                  # ... 700
        self.assertEqual(self.checked_at(), [600, 700])

    def test_healthy_host_is_rechecked_but_spared(self):
        big = "big.example"
        # 600 errors against 1000 successes: a 0.375 failure rate, under the bar.
        with self.db.lock:
            self.db.conn.executemany(
                "INSERT INTO images(gbif_id, image_no, image_key, urls, host, status) "
                "VALUES(?, 0, ?, 'u', ?, ?)",
                [(i, f"k{i}", big, ddb.ST_SUCCESS) for i in range(1000)])
            self.db.conn.commit()
        iidb.host_error_counts[big] = 599
        iidb.host_success_counts[big] = 1000

        with self.assertNoLogs(iidb.logger, "ERROR"):
            self.add_errors(1, host=big)                      # 600
        self.assertEqual(self.checked_at(), [600])
        self.assertNotIn(big, self.db.quarantined_host_set())
        self.assertFalse(iidb.is_host_circuit_broken(f"https://{big}/img.jpg"))


if __name__ == "__main__":
    unittest.main()
