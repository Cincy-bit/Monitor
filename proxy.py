#!/usr/bin/env python3
"""
Monitor The Situation - Proxy Server
Port: 8082
"""

import http.server
import urllib.request
import urllib.error
import json
import base64
import os
import ssl
import re
import html
import threading
import io
import time as _time
import tempfile
from socketserver import ThreadingMixIn
import urllib.parse
from urllib.parse import urlparse, parse_qs, urljoin
import http.client
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

PORT = 8082
SERVE_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_carto_api_key():
    """Resolve the CARTO API key: the CARTO_API_KEY environment variable
    first (unchanged, existing behavior), then a local carto_api_key.txt
    file living next to this script as a fallback.

    Added because getting an env var into the *specific* shell that ends up
    running `python3 proxy.py` has repeatedly tripped up on this project —
    appending to ~/.zshrc (or ~/.bash_profile) only takes effect in a NEW
    shell session (a fresh terminal window/tab, or after `source`-ing that
    file); a terminal window that was already open when the profile was
    edited keeps its old (keyless) environment no matter how many times the
    proxy is restarted in it. A plain text file read fresh on every process
    start sidesteps all of that — no shell profile, no sourcing, no need to
    open a new window.

    To use it: create a file named carto_api_key.txt in this same folder
    (next to proxy.py) containing nothing but the key. Whitespace around it
    is stripped. The env var still wins if both are set, so this never
    changes behavior for a setup that already has the env var working.
    Returns (key, source) where source is 'env', 'file', or None."""
    env_key = os.environ.get("CARTO_API_KEY", "").strip()
    if env_key:
        return env_key, "env"
    try:
        key_path = os.path.join(SERVE_DIR, "carto_api_key.txt")
        with open(key_path, "r") as _f:
            file_key = _f.read().strip()
        if file_key:
            return file_key, "file"
    except (FileNotFoundError, OSError):
        pass
    return "", None

# SSL context that doesn't verify certs (for local dev)
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# Radar time-extent cache: base URL -> (fetched_at, (start_ms, end_ms))
# Tiny and short-lived — just avoids hitting NOAA's metadata endpoint on every
# single /radar-meta poll when the underlying window barely moves.
_RADAR_META_CACHE = {}

# NWS grid-office lookup cache — see _nws_points() below. Grid assignment
# for a fixed lat/lon is effectively permanent, so a long TTL is safe.
_NWS_POINTS_CACHE = {}
_NWS_POINTS_TTL = 7 * 24 * 3600

# Article date cache: url -> first-seen ISO pubDate
# Prevents re-stamped podcast/feature articles from always appearing as "just now"
_article_date_cache = {}
_article_date_cache_lock = threading.Lock()
_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".article_date_cache.json")

def _load_date_cache():
    global _article_date_cache
    try:
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, "r") as _f:
                _article_date_cache = json.load(_f)
    except Exception:
        _article_date_cache = {}

def _save_date_cache():
    try:
        with open(_CACHE_FILE, "w") as _f:
            json.dump(_article_date_cache, _f)
    except Exception:
        pass

_load_date_cache()

# Cache for mempool.space's hashrate/difficulty endpoints.
#
# ROOT CAUSE of the "Failed: The operation was aborted." chart failures:
# mempool.space rate-limits per IP (undisclosed limits; repeat offenders get
# soft-banned per their API docs), and a soft-blocked/throttled connection
# does NOT time out at the socket level — it stalls while dribbling bytes.
# urllib's timeout= is PER SOCKET OPERATION, so each arriving byte resets the
# timer and resp.read() can hang for minutes. The proxy thread sat there
# while the browser's AbortController fired, hence "aborted". No timeout
# budget increase can fix that; only a hard wall-clock deadline can.
#
# Fixes applied here:
#   1. _fetch_with_deadline() bounds the TOTAL fetch time (headers + body),
#      not just per-operation gaps.
#   2. Hashrate history is daily-candle data — it is cached for hours, not
#      90 seconds, and persisted to disk so restarts keep a warm copy.
#   3. On upstream failure, a stale cached copy is served instead of an
#      error. Old hashrate history beats no hashrate history.
#   4. /hashrate-history?span= falls back to blockchain.info's hash-rate
#      chart when mempool.space stalls or errors (and serves 4y/10y, which
#      mempool.space does not offer at all).
_MEMPOOL_BASE = os.environ.get("MEMPOOL_BASE", "https://mempool.space")
_BLOCKCHAIN_BASE = os.environ.get("BLOCKCHAIN_BASE", "https://api.blockchain.info")

_mempool_cache = {}
_mempool_cache_lock = threading.Lock()
_MEMPOOL_CACHE_TTL = 90               # difficulty-adjustment freshness (seconds)
_HASHRATE_HISTORY_TTL = 6 * 3600      # daily-candle history moves on hours, not seconds
_NETWORK_HASHRATE_TTL = 120           # "current network hashrate" is a live reading — keep it fresh
_MEMPOOL_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".mempool_cache.json")
_mempool_cache_dirty = False
_mempool_cache_last_save = 0.0

def _mempool_cache_load():
    global _mempool_cache
    try:
        if os.path.exists(_MEMPOOL_CACHE_FILE):
            with open(_MEMPOOL_CACHE_FILE, "r") as f:
                raw = json.load(f)
            # Bodies (JSON text AND binary images) are stored base64-encoded
            # so the round-trip through the JSON cache file is lossless for
            # any byte sequence. Decode per-entry so one corrupted/legacy
            # entry (e.g. from before this fix) can't take down the whole
            # cache load -- it's just skipped and re-fetched live instead.
            loaded = {}
            skipped = 0
            for k, v in raw.items():
                try:
                    loaded[k] = (v[0], base64.b64decode(v[1]), v[2], v[3])
                except Exception:
                    skipped += 1
            _mempool_cache = loaded
            msg = f"[mempool cache] restored {len(_mempool_cache)} entries from disk"
            if skipped:
                msg += f" ({skipped} corrupted/unreadable entries skipped)"
            print(msg)
    except Exception as e:
        print(f"[mempool cache] disk load failed: {e}")
        _mempool_cache = {}

def _mempool_cache_save(force=False):
    global _mempool_cache_dirty, _mempool_cache_last_save
    # Throttle disk writes to at most one per 5s
    now = _time.time()
    if not force and (now - _mempool_cache_last_save) < 5:
        return
    try:
        with _mempool_cache_lock:
            # base64 round-trips ANY bytes losslessly (unlike utf-8 decode,
            # which silently corrupts binary data like PNG bytes -- see the
            # module-level comment above _mempool_cache_load).
            raw = {k: [v[0], base64.b64encode(v[1]).decode("ascii"), v[2], v[3]] for k, v in _mempool_cache.items()}
            _mempool_cache_dirty = False
            _mempool_cache_last_save = now
        with open(_MEMPOOL_CACHE_FILE, "w") as f:
            json.dump(raw, f)
    except Exception:
        pass

_mempool_cache_load()

def _mempool_cache_get(url, ttl=_MEMPOOL_CACHE_TTL):
    """Fresh entry only."""
    with _mempool_cache_lock:
        entry = _mempool_cache.get(url)
        if entry and (_time.time() - entry[0]) < ttl:
            return entry[1], entry[2], entry[3]
    return None

def _mempool_cache_get_stale(url):
    """Any age — used when every live source failed. Stale data beats an error."""
    with _mempool_cache_lock:
        entry = _mempool_cache.get(url)
        if entry:
            return entry[1], entry[2], entry[3]
    return None

def _mempool_cache_set(url, data, content_type, source="mempool.space"):
    global _mempool_cache_dirty
    with _mempool_cache_lock:
        _mempool_cache[url] = (_time.time(), data, content_type, source)
        _mempool_cache_dirty = True
        # Prune occasionally so this can't grow unbounded
        if len(_mempool_cache) > 200:
            oldest = sorted(_mempool_cache.items(), key=lambda kv: kv[1][0])[:100]
            for k, _ in oldest:
                del _mempool_cache[k]
    _mempool_cache_save()

def _fetch_with_deadline(req, ctx, per_op_timeout, deadline_s):
    """
    urlopen + full body read, bounded by a HARD wall-clock deadline.

    urllib's timeout= only bounds each individual socket operation (connect,
    each recv). A stalled upstream that sends one byte every few seconds —
    which is what throttling/soft-blocking looks like — resets that timer
    indefinitely, so resp.read() never returns and no exception ever fires.
    Running the whole exchange in a daemon thread and joining it with a
    deadline guarantees the caller gets control back. The abandoned thread
    is a daemon and dies with the process; its socket is reclaimed by the OS
    when the far end finally closes.
    """
    box = {}
    def _work():
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=per_op_timeout) as resp:
                box['data'] = resp.read()
                box['ct'] = resp.headers.get("Content-Type", "application/json")
        except Exception as e:
            box['err'] = e
    t = threading.Thread(target=_work, daemon=True)
    t.start()
    t.join(deadline_s)
    if t.is_alive():
        raise TimeoutError(f"upstream stalled: no complete response within {deadline_s}s")
    if 'err' in box:
        raise box['err']
    return box['data'], box['ct']

# Timeframes the hashrate chart can request, in seconds of history.
_HASHRATE_WINDOWS = {
    '24h': 86400, '1w': 604800, '1m': 2592000, '3m': 7776000, '6m': 15552000,
    '1y': 31536000, '2y': 63072000, '3y': 94608000, '4y': 126144000, '10y': 315360000,
}
# mempool.space natively serves at most 3y; 4y/10y go straight to the fallback.
_MEMPOOL_SPANS = {'24h', '1w', '1m', '3m', '6m', '1y', '2y', '3y'}
# blockchain.info charts API timespans (verified live), smallest-first.
_BLOCKCHAIN_TIMESPANS = [
    (2592000, '30days'), (7776000, '90days'), (15552000, '180days'),
    (31536000, '1year'), (63072000, '2years'), (126144000, '4years'), (315360000, '10years'),
]

ALLOWED_ORIGINS = [
    "finance.yahoo.com",
    "query1.finance.yahoo.com",
    "query2.finance.yahoo.com",
    "feeds.finance.yahoo.com",
    "api.coingecko.com",
    "rss.cnn.com",
    "feeds.bbci.co.uk",
    "feeds.reuters.com",
    "rss.nytimes.com",
    "mempool.space",
    "geocoding-api.open-meteo.com",
    "api.open-meteo.com",
    "feeds.finance.yahoo.com",
    "api.blockchair.com",
    "blockchain.info",
    "blockstream.info",
    # New sources
    "therage.co",
    "www.therage.co",
    "wsj.com",
    "www.wsj.com",
    "feeds.wsj.com",
    "bloomberg.com",
    "www.bloomberg.com",
    "cnbc.com",
    "www.cnbc.com",
    "search.cnbc.com",
    "reuters.com",
    "www.reuters.com",
    "seekingalpha.com",
    "www.seekingalpha.com",
    "apnews.com",
    "www.apnews.com",
    "bleacherreport.com",
    "www.bleacherreport.com",
    "cbssports.com",
    "www.cbssports.com",
    "benzinga.com",
    "www.benzinga.com",
    "sportingnews.com",
    "www.sportingnews.com",
    "washingtonpost.com",
    "feeds.washingtonpost.com",
    "aljazeera.com",
    "www.aljazeera.com",
    "zerohedge.com",
    "www.zerohedge.com",
    "cms.zerohedge.com",
    "feeds.feedburner.com",
    "api.foxsports.com",
    "foxsports.com",
    "www.foxsports.com",
]


# ── HRRR future radar frames (NOAA's short-term weather MODEL — a genuine
# physics-based forecast, not pixel-extrapolation) ──
#
# Pure pixel-advection nowcasting (sliding existing radar echoes along
# their current motion vector) only stays physically meaningful for
# roughly 30-60 minutes — it can't predict storms forming, dissipating, or
# changing direction. A "couple hours" forecast needs an actual model.
# HRRR (High-Resolution Rapid Refresh) is NOAA's free, no-key, hourly-
# updated, radar-assimilated 3km model, and — importantly — it natively
# outputs a field called REFC (Composite Reflectivity, in dBZ): the SAME
# physical quantity and units as the live MRMS radar this app already
# shows, so future frames can share the live frames' color language
# instead of needing a visually different bolt-on.
#
# Fetched via NOAA's NOMADS "grib-filter" CGI endpoint, which subsets a
# single field + region server-side — this avoids downloading a full
# multi-hundred-MB model file just to get one small field for one small
# area. NOMADS asks that requests be paced (a short pause between them),
# respected below.
#
# HONESTLY UNVERIFIED (no network, no pygrib in this sandbox — the same
# category of limitation already flagged for the pysteps nowcasting work
# earlier in this project): the exact sub-hourly file naming convention
# (wrfsubhf{NN}.grib2 is a well-informed best guess, not confirmed against
# a live NOMADS directory listing) and the real publish latency after a
# run's nominal init hour. Both fail SAFELY though — a bad sub-hourly guess
# just degrades to hourly-only steps (see _fetch_hrrr_grib_messages'
# caller), and a bad latency guess just means _hrrr_candidate_runs tries a
# few more hours back until one actually works. First live run is the real
# check; if the sub-hourly step is silently missing from the frame list,
# that file-naming guess is the first thing to inspect.
try:
    import numpy as _hrrr_np
    _HRRR_NUMPY_OK = True
except Exception:
    _HRRR_NUMPY_OK = False

try:
    import pygrib as _pygrib
    _HRRR_PYGRIB_OK = True
except Exception:
    _HRRR_PYGRIB_OK = False

HRRR_AVAILABLE = _HRRR_NUMPY_OK and _HRRR_PYGRIB_OK

import tempfile as _tempfile

_HRRR_2D_FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl"
_HRRR_SUB_FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_sub.pl"
_HRRR_HEADERS = {"User-Agent": "personal-dashboard contact@example.com", "Accept": "*/*"}
# Conservative guess at how long after a run's nominal init hour NCEP has
# actually finished publishing its surface fields. If the freshest guessed
# run is consistently unavailable on a live check, widen this first.
_HRRR_PUBLISH_LATENCY_MIN = 80
_HRRR_MAX_RUN_LOOKBACK = 3   # try up to this many hours further back if the
                             # freshest guess isn't actually published yet
_HRRR_NOMADS_PAUSE_S = 1.5   # NOMADS asks callers to pace requests


def _hrrr_candidate_runs(now_dt=None):
    """Ordered list (newest first) of candidate HRRR run init datetimes to
    try, most-likely-already-published first."""
    now_dt = now_dt or datetime.now(timezone.utc)
    guess = (now_dt - timedelta(minutes=_HRRR_PUBLISH_LATENCY_MIN)).replace(minute=0, second=0, microsecond=0)
    return [guess - timedelta(hours=i) for i in range(_HRRR_MAX_RUN_LOOKBACK + 1)]


def _hrrr_future_targets(run_dt, now_dt=None, total_minutes=180):
    """Ordered list of {'valid_dt','elapsed_min','forecast_hour','subhourly'}
    describing which HRRR outputs to fetch for one run: 15-min steps for
    the portion of the first forecast hour that's still ahead of `now_dt`,
    hourly beyond that, up to total_minutes past now_dt. Every HRRR output
    step is some whole multiple of 15 minutes after run_dt (sub-hourly:
    0/15/30/45/60, hourly beyond: 60/120/180/...); candidates before
    `now_dt` are skipped, which is what naturally makes the returned list
    start close to "now" even though run_dt itself is roughly
    `_HRRR_PUBLISH_LATENCY_MIN` minutes in the past."""
    now_dt = now_dt or datetime.now(timezone.utc)
    end_dt = now_dt + timedelta(minutes=total_minutes)
    out = []
    elapsed = 0
    while True:
        valid_dt = run_dt + timedelta(minutes=elapsed)
        if valid_dt > end_dt:
            break
        if valid_dt >= now_dt:
            out.append({
                'valid_dt': valid_dt,
                'elapsed_min': elapsed,
                'forecast_hour': elapsed // 60,
                'subhourly': elapsed <= 60 and elapsed % 60 != 0,
            })
        elapsed += 15 if elapsed < 60 else 60
    return out


def _hrrr_run_dir(run_dt):
    return f"/hrrr.{run_dt.strftime('%Y%m%d')}/conus"


def _hrrr_2d_url(run_dt, forecast_hour, bbox):
    lon_min, lat_min, lon_max, lat_max = bbox
    return (f"{_HRRR_2D_FILTER_URL}?dir={_hrrr_run_dir(run_dt)}"
            f"&file=hrrr.t{run_dt.strftime('%H')}z.wrfsfcf{forecast_hour:02d}.grib2"
            f"&var_REFC=on&subregion="
            f"&toplat={lat_max}&bottomlat={lat_min}&leftlon={lon_min}&rightlon={lon_max}")


def _hrrr_sub_url(run_dt, forecast_hour, bbox):
    lon_min, lat_min, lon_max, lat_max = bbox
    return (f"{_HRRR_SUB_FILTER_URL}?dir={_hrrr_run_dir(run_dt)}"
            f"&file=hrrr.t{run_dt.strftime('%H')}z.wrfsubhf{forecast_hour:02d}.grib2"
            f"&var_REFC=on&subregion="
            f"&toplat={lat_max}&bottomlat={lat_min}&leftlon={lon_min}&rightlon={lon_max}")


def _fetch_hrrr_grib_messages(url):
    """Fetch a NOMADS grib-filter response and return pygrib message
    objects from it. Writes to a temp file since pygrib needs a real
    filesystem path, not bytes."""
    req = urllib.request.Request(url, headers=_HRRR_HEADERS)
    data, _ct = _fetch_with_deadline(req, ssl_ctx, 12, 25)
    if not data or len(data) < 50:
        raise ValueError("empty HRRR grib response")
    fd, path = _tempfile.mkstemp(suffix=".grib2")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        grbs = _pygrib.open(path)
        messages = list(grbs)
        grbs.close()
        return messages
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def _hrrr_pick_message_by_time(messages, valid_dt):
    """Match a specific sub-hourly message by its OWN reported valid time
    rather than assuming message order/index — the sub-hourly file's
    internal structure is the single least-certain part of this
    integration, so trust pygrib's own per-message metadata over a
    positional guess."""
    for msg in messages:
        try:
            msg_valid = msg.validDate
            if msg_valid.tzinfo is None:
                msg_valid = msg_valid.replace(tzinfo=timezone.utc)
            if abs((msg_valid - valid_dt).total_seconds()) < 60:
                return msg
        except Exception:
            continue
    return None


# NWS-style composite reflectivity color scale (dBZ -> RGB), approximating
# the standard scale NOAA's own MRMS renderer uses, so a future HRRR frame
# doesn't look like a visually different product from the live frames
# right before it in the same loop.
_HRRR_DBZ_RAMP = [
    (5, (100, 180, 255)),
    (15, (60, 140, 255)),
    (25, (60, 200, 90)),
    (35, (250, 230, 60)),
    (45, (250, 150, 40)),
    (55, (230, 50, 50)),
    (65, (200, 40, 180)),
    (75, (255, 255, 255)),
]


def _hrrr_dbz_to_rgba(arr):
    """arr: 2D numpy array (or masked array) of dBZ values. Returns an
    (H,W,4) uint8 RGBA array — masked/NaN/below-first-threshold pixels are
    fully transparent, same "unknown is not the same as clear" reasoning
    used for the nowcast NaN-edge handling elsewhere in this project."""
    filled = _hrrr_np.ma.filled(arr, _hrrr_np.nan) if _hrrr_np.ma.isMaskedArray(arr) else _hrrr_np.asarray(arr, dtype='float64')
    h, w = filled.shape
    out = _hrrr_np.zeros((h, w, 4), dtype=_hrrr_np.uint8)
    valid = ~_hrrr_np.isnan(filled)
    below_first = valid & (filled < _HRRR_DBZ_RAMP[0][0])
    out[~valid | below_first, 3] = 0
    for i, (thresh, (r, g, b)) in enumerate(_HRRR_DBZ_RAMP):
        hi = _HRRR_DBZ_RAMP[i + 1][0] if i + 1 < len(_HRRR_DBZ_RAMP) else float('inf')
        band = valid & (filled >= thresh) & (filled < hi)
        out[band, 0] = r
        out[band, 1] = g
        out[band, 2] = b
        out[band, 3] = 200
    return out


_HRRR_CACHE = {}
_HRRR_CACHE_LOCK = threading.Lock()
_HRRR_COMPUTE_LOCKS = {}
_HRRR_CACHE_TTL = 70 * 60  # a bit past HRRR's own ~hourly update cadence


def _hrrr_compute_lock(key):
    with _HRRR_CACHE_LOCK:
        lk = _HRRR_COMPUTE_LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _HRRR_COMPUTE_LOCKS[key] = lk
        return lk


def _hrrr_cache_get(key, ttl=_HRRR_CACHE_TTL):
    with _HRRR_CACHE_LOCK:
        entry = _HRRR_CACHE.get(key)
        if entry and (_time.time() - entry[0]) < ttl:
            return entry[1]
    return None


def _hrrr_cache_set(key, value):
    with _HRRR_CACHE_LOCK:
        _HRRR_CACHE[key] = (_time.time(), value)
        if len(_HRRR_CACHE) > 30:
            oldest = sorted(_HRRR_CACHE.items(), key=lambda kv: kv[1][0])[:15]
            for k, _ in oldest:
                del _HRRR_CACHE[k]


def _compute_hrrr_future(lat, lon, w, h, total_minutes=180):
    """Find the freshest published HRRR run, fetch REFC for each target
    valid time, and render each to a color-mapped PNG at the requested
    size. Returns {'run_init_ms', 'frames': [{'valid_ms','png'}, ...]}."""
    if not HRRR_AVAILABLE:
        raise RuntimeError("HRRR dependencies (pygrib/numpy) not installed")
    from PIL import Image
    bbox = ProxyHandler._radar_bbox(lat, lon, w, h)
    now_dt = datetime.now(timezone.utc)

    last_err = None
    for run_dt in _hrrr_candidate_runs(now_dt):
        try:
            targets = _hrrr_future_targets(run_dt, now_dt, total_minutes)
            if not targets:
                continue
            frames = []
            subh_cache = {}  # multiple 15-min targets share ONE sub-hourly file
            for tgt in targets:
                if tgt['subhourly']:
                    fh = tgt['forecast_hour']
                    if fh not in subh_cache:
                        subh_cache[fh] = _fetch_hrrr_grib_messages(_hrrr_sub_url(run_dt, fh, bbox))
                        _time.sleep(_HRRR_NOMADS_PAUSE_S)
                    msg = _hrrr_pick_message_by_time(subh_cache[fh], tgt['valid_dt'])
                else:
                    messages = _fetch_hrrr_grib_messages(_hrrr_2d_url(run_dt, tgt['forecast_hour'], bbox))
                    _time.sleep(_HRRR_NOMADS_PAUSE_S)
                    msg = messages[0] if messages else None
                if msg is None:
                    continue
                rgba = _hrrr_dbz_to_rgba(msg.values)
                img = Image.fromarray(rgba, mode="RGBA").resize((w, h), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                frames.append({'valid_ms': int(tgt['valid_dt'].timestamp() * 1000), 'png': buf.getvalue()})
            if frames:
                return {'run_init_ms': int(run_dt.timestamp() * 1000), 'frames': frames}
            last_err = RuntimeError("no frames could be parsed for this run")
        except Exception as e:
            last_err = e
            print(f"[hrrr] run {run_dt.isoformat()} failed: {e}", flush=True)
            continue
    raise last_err or RuntimeError("no HRRR run available")


class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=SERVE_DIR, **kwargs)

    def do_GET(self):
        # Serve the dashboard directly at / and /monitor (explicit text/html — bypasses OS mime db)
        if self.path in ('/', '/monitor', '/monitor.html', '/index.html'):
            self.serve_dashboard()
            return
        # Path-style proxy: /proxy/http://hostname/path  (used for local miners + external APIs)
        if self.path.startswith("/proxy/http://") or self.path.startswith("/proxy/https://"):
            self.handle_path_proxy()
        elif self.path.startswith("/hashrate-history"):
            self.handle_hashrate_history()
        elif self.path.startswith("/network-hashrate"):
            self.handle_network_hashrate()
        elif self.path.startswith("/proxy?"):
            self.handle_query_proxy()
        elif self.path.startswith("/yahoo?"):
            self.handle_yahoo()
        elif self.path.startswith("/futures-price?"):
            self.handle_futures_price()
        elif self.path.startswith("/debug-price?"):
            self.handle_debug_price()
        elif self.path.startswith("/quote?"):
            self.handle_quote()
        elif self.path.startswith("/news"):
            self.handle_news()
        elif self.path.startswith("/weather"):
            self.handle_weather()
        elif self.path.startswith("/radar-meta"):
            self.handle_radar_meta()
        elif self.path.startswith("/radar-frame"):
            self.handle_radar_frame()
        elif self.path.startswith("/radar-basemap"):
            self.handle_radar_basemap()
        elif self.path.startswith("/radar-future-meta"):
            self.handle_radar_future_meta()
        elif self.path.startswith("/radar-future-frame"):
            self.handle_radar_future_frame()
        elif self.path.startswith("/reader?"):
            self.handle_reader()
        elif self.path.startswith("/financials"):
            self.handle_financials()
        elif self.path.startswith("/primal-stats"):
            self.handle_primal_stats()
            return
        elif self.path.startswith("/primal-notes"):
            self.handle_primal_notes()
            return
        elif self.path.startswith("/asset-news"):
            self.handle_asset_news()
        elif self.path == "/miners":
            self.handle_miners_get()
        elif self.path.startswith("/ogp?"):
            self.handle_ogp()
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/miners":
            self.handle_miners_post()
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    MINERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "miners.json")

    def json_response(self, data):
        """Send a JSON response with correct Content-Length to prevent keep-alive stream corruption."""
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def serve_dashboard(self):
        """Serve monitor.html with an explicit text/html content type — never relies on OS mime db."""
        html_path = os.path.join(SERVE_DIR, "monitor.html")
        try:
            with open(html_path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            # NOTE: http.server's send_error() encodes the message as Latin-1.
            # Non-Latin-1 characters (em dashes, curly quotes, etc.) make
            # send_error() itself raise UnicodeEncodeError mid-response,
            # which kills the connection and shows the browser an empty
            # response instead of a real 404 page. Keep this message
            # plain-ASCII only.
            self.send_error(404, "monitor.html not found - make sure it's in the same folder as proxy.py")

    def handle_miners_get(self):
        try:
            if os.path.exists(self.MINERS_FILE):
                with open(self.MINERS_FILE) as f:
                    data = f.read()
            else:
                data = "[]"
        except Exception:
            data = "[]"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data.encode())

    def handle_miners_post(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            miners = json.loads(body)
            # Write atomically: a concurrent GET (handle_miners_get, running
            # on its own thread — this server is a ThreadingHTTPServer) can
            # land in the middle of a plain `open(path,"w")`, which truncates
            # the file to empty the instant it's opened, before json.dump has
            # written anything back. A GET landing in that window would read
            # an empty/partial file. The client already tolerates a failed or
            # unparseable /miners response by keeping its existing in-memory
            # miner list (see loadMinersFromServer's catch block), so this
            # specific race was never observed to actually wipe the miner
            # list client-side — but it's still a real, cheaply-avoided race
            # on the file itself (found while investigating a related report
            # of miners briefly appearing disconnected). Standard fix: write
            # to a temp file in the same directory, then atomically replace
            # the real file — a reader can only ever see the fully-old or
            # fully-new content, never a partial write.
            fd, tmp_path = tempfile.mkstemp(
                dir=os.path.dirname(self.MINERS_FILE) or ".",
                prefix=".miners.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(miners, f)
                os.replace(tmp_path, self.MINERS_FILE)
            except Exception:
                try: os.unlink(tmp_path)
                except OSError: pass
                raise
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        except Exception as e:
            self.send_error(500, str(e))

    def handle_path_proxy(self):
        """Handle /proxy/http://host/path — used by miner fetches and BTC hashrate APIs."""
        target = self.path[len("/proxy/"):]
        parsed = urlparse(target)
        netloc = parsed.netloc

        # Allow private/local IPs for miners
        is_local = bool(
            re.match(r'^192\.168\.\d+\.\d+(:\d+)?$', netloc) or
            re.match(r'^10\.\d+\.\d+\.\d+(:\d+)?$', netloc) or
            re.match(r'^172\.(1[6-9]|2\d|3[01])\.\d+\.\d+(:\d+)?$', netloc) or
            re.match(r'^(localhost|127\.\d+\.\d+\.\d+)(:\d+)?$', netloc)
        )
        is_allowed = any(netloc == o or netloc.endswith('.' + o) for o in ALLOWED_ORIGINS)

        if not is_local and not is_allowed:
            # /proxy/ path only listens on 127.0.0.1 — open to all domains (needed for Nostr CDNs/avatars)
            pass

        # mempool.space's hashrate/difficulty endpoints get hit repeatedly in
        # short windows (spark poll + modal open + tab switches) — serve from
        # cache when we have a fresh copy instead of re-hitting mempool.space
        # every time. Hashrate history is daily-candle data and gets a long
        # TTL; difficulty-adjustment stays short since it's the live reading.
        is_mempool_history = netloc == "mempool.space" and "/api/v1/mining/hashrate/" in target
        is_mempool_stats = is_mempool_history or (
            netloc == "mempool.space" and "/api/v1/difficulty-adjustment" in target
        )
        if is_mempool_stats:
            ttl = _HASHRATE_HISTORY_TTL if is_mempool_history else _MEMPOOL_CACHE_TTL
            cached = _mempool_cache_get(target, ttl)
            if cached:
                data, ct, src = cached
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("X-Hashrate-Source", src)
                self.end_headers()
                self.wfile.write(data)
                return

        try:
            # Detect if this is an image/video request and use appropriate headers.
            # Extension/keyword match covers the common case, but a lot of Nostr
            # media (Primal/blossom-style CDNs especially) is served from a bare
            # content hash with NO file extension and no 'image'/'media' keyword
            # in the path at all, e.g. https://blossom.primal.net/<sha256>. Those
            # were falling into the JSON-API branch below and picking up
            # Accept: application/json plus a Referer of https://mempool.space/ —
            # a combination some media hosts silently reject, which showed up as
            # images/videos that simply never rendered (no visible error, since
            # the client's onerror just falls through to the raw URL).
            target_no_qs = target.lower().split('?')[0]
            target_host = netloc.split(':')[0].lower()
            KNOWN_JSON_API_HOSTS = {
                'mempool.space', 'api.blockchair.com',
                'blockchain.info', 'api.blockchain.info',
            }
            is_image = target_no_qs.endswith(('.jpg','.jpeg','.png','.gif','.webp','.svg','.avif')) or \
                       any(h in target.lower() for h in ('image','avatar','picture','pfp','media'))
            is_video = target_no_qs.endswith(('.mp4','.mov','.webm','.m4v'))
            # Not a recognized JSON API and not a local device (miner) — most
            # likely a media blob served without a helpful extension/keyword.
            is_media_by_exclusion = (
                not is_image and not is_video and not is_local
                and target_host not in KNOWN_JSON_API_HOSTS
                and '/api/' not in target.lower()
                and not target_no_qs.endswith('.json')
            )
            if is_image or is_video or is_media_by_exclusion:
                hdrs = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.google.com/",
                    "Sec-Fetch-Dest": "image",
                    "Sec-Fetch-Mode": "no-cors",
                    "Sec-Fetch-Site": "cross-site",
                }
            else:
                hdrs = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                # The mempool.space Referer was previously sent to EVERY non-image
                # target — including miners, blockchair, blockchain.info, and LNURL
                # callback hosts, none of which want mempool.space as their Referer.
                # Only mempool.space itself gets it now.
                if target_host == 'mempool.space':
                    hdrs["Referer"] = "https://mempool.space/"
            req = urllib.request.Request(target, headers=hdrs)
            ctx = ssl_ctx if target.startswith("https://") else None
            # Local network devices (miners) may respond slowly.
            # IMPORTANT: urlopen's timeout= is per-socket-operation and does NOT
            # bound a stalled/dribbling connection — _fetch_with_deadline does.
            # That stall, not "the budget being too small", was the source of
            # the browser's "The operation was aborted." errors.
            fetch_timeout = 12 if is_local else 14
            if is_mempool_stats:
                # Single attempt with a hard 10s wall-clock deadline. If
                # mempool.space is throttling us, a retry just waits again —
                # the stale cache below is the real safety net.
                try:
                    data, ct = _fetch_with_deadline(req, ctx, 10, 10)
                except Exception as e:
                    print(f"[mempool] {target} failed ({e}) — serving stale cache if available")
                    stale = _mempool_cache_get_stale(target)
                    if stale:
                        sdata, sct, ssrc = stale
                        self.send_response(200)
                        self.send_header("Content-Type", sct)
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.send_header("X-Hashrate-Source", "stale-cache")
                        self.end_headers()
                        self.wfile.write(sdata)
                        return
                    raise
                _mempool_cache_set(target, data, ct, "mempool.space")
            else:
                try:
                    data, ct = _fetch_with_deadline(req, ctx, fetch_timeout, 20)
                except (urllib.error.URLError, TimeoutError) as e:
                    # One retry, for local (miner) and external requests alike.
                    # Local miners are on the LAN and a failure is almost
                    # always a transient Wi-Fi/ARP/DHCP blip rather than the
                    # miner actually being down — retrying once here avoids
                    # reporting a miner "offline" (and the client recording a
                    # fake 0 H/s sample) for a hiccup that clears in well
                    # under a second.
                    print(f"Path proxy retry for {target} after: {e}")
                    retry_timeout = 4 if is_local else 8
                    data, ct = _fetch_with_deadline(req, ctx, retry_timeout, 10)
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Access-Control-Allow-Origin", "*")
            if is_mempool_stats:
                self.send_header("X-Hashrate-Source", "mempool.space")
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            print(f"Path proxy error for {target}: {e}")
            # send_error() encodes the message as Latin-1 — keep it ASCII-only
            self.send_error(502, str(e).encode("ascii", "replace").decode("ascii"))

    def handle_hashrate_history(self):
        """
        GET /hashrate-history?span=1w|1m|3m|6m|1y|2y|3y|4y|10y|24h

        One reliable endpoint for the BTC network hashrate chart. Order:
          1. Fresh cache (6h TTL — this is daily-candle data)
          2. mempool.space, with a hard 10s wall-clock deadline
          3. blockchain.info hash-rate chart (normalized to mempool's shape)
          4. Stale cache of any age
          5. 502

        mempool.space has no 4y/10y series, so those spans skip straight to
        blockchain.info. The response always uses mempool.space's shape
        ({hashrates: [{timestamp, avgHashrate}], ...}) so the client doesn't
        care which source answered; X-Hashrate-Source says which one did.
        """
        qs = parse_qs(urlparse(self.path).query)
        span = qs.get('span', ['1w'])[0]
        if span not in _HASHRATE_WINDOWS:
            self.send_error(400, "unknown span - use one of: " + ",".join(_HASHRATE_WINDOWS))
            return

        cache_key = f"hashrate-history:{span}"
        cached = _mempool_cache_get(cache_key, _HASHRATE_HISTORY_TTL)
        if cached:
            data, ct, src = cached
            self._send_hashrate(data, ct, src)
            return

        hdrs = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,*/*",
        }

        # ── Source 1: mempool.space (native spans only) ──
        if span in _MEMPOOL_SPANS:
            try:
                url = f"{_MEMPOOL_BASE}/api/v1/mining/hashrate/{span}"
                req = urllib.request.Request(url, headers=hdrs)
                data, ct = _fetch_with_deadline(req, ssl_ctx if url.startswith("https://") else None, 10, 10)
                d = json.loads(data)
                if not d.get("hashrates"):
                    raise ValueError("empty hashrates in mempool.space response")
                # DIAGNOSTIC (v2.9.53): log the actual point count mempool.space
                # served for this span. Added after a report of the watchlist/
                # modal hashrate chart rendering as a near-straight 2-point line
                # for a 1w span — this response was accepted here (non-empty,
                # so the check above passed) with no visibility into HOW sparse
                # it actually was. mempool.space's own docs are ambiguous about
                # whether the 1w hashrate endpoint is always hourly-resolution,
                # so this makes it observable directly rather than inferred:
                # if this ever again shows a suspiciously low count (single or
                # low double digits) for a 1w/1m span, that confirms the sparse
                # response is coming from mempool.space itself, not something
                # proxy.py or the client is doing to it.
                print(f"[hashrate-history] mempool.space {span}: served {len(d.get('hashrates', []))} pts", flush=True)
                _mempool_cache_set(cache_key, data, ct, "mempool.space")
                self._send_hashrate(data, ct, "mempool.space")
                return
            except Exception as e:
                print(f"[hashrate-history] mempool.space {span} failed: {e} — trying blockchain.info", flush=True)

        # ── Source 2: blockchain.info charts (also the only source for 4y/10y) ──
        try:
            window = _HASHRATE_WINDOWS[span]
            ts_param = next(tp for lim, tp in _BLOCKCHAIN_TIMESPANS if window <= lim)
            url = f"{_BLOCKCHAIN_BASE}/charts/hash-rate?timespan={ts_param}&format=json"
            req = urllib.request.Request(url, headers=hdrs)
            data, ct = _fetch_with_deadline(req, ssl_ctx if url.startswith("https://") else None, 10, 12)
            d = json.loads(data)
            vals = d.get("values") or []
            unit = (d.get("unit") or "").lower()
            mult = 1e12 if "th/s" in unit else (1e9 if "gh/s" in unit else 1.0)
            cutoff = int(_time.time()) - window - 86400  # one day of slack for chart edges
            pts = [{"timestamp": int(v["x"]), "avgHashrate": v["y"] * mult}
                   for v in vals if v.get("x") and v.get("y") and v["x"] >= cutoff]
            if not pts:
                raise ValueError("no points in blockchain.info response")
            out = json.dumps({"hashrates": pts, "difficulty": [], "source": "blockchain.info"}).encode("utf-8")
            print(f"[hashrate-history] {span}: served {len(pts)} pts from blockchain.info fallback", flush=True)
            _mempool_cache_set(cache_key, out, "application/json", "blockchain.info")
            self._send_hashrate(out, "application/json", "blockchain.info")
            return
        except Exception as e:
            print(f"[hashrate-history] blockchain.info {span} failed: {e}", flush=True)

        # ── Source 3: stale cache of any age — old data beats no data ──
        stale = _mempool_cache_get_stale(cache_key)
        if stale:
            data, ct, src = stale
            print(f"[hashrate-history] {span}: all live sources failed — serving stale cache", flush=True)
            self._send_hashrate(data, ct, "stale-cache")
            return

        self.send_error(502, "all hashrate sources unavailable")

    def _send_hashrate(self, data, ct, source):
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Hashrate-Source", source)
        self.end_headers()
        self.wfile.write(data)

    def handle_network_hashrate(self):
        """
        GET /network-hashrate

        One canonical "current BTC network hashrate" reading (H/s) for the
        whole dashboard — watchlist row, ticker item, and chart modal all
        display THIS number, so they can never disagree with each other.

        Response: {"hashrate": <H/s float>, "source": "...", "ts": <unix>}
        X-Hashrate-Source header mirrors the source (or "stale-cache").

        Order:
          1. Fresh cache (2 min TTL — this is a live reading, not candles)
          2. mempool.space /api/v1/mining/hashrate/24h -> currentHashrate,
             falling back to the latest daily candle average.
             NOTE: /api/v1/difficulty-adjustment no longer returns
             currentHashrate (mempool.space removed the field) — the old
             client-side chain keyed off it and silently fell through to
             worse sources.
          3. blockchair /bitcoin/stats -> data.hashrate_24h
             (a STRING in H/s; hashrate_mean_1h does not exist)
          4. blockchain.info charts/hash-rate latest daily point
             (/q/hashrate is NOT used: it currently reads ~25% below
             blockchain.info's own chart data and appears unmaintained)
          5. Stale cache of any age
          6. 502
        """
        cache_key = "network-hashrate:current"
        cached = _mempool_cache_get(cache_key, _NETWORK_HASHRATE_TTL)
        if cached:
            data, ct, src = cached
            self._send_hashrate(data, ct, src)
            return

        hdrs = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,*/*",
        }

        def _emit(val, source):
            out = json.dumps({
                "hashrate": float(val),
                "source": source,
                "ts": int(_time.time()),
            }).encode("utf-8")
            _mempool_cache_set(cache_key, out, "application/json", source)
            print(f"[network-hashrate] {val/1e18:.2f} EH/s from {source}", flush=True)
            self._send_hashrate(out, "application/json", source)

        # ── Source 1: mempool.space ──
        try:
            url = f"{_MEMPOOL_BASE}/api/v1/mining/hashrate/24h"
            req = urllib.request.Request(url, headers=hdrs)
            data, _ct = _fetch_with_deadline(req, ssl_ctx if url.startswith("https://") else None, 10, 10)
            d = json.loads(data)
            val = d.get("currentHashrate")
            if not val or val <= 0:
                hrs = d.get("hashrates") or []
                val = hrs[-1].get("avgHashrate") if hrs else None
            if not val or val <= 0:
                raise ValueError("no currentHashrate in mempool.space response")
            _emit(val, "mempool.space")
            return
        except Exception as e:
            print(f"[network-hashrate] mempool.space failed: {e} — trying blockchair", flush=True)

        # ── Source 2: blockchair (hashrate_24h is a string, H/s) ──
        try:
            url = "https://api.blockchair.com/bitcoin/stats"
            req = urllib.request.Request(url, headers=hdrs)
            data, _ct = _fetch_with_deadline(req, ssl_ctx, 10, 10)
            d = json.loads(data)
            val = float((d.get("data") or {}).get("hashrate_24h") or 0)
            if val <= 0:
                raise ValueError("missing hashrate_24h in blockchair response")
            _emit(val, "blockchair")
            return
        except Exception as e:
            print(f"[network-hashrate] blockchair failed: {e} — trying blockchain.info", flush=True)

        # ── Source 3: blockchain.info chart, latest daily point ──
        try:
            url = f"{_BLOCKCHAIN_BASE}/charts/hash-rate?timespan=30days&format=json"
            req = urllib.request.Request(url, headers=hdrs)
            data, _ct = _fetch_with_deadline(req, ssl_ctx if url.startswith("https://") else None, 10, 12)
            d = json.loads(data)
            vals = d.get("values") or []
            unit = (d.get("unit") or "").lower()
            mult = 1e12 if "th/s" in unit else (1e9 if "gh/s" in unit else 1.0)
            if not vals or not vals[-1].get("y"):
                raise ValueError("no points in blockchain.info response")
            _emit(vals[-1]["y"] * mult, "blockchain.info")
            return
        except Exception as e:
            print(f"[network-hashrate] blockchain.info failed: {e}", flush=True)

        # ── Source 4: stale cache of any age — old data beats no data ──
        stale = _mempool_cache_get_stale(cache_key)
        if stale:
            data, ct, src = stale
            print("[network-hashrate] all live sources failed — serving stale cache", flush=True)
            self._send_hashrate(data, ct, "stale-cache")
            return

        self.send_error(502, "all network hashrate sources unavailable")

    def handle_query_proxy(self):
        """Handle /proxy?url=https://... (query-param style)"""
        qs = parse_qs(urlparse(self.path).query)
        target = qs.get("url", [None])[0]
        if not target:
            self.send_error(400, "Missing url parameter")
            return
        parsed = urlparse(target)
        if not any(parsed.netloc.endswith(o) for o in ALLOWED_ORIGINS):
            self.send_error(403, f"Domain not allowed: {parsed.netloc}")
            return
        try:
            req = urllib.request.Request(target, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "application/json,text/xml,application/xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            })
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as resp:
                data = resp.read()
                ct = resp.headers.get("Content-Type", "application/json")
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self.send_error(502, str(e))

    def handle_futures_price(self):
        """
        Returns live price + % change for any symbol (indices, stocks, ETFs).
        For index symbols: switches to futures when the regular market is closed.
        For stocks/ETFs: uses Yahoo pre/post market prices during extended hours.
        ALWAYS returns a valid JSON response — never a 502 or empty result.
        """
        import json as _json

        FUTURES_MAP = {
            '^GSPC': 'ES=F',   # S&P 500 -> E-mini S&P 500 futures
            '^IXIC': 'NQ=F',   # Nasdaq Composite -> E-mini Nasdaq-100 futures
            '^DJI':  'YM=F',   # Dow Jones -> E-mini Dow futures
            '^RUT':  'RTY=F',  # Russell 2000 -> E-mini Russell futures
            '^NYA':  'ES=F',
            '^FTSE': 'ES=F',
            '^N225': 'NQ=F',
        }

        # Symbols that trade continuously (24/7 or near-24/7) and should NEVER
        # be labeled PRE or POST — NYSE clock guards don't apply to them.
        # Commodity/financial futures (=F), bond yields (^TNX, ^TYX), VIX, crypto.
        # For these: always trust Yahoo's marketState and use regularMarketPrice directly.
        CONTINUOUS_SYMS = {'^TNX', '^TYX', '^VIX', '^FTSE', '^N225'}

        qs = parse_qs(urlparse(self.path).query)
        symbol = qs.get('symbol', [''])[0]
        if not symbol:
            self.send_error(400, 'Missing symbol'); return

        # Commodity/financial futures: any symbol ending in =F (GC=F, SI=F, CL=F, NG=F, etc.)
        is_continuous = (symbol in CONTINUOUS_SYMS or symbol.endswith('=F') or
                         symbol.endswith('-USD') or symbol.endswith('-USDT'))

        futures_sym = FUTURES_MAP.get(symbol)
        # Continuous symbols never use futures routing — they ARE their own live price
        if is_continuous:
            futures_sym = None

        HDR = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://finance.yahoo.com',
        }

        def _et_session_windows():
            """
            Compute NYSE session boundaries as UTC timestamps using the wall-clock ET time.
            On weekends, rolls back to the most recent Friday so candle extraction
            correctly identifies Friday's post-market candles (4–8 PM ET Friday).
            """
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            now_utc = _dt.now(_tz.utc)
            # Proper DST: second Sunday in March → first Sunday in November
            y = now_utc.year
            dst_s = _dt(y, 3, 8, 7, 0, tzinfo=_tz.utc) + _td(days=(6 - _dt(y, 3, 8).weekday()) % 7)
            dst_e = _dt(y, 11, 1, 6, 0, tzinfo=_tz.utc) + _td(days=(6 - _dt(y, 11, 1).weekday()) % 7)
            et_off = _td(hours=-4) if dst_s <= now_utc < dst_e else _td(hours=-5)
            now_et = now_utc + et_off
            # BUG FIX: midnight–4:00 AM ET on a normal weekday is still the
            # OVERNIGHT CARRY of LAST NIGHT's session, not the start of today's
            # own pre-market window. The old logic only rolled ref_et back on
            # Sat/Sun, so e.g. at Tuesday 1:00 AM it left ref_et = Tuesday,
            # making 'regular_close'/'post_close' Tuesday 4–8 PM ET — both still
            # ~15-19 hours in the FUTURE relative to "now". The candle-window
            # loop below then searches for post-market candles in a window that
            # hasn't happened yet, finds nothing, and the price silently falls
            # back to the plain regular-session close instead of last night's
            # real post-market print. Roll back to the prior trading day first
            # whenever we're in that dead zone, THEN apply the existing
            # Sat/Sun→Friday rollback on the result.
            ref_et = now_et
            if ref_et.hour < 4:          # 12:00am–3:59am ET → still last night
                ref_et = ref_et - _td(days=1)
            if ref_et.weekday() == 5:    # Saturday → back 1 day to Friday
                ref_et = ref_et - _td(days=1)
            elif ref_et.weekday() == 6:  # Sunday → back 2 days to Friday
                ref_et = ref_et - _td(days=2)
            # Midnight of the reference trading day in ET, expressed as UTC
            midnight_et = _dt(ref_et.year, ref_et.month, ref_et.day, tzinfo=_tz.utc) - et_off
            # Next trading day after ref_et (skips the weekend), used to widen
            # the "post-market" candle window all the way to the next
            # pre-market open instead of a hard 8:00 PM cutoff. If Yahoo's
            # public chart feed does carry any later overnight prints (e.g.
            # Blue Ocean ATS ticks folded into the same 1m candle stream),
            # this lets them be picked up as live extended-hours data instead
            # of being discarded; if it doesn't, this window simply stays
            # empty past 8 PM and behavior is unchanged.
            next_day = ref_et + _td(days=1)
            while next_day.weekday() >= 5:
                next_day = next_day + _td(days=1)
            next_midnight_et = _dt(next_day.year, next_day.month, next_day.day, tzinfo=_tz.utc) - et_off
            return {
                'pre_open':      (midnight_et + _td(hours=4,  minutes=0)).timestamp(),
                'regular_open':  (midnight_et + _td(hours=9,  minutes=30)).timestamp(),
                'regular_close': (midnight_et + _td(hours=16, minutes=0)).timestamp(),
                'post_close':    (next_midnight_et + _td(hours=4, minutes=0)).timestamp(),
                'now_et_str':    now_et.strftime('%H:%M'),
            }

        def fetch_v8_chart(sym, include_pre_post=True):
            """
            Use v8 chart API (no crumb/cookie needed).
            includePrePost=true fetches extended-hours candle data.
            Try query2 first (no rate-limit enforcement), then query1.

            Pre/post prices extracted from OHLCV candle timestamps (most reliable).
            Session boundaries always derived from wall-clock ET, never first-candle timestamp —
            this fixes ETFs/preferred stocks with zero pre-market volume (VXUS, STRK, etc.)
            where the first candle is from the previous regular session.
            """
            ipp = 'true' if include_pre_post else 'false'
            # Use 1m interval + range=2d for stocks/ETFs:
            #   - 1m gives finer granularity so sparse pre-market ticks aren't skipped
            #   - range=2d guarantees today's pre-market candles exist even when Yahoo's
            #     range=1d window begins at the regular open for low-volume ETFs
            # Futures/indices use 2m+1d (Yahoo caps 1m data for those symbols).
            is_futures_or_index = sym.endswith('=F') or sym.startswith('^')
            interval    = '2m' if is_futures_or_index else '1m'
            # Futures/indices: use 5d on weekends so Sunday session candles are included
            # (range=1d on Saturday/Sunday only covers that day — no futures trade then).
            # Weekdays: 1d is fine. Stocks/ETFs always use 2d for pre-market coverage.
            from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td2
            _now_utc = _dt2.now(_tz2.utc)
            _y = _now_utc.year
            _ds = _dt2(_y,3,8,7,0,tzinfo=_tz2.utc)+_td2(days=(6-_dt2(_y,3,8).weekday())%7)
            _de = _dt2(_y,11,1,6,0,tzinfo=_tz2.utc)+_td2(days=(6-_dt2(_y,11,1).weekday())%7)
            _et_off = _td2(hours=-4) if _ds <= _now_utc < _de else _td2(hours=-5)
            _dow = (_now_utc + _et_off).weekday()  # 5=Sat, 6=Sun
            _is_weekend = (_dow >= 5)
            if is_futures_or_index:
                chart_range = '5d' if _is_weekend else '1d'
            elif not include_pre_post:
                chart_range = '1d'
            else:
                chart_range = '2d'
            for host in ('query2.finance.yahoo.com', 'query1.finance.yahoo.com'):
                try:
                    url = (
                        f"https://{host}/v8/finance/chart/{sym}"
                        f"?interval={interval}&range={chart_range}&includePrePost={ipp}"
                    )
                    req = urllib.request.Request(url, headers=HDR)
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                        data = _json.loads(r.read())
                    result = data.get('chart', {}).get('result', [{}])[0]
                    meta = result.get('meta', {})
                    reg_price  = meta.get('regularMarketPrice') or 0
                    prev_close = meta.get('chartPreviousClose') or meta.get('previousClose') or 0
                    # Timestamp (unix seconds) of when reg_price was actually struck.
                    # Critical for once-a-day-priced assets (mutual funds): their NAV
                    # doesn't update until well after the 4pm ET close, so without this
                    # we can't tell "today's close" apart from "still showing yesterday's
                    # close because today's NAV hasn't posted yet".
                    mkt_time   = meta.get('regularMarketTime') or 0
                    # Compute pct ourselves from chartPreviousClose — same baseline the chart uses.
                    # Yahoo's regularMarketChangePercent can use a different prev close than
                    # chartPreviousClose (e.g. after weekends or data delays), causing the
                    # watchlist % to diverge from the 1D chart's Change stat.
                    reg_pct    = ((reg_price - prev_close) / prev_close * 100) if prev_close else 0
                    mkt_state  = meta.get('marketState', 'UNKNOWN')

                    if not reg_price:
                        continue  # no data at all, try other host

                    # Once-a-day-priced assets (mutual funds) have no real minute-level
                    # data, so the interval=1m request above often gets served meta that's
                    # stale relative to what Yahoo actually has posted. Detect "basically no
                    # real candles came back" and re-fetch with a plain daily interval.
                    #
                    # BUG FIX: this same daily-bar re-derivation is ALSO required for every
                    # normal, liquid stock/ETF request — not just sparse-candle ones. We
                    # request range=2d (so today's pre-market candles exist even for
                    # low-volume names), but Yahoo's meta.chartPreviousClose for a 2-day
                    # window is the close BEFORE the window starts — i.e. TWO trading days
                    # ago, not yesterday. For a liquid symbol like MSTR that never triggers
                    # the <5-candle branch, this silently made prev_close (and therefore
                    # every %-change derived from it, in every market session) off by a
                    # full trading day. Always re-derive prevClose from actual calendar-
                    # dated daily bars whenever chart_range == '2d', not only when candles
                    # are sparse.
                    _candle_count = len(result.get('timestamp') or [])
                    if _candle_count < 5 or chart_range == '2d':
                        try:
                            # BUG FIX: range=5d was frequently returning only ONE daily bar
                            # for sparsely-charted mutual funds (Yahoo's chart API just has
                            # thin history for these symbols). With only 1 bar available,
                            # there's nothing to use as a distinct previous close, so the
                            # logic below was forced to compare reg_price to itself — a
                            # second, different source of the exact-0% bug. range=1mo
                            # reliably returns enough trading days for a real comparison.
                            daily_url = f"https://{host}/v8/finance/chart/{sym}?interval=1d&range=1mo&includePrePost=false"
                            req_d = urllib.request.Request(daily_url, headers=HDR)
                            with urllib.request.urlopen(req_d, context=ssl_ctx, timeout=10) as rd:
                                ddata = _json.loads(rd.read())
                            dresult = ddata.get('chart', {}).get('result', [{}])[0]
                            dmeta   = dresult.get('meta', {})
                            d_reg_price = dmeta.get('regularMarketPrice') or 0

                            # DO NOT trust dmeta['chartPreviousClose']/['previousClose'] — for
                            # this mutual fund, that field has been independently confirmed
                            # wrong on the v8 chart API, quoteSummary, AND v7 quote, all
                            # returning the SAME wrong value. That means Yahoo's own
                            # "previousClose" metadata is stale for this symbol/asset class,
                            # not that any one of our requests is malformed. Instead, derive
                            # previous close ourselves from the actual daily bars: find which
                            # bar (by real calendar date, in the exchange's own timezone) is
                            # "today", and take the close immediately before it. This can't be
                            # thrown off by a metadata field lagging behind reality.
                            d_ts = dresult.get('timestamp') or []
                            d_closes = ((dresult.get('indicators', {})
                                                 .get('quote', [{}])[0]
                                                 .get('close')) or [])
                            # BUG FIX: `.get('gmtoffset', -14400)` only falls back to -14400
                            # when the key is MISSING — if Yahoo returns it as explicit
                            # `null` (plausible for mutual funds, which have no real
                            # exchange/session info), gmtoffset ends up None instead. That
                            # then throws inside _local_date's `ts + gmtoffset` below, which
                            # gets silently swallowed by this block's `except Exception`,
                            # leaving prev_close/reg_price at their original — wrong,
                            # 0%-producing — values. Explicitly coalesce None too.
                            gmtoffset = dmeta.get('gmtoffset')
                            if gmtoffset is None:
                                gmtoffset = -14400  # seconds; assume ET (EDT) if Yahoo gives us nothing
                            bars = [(t, c) for t, c in zip(d_ts, d_closes) if t is not None and c is not None]

                            # BUG FIX: Yahoo sometimes serves the SAME closing price for
                            # consecutive daily bars of a thinly-traded mutual fund (a
                            # forward-filled/placeholder "today" row before the next NAV
                            # posts, or a stale duplicate), but with tiny float32-vs-float64
                            # rounding noise between the two copies — e.g. 265.88 vs
                            # 265.8800048828125. That's a ~4.9e-6 absolute difference, which
                            # slipped past the old exact-equality checks (< 1e-6) below, so
                            # the duplicate got used as a fake "distinct" previous close and
                            # produced a silent, wrong -0.00% instead of yesterday's real
                            # move. Use a relative tolerance instead of a near-zero absolute
                            # one so near-duplicates are recognized as "the same price" too.
                            _REL_EPS = 1e-4  # ~0.01% — comfortably wider than float32 noise,
                                              # comfortably narrower than any real daily move
                            def _same_price(a, b):
                                if not a or not b:
                                    return False
                                return abs(a - b) <= max(abs(a), abs(b)) * _REL_EPS

                            d_prev_close = 0
                            if bars:
                                from datetime import datetime as _dt3, timezone as _tz3
                                def _local_date(ts):
                                    return _dt3.fromtimestamp(ts + gmtoffset, tz=_tz3.utc).date()
                                today_local = _local_date(_dt3.now(_tz3.utc).timestamp())
                                last_bar_date = _local_date(bars[-1][0])
                                # BUG FIX: once-a-day-priced assets (mutual funds) don't get a
                                # fresh intraday tick beyond their daily bars — the "current"
                                # price IS the most recent daily bar's close. The old logic
                                # assumed that whenever the last bar wasn't dated "today", the
                                # live reg_price must be a fresher price the chart hadn't caught
                                # up to yet, and used that same last bar as prevClose — but for
                                # mutual funds reg_price VS bars[-1] are the same value, so
                                # comparing them always yielded exactly 0% change. Detect that
                                # case (reg_price matches the last bar almost exactly) and step
                                # back for prevClose, same as the "dated today" branch below —
                                # that gives the most recent actual day-over-day move, which is
                                # what "today's return" should show until the next NAV posts.
                                _reg_is_last_bar = _same_price(d_reg_price, bars[-1][1])
                                # Start just before whichever bar represents "today"/"current",
                                # then scan further back past any near-duplicate closes (the
                                # placeholder/forward-filled rows described above) until we hit
                                # a genuinely distinct price — that's the real previous close.
                                start_idx = len(bars) - (2 if (last_bar_date == today_local or _reg_is_last_bar) else 1)
                                idx = start_idx
                                while idx >= 0:
                                    candidate = bars[idx][1]
                                    if candidate and not _same_price(candidate, d_reg_price):
                                        d_prev_close = candidate
                                        break
                                    idx -= 1
                                # else: every bar we have is a near-duplicate of the current
                                # price (or there weren't enough bars) — leave d_prev_close at
                                # 0 below rather than fabricate a 0% move from a duplicate.

                            if d_reg_price and d_prev_close:
                                _reason = (f"only {_candle_count} intraday candles" if _candle_count < 5
                                           else f"range={chart_range} meta.chartPreviousClose is pre-window, not pre-today")
                                print(f"[v8] {sym}: {_reason} — "
                                      f"using date-derived daily close: price {reg_price}->{d_reg_price} "
                                      f"prev {prev_close}->{d_prev_close} (was meta-trusted value "
                                      f"{dmeta.get('chartPreviousClose') or dmeta.get('previousClose')})")
                                reg_price  = d_reg_price
                                prev_close = d_prev_close
                                mkt_time   = dmeta.get('regularMarketTime') or mkt_time
                                reg_pct    = (reg_price - prev_close) / prev_close * 100
                        except Exception as ex:
                            print(f"[v8] {sym}: daily cross-check failed: {ex}")

                    candle_pre_price  = 0
                    candle_post_price = 0
                    candle_pre_pct    = 0
                    candle_post_pct   = 0
                    candle_pre_ts     = 0   # unix ts of the candle that produced candle_pre_price
                    candle_post_ts    = 0   # unix ts of the candle that produced candle_post_price

                    if include_pre_post:
                        try:
                            timestamps = result.get('timestamp') or []
                            closes = (result.get('indicators', {})
                                      .get('quote', [{}])[0]
                                      .get('close') or [])
                            if timestamps and closes:
                                win = _et_session_windows()
                                pre_open_utc      = win['pre_open']
                                regular_open_utc  = win['regular_open']
                                regular_close_utc = win['regular_close']
                                post_close_utc    = win['post_close']

                                # Walk all candles, recording last valid close per window.
                                last_ts = 0
                                last_cl = 0
                                for ts, cl in zip(timestamps, closes):
                                    if cl is None or cl <= 0:
                                        continue
                                    last_ts = ts
                                    last_cl = cl
                                    if pre_open_utc <= ts < regular_open_utc:
                                        candle_pre_price = cl
                                        candle_pre_ts    = ts
                                    elif regular_close_utc <= ts < post_close_utc:
                                        candle_post_price = cl
                                        candle_post_ts    = ts

                                # KEY FIX: for ETFs/preferred stocks (VXUS, STRK) Yahoo's
                                # range=2d response often covers only YESTERDAY — today's
                                # pre/post candles haven't been written yet. But Yahoo DOES
                                # update meta.regularMarketPrice in real-time to the current
                                # traded price regardless of session.
                                #
                                # Yahoo also returns marketState=None in chart meta for many
                                # ETFs/preferred stocks even during extended hours, so we use
                                # the wall-clock session windows instead of mkt_state.
                                import time as _time
                                now_ts = _time.time()
                                clock_in_pre  = pre_open_utc      <= now_ts < regular_open_utc
                                clock_in_post = regular_close_utc <= now_ts < post_close_utc
                                # Use regularMarketPrice as pre-market fallback when:
                                # (a) it has moved away from prev_close, OR
                                # (b) Yahoo itself reports marketState=PRE — meaning it IS
                                #     the live pre-market price even if it coincidentally equals
                                #     yesterday's close (e.g. STRK with light pre-market volume).
                                if candle_pre_price == 0 and clock_in_pre and reg_price and prev_close and (reg_price != prev_close or mkt_state == 'PRE'):
                                    candle_pre_price = reg_price
                                    candle_pre_ts    = mkt_time or now_ts
                                    print(f"[v8-candles] {sym}: using meta.regularMarketPrice={reg_price:.4f} as PRE price (no today candles yet, mkt_state={mkt_state})")
                                if candle_post_price == 0 and clock_in_post and reg_price and prev_close and reg_price != prev_close:
                                    candle_post_price = reg_price
                                    candle_post_ts    = mkt_time or now_ts
                                    print(f"[v8-candles] {sym}: using meta.regularMarketPrice={reg_price:.4f} as POST price (no today candles yet)")

                                if candle_pre_price and prev_close:
                                    candle_pre_pct = (candle_pre_price - prev_close) / prev_close * 100
                                if candle_post_price and prev_close:
                                    candle_post_pct = (candle_post_price - prev_close) / prev_close * 100

                                print(f"[v8-candles] {sym}: ET={win['now_et_str']} state={mkt_state} "
                                      f"candles={len(timestamps)} last={last_ts}({last_cl:.4f}) "
                                      f"pre={candle_pre_price:.4f}@{candle_pre_ts} post={candle_post_price:.4f}@{candle_post_ts}")
                        except Exception as ex:
                            print(f"[futures-price] candle extraction failed for {sym}: {ex}")

                    # Candle-derived prices beat meta fields (candles are tick-accurate).
                    # NOTE: meta.preMarketPrice/postMarketPrice carry no timestamp of their
                    # own from Yahoo, so when we fall back to them (candle_*_ts is 0) we use
                    # mkt_time (regularMarketTime) as the best available estimate — better
                    # than claiming "right now" when we don't actually know.
                    final_pre_price  = candle_pre_price  or meta.get('preMarketPrice')  or 0
                    final_pre_ts     = candle_pre_ts if candle_pre_price else (mkt_time or 0)
                    final_post_price = candle_post_price or meta.get('postMarketPrice') or 0
                    final_post_ts    = candle_post_ts if candle_post_price else (mkt_time or 0)

                    # BUG FIX: never trust Yahoo's raw meta.preMarketChangePercent /
                    # meta.postMarketChangePercent fields. Yahoo computes those against
                    # *today's* regularMarketPrice (i.e. "how much has it moved since the
                    # regular close"), not against prev_close ("total change from yesterday's
                    # close") like every other pct in this app and like the watchlist/chart
                    # are labeled to show. Falling back to them (which happened whenever
                    # candle_pre_pct/candle_post_pct came out 0 — common for choppy,
                    # thinly-candled post-market data) silently swapped in a number
                    # answering a different question, which could even flip the sign
                    # relative to the displayed price. Always derive pct ourselves from
                    # the final price vs prev_close so price and pct are always consistent.
                    final_pre_pct  = ((final_pre_price  - prev_close) / prev_close * 100) if (final_pre_price  and prev_close) else 0
                    final_post_pct = ((final_post_price - prev_close) / prev_close * 100) if (final_post_price and prev_close) else 0

                    print(f"[v8] {sym}: state={mkt_state} reg={reg_price:.4f} "
                          f"pre_meta={meta.get('preMarketPrice') or 0:.4f} pre_candle={candle_pre_price:.4f} "
                          f"post_meta={meta.get('postMarketPrice') or 0:.4f} post_candle={candle_post_price:.4f}")

                    return {
                        'regularMarketPrice':         reg_price,
                        'postMarketPrice':            final_post_price,
                        'postMarketChangePercent':    final_post_pct,
                        'postMarketTime':              final_post_ts,
                        'preMarketPrice':             final_pre_price,
                        'preMarketChangePercent':     final_pre_pct,
                        'preMarketTime':               final_pre_ts,
                        'regularMarketPreviousClose': prev_close,
                        'regularMarketChangePercent': reg_pct,
                        'marketState':                mkt_state,
                        'regularMarketTime':          mkt_time,
                    }
                except Exception as ex:
                    print(f"[futures-price] v8 {host} failed for {sym}: {ex}")

            # ── LAST-RESORT FALLBACK: the interval=1m/2d request above failed or
            # returned reg_price=0 on BOTH hosts. This happens for mutual funds
            # that have no true intraday quotes at all — Yahoo sometimes errors
            # out (rather than returning a sparse-but-valid candle set) when asked
            # for 1-minute data on these symbols, which meant we never reached the
            # daily-bar cross-check above and fell through to a stale/0%-producing
            # value pulled from quoteSummary elsewhere in this file. Retry using
            # ONLY a plain daily chart (which mutual funds always support) and
            # derive price/prevClose from calendar-dated daily bars, exactly like
            # the low-candle-count branch above.
            for host in ('query2.finance.yahoo.com', 'query1.finance.yahoo.com'):
                try:
                    daily_url = f"https://{host}/v8/finance/chart/{sym}?interval=1d&range=10d&includePrePost=false"
                    req_d = urllib.request.Request(daily_url, headers=HDR)
                    with urllib.request.urlopen(req_d, context=ssl_ctx, timeout=10) as rd:
                        ddata = _json.loads(rd.read())
                    dresult = ddata.get('chart', {}).get('result', [{}])[0]
                    dmeta   = dresult.get('meta', {})
                    d_reg_price = dmeta.get('regularMarketPrice') or 0
                    if not d_reg_price:
                        continue
                    gmtoffset = dmeta.get('gmtoffset')
                    if gmtoffset is None:
                        gmtoffset = -14400
                    d_ts     = dresult.get('timestamp') or []
                    d_closes = ((dresult.get('indicators', {})
                                         .get('quote', [{}])[0]
                                         .get('close')) or [])
                    bars = [(t, c) for t, c in zip(d_ts, d_closes) if t is not None and c is not None]
                    if not bars:
                        continue
                    from datetime import datetime as _dt5, timezone as _tz5
                    def _local_date5(ts):
                        return _dt5.fromtimestamp(ts + gmtoffset, tz=_tz5.utc).date()
                    today_local   = _local_date5(_dt5.now(_tz5.utc).timestamp())
                    last_bar_date = _local_date5(bars[-1][0])
                    reg_is_last_bar = abs(d_reg_price - bars[-1][1]) < 1e-6
                    if (last_bar_date == today_local or reg_is_last_bar) and len(bars) >= 2:
                        d_prev_close = bars[-2][1]
                    else:
                        d_prev_close = bars[-1][1]
                    if not d_prev_close:
                        continue
                    pct = (d_reg_price - d_prev_close) / d_prev_close * 100
                    print(f"[v8-daily-fallback] {sym}: 1m/2d chart unavailable on both hosts — "
                          f"derived from pure daily bars: price={d_reg_price} prev={d_prev_close} pct={pct:+.2f}%")
                    return {
                        'regularMarketPrice':         d_reg_price,
                        'postMarketPrice':            0,
                        'postMarketChangePercent':    0,
                        'preMarketPrice':             0,
                        'preMarketChangePercent':     0,
                        'regularMarketPreviousClose': d_prev_close,
                        'regularMarketChangePercent': pct,
                        'marketState':                dmeta.get('marketState') or 'REGULAR',
                        'regularMarketTime':          dmeta.get('regularMarketTime') or 0,
                    }
                except Exception as ex:
                    print(f"[v8-daily-fallback] {sym}: {host} failed: {ex}")
            return {}

        def fetch_quoteSummary(sym):
            """
            Yahoo /v10/finance/quoteSummary?modules=price — more reliable than v7/quote
            for pre/post market prices of ETFs and preferred stocks.

            Yahoo's chart API (v8) only populates meta.preMarketPrice for symbols with
            ACTIVE pre-market trading (MSTR, TSLA, etc.). For low-volume ETFs (VXUS)
            and preferred stocks (STRK) it returns 0 even when trades did occur.
            quoteSummary/price uses a different pricing engine and populates these fields
            for a much wider set of symbols. No crumb required from home/local IPs.
            """
            try:
                url = (f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}'
                       f'?modules=price&includePrePost=true&corsDomain=finance.yahoo.com')
                req = urllib.request.Request(url, headers=HDR)
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as r:
                    data = _json.loads(r.read())
                price = (data.get('quoteSummary', {})
                             .get('result', [{}])[0]
                             .get('price', {}))
                if not price:
                    return {}
                def _val(key):
                    v = price.get(key)
                    if isinstance(v, dict):
                        return v.get('raw') or 0
                    return v or 0
                result = {
                    'regularMarketPrice':         _val('regularMarketPrice'),
                    'regularMarketPreviousClose': _val('regularMarketPreviousClose'),
                    'regularMarketChangePercent': _val('regularMarketChangePercent'),
                    'preMarketPrice':             _val('preMarketPrice'),
                    'preMarketChangePercent':     _val('preMarketChangePercent'),
                    'postMarketPrice':            _val('postMarketPrice'),
                    'postMarketChangePercent':    _val('postMarketChangePercent'),
                    'marketState':                price.get('marketState', 'UNKNOWN'),
                    'regularMarketTime':          _val('regularMarketTime'),
                }
                print(f"[quoteSummary] {sym}: state={result['marketState']} "
                      f"pre={result['preMarketPrice']:.4f} post={result['postMarketPrice']:.4f}")
                return result
            except Exception as ex:
                print(f"[quoteSummary] {sym} failed: {ex}")
            return {}

        def fetch_stooq(sym):
            """
            Stooq provides real-time/delayed quotes for futures via a simple CSV endpoint.
            Symbol mapping: ES=F -> @ES.US, NQ=F -> @NQ.US etc.
            Returns dict with regularMarketPrice and regularMarketPreviousClose.
            """
            STOOQ_MAP = {
                'ES=F': '@ES.US', 'NQ=F': '@NQ.US', 'YM=F': '@YM.US',
                'RTY=F': '@RTY.US', 'GC=F': '@GC.US', 'CL=F': '@CL.US',
            }
            stooq_sym = STOOQ_MAP.get(sym)
            if not stooq_sym:
                return {}
            try:
                url = f"https://stooq.com/q/l/?s={stooq_sym}&f=sd2t2ohlcvn"
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'text/csv',
                })
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as r:
                    text = r.read().decode('utf-8', errors='replace')
                # CSV: Symbol,Date,Time,Open,High,Low,Close,Volume,Name
                lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
                if len(lines) >= 2:
                    parts = lines[1].split(',')
                    if len(parts) >= 7:
                        close = float(parts[6]) if parts[6] not in ('N/D', '') else 0
                        open_ = float(parts[3]) if parts[3] not in ('N/D', '') else 0
                        if close > 0:
                            # NOTE: on weekends, open_ is the Sunday-reopen price, not Friday close.
                            # We tag prev as 0 here so the caller's fut_prev fallback logic will
                            # use base_prev (the index's Friday close) instead — much more accurate.
                            from datetime import datetime as _dt
                            _is_weekday = _dt.now().weekday() < 5
                            prev_approx = open_ if _is_weekday else 0
                            print(f"[Stooq] {sym} ({stooq_sym}): {close} open={open_} prev_approx={prev_approx}")
                            return {
                                'regularMarketPrice': close,
                                'regularMarketPreviousClose': prev_approx,
                                'marketState': 'REGULAR',
                            }
            except Exception as ex:
                print(f"[futures-price] Stooq failed for {sym}: {ex}")
            return {}

        def send_result(result):
            out = _json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(out)

        # --- Fetch base symbol AND futures in parallel (all using v8, no crumb needed) ---
        import threading
        base = {}
        fut_data = {}

        def fetch_v7_quote(sym):
            """
            Yahoo v7/finance/quote — last-resort fallback only.
            Frequently requires crumb+cookie; often returns empty results.
            quoteSummary/price (above) is preferred for pre/post prices.
            """
            fields = (
                'regularMarketPrice,regularMarketPreviousClose,regularMarketChangePercent,'
                'preMarketPrice,preMarketChangePercent,'
                'postMarketPrice,postMarketChangePercent,marketState'
            )
            try:
                url = f'https://query2.finance.yahoo.com/v7/finance/quote?symbols={sym}&fields={fields}'
                req = urllib.request.Request(url, headers=HDR)
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=6) as r:
                    data = _json.loads(r.read())
                result = data.get('quoteResponse', {}).get('result', [])
                if result:
                    q = result[0]
                    return {
                        'regularMarketPrice':         q.get('regularMarketPrice') or 0,
                        'regularMarketPreviousClose': q.get('regularMarketPreviousClose') or 0,
                        'regularMarketChangePercent': q.get('regularMarketChangePercent') or 0,
                        'preMarketPrice':             q.get('preMarketPrice') or 0,
                        'preMarketChangePercent':     q.get('preMarketChangePercent') or 0,
                        'postMarketPrice':            q.get('postMarketPrice') or 0,
                        'postMarketChangePercent':    q.get('postMarketChangePercent') or 0,
                        'marketState':                q.get('marketState', 'UNKNOWN'),
                    }
            except Exception:
                pass
            return {}

        def fetch_base():
            nonlocal base
            # Run v8 chart, quoteSummary, and v7 quote in parallel.
            # Priority for pre/post prices (highest to lowest):
            #   1. v8 candle-derived prices  (tick-accurate, when candles exist)
            #   2. quoteSummary/price        (reliable for ETFs & preferred stocks)
            #   3. v8 meta.preMarketPrice    (often 0 for low-volume symbols)
            #   4. v7 quote                  (deprecated, often fails without auth)
            v8_result  = {}
            qs_result  = {}
            v7_result  = {}
            import threading as _thr
            t_v8 = _thr.Thread(target=lambda: v8_result.update(fetch_v8_chart(symbol, include_pre_post=True) or {}))
            t_qs = _thr.Thread(target=lambda: qs_result.update(fetch_quoteSummary(symbol) or {}))
            t_v7 = _thr.Thread(target=lambda: v7_result.update(fetch_v7_quote(symbol) or {}))
            t_v8.start(); t_qs.start(); t_v7.start()
            t_v8.join(); t_qs.join(); t_v7.join()

            # Start with v8 as the base (has reg price + candle-derived pre/post)
            base = v8_result.copy() if v8_result else {}

            # Layer in quoteSummary for pre/post prices it catches that candles miss
            # (ETFs with sparse volume, preferred stocks, recently-listed tickers)
            for src in (qs_result, v7_result):
                if not src:
                    continue
                src_label = 'v7' if src is v7_result else 'qs'
                src_state = src.get('marketState', 'UNKNOWN') or 'UNKNOWN'
                # More current marketState from any source wins
                if src_state != 'UNKNOWN':
                    if base.get('marketState', 'UNKNOWN') == 'UNKNOWN':
                        base['marketState'] = src_state
                # Fill in missing pre/post prices (never override a good candle value)
                for field in ('preMarketPrice', 'preMarketChangePercent',
                              'postMarketPrice', 'postMarketChangePercent'):
                    if not base.get(field) and src.get(field):
                        base[field] = src[field]
                        print(f"[fetch_base] {symbol}: filled {field}={src[field]:.4f} from {src_label}")
                # quoteSummary/v7 don't give us a genuine tick timestamp for their
                # pre/post prices — if we just borrowed one of those fields above,
                # make sure we don't leave a stale/zero *Time value claiming it's
                # from v8's candle extraction. Best honest estimate is this
                # source's own regularMarketTime.
                if base.get('preMarketPrice') and not base.get('preMarketTime') and src.get('regularMarketTime'):
                    base['preMarketTime'] = src['regularMarketTime']
                if base.get('postMarketPrice') and not base.get('postMarketTime') and src.get('regularMarketTime'):
                    base['postMarketTime'] = src['regularMarketTime']
                # NOTE: quoteSummary's own regularMarketChangePercent/PreviousClose is
                # deliberately NOT trusted here anymore. It was tried as the authoritative
                # source, but for mutual funds it returns the SAME stale previousClose as the
                # v8 chart meta — confirming Yahoo's own "previousClose" field lags for this
                # asset class across every endpoint, not just one. base's regularMarketChangePercent
                # (from fetch_v8_chart, now date-derived from actual daily bars rather than any
                # Yahoo meta field) is more trustworthy and is left untouched here.
                # Fill reg price if v8 failed entirely
                if not base.get('regularMarketPrice') and src.get('regularMarketPrice'):
                    base['regularMarketPrice'] = src['regularMarketPrice']
                if not base.get('regularMarketPreviousClose') and src.get('regularMarketPreviousClose'):
                    base['regularMarketPreviousClose'] = src['regularMarketPreviousClose']
                if not base.get('regularMarketTime') and src.get('regularMarketTime'):
                    base['regularMarketTime'] = src['regularMarketTime']
                # STRK / low-volume preferred stock fix:
                # Yahoo's preMarketPrice field is 0 for these symbols even in both v8 and
                # quoteSummary. But quoteSummary DOES return the correct live price in
                # regularMarketPrice when marketState=PRE. Synthesize preMarketPrice from it.
                if (not base.get('preMarketPrice')
                        and src_state == 'PRE'
                        and src.get('regularMarketPrice')
                        and src.get('regularMarketPreviousClose')
                        and src['regularMarketPrice'] != src['regularMarketPreviousClose']):
                    synth = src['regularMarketPrice']
                    prev  = src['regularMarketPreviousClose']
                    base['preMarketPrice'] = synth
                    base['preMarketChangePercent'] = (synth - prev) / prev * 100
                    base['preMarketTime'] = src.get('regularMarketTime') or 0
                    base['marketState'] = 'PRE'
                    print(f"[fetch_base] {symbol}: synthesised preMarketPrice={synth:.4f} "                          f"from {src_label}.regularMarketPrice (state=PRE, prev={prev:.4f})")

        def fetch_fut():
            nonlocal fut_data
            if not futures_sym: return
            # v8 with includePrePost=true gives us the live overnight/weekend futures price
            fut_data = fetch_v8_chart(futures_sym, include_pre_post=True)
            # If v8 returned nothing or 0 price, try Stooq as backup
            if not fut_data.get('regularMarketPrice'):
                stooq = fetch_stooq(futures_sym)
                if stooq.get('regularMarketPrice'):
                    fut_data = stooq

        t1 = threading.Thread(target=fetch_base)
        t2 = threading.Thread(target=fetch_fut)
        t1.start(); t2.start()
        t1.join(); t2.join()

        market_state = base.get('marketState', 'UNKNOWN')
        base_price   = base.get('regularMarketPrice') or 0
        base_prev    = base.get('regularMarketPreviousClose') or 0
        base_pct     = base.get('regularMarketChangePercent') or (
            ((base_price - base_prev) / base_prev * 100) if base_prev else 0
        )
        post_price = base.get('postMarketPrice') or 0
        post_time  = base.get('postMarketTime') or 0    # unix ts price was actually struck (0 = unknown)
        pre_price  = base.get('preMarketPrice') or 0
        pre_time   = base.get('preMarketTime') or 0
        # BUG FIX: don't trust base.get('postMarketChangePercent')/('preMarketChangePercent')
        # here. Those can originate from Yahoo's raw quoteSummary/v7 fields (computed vs
        # today's regularMarketPrice, not vs prev_close) via the field-by-field fill-in
        # above, and — because price and pct are filled in independently per source — the
        # price shown could even come from one source while its pct came from another.
        # Always recompute both from the FINAL price vs base_prev here, at a single choke
        # point, so price and pct are guaranteed to agree and use the same "vs previous
        # close" baseline as everything else in the app.
        post_pct = ((post_price - base_prev) / base_prev * 100) if (post_price and base_prev) else 0
        pre_pct  = ((pre_price  - base_prev) / base_prev * 100) if (pre_price  and base_prev) else 0
        # POST-MARKET/OVERNIGHT DISPLAY PCT: the PRE/POST badge is meant to show
        # "how has the price moved THIS session" — for post-market/overnight that
        # means since the regular session's own close, not since the previous
        # day's close. (Yahoo's own "Overnight" quote does the same: it shows
        # +0.54% measured off today's 4pm close, not off yesterday's close.)
        # Using base_prev here instead — as post_pct above still does — produces
        # a technically-correct "since yesterday" number that visually looks like
        # the opposite sign/magnitude of what every other source shows next to a
        # POST/overnight quote, which reads as "wrong" even though the math is
        # sound. Kept as a SEPARATE variable (not touching post_pct/base_prev)
        # because 'prev' is still sent as base_prev to the client for portfolio
        # day-change and chart-baseline math elsewhere, which correctly wants
        # the true previous-trading-day close, not today's close.
        post_pct_vs_close = ((post_price - base_price) / base_price * 100) if (post_price and base_price) else 0
        # When this regularMarketPrice/PreviousClose pair was actually struck.
        # For once-a-day-priced assets (mutual funds) this can lag well behind
        # "now" even after the market has closed for the day — see asOf usage below.
        as_of      = base.get('regularMarketTime') or 0
        # Futures live price — v8 with includePrePost=true gives overnight data
        fut_price  = fut_data.get('regularMarketPrice') or 0
        fut_prev   = (fut_data.get('regularMarketPreviousClose') or
                      base_prev or 0)
        print(f"[extended-price] {symbol}: market_state={market_state} base_price={base_price} "
              f"post_price={post_price} pre_price={pre_price} fut_price={fut_price} "
              f"fut_sym={futures_sym}")

        # ── CONTINUOUS / ALWAYS-ON SYMBOLS ───────────────────────────────────────
        # Commodity futures (GC=F, SI=F, CL=F…), crypto (BTC-USD…), bond yields (^TNX…),
        # and VIX trade continuously or are index-derived. NYSE clock doesn't apply.
        # Always return regularMarketPrice with no PRE/POST badge.
        # hasExtendedData=False so sparkline never requests includePrePost.
        if is_continuous and base_price > 0:
            print(f'[CONTINUOUS] {symbol}: {base_price:.4f} ({base_pct:+.2f}%)')
            send_result({'symbol': symbol, 'price': base_price, 'pct': base_pct,
                         'prev': base_prev, 'marketState': 'REGULAR',
                         'isFutures': False, 'futuresSym': None,
                         'hasExtendedData': False, 'asOf': as_of})
            return

        def _pct(price, prev):
            if prev and prev > 0:
                return (price - prev) / prev * 100
            return 0

        def _send_futures(state_label):
            pct = _pct(fut_price, fut_prev) if fut_prev else fut_data.get('regularMarketChangePercent') or 0
            print(f"[{state_label}] {symbol} via {futures_sym}: {fut_price:.2f} ({pct:+.2f}%)")
            import time as _t
            send_result({'symbol': symbol, 'price': fut_price, 'pct': pct,
                         'prev': fut_prev, 'marketState': state_label,
                         'isFutures': True, 'futuresSym': futures_sym, 'asOf': _t.time()})

        # ── WALL-CLOCK ET TIME CHECK ─────────────────────────────────────────────
        # Yahoo's marketState can lag by several minutes at open/close transitions.
        # Use actual clock time (ET = UTC-5 standard / UTC-4 DST) as ground truth.
        from datetime import timedelta
        now_utc = datetime.now(timezone.utc)
        # Determine ET offset: DST runs second Sunday in March → first Sunday in November
        year = now_utc.year
        dst_start = datetime(year, 3,  8, 7, 0, tzinfo=timezone.utc) + timedelta(days=(6 - datetime(year, 3,  8).weekday()) % 7)
        dst_end   = datetime(year, 11, 1, 6, 0, tzinfo=timezone.utc) + timedelta(days=(6 - datetime(year, 11, 1).weekday()) % 7)
        et_offset = timedelta(hours=-4) if dst_start <= now_utc < dst_end else timedelta(hours=-5)
        now_et = now_utc + et_offset
        dow = now_et.weekday()           # 0=Mon … 4=Fri, 5=Sat, 6=Sun
        hm  = now_et.hour * 60 + now_et.minute  # minutes since midnight ET
        # NYSE regular session: Mon–Fri 9:30–16:00 ET
        clock_regular = (0 <= dow <= 4) and (570 <= hm < 960)   # 9:30=570, 16:00=960
        # NYSE pre-market: Mon–Fri 4:00–9:30 ET
        clock_pre     = (0 <= dow <= 4) and (240 <= hm < 570)
        # NYSE post-market: Mon–Fri 16:00–20:00 ET
        clock_post    = (0 <= dow <= 4) and (960 <= hm < 1200)
        et_time_str = now_et.strftime('%a %H:%M')
        print(f'[futures-price] wall-clock ET: {et_time_str} regular={clock_regular} pre={clock_pre} post={clock_post}, Yahoo says market_state={market_state}')

        # ── REGULAR SESSION ──────────────────────────────────────────────────────
        # Trust clock over Yahoo's marketState (Yahoo can lag 1-5 min at open/close).
        # clock_post acts as a hard override: if wall clock says it's past 4 PM ET,
        # NEVER route to REGULAR even if Yahoo still says 'REGULAR' (stale lag).
        # Similarly clock_pre overrides stale REGULAR at open (shouldn't happen but safe).
        # Weekend guard (dow 5=Sat, 6=Sun): Yahoo sometimes returns marketState='REGULAR'
        # on Saturday/Sunday before it refreshes — clock_regular is already False on weekends,
        # but the Yahoo state alone could still slip through. Block it explicitly.
        clock_weekend = (dow >= 5)
        if (market_state == 'REGULAR' or clock_regular) and not clock_post and not clock_pre and not clock_weekend and base_price > 0:
            print(f'[REGULAR] {symbol}: {base_price:.2f} ({base_pct:+.2f}%) clock={clock_regular} yahoo={market_state}')
            send_result({'symbol': symbol, 'price': base_price, 'pct': base_pct,
                         'prev': base_prev, 'marketState': 'REGULAR',
                         'isFutures': False, 'futuresSym': futures_sym,
                         'hasExtendedData': False, 'asOf': as_of})
            return

        # ── POST-MARKET (4:00–8:00 PM ET weekdays) ───────────────────────────────
        if market_state in ('POST', 'POSTPOST') or clock_post:
            if fut_price > 0 and futures_sym:
                _send_futures('POST')
                return
            if post_price > 0:
                # PRIMARY pct: current LIVE price vs YESTERDAY's close — the
                # same baseline as REGULAR/PRE, just with the freshest price
                # plugged in. Confirmed against Yahoo's own After Hours quote:
                # price 96.51 vs prev close 97.33 = -0.84%, matching Yahoo's
                # headline exactly. This is NOT frozen at the 4pm print
                # (that undercounts any post-market move) and NOT measured
                # vs the 4pm close either (that answers "how far since the
                # bell", a different question — +0.44% here, not -0.84%).
                # `post_pct` already computed this a few lines up.
                # extPct is the SEPARATE secondary number — how far price has
                # moved since the close — for UI that wants to annotate the
                # live extended price the way Yahoo's own watchlist shows a
                # small overnight/post-market % under the main one, later at
                # night once it switches to its dual-row "At close" + small
                # "Overnight" display.
                ext_pct = post_pct_vs_close or _pct(post_price, base_price) or post_pct
                # BUG FIX: this used to stamp asOf with the CURRENT wall-clock
                # time (_t.time()) regardless of how old the underlying candle
                # actually was. That made a price that hadn't moved in hours
                # look perfectly live to the client's staleness check — which
                # is exactly why a frozen post-market print could sit on screen
                # looking "live" all night. Use the real candle/print timestamp
                # (post_time) when we have one; only fall back to "now" if we
                # genuinely have no timestamp at all (better than showing 0).
                import time as _t
                real_asof = post_time or _t.time()
                send_result({'symbol': symbol, 'price': post_price, 'pct': post_pct,
                             'extPct': ext_pct, 'extPrice': post_price,
                             'prev': base_prev, 'marketState': 'POST',
                             'isFutures': False, 'futuresSym': futures_sym,
                             'hasExtendedData': True, 'asOf': real_asof})
                return
            # 4:00–4:15 PM ET gap: futures maintenance, no post-market data yet
            # Show last regular-session close labeled POST so UI shows "Post-Market"
            if base_price > 0:
                print(f"[POST gap] {symbol}: maintenance window, showing close {base_price:.2f}")
                send_result({'symbol': symbol, 'price': base_price, 'pct': base_pct,
                             'prev': base_prev, 'marketState': 'POST',
                             'isFutures': False, 'futuresSym': futures_sym,
                             'hasExtendedData': False, 'asOf': as_of})
                return

        # ── PRE-MARKET (4:00–9:30 AM ET weekdays) ────────────────────────────────
        if market_state == 'PRE' or clock_pre:
            if fut_price > 0 and futures_sym:
                _send_futures('PRE')
                return
            if pre_price > 0:
                pct = pre_pct or _pct(pre_price, base_prev)
                import time as _t
                real_asof = pre_time or _t.time()
                send_result({'symbol': symbol, 'price': pre_price, 'pct': pct,
                             'prev': base_prev, 'marketState': 'PRE',
                             'isFutures': False, 'futuresSym': futures_sym,
                             'hasExtendedData': True, 'asOf': real_asof})
                return
            # BUG FIX: this used to fall straight through to the "PRE gap"
            # block below whenever pre_price was 0 — which is the NORMAL
            # case for most symbols from 4:00am right up until real
            # pre-market volume picks up (often not until ~6-7am ET). That
            # block showed base_price (=regularMarketPrice = YESTERDAY's
            # already-known close, since today's regular session hasn't
            # opened yet), silently discarding the entire evening/overnight
            # move and resetting the displayed %/badge to a misleading
            # flat "Pre-market" state hours before any real pre-market data
            # exists. Keep carrying last night's post-market/overnight close
            # (post_price — same value shown all night) and keep
            # marketState='POST' until real pre-market trades actually
            # appear (pre_price>0 above). The client badge now reads
            # marketState directly (see getSessionBadge in monitor.html)
            # instead of guessing off wall-clock alone, so this keeps the
            # UI correctly on "Post-Market" through the whole dead zone.
            if post_price > 0:
                ext_pct = post_pct_vs_close or _pct(post_price, base_price) or post_pct
                import time as _t
                real_asof = post_time or _t.time()
                print(f"[PRE-carry] {symbol}: past 4am, no real pre-market print yet — "
                      f"keeping post-market close {post_price:.2f} tagged POST (real ts={post_time})")
                send_result({'symbol': symbol, 'price': post_price, 'pct': post_pct,
                             'extPct': ext_pct, 'extPrice': post_price,
                             'prev': base_prev, 'marketState': 'POST',
                             'isFutures': False, 'futuresSym': futures_sym,
                             'hasExtendedData': True, 'asOf': real_asof})
                return
            # Genuine pre-market gap: no pre-market AND no post-market/overnight
            # data at all (e.g. brand-new listing, or a symbol that never had a
            # post-market print). Show last regular-session close labeled PRE
            # so the UI badge still appears and the sparkline fetches
            # includePrePost candles if/when real data shows up.
            if base_price > 0:
                print(f"[PRE gap] {symbol}: no pre-market or post-market data at all, showing last close {base_price:.2f}")
                send_result({'symbol': symbol, 'price': base_price, 'pct': base_pct,
                             'prev': base_prev, 'marketState': 'PRE',
                             'isFutures': False, 'futuresSym': futures_sym,
                             'hasExtendedData': False, 'asOf': as_of})
                return

        # ── CLOSED / OVERNIGHT / WEEKEND ─────────────────────────────────────────
        if fut_price > 0:
            _send_futures('CLOSED')
            return

        # ── POST-MARKET CARRY: after 8 PM ET and overnight/weekends, keep showing
        # the last post-market price with POST tag rather than reverting to the
        # regular close. Yahoo still returns postMarketPrice overnight so we use it.
        # This covers the gap from 8 PM Friday → 4 AM Monday (pre-market opens).
        if post_price > 0 and not clock_regular and not clock_pre:
            # Same convention as the regular POST branch above: `pct` is the
            # live carried price vs YESTERDAY's close (matches Yahoo's
            # headline number through the evening); `extPct` is the
            # since-4pm-close move, kept only as a secondary annotation.
            ext_pct = post_pct_vs_close or _pct(post_price, base_price) or post_pct
            # DIAGNOSTIC: is this actually a live/moving Blue Ocean overnight
            # print, or the same 8pm post-market close being carried forward
            # unchanged? post_time is Yahoo's own postMarketTime timestamp —
            # if it's within the last ~30 min, Yahoo IS updating this field
            # overnight (Blue Ocean data is flowing through postMarketPrice)
            # and a real overnight-pricing feature is just a badge/label away.
            # If it's stuck at ~8:00pm ET no matter when you check, Yahoo's
            # public quote fields aren't carrying Blue Ocean data at all and
            # a genuinely different endpoint/field would be needed instead.
            import time as _t2
            _age_min = ((_t2.time() - post_time) / 60) if post_time else None
            _fresh = 'FRESH (live overnight data!)' if (_age_min is not None and _age_min < 30) else f'STALE ({_age_min:.0f}min old — frozen post-market print, not live)' if _age_min is not None else 'UNKNOWN (no timestamp)'
            print(f"[POST-carry] {symbol}: overnight/weekend, carrying post-market price {post_price:.2f} (real ts={post_time}) — {_fresh}")
            import time as _t
            real_asof = post_time or _t.time()
            send_result({'symbol': symbol, 'price': post_price, 'pct': post_pct,
                         'extPct': ext_pct, 'extPrice': post_price,
                         'prev': base_prev, 'marketState': 'POST',
                         'isFutures': False, 'futuresSym': futures_sym,
                         'hasExtendedData': True, 'asOf': real_asof})
            return

        # ── LAST RESORT: stale close (better than nothing) ───────────────────────
        if base_price > 0:
            # Use clock-derived state so PRE/POST badges show even when Yahoo returns
            # a stale 'CLOSED' or 'UNKNOWN' state during extended hours.
            stale_state = ('PRE' if clock_pre else
                           'POST' if clock_post else
                           market_state)
            print(f"[STALE] {symbol}: no live data, showing last close {base_price:.2f} state={stale_state}")
            send_result({'symbol': symbol, 'price': base_price, 'pct': base_pct,
                         'prev': base_prev, 'marketState': stale_state,
                         'isFutures': False, 'futuresSym': futures_sym,
                         'hasExtendedData': False, 'asOf': as_of})
            return

        print(f"[futures-price] COMPLETE FAILURE for {symbol}")
        send_result({'symbol': symbol, 'price': 0, 'pct': 0, 'prev': 0,
                     'marketState': 'UNKNOWN', 'isFutures': False, 'futuresSym': futures_sym,
                     'asOf': 0})

    def handle_debug_price(self):
        """Raw Yahoo diagnostic — hit /debug-price?symbol=VXUS in browser."""
        import json as _json
        from urllib.parse import parse_qs, urlparse
        qs  = parse_qs(urlparse(self.path).query)
        sym = qs.get('symbol', ['VXUS'])[0].upper()

        out = {'symbol': sym, 'endpoints': {}}

        def fetch(url):
            try:
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://finance.yahoo.com',
                })
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                    return _json.loads(r.read()), None
            except Exception as e:
                return None, str(e)

        import datetime, time
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        out['now_utc'] = now_utc.isoformat()
        out['now_ts']  = time.time()

        # ET DST
        y = now_utc.year
        dst_s = datetime.datetime(y,3,8,7,0,tzinfo=datetime.timezone.utc) + datetime.timedelta(days=(6-datetime.datetime(y,3,8).weekday())%7)
        dst_e = datetime.datetime(y,11,1,6,0,tzinfo=datetime.timezone.utc) + datetime.timedelta(days=(6-datetime.datetime(y,11,1).weekday())%7)
        et_off = datetime.timedelta(hours=-4) if dst_s <= now_utc < dst_e else datetime.timedelta(hours=-5)
        now_et = now_utc + et_off
        # Same day-rollback fix as _et_session_windows(): 12am-4am ET is still
        # last night's overnight carry, and the post window extends to the
        # next trading day's pre-market open rather than a hard 8pm cutoff.
        ref_et = now_et
        if ref_et.hour < 4:
            ref_et = ref_et - datetime.timedelta(days=1)
        if ref_et.weekday() == 5:
            ref_et = ref_et - datetime.timedelta(days=1)
        elif ref_et.weekday() == 6:
            ref_et = ref_et - datetime.timedelta(days=2)
        midnight_et = datetime.datetime(ref_et.year, ref_et.month, ref_et.day, tzinfo=datetime.timezone.utc) - et_off
        next_day = ref_et + datetime.timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day = next_day + datetime.timedelta(days=1)
        next_midnight_et = datetime.datetime(next_day.year, next_day.month, next_day.day, tzinfo=datetime.timezone.utc) - et_off
        windows = {
            'pre_open':      (midnight_et + datetime.timedelta(hours=4)).timestamp(),
            'regular_open':  (midnight_et + datetime.timedelta(hours=9, minutes=30)).timestamp(),
            'regular_close': (midnight_et + datetime.timedelta(hours=16)).timestamp(),
            'post_close':    (next_midnight_et + datetime.timedelta(hours=4)).timestamp(),
            'now_et':        now_et.strftime('%H:%M ET'),
            'ref_trading_day': ref_et.strftime('%A %Y-%m-%d'),
        }
        out['windows'] = windows
        now_ts = time.time()
        out['clock_pre']  = windows['pre_open']      <= now_ts < windows['regular_open']
        out['clock_reg']  = windows['regular_open']  <= now_ts < windows['regular_close']
        out['clock_post'] = windows['regular_close'] <= now_ts < windows['post_close']

        for label, url in [
            ('v8_1m_2d_ipp', f'https://query2.finance.yahoo.com/v8/finance/chart/{sym}?interval=1m&range=2d&includePrePost=true'),
            ('v8_2m_1d_ipp', f'https://query2.finance.yahoo.com/v8/finance/chart/{sym}?interval=2m&range=1d&includePrePost=true'),
            ('quoteSummary',  f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules=price'),
        ]:
            data, err = fetch(url)
            if err:
                out['endpoints'][label] = {'error': err}
                continue
            if 'quoteSummary' in label:
                p = data.get('quoteSummary',{}).get('result',[{}])[0].get('price',{})
                def rv(k):
                    v = p.get(k)
                    return v.get('raw') if isinstance(v, dict) else v
                out['endpoints'][label] = {
                    'marketState': p.get('marketState'),
                    'regularMarketPrice': rv('regularMarketPrice'),
                    'preMarketPrice': rv('preMarketPrice'),
                    'postMarketPrice': rv('postMarketPrice'),
                    'regularMarketPreviousClose': rv('regularMarketPreviousClose'),
                }
            else:
                res  = data.get('chart',{}).get('result',[{}])[0]
                meta = res.get('meta',{})
                ts_list = res.get('timestamp') or []
                closes  = (res.get('indicators',{}).get('quote',[{}])[0].get('close') or [])
                pairs   = [(t,c) for t,c in zip(ts_list, closes) if c is not None]
                def fmts(ts):
                    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime('%m/%d %H:%M UTC')
                # find candles in each window
                pre_candles  = [(t,c) for t,c in pairs if windows['pre_open']      <= t < windows['regular_open']]
                post_candles = [(t,c) for t,c in pairs if windows['regular_close'] <= t < windows['post_close']]
                out['endpoints'][label] = {
                    'marketState': meta.get('marketState'),
                    'regularMarketPrice': meta.get('regularMarketPrice'),
                    'chartPreviousClose': meta.get('chartPreviousClose'),
                    'preMarketPrice_meta': meta.get('preMarketPrice'),
                    'postMarketPrice_meta': meta.get('postMarketPrice'),
                    'gmtoffset': meta.get('gmtoffset'),
                    'total_candles': len(pairs),
                    'first3': [{'ts': t, 'utc': fmts(t), 'close': c} for t,c in pairs[:3]],
                    'last5':  [{'ts': t, 'utc': fmts(t), 'close': c} for t,c in pairs[-5:]],
                    'pre_window_candles':  [{'ts': t, 'utc': fmts(t), 'close': c} for t,c in pre_candles[-5:]],
                    'post_window_candles': [{'ts': t, 'utc': fmts(t), 'close': c} for t,c in post_candles[-5:]],
                }

        body = _json.dumps(out, indent=2).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def handle_yahoo(self):
        import json as _json
        qs = parse_qs(urlparse(self.path).query)
        symbol = qs.get("symbol", [""])[0]
        interval = qs.get("interval", ["1d"])[0]
        range_ = qs.get("range", ["1d"])[0]
        # Allow caller to request pre/post market data (needed for futures sparklines)
        include_pre_post = qs.get("includePrePost", ["false"])[0].lower() in ('true', '1')
        if not symbol:
            self.send_error(400, "Missing symbol")
            return

        def fetch_url(url):
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://finance.yahoo.com"
            })
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=12) as resp:
                return _json.loads(resp.read())

        try:
            ipp = 'true' if include_pre_post else 'false'
            intraday_url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                f"?interval={interval}&range={range_}&includePrePost={ipp}"
            )
            data = fetch_url(intraday_url)
            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})

            # Correct chartPreviousClose/regularMarketChangePercent for once-a-day-priced
            # assets (mutual funds). We do this UNCONDITIONALLY, not just when
            # regularMarketChangePercent is missing — Yahoo can return a non-null
            # regularMarketChangePercent that's still wrong, computed from the same stale
            # chartPreviousClose we can't trust (confirmed: v8 chart meta, quoteSummary, and
            # v7 quote all independently return the SAME wrong previousClose for these
            # symbols). This endpoint feeds loadPortChart's portfolio-total calculation
            # directly via rt.meta.chartPreviousClose, so a wrong value here silently wrongs
            # the total portfolio return shown in the UI, not just the per-holding number.
            try:
                # BUG FIX: range=5d often returned only 1 daily bar for sparsely-
                # charted mutual funds, leaving nothing to derive a distinct previous
                # close from (see matching fix in handle_futures_price). range=1mo
                # reliably returns enough trading days for a real comparison.
                daily_url = (
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    f"?interval=1d&range=1mo&includePrePost=false"
                )
                daily_data = fetch_url(daily_url)
                dresult = daily_data.get("chart", {}).get("result", [{}])[0]
                dmeta   = dresult.get("meta", {})
                d_ts     = dresult.get("timestamp") or []
                d_closes = (dresult.get("indicators", {}).get("quote", [{}])[0].get("close") or [])
                # Same dict.get() pitfall as the /futures-price copy of this logic: only
                # falls back when the key is MISSING, not when Yahoo returns it as null
                # (plausible for mutual funds, which have no real exchange/session info).
                # A null here throws inside _local_date below, gets swallowed by the
                # `except Exception: pass` around this whole block, and silently reverts
                # to Yahoo's original (wrong) chartPreviousClose for that request.
                gmtoffset = dmeta.get("gmtoffset")
                if gmtoffset is None:
                    gmtoffset = -14400
                bars = [(t, c) for t, c in zip(d_ts, d_closes) if t is not None and c is not None]

                curr_price = meta.get("regularMarketPrice") or dmeta.get("regularMarketPrice")
                derived_prev = None
                if bars and curr_price:
                    # NOTE: this file's top-level `from datetime import datetime, timezone`
                    # binds `datetime` to the CLASS, not the module — datetime.datetime would
                    # AttributeError here. Alias locally to avoid that trap.
                    from datetime import datetime as _dtY, timezone as _tzY
                    def _local_date(ts):
                        return _dtY.fromtimestamp(ts + gmtoffset, tz=_tzY.utc).date()
                    today_local = _local_date(_dtY.now(_tzY.utc).timestamp())
                    last_bar_date = _local_date(bars[-1][0])
                    # Same mutual-fund fix as the /futures-price copy: once-a-day-priced
                    # assets don't get a fresh intraday tick beyond their daily bars — the
                    # "current" price IS the last bar's close. If we don't detect that and
                    # just compare curr_price to that same bar, we always get exactly 0%.
                    _curr_is_last_bar = abs(curr_price - bars[-1][1]) < 1e-6
                    if (last_bar_date == today_local or _curr_is_last_bar):
                        if len(bars) >= 2:
                            derived_prev = bars[-2][1]
                        # else: only one bar even with the wider lookback — leave
                        # derived_prev as None (guard below) rather than fabricating
                        # a same-value 0% comparison.
                    else:
                        # Today's bar hasn't posted to the daily chart yet, but the live
                        # price is already fresh — the last bar we DO have is yesterday's
                        # close, which is exactly the baseline we want.
                        derived_prev = bars[-1][1]

                # Guard: never accept a derived prevClose identical to curr_price —
                # that's a degenerate comparison, not a real 0% move.
                if derived_prev and curr_price and abs(curr_price - derived_prev) < 1e-6:
                    print(f"[handle_yahoo] {symbol}: derived prevClose == price — "
                          f"not enough distinct daily bars, skipping override")
                    derived_prev = None

                if derived_prev and curr_price:
                    pct = (curr_price - derived_prev) / derived_prev * 100
                    data["chart"]["result"][0]["meta"]["regularMarketChangePercent"] = pct
                    data["chart"]["result"][0]["meta"]["chartPreviousClose"] = derived_prev
                    data["chart"]["result"][0]["meta"]["regularMarketPreviousClose"] = derived_prev
            except Exception as ex:
                print(f"[handle_yahoo] {symbol}: daily cross-check failed: {ex}")  # Best effort — return original data

            out = _json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(out)
        except Exception as e:
            self.send_error(502, str(e))

    def handle_quote(self):
        """Fetch real-time quote data from Yahoo Finance v7 API — always includes regularMarketChangePercent."""
        qs = parse_qs(urlparse(self.path).query)
        symbols = qs.get("symbols", [""])[0]
        if not symbols:
            self.send_error(400, "Missing symbols")
            return
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={symbols}&fields=regularMarketPrice,regularMarketChangePercent,"
            f"regularMarketChange,regularMarketPreviousClose,shortName,symbol"
        )
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://finance.yahoo.com"
            })
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=12) as resp:
                data = resp.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self.send_error(502, str(e))

    def handle_news(self):
        """Fetch news from RSS feeds by category — all feeds fetched concurrently"""
        qs = parse_qs(urlparse(self.path).query)
        cat = qs.get("cat", ["all"])[0]

        FEEDS = {
            # ── MARKET ──────────────────────────────────────────────────────────
            "market": [
                ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "market"),
                ("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "market"),
                ("https://feeds.content.dowjones.io/public/rss/mw_marketpulse", "market"),
                ("https://feeds.bloomberg.com/markets/news.rss", "market"),
                ("https://cms.zerohedge.com/fullrss2.xml", "market"),
            ],
            # ── US NEWS ─────────────────────────────────────────────────────────
            "us": [
                ("https://rss.nytimes.com/services/xml/rss/nyt/US.xml", "us"),
                ("https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml", "us"),
                ("https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml", "us"),
                ("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000113", "us"),
                ("https://thehill.com/feed/", "us"),
                ("https://rss.politico.com/politics-news.xml", "us"),
                ("https://rss.nytimes.com/services/xml/rss/nyt/Washington.xml", "us"),
            ],
            # ── WORLD NEWS ──────────────────────────────────────────────────────
            # BBC general world/rss.xml removed — it publishes US stories too.
            # Using targeted regional feeds only so "world" stays international.
            "world": [
                ("https://feeds.bbci.co.uk/news/world/middle_east/rss.xml", "world"),
                ("https://feeds.bbci.co.uk/news/world/europe/rss.xml", "world"),
                ("https://feeds.bbci.co.uk/news/world/asia/rss.xml", "world"),
                ("https://feeds.bbci.co.uk/news/world/latin_america/rss.xml", "world"),
                ("https://feeds.bbci.co.uk/news/world/africa/rss.xml", "world"),
                ("https://www.aljazeera.com/xml/rss/all.xml", "world"),
                ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "world"),
                ("https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml", "world"),
            ],
            # ── SPORTS ──────────────────────────────────────────────────────────
            "sports": [],  # sports category disabled
        }
        FEEDS["all"] = FEEDS["market"] + FEEDS["us"] + FEEDS["world"] + FEEDS["sports"]

        feed_list = FEEDS.get(cat, FEEDS["all"])
        articles = []
        lock = threading.Lock()

        # Per-domain UA tuning — helps with Reuters, ESPN, WSJ bot detection
        FEED_UA = {
            "reuters.com":      "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "wsj.com":          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "espn.com":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "foxsports.com":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "api.foxsports.com": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "therage.co":       "Mozilla/5.0 (compatible; Feedfetcher-Google; +http://www.google.com/feedfetcher.html)",
            "bloomberg.com":    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "washingtonpost.com": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "zerohedge.com":    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "cms.zerohedge.com": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "aljazeera.com":    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "feedburner.com":   "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        }
        DEFAULT_FEED_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

        def get_feed_ua(url):
            for domain, ua in FEED_UA.items():
                if domain in url:
                    return ua
            return DEFAULT_FEED_UA

        def extract_link(item_text):
            """Extract article URL from RSS item."""
            # <link>URL</link> plain or CDATA wrapped
            m = re.search(r"<link>(?:<!\[CDATA\[)?(https?://[^\]<\s]+?)(?:\]\]>)?</link>", item_text, re.DOTALL)
            if m: return m.group(1).strip()
            # Atom-style <link href="URL"/> with double quotes
            m = re.search(r'<link[^>]+href="([^"]+)"', item_text)
            if m: return m.group(1).strip()
            # Atom-style <link href='URL'/> with single quotes
            m = re.search(r"<link[^>]+href='([^']+)'", item_text)
            if m: return m.group(1).strip()
            # <guid isPermaLink="true">URL</guid>
            m = re.search(r'<guid[^>]+isPermaLink="true"[^>]*>(https?://[^<]+)</guid>', item_text, re.I)
            if m: return m.group(1).strip()
            # <guid>URL</guid> where the value is a URL
            m = re.search(r"<guid[^>]*>(https?://[^<]+)</guid>", item_text)
            if m: return m.group(1).strip()
            return None

        # Feeds known to be slow (high-latency CDNs, international servers)
        SLOW_FEEDS = {"aljazeera.com"}

        def fetch_feed(feed_url, tag):
            try:
                ua = get_feed_ua(feed_url)
                feed_timeout = 12 if any(d in feed_url for d in SLOW_FEEDS) else 7
                req = urllib.request.Request(feed_url, headers={
                    "User-Agent": ua,
                    "Accept": "application/rss+xml,application/xml,text/xml,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "no-cache",
                })
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=feed_timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                # Extract channel-level date as fallback for items with no date
                ch_date_m = re.search(r"<lastBuildDate>(.*?)</lastBuildDate>", raw)
                channel_date = ""
                if ch_date_m:
                    try:
                        channel_date = parsedate_to_datetime(ch_date_m.group(1).strip()).astimezone(timezone.utc).isoformat()
                    except Exception:
                        pass
                items = re.findall(r"<item>(.*?)</item>", raw, re.DOTALL)
                local = []
                for item in items[:12]:
                    title   = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
                    # pubDate is the standard RSS field — always prefer it
                    # dc:date is used by some feeds as an alternative
                    # Do NOT fall back to <updated> — it's used by non-date content in ESPN feeds
                    pubdate = (re.search(r"<pubDate>(.*?)</pubDate>", item) or
                               re.search(r"<dc:date>(.*?)</dc:date>", item))
                    desc    = re.search(r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>", item, re.DOTALL)
                    link = extract_link(item)
                    if title and link:
                        t = re.sub(r"<[^>]+>", "", title.group(1) or "").strip()
                        d = ""
                        if desc:
                            # 1. Unescape HTML entities (e.g. ZeroHedge encodes HTML as &lt;div&gt;)
                            raw_d = html.unescape(desc.group(1) or "")
                            # 2. Strip all HTML tags
                            raw_d = re.sub(r"<[^>]+>", "", raw_d)
                            # 3. Collapse whitespace runs left behind by removed tags
                            raw_d = re.sub(r"\s+", " ", raw_d).strip()
                            # 4. Strip leading title repetition (e.g. ZeroHedge prepends the title
                            #    then dumps the article body — "Title Authored by X via Y ...")
                            t_norm = re.sub(r"\s+", " ", t).strip().lower()
                            d_norm = raw_d.lower()
                            if t_norm and d_norm.startswith(t_norm):
                                raw_d = raw_d[len(t_norm):].lstrip(" .,;:-")
                            # 5. Truncate at a clean word boundary and add ellipsis if cut
                            if len(raw_d) > 220:
                                cut = raw_d[:220].rsplit(" ", 1)[0].rstrip(" .,;:-")
                                d = cut + "…"
                            else:
                                d = raw_d
                        pd = pubdate.group(1).strip() if pubdate else ""
                        # Normalize to ISO 8601 so JS new Date() parses it reliably
                        if pd:
                            normalized = ""
                            # Try RFC 2822 (most RSS feeds including ESPN)
                            try:
                                normalized = parsedate_to_datetime(pd).astimezone(timezone.utc).isoformat()
                            except Exception:
                                pass
                            # Try ISO 8601 variants
                            if not normalized:
                                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                                            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                                            "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"):
                                    try:
                                        dt = datetime.strptime(pd.strip(), fmt)
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        normalized = dt.astimezone(timezone.utc).isoformat()
                                        break
                                    except Exception:
                                        pass
                            # Strip trailing offset and retry
                            if not normalized:
                                try:
                                    clean = re.sub(r"[+-]\d{2}:?\d{2}$", "", pd.strip()).strip()
                                    normalized = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
                                except Exception:
                                    pass
                            pd = normalized  # empty string if all failed
                        # For ESPN: pubDate is re-stamped to current time on every feed fetch.
                        # Discard it entirely and derive a synthetic date from the story ID.
                        # ESPN story IDs are monotonically increasing integers so this gives
                        # correct relative ordering even without a real timestamp.
                        if pd and "espn.com" in (link or ""):
                            pd = ""
                        # For non-ESPN: try /YYYYMMDD/ in URL
                        if not pd and link and "espn.com" not in link:
                            url_date = re.search(r'/(\d{4})(\d{2})(\d{2})(?:/|$|-)', link)
                            if url_date:
                                try:
                                    y,m,d_ = url_date.group(1), url_date.group(2), url_date.group(3)
                                    pd = datetime(int(y),int(m),int(d_), tzinfo=timezone.utc).isoformat()
                                except Exception:
                                    pass
                        # ESPN story ID -> synthetic date for stable ordering.
                        # IDs are monotonically increasing: ~35139 IDs/day.
                        # Anchor: id 48145145 = 2026-03-08 (calibrated from live data).
                        if not pd and link and "espn.com" in link:
                            sid_m = re.search(r'/_/id/(\d{7,9})/', link)
                            if sid_m:
                                try:
                                    from datetime import timedelta
                                    sid = int(sid_m.group(1))
                                    anchor_id = 48145145
                                    anchor_dt = datetime(2026, 3, 8, tzinfo=timezone.utc)
                                    delta_days = (sid - anchor_id) / 35139.0
                                    pd = (anchor_dt + timedelta(days=delta_days)).isoformat()
                                except Exception:
                                    pass
                        # No channel_date fallback — articles with no parseable date
                        # get pd="" which sorts them to the bottom in JS (-Infinity)
                        if t and not t.lower().startswith("bbc"):
                            # Stable date cache: once we assign a date to a URL, keep it.
                            # This prevents podcast/feature articles from always showing "just now"
                            # because ESPN re-stamps them with current time on every feed refresh.
                            cache_key = link.split('?')[0].rstrip('/')
                            with _article_date_cache_lock:
                                if cache_key in _article_date_cache:
                                    # Use the oldest known date for this article
                                    cached = _article_date_cache[cache_key]
                                    if pd and cached:
                                        pd = min(pd, cached)  # ISO strings compare lexicographically
                                    elif cached:
                                        pd = cached
                                if pd:
                                    _article_date_cache[cache_key] = pd
                                # Prune cache if it grows too large
                                if len(_article_date_cache) > 2000:
                                    keys = list(_article_date_cache.keys())
                                    for k in keys[:500]:
                                        del _article_date_cache[k]
                            # Persist RSS-derived dates to disk so they survive proxy
                            # restarts — without this ESPN re-stamps wipe the cache
                            if pd:
                                _save_date_cache()
                            local.append({"title": t, "link": link, "pubDate": pd, "description": d, "tag": tag})
                with lock:
                    articles.extend(local)
            except Exception as e:
                print(f"Feed error [{tag}] {feed_url}: {e}")

        # Fetch all feeds concurrently — all threads share a single 14s wall-clock window
        # (raised from 9s to accommodate slow feeds like Al Jazeera)
        threads = [threading.Thread(target=fetch_feed, args=(url, tag), daemon=True) for url, tag in feed_list]
        for t in threads: t.start()
        deadline = __import__('time').time() + 14
        for t in threads:
            remaining = deadline - __import__('time').time()
            if remaining > 0:
                t.join(timeout=remaining)

        # Deduplicate: first by URL (catches same story re-published with new timestamp),
        # then by title prefix (catches same story from different sources)
        seen_urls = set()
        seen_titles = set()
        unique = []
        for a in articles:
            url_key = a.get("link", "").split("?")[0].rstrip("/")  # strip query params
            title_key = a["title"][:60].lower().strip()
            if url_key and url_key in seen_urls:
                continue
            if title_key in seen_titles:
                continue
            if url_key:
                seen_urls.add(url_key)
            seen_titles.add(title_key)
            unique.append(a)

        # Drop articles older than 7 days to prevent stale RSS feeds surfacing old content
        cutoff = datetime.now(timezone.utc).timestamp() - 7 * 24 * 3600
        def within_age(a):
            pd = a.get("pubDate", "")
            if not pd:
                return True  # no date = keep
            try:
                dt = datetime.fromisoformat(pd)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp() >= cutoff
            except Exception:
                return True
        unique = [a for a in unique if within_age(a)]

        # For articles still missing a pubDate, try to fetch it from the article page
        # (ESPN podcast/feature articles often omit pubDate from RSS)
        _now_ts = datetime.now(timezone.utc).timestamp()
        def _needs_real_date(a):
            if not any(h in a.get("link","") for h in ["espn.com","cnbc.com","politico.com"]):
                return False
            pd = a.get("pubDate","")
            if not pd:
                return True
            try:
                dt = datetime.fromisoformat(pd)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                age_secs = _now_ts - dt.timestamp()
                return age_secs < 300  # pubDate within 5 min = ESPN re-stamped it
            except Exception:
                return False
        dateless = [a for a in unique if _needs_real_date(a)]
        if dateless:
            def fetch_article_date(article):
                url = article.get("link","")
                cache_key = url.split("?")[0].rstrip("/")
                # Check cache first
                with _article_date_cache_lock:
                    if cache_key in _article_date_cache and _article_date_cache[cache_key]:
                        article["pubDate"] = _article_date_cache[cache_key]
                        return
                try:
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                        "Accept": "text/html,*/*",
                    })
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=3) as r:
                        # Only read first 8KB - meta tags are always in <head>
                        head_html = r.read(8192).decode("utf-8", errors="replace")
                    # Try Open Graph / JSON-LD date (works on ESPN, CNBC, Politico)
                    pub_m = (re.search(r'article:published_time[^>]+content="([^"]+)"', head_html) or
                             re.search(r"article:published_time[^>]+content='([^']+)'", head_html) or
                             re.search(r'"datePublished"\s*:\s*"([^"]+)"', head_html) or
                             re.search(r'<time[^>]+datetime="([0-9T:+\-Z]{10,30})"', head_html))
                    if pub_m:
                        raw_date = pub_m.group(1).strip()
                        try:
                            pd = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
                            article["pubDate"] = pd
                            with _article_date_cache_lock:
                                _article_date_cache[cache_key] = pd
                                _save_date_cache()
                            print(f"[News] Fetched date for dateless article: {pd[:19]} {url[:60]}")
                        except Exception:
                            pass
                except Exception as e:
                    pass  # silently skip - article keeps existing pubDate

            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor(max_workers=3) as ex:
                list(ex.map(fetch_article_date, dateless[:5]))  # limit to 5 per refresh to stay under client timeout

        # Sort newest first — articles with no date go to the end
        def sort_key(a):
            pd = a.get("pubDate", "")
            if not pd:
                return ""
            return pd  # ISO 8601 strings sort lexicographically = chronologically

        unique.sort(key=sort_key, reverse=True)
        self.json_response(unique[:200])

    def handle_ogp(self):
        """Fetch Open Graph metadata for link previews.

        Two things were silently killing previews for a chunk of real-world
        posts (notably x.com/twitter.com status links, which is exactly what
        was reported missing):

        1. UA/headers: the old request identified itself as
           "MonitorDashboard/1.0" with no Accept-Encoding/Language/Referer —
           a fingerprint that reads as a bot to most anti-scraping stacks and
           gets served a stripped shell page (no og:image) or an outright
           4xx/consent wall instead of the real markup. x.com/twitter.com in
           particular only server-render a tweet's OG/card tags for a known
           crawler UA (Twitterbot/…); a generic UA gets the client-side React
           shell with none of the per-tweet meta tags. Other sites behind
           bot-detection (Cloudflare etc.) care about a believable full
           header set, not just the UA string.
        2. Encoding: no Accept-Encoding was sent, but some CDNs gzip the
           response anyway — decoding raw gzip bytes as utf-8 text silently
           yields zero regex matches, which looks identical to "no og tags"
           downstream and just fails closed with an empty card.

        Fix: send a real browser header set (matching what already works for
        the image path-proxy below), special-case a Twitterbot UA for
        x.com/twitter.com, explicitly gunzip/inflate by sniffing magic bytes
        (don't trust Content-Encoding), use the wall-clock-deadline fetcher
        so a stalling host can't hang the request, and retry once on
        transient failures before giving up.
        """
        qs = parse_qs(urlparse(self.path).query)
        url = qs.get("url", [None])[0]
        if not url:
            self.send_error(400, "Missing url"); return
        host = urlparse(url).netloc.lower()
        is_twitter = host in ("x.com", "twitter.com", "www.x.com", "www.twitter.com") or host.endswith((".x.com", ".twitter.com"))

        def _do_fetch():
            if is_twitter:
                # Twitter/X only server-renders per-tweet og:image/og:title for
                # recognized crawler UAs — everyone else gets the JS app shell.
                hdrs = {
                    "User-Agent": "Twitterbot/1.0",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                }
            else:
                hdrs = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate",
                    "Referer": "https://www.google.com/",
                    "Upgrade-Insecure-Requests": "1",
                }
            req = urllib.request.Request(url, headers=hdrs)
            ctx = ssl_ctx if url.startswith("https://") else None
            return _fetch_with_deadline(req, ctx, 7, 9)

        try:
            try:
                data, ct = _do_fetch()
            except Exception as e:
                print(f"[ogp] {url} failed ({e}) — retrying once")
                data, ct = _do_fetch()

            # Some hosts gzip/deflate the body even when we didn't explicitly
            # request it — decompress by magic-byte sniffing rather than
            # trusting Content-Encoding.
            if data[:2] == b"\x1f\x8b":
                import gzip as _gzip
                try: data = _gzip.decompress(data)
                except Exception: pass
            elif data[:2] in (b"\x78\x9c", b"\x78\x01", b"\x78\xda"):
                import zlib as _zlib
                try: data = _zlib.decompress(data)
                except Exception: pass

            raw = data[:300000].decode("utf-8", errors="replace")

            def og(prop):
                m = re.search(rf'<meta[^>]+property=["\']og:{prop}["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
                if not m:
                    m = re.search(rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:{prop}["\']', raw, re.I)
                return m.group(1).strip() if m else ""
            def tw(name):
                m = re.search(rf'<meta[^>]+name=["\']twitter:{name}["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
                if not m:
                    m = re.search(rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:{name}["\']', raw, re.I)
                return m.group(1).strip() if m else ""
            title = og("title") or tw("title") or re.search(r"<title[^>]*>(.*?)</title>", raw, re.I|re.S)
            if hasattr(title, 'group'): title = re.sub(r"<[^>]+>","",title.group(1)).strip()
            elif not isinstance(title, str): title = ""
            image = og("image") or tw("image") or tw("image:src") or ""
            if image:
                # og:image is sometimes protocol-relative or root-relative.
                image = urljoin(url, image)
            result = {
                "title": html.unescape(title)[:120],
                "description": html.unescape(og("description") or tw("description"))[:200],
                "image": image,
                "site": html.unescape(og("site_name") or "")[:60],
                "url": url
            }
            self.json_response(result)
        except Exception as e:
            print(f"[ogp] {url} failed: {e}")
            self.json_response({"title":"","description":"","image":"","site":"","url":url,"error":str(e)})


    # ── Persistent cookie jar (lives for the proxy process lifetime) ─────────
    _cookie_jar = {}
    _cookie_lock = threading.Lock()

    def _store_cookies(self, host, headers):
        with self._cookie_lock:
            jar = self._cookie_jar.setdefault(host, {})
            for hdr, val in headers:
                if hdr.lower() == 'set-cookie':
                    pair = val.split(';')[0].strip()
                    if '=' in pair:
                        k, _, v = pair.partition('=')
                        jar[k.strip()] = v.strip()

    def _get_cookies(self, host):
        with self._cookie_lock:
            combined = {}
            for domain, jar in self._cookie_jar.items():
                if host == domain or host.endswith('.' + domain):
                    combined.update(jar)
            return '; '.join(f'{k}={v}' for k, v in combined.items())

    def _raw_get(self, url):
        """Single GET via http.client. Always tries HTTPS first (fixes Errno 61).
        Uses domain-specific headers to improve bot-detection pass rate."""
        parsed = urlparse(url)
        host = parsed.netloc
        path = (parsed.path or '/') + (('?' + parsed.query) if parsed.query else '')
        cookies = self._get_cookies(host)
        if any(d in host for d in ('wsj', 'barrons', 'dowjones')):
            referer = 'https://www.google.com/search?q=finance+news'
            ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        elif 'bloomberg' in host:
            referer = 'https://www.google.com/search?q=bloomberg+markets'
            ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        elif 'seekingalpha' in host:
            referer = 'https://www.google.com/search?q=seeking+alpha+finance'
            ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        elif 'cnbc' in host:
            referer = 'https://www.google.com/search?q=cnbc+finance+news'
            ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        else:
            referer = 'https://www.google.com/'
            ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        hdrs = {
            'Host': host,
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Referer': referer,
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
            'Connection': 'close',
            'Cache-Control': 'max-age=0',
        }
        if cookies:
            hdrs['Cookie'] = cookies
        conn = None
        try:
            conn = http.client.HTTPSConnection(host, timeout=20, context=ssl_ctx)
            conn.request('GET', path, headers=hdrs)
            resp = conn.getresponse()
        except OSError:
            try:
                if conn: conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(host, timeout=20)
            conn.request('GET', path, headers=hdrs)
            resp = conn.getresponse()
        self._store_cookies(host, resp.getheaders())
        raw = resp.read(3 * 1024 * 1024)
        try:
            conn.close()
        except Exception:
            pass
        return resp, raw, url

    def _decode_response(self, resp, raw):
        import gzip, zlib
        enc = next((v for h, v in resp.getheaders() if h.lower() == 'content-encoding'), '')
        try:
            if 'gzip' in enc:
                raw = gzip.decompress(raw)
            elif 'deflate' in enc:
                try:
                    raw = zlib.decompress(raw)
                except zlib.error:
                    raw = zlib.decompress(raw, -zlib.MAX_WBITS)
        except Exception:
            pass
        ct = next((v for h, v in resp.getheaders() if h.lower() == 'content-type'), '')
        charset = 'utf-8'
        if 'charset=' in ct:
            charset = ct.split('charset=')[-1].strip().split(';')[0].strip() or 'utf-8'
        else:
            sniff = raw[:2048].decode('ascii', errors='replace').lower()
            m = re.search(r'charset=["\'"]?([\w-]+)', sniff)
            if m:
                charset = m.group(1)
        try:
            return raw.decode(charset, errors='replace')
        except (LookupError, UnicodeDecodeError):
            return raw.decode('utf-8', errors='replace')

    def handle_reader(self):
        """Fetch article HTML for Readability. HTTPS-first, cookie jar, gzip,
        redirect following. For paywalled sites, tries Google AMP cache first,
        then direct with paywall-bypass headers. Returns blocked:true for hard
        401/403 so client can show RSS description fallback."""
        from urllib.parse import quote as uq
        params = parse_qs(urlparse(self.path).query)
        url = params.get('url', [None])[0]
        if not url:
            self.send_error(400, "Missing url")
            return

        PAYWALL_DOMAINS = ('wsj.com', 'bloomberg.com', 'seekingalpha.com',
                           'ft.com', 'nytimes.com', 'theatlantic.com', 'wired.com')

        def try_amp_cache(original_url):
            """Try Google AMP cache for the URL."""
            try:
                parsed = urlparse(original_url)
                host_encoded = parsed.netloc.replace('.', '-').replace('-', '--', 0)
                path = parsed.path.lstrip('/')
                amp_url = f"https://{parsed.netloc.replace('.', '-')}.cdn.ampproject.org/v/s/{parsed.netloc}{parsed.path}"
                resp, raw, _ = self._raw_get(amp_url)
                if resp.status == 200:
                    return self._decode_response(resp, raw)
            except Exception:
                pass
            return None

        def try_with_paywall_bypass(url):
            """Try fetching with paywall-bypass tricks: Googlebot UA, referer spoofing."""
            parsed = urlparse(url)
            host = parsed.netloc
            path = (parsed.path or '/') + (('?' + parsed.query) if parsed.query else '')
            hdrs = {
                'Host': host,
                'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Referer': 'https://www.google.com/',
                'X-Forwarded-For': '66.249.66.1',  # Google IP
                'Cache-Control': 'no-cache',
                'Connection': 'close',
            }
            cookies = self._get_cookies(host)
            if cookies:
                hdrs['Cookie'] = cookies
            try:
                conn = http.client.HTTPSConnection(host, timeout=20, context=ssl_ctx)
                conn.request('GET', path, headers=hdrs)
                resp = conn.getresponse()
                self._store_cookies(host, resp.getheaders())
                raw = resp.read(3 * 1024 * 1024)
                conn.close()
                return resp, raw
            except Exception:
                return None, None

        try:
            is_paywall = any(d in url for d in PAYWALL_DOMAINS)
            current = url
            html = None

            # For paywall sites, try Googlebot UA first
            if is_paywall:
                resp_pb, raw_pb = try_with_paywall_bypass(url)
                if resp_pb and resp_pb.status == 200:
                    html = self._decode_response(resp_pb, raw_pb)

            # Standard fetch with redirect following
            if not html:
                for _ in range(10):
                    resp, raw, current = self._raw_get(current)
                    if resp.status in (301, 302, 303, 307, 308):
                        loc = next((v for h, v in resp.getheaders() if h.lower() == 'location'), '')
                        if not loc:
                            break
                        current = urljoin(current, loc)
                        continue
                    break

                if resp.status in (401, 403):
                    # Tell client it's blocked — client will show RSS description fallback
                    self.json_response({"blocked": True, "html": "", "url": current})
                    return
                if resp.status not in (200, 203):
                    raise ValueError(f"HTTP {resp.status}: {resp.reason}")
                html = self._decode_response(resp, raw)

            self.json_response({"html": html, "url": current})
        except Exception as e:
            self.json_response({"error": str(e), "html": "", "url": url})

    # ── NWS (National Weather Service) augmentation ──────────────────────
    #
    # Reported: temperature/wind/humidity all noticeably diverging from
    # Apple Weather for the same US location — confirmed NOT a coding bug
    # (renderWxModal/wxCurrentObs pass Open-Meteo's fields through with no
    # double conversion, no stale merge — the divergence is a genuine
    # forecast-MODEL difference between Open-Meteo's default US blend and
    # Apple's WeatherKit, which leans on NWS data for US points). Per
    # explicit direction: temperature/wind/humidity/precipitation are now
    # overridden with real NWS data (api.weather.gov, free, no key, US-only)
    # for locations NWS covers, since that's the same source family behind
    # most US weather apps. NWS has NO air-quality or UV-index data at all
    # (AQI is EPA/AirNow's domain; UV isn't part of NWS's forecast product)
    # — those two fields intentionally stay Open-Meteo-sourced regardless.
    #
    # Every step below is independently wrapped so ANY failure (non-US
    # location, timeout, a station reporting nothing usable, an unexpected
    # response shape) leaves the Open-Meteo baseline for that field
    # completely untouched — this is a pure enhancement layer that can
    # never take the widget down or blank out data Open-Meteo already
    # supplied.
    NWS_HEADERS = {"User-Agent": "personal-dashboard contact@example.com", "Accept": "application/geo+json"}
    # Per-call timeout for the individual api.weather.gov requests below
    # (shorter than _fetch_json's generic 15s default used elsewhere in
    # this file), plus a total wall-clock budget for the whole augmentation
    # pass (points lookup + current-obs + hourly + daily combined) — see
    # the ROOT CAUSE note above this class for why an unbounded chain of
    # otherwise well-intentioned per-step timeouts could still add up to
    # far more latency than a client is willing to wait on, even though
    # every individual step was "only" 15s and every step already degrades
    # gracefully on outright failure. _nws_points' own successful lookup is
    # 7-day cached, so in practice this budget is only fully exercised on a
    # cold cache for a given location.
    _NWS_CALL_TIMEOUT = 5
    _NWS_TOTAL_BUDGET = 8.0
    _NWS_COMPASS = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
        'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5, 'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5,
    }

    def _nws_points(self, lat, lon):
        """Resolve (lat,lon) -> NWS forecast/forecastHourly/observationStations
        URLs, cached long-term (grid assignment is effectively permanent for
        a fixed point). Returns None for out-of-US-coverage points or any
        transient failure — caller treats that as "skip NWS entirely"."""
        key = (round(lat, 3), round(lon, 3))
        cached = _NWS_POINTS_CACHE.get(key)
        if cached and (_time.time() - cached[0]) < _NWS_POINTS_TTL:
            return cached[1]
        try:
            data = self._fetch_json(f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}", headers=self.NWS_HEADERS, timeout=self._NWS_CALL_TIMEOUT)
            props = data.get('properties') or {}
            result = {
                'forecast': props.get('forecast'),
                'forecastHourly': props.get('forecastHourly'),
                'observationStations': props.get('observationStations'),
            }
            if not (result['forecast'] and result['forecastHourly'] and result['observationStations']):
                result = None
        except urllib.error.HTTPError:
            # 404 = genuinely outside NWS/US coverage — cache the negative
            # result too, so a non-US location doesn't re-hit this every request.
            result = None
        except Exception as e:
            print(f"[nws] points lookup failed for {lat},{lon}: {e}")
            return None  # transient failure — don't cache, just retry next time
        _NWS_POINTS_CACHE[key] = (_time.time(), result)
        return result

    def _nws_current_obs(self, points):
        """Nearest real station's latest observation: temp/humidity/wind.
        Returns None if the station has no usable fresh reading at all."""
        try:
            stations = self._fetch_json(points['observationStations'], headers=self.NWS_HEADERS, timeout=self._NWS_CALL_TIMEOUT)
            feats = stations.get('features') or []
            if not feats:
                return None
            station_id = feats[0]['properties']['stationIdentifier']
            obs = self._fetch_json(f"https://api.weather.gov/stations/{station_id}/observations/latest", headers=self.NWS_HEADERS, timeout=self._NWS_CALL_TIMEOUT)
            p = obs.get('properties') or {}

            def val(field):
                v = (p.get(field) or {}).get('value')
                return v if isinstance(v, (int, float)) else None

            temp_c = val('temperature')
            rh = val('relativeHumidity')
            wind_kmh = val('windSpeed')
            wind_dir = val('windDirection')
            if temp_c is None and rh is None and wind_kmh is None:
                return None  # station reporting nothing usable — treat as a failed attempt
            return {
                'temperature_2m': round(temp_c * 9 / 5 + 32, 1) if temp_c is not None else None,
                'relative_humidity_2m': round(rh) if rh is not None else None,
                'wind_speed_10m': round(wind_kmh * 0.621371, 1) if wind_kmh is not None else None,
                'wind_direction_10m': wind_dir,
            }
        except Exception as e:
            print(f"[nws] current-obs failed: {e}")
            return None

    def _nws_apply_current(self, current, obs):
        """Overwrites only the fields NWS actually returned a value for.
        Returns True iff temperature was overwritten — the caller uses this
        to decide whether Open-Meteo's minutely_15 blend (which otherwise
        silently out-prioritizes this in wxCurrentObs()) needs dropping."""
        temp_overwritten = False
        for field in ('temperature_2m', 'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m'):
            v = obs.get(field)
            if v is not None:
                current[field] = v
                if field == 'temperature_2m':
                    temp_overwritten = True
        return temp_overwritten

    def _nws_hourly(self, points):
        """Returns a list of (utc_epoch_seconds, tempF, windMph, windDirDeg,
        popPercent) tuples from NWS's hourly forecast, or None on failure."""
        try:
            url = points['forecastHourly']
            url += ('&' if '?' in url else '?') + 'units=us'
            data = self._fetch_json(url, headers=self.NWS_HEADERS, timeout=self._NWS_CALL_TIMEOUT)
            periods = (data.get('properties') or {}).get('periods') or []
            out = []
            for per in periods:
                start = per.get('startTime')
                if not start:
                    continue
                try:
                    epoch = datetime.fromisoformat(start).timestamp()
                except Exception:
                    continue
                temp = per.get('temperature')
                if temp is not None and per.get('temperatureUnit') == 'C':
                    temp = temp * 9 / 5 + 32
                wind_mph = None
                nums = [int(n) for n in re.findall(r'\d+', per.get('windSpeed') or '')]
                if nums:
                    wind_mph = max(nums)  # a gusty range like "5 to 10 mph" reports the higher bound
                wind_dir = self._NWS_COMPASS.get((per.get('windDirection') or '').upper())
                pop = (per.get('probabilityOfPrecipitation') or {}).get('value')
                out.append((epoch, temp, wind_mph, wind_dir, pop))
            return out or None
        except Exception as e:
            print(f"[nws] hourly fetch failed: {e}")
            return None

    def _nws_apply_hourly(self, om_hourly, nws_hourly, tz_name):
        """Overlays NWS's per-hour temp/wind/precip-probability onto
        Open-Meteo's own hourly arrays, matched by nearest timestamp within
        a 40-minute tolerance (NWS periods land on the hour and should
        align almost exactly with Open-Meteo's own hourly grid). An index
        with no close-enough NWS point keeps Open-Meteo's original value —
        e.g. NWS's hourly horizon is shorter than Open-Meteo's 7-day one."""
        if not nws_hourly or not om_hourly.get('time'):
            return
        try:
            tz = ZoneInfo(tz_name) if tz_name and tz_name != 'auto' else None
        except Exception:
            tz = None
        if tz is None:
            return  # can't safely align timestamps without a real IANA tz name
        TOL = 40 * 60
        for i, t_str in enumerate(om_hourly['time']):
            try:
                epoch = datetime.fromisoformat(t_str).replace(tzinfo=tz).timestamp()
            except Exception:
                continue
            best, best_diff = None, None
            for cand in nws_hourly:
                diff = abs(cand[0] - epoch)
                if diff <= TOL and (best_diff is None or diff < best_diff):
                    best, best_diff = cand, diff
            if best is None:
                continue
            _, temp, wind_mph, wind_dir, pop = best
            if temp is not None:
                om_hourly['temperature_2m'][i] = temp
            if wind_mph is not None and 'wind_speed_10m' in om_hourly:
                om_hourly['wind_speed_10m'][i] = wind_mph
            if wind_dir is not None and 'wind_direction_10m' in om_hourly:
                om_hourly['wind_direction_10m'][i] = wind_dir
            if pop is not None and 'precipitation_probability' in om_hourly:
                om_hourly['precipitation_probability'][i] = pop

    def _nws_daily(self, points):
        """Returns (highs_by_date, lows_by_date) dicts keyed by 'YYYY-MM-DD'
        local calendar date, or (None, None) on failure. NWS's startTime
        already carries the correct local UTC offset, so the date portion
        (before 'T') IS the correct local calendar date — no timezone math
        needed here."""
        try:
            url = points['forecast']
            url += ('&' if '?' in url else '?') + 'units=us'
            data = self._fetch_json(url, headers=self.NWS_HEADERS, timeout=self._NWS_CALL_TIMEOUT)
            periods = (data.get('properties') or {}).get('periods') or []
            highs, lows = {}, {}
            for per in periods:
                start = per.get('startTime') or ''
                date = start[:10]
                temp = per.get('temperature')
                if not date or temp is None:
                    continue
                if per.get('temperatureUnit') == 'C':
                    temp = temp * 9 / 5 + 32
                if per.get('isDaytime'):
                    highs[date] = temp
                else:
                    lows[date] = temp
            if not highs and not lows:
                return None, None
            return highs, lows
        except Exception as e:
            print(f"[nws] daily fetch failed: {e}")
            return None, None

    def _nws_apply_daily(self, om_daily, highs, lows):
        dates = om_daily.get('time') or []
        maxes = om_daily.get('temperature_2m_max')
        mins = om_daily.get('temperature_2m_min')
        for i, date in enumerate(dates):
            if maxes is not None and highs and date in highs:
                maxes[i] = highs[date]
            if mins is not None and lows and date in lows:
                mins[i] = lows[date]

    def handle_weather(self):
        """Fetch weather from Open-Meteo (free, no API key) using ZIP or lat/lon."""
        params = parse_qs(urlparse(self.path).query)
        zip_code = params.get('zip', [None])[0]
        lat_p = params.get('lat', [None])[0]
        lon_p = params.get('lon', [None])[0]
        try:
            if zip_code and not (lat_p and lon_p):
                from urllib.parse import quote as uq
                geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={uq(zip_code)}&count=3&language=en&format=json"
                req = urllib.request.Request(geo_url, headers={"User-Agent":"Mozilla/5.0"})
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=6) as r:
                    geo = json.loads(r.read())
                results = geo.get('results', [])
                match = next((x for x in results if str((x.get('postcodes') or [None])[0]) == str(zip_code)), results[0] if results else None)
                if not match:
                    raise ValueError(f"ZIP code {zip_code} not found")
                lat = match['latitude']
                lon = match['longitude']
                location_name = f"{match.get('name','')}, {match.get('admin1','')}"
                timezone = match.get('timezone', 'auto')
            elif lat_p and lon_p:
                lat, lon = float(lat_p), float(lon_p)
                location_name = params.get('name', ['Unknown'])[0]
                timezone = params.get('tz', ['auto'])[0]
            else:
                raise ValueError("Provide zip or lat+lon")
            from urllib.parse import quote as uq
            wx_url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={lat}&longitude={lon}"
                f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,"
                f"wind_speed_10m,wind_direction_10m,precipitation,uv_index,cloud_cover"
                f"&minutely_15=temperature_2m,apparent_temperature,weather_code,precipitation"
                f"&hourly=temperature_2m,apparent_temperature,weather_code,precipitation_probability,"
                f"precipitation,wind_speed_10m,wind_direction_10m"
                f"&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,"
                f"precipitation_probability_max,uv_index_max,wind_speed_10m_max,sunrise,sunset"
                f"&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"
                f"&forecast_days=7&timezone={uq(str(timezone))}"
            )
            aq_url = (
                f"https://air-quality-api.open-meteo.com/v1/air-quality"
                f"?latitude={lat}&longitude={lon}"
                f"&current=us_aqi,pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,ozone"
                f"&timezone={uq(str(timezone))}"
            )
            req2 = urllib.request.Request(wx_url, headers={"User-Agent":"Mozilla/5.0"})
            req3 = urllib.request.Request(aq_url, headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req2, context=ssl_ctx, timeout=8) as r:
                wx = json.loads(r.read())
            try:
                with urllib.request.urlopen(req3, context=ssl_ctx, timeout=6) as r:
                    wx['air_quality'] = json.loads(r.read())
            except Exception:
                wx['air_quality'] = None
            wx['location'] = location_name
            wx['lat'] = lat
            wx['lon'] = lon

            # ── NWS augmentation (temp/wind/humidity/precip only — see the
            # helper methods above for full rationale). Never allowed to
            # affect the response if anything about it fails.
            nws_start = _time.time()
            try:
                pts = self._nws_points(lat, lon)
            except Exception as e:
                print(f"[nws] augmentation skipped: {e}")
                pts = None
            if pts:
                nws_deadline = nws_start + self._NWS_TOTAL_BUDGET
                try:
                    cur_obs = self._nws_current_obs(pts)
                    if cur_obs and wx.get('current') is not None:
                        if self._nws_apply_current(wx['current'], cur_obs):
                            wx.pop('minutely_15', None)
                except Exception as e:
                    print(f"[nws] current-obs step failed: {e}")
                if _time.time() < nws_deadline:
                    try:
                        hourly_list = self._nws_hourly(pts)
                        if hourly_list and wx.get('hourly'):
                            self._nws_apply_hourly(wx['hourly'], hourly_list, wx.get('timezone'))
                    except Exception as e:
                        print(f"[nws] hourly step failed: {e}")
                else:
                    print(f"[nws] hourly step skipped: over {self._NWS_TOTAL_BUDGET}s augmentation budget")
                if _time.time() < nws_deadline:
                    try:
                        highs, lows = self._nws_daily(pts)
                        if wx.get('daily'):
                            self._nws_apply_daily(wx['daily'], highs, lows)
                    except Exception as e:
                        print(f"[nws] daily step failed: {e}")
                else:
                    print(f"[nws] daily step skipped: over {self._NWS_TOTAL_BUDGET}s augmentation budget")
                wx['nws_augmented'] = True
            else:
                wx['nws_augmented'] = False

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(wx).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())


    # ── Live radar (NOAA MRMS composite reflectivity — free, no API key) ──
    #
    # NOAA's public time-enabled radar mosaic keeps a rolling ~4-hour window
    # of frames, refreshed roughly every 10 minutes. That's the largest
    # window any free/no-key radar source reliably provides — RainViewer's
    # free tier was restricted to a ~2h past-only window at low zoom as of
    # Jan 2026, and going further back (e.g. a true 12h loop) would require
    # either a paid archive or our own long-running snapshot cache, which
    # isn't worth the complexity here. Two mirror hosts are tried in order
    # in case one is down. US coverage only (CONUS/AK/HI/PR/Guam).
    _RADAR_BASES = [
        "https://mapservices.weather.noaa.gov/eventdriven/rest/services/radar/radar_base_reflectivity_time/ImageServer",
        "https://idpgis.ncep.noaa.gov/arcgis/rest/services/radar/radar_base_reflectivity_time/ImageServer",
    ]
    _RADAR_HEADERS = {"User-Agent": "personal-dashboard contact@example.com", "Accept": "*/*"}

    def _radar_time_extent(self):
        """Return (start_ms, end_ms) for the live mosaic's current moving
        window, trying each mirror. Cached ~3 minutes since it barely moves
        between successive polls."""
        now = _time.time()
        cached = _RADAR_META_CACHE.get('extent')
        if cached and (now - cached[0]) < 180:
            return cached[1]
        last_err = None
        for base in self._RADAR_BASES:
            try:
                info = self._fetch_json(f"{base}?f=json", headers=self._RADAR_HEADERS)
                ext = (info.get("timeInfo") or {}).get("timeExtent")
                if ext and len(ext) == 2:
                    result = (int(ext[0]), int(ext[1]))
                    _RADAR_META_CACHE['extent'] = (now, result)
                    return result
                print(f"[radar] {base}: response had no usable timeExtent")
            except Exception as e:
                print(f"[radar] time-extent fetch failed for {base}: {e}", flush=True)
                last_err = e
                continue
        print(f"[radar] time extent unavailable from any mirror — last error: {last_err}", flush=True)
        raise last_err or RuntimeError("no radar time extent available from any mirror")

    @staticmethod
    def _radar_bbox(lat, lon, w, h, half_miles=140.0):
        """Lat/lon bbox centered on (lat, lon), sized so the physical
        ground distance matches the requested image aspect ratio (no
        north-south/east-west stretch)."""
        import math
        aspect = w / float(h)
        lat_half = half_miles / 69.0
        lon_half = (half_miles * aspect) / (69.0 * max(math.cos(math.radians(lat)), 0.15))
        return (lon - lon_half, lat - lat_half, lon + lon_half, lat + lat_half)

    def handle_radar_meta(self):
        """Return the list of available frame timestamps (~15 min apart)
        across the live mosaic's reliable window (up to ~4h)."""
        params = parse_qs(urlparse(self.path).query)
        try:
            lat = float(params.get('lat', [None])[0])
            lon = float(params.get('lon', [None])[0])
        except (TypeError, ValueError):
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "lat and lon are required"}).encode())
            return
        try:
            start_ms, end_ms = self._radar_time_extent()
            step_ms = 15 * 60 * 1000
            # NOAA's advertised time extent runs slightly ahead of when the
            # mosaic tiles for that exact timestamp are actually finished
            # compositing. Requesting a time right at (or within a few
            # minutes of) the advertised "latest" often returns a valid,
            # error-free, but EMPTY image — no reflectivity data anywhere in
            # the bbox — because that slice hasn't finished building yet.
            # This surfaces as the newest frame(s) in the loop rendering as
            # a blank basemap with nothing over it, right after a frame just
            # 15 min older shows real storm data — a data-availability lag,
            # not a fetch failure, so it never raises and never hits the
            # except branch below. Cap how recent a frame we'll ever offer
            # by a fixed buffer so we don't serve timestamps NOAA hasn't
            # actually finished compositing yet.
            # v2.9.47: bumped 10min -> 20min. Reported symptom: the header
            # mini-preview (which always requests exactly this array's last
            # entry, with no animation to visually mask a single bad frame)
            # intermittently showed a plain basemap with no storm cells at
            # all, while the SAME modal, opened moments later and scrubbed
            # back one frame, showed real data 15 min older — i.e. this
            # exact lag, not a fetch failure or a preview-specific bug.
            # Confirmed directly from a reported case: the blank frame was
            # only ~13 minutes old at the moment it was viewed — already
            # past the old 10-minute buffer, meaning actual MRMS compositing
            # lag exceeded 10 minutes that time. 20 min is still just an
            # estimate, not a guarantee (real-world compositing lag isn't
            # observable from the client side, and can presumably run even
            # longer under heavier data volume, e.g. active severe weather)
            # — if blank latest-frames are still seen, this is still the
            # first value to increase further.
            RADAR_LAG_BUFFER_MS = 20 * 60 * 1000
            now_ms = int(_time.time() * 1000)
            capped_end_ms = min(end_ms, now_ms - RADAR_LAG_BUFFER_MS)
            # Don't let the cap collapse the window to nothing if the
            # advertised extent is already narrower than the buffer (e.g.
            # right after NOAA's own service restarts) — fall back to the
            # uncapped end rather than return zero frames.
            if capped_end_ms < start_ms:
                capped_end_ms = end_ms
            frames = []
            t = capped_end_ms
            while t >= start_ms and len(frames) < 17:
                frames.append(t)
                t -= step_ms
            frames.reverse()

            self.json_response({
                "frames": frames,
                "lat": lat,
                "lon": lon,
                "half_miles": 140,
                "source": "NOAA MRMS composite reflectivity",
                "coverage": "US only (CONUS/AK/HI/PR/Guam)",
            })
        except Exception as e:
            print(f"[radar] /radar-meta failed for lat={lat} lon={lon}: {e}", flush=True)
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": f"radar metadata unavailable: {e}"}).encode())

    def handle_radar_frame(self):
        """Proxy a single radar reflectivity PNG frame for a given
        location/time. Frames are cached (reusing the mempool disk cache
        helpers) since a past frame never changes — only the newest one is
        ever re-requested against a moving timestamp."""
        params = parse_qs(urlparse(self.path).query)
        try:
            lat = float(params.get('lat', [None])[0])
            lon = float(params.get('lon', [None])[0])
            t_ms = int(params.get('t', [None])[0])
        except (TypeError, ValueError):
            self.send_error(400, "lat, lon and t are required")
            return
        try:
            w = max(60, min(int(params.get('w', ['300'])[0] or 300), 900))
            h = max(60, min(int(params.get('h', ['220'])[0] or 220), 700))
        except ValueError:
            w, h = 300, 220

        cache_key = f"radar-frame:{round(lat,2)}:{round(lon,2)}:{t_ms}:{w}x{h}"
        cached = _mempool_cache_get(cache_key, ttl=900)
        if cached:
            data, ct, _src = cached
            self.send_response(200)
            self.send_header("Content-Type", ct or "image/png")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "public, max-age=300")
            self.end_headers()
            self.wfile.write(data)
            return

        bbox = self._radar_bbox(lat, lon, w, h)
        last_err = None
        for base in self._RADAR_BASES:
            try:
                url = (f"{base}/exportImage?bbox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
                       f"&bboxSR=4326&imageSR=4326&size={w},{h}&format=png&transparent=true"
                       f"&time={t_ms}&f=image")
                req = urllib.request.Request(url, headers=self._RADAR_HEADERS)
                data, ct = _fetch_with_deadline(req, ssl_ctx, 6, 9)
                if not data or len(data) < 100:
                    raise ValueError("empty radar image response")
                _mempool_cache_set(cache_key, data, ct or "image/png", "noaa-mrms")
                self.send_response(200)
                self.send_header("Content-Type", ct or "image/png")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "public, max-age=300")
                self.end_headers()
                self.wfile.write(data)
                return
            except Exception as e:
                print(f"[radar] frame fetch failed for {base} (lat={lat} lon={lon} t={t_ms}): {e}", flush=True)
                last_err = e
                continue
        print(f"[radar] frame unavailable from any mirror for lat={lat} lon={lon} t={t_ms}: {last_err}", flush=True)
        self.send_response(502)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"error": f"radar frame unavailable: {last_err}"}).encode())

    # ── Radar basemap (CARTO Dark Matter — free, no API key) ──
    #
    # Gives the radar loop visual context (state/country borders, coastline,
    # water, roads) instead of storm cells floating on a flat background.
    # Reuses _radar_bbox with the SAME lat/lon/w/h a caller passes to
    # /radar-frame so the two images cover the same ground extent. Unlike
    # radar frames, a basemap for a given location doesn't change, so it's
    # cached far longer (24h).
    #
    # v2.9.43 — switched off Esri (Canvas/World_Dark_Gray_Base + _Reference,
    # World_Street_Map) after actually reading Esri's own live service
    # descriptions rather than assuming: World_Dark_Gray_Reference's
    # description says, verbatim, "This layer provides labels for selected
    # cities and towns ... in support of the World Dark Gray Base map" — it
    # is ONLY a labels layer, not boundaries/coastline/roads, so every prior
    # version compositing it over Base (v2.9.17 - v2.9.42) could only ever
    # add unwanted text, never the state/coastline lines that were actually
    # the point — explaining exactly the two symptoms reported (light/
    # unlined main map, labeled-but-still-unlined mini map). Esri has no
    # separate "boundaries + water, no labels" service to pair with Base
    # instead — their boundary reference layers bundle labels in with the
    # lines. CARTO's Dark Matter basemap is built specifically to split into
    # a "dark_all" (labeled) / "dark_nolabels" (identical cartography, label
    # layer switched off) pair, which is exactly what's needed for a
    # labeled main map + unlabeled-but-still-lined mini preview, so this
    # replaces the Esri approach rather than continuing to extend something
    # that structurally can't do it. Free, standard OSM+CARTO attribution
    # (see the credit line next to the modal's radar section) — CARTO's
    # anonymous no-key tier now requires a free API key as of their recent
    # policy change (confirmed via CARTO's own current FAQ,
    # docs.carto.com/faqs/carto-basemaps): requests without one still
    # return a normal, correctly-typed PNG — no fetch error, nothing this
    # code could detect on its own — but the tile is stamped with a
    # repeated "API key required" watermark in place of usable cartography,
    # which is exactly what "map lines/ocean lines are gone" looks like.
    # Get a free key (no CARTO account needed, ~1 minute) at
    # https://carto.com/basemaps/apikey and set CARTO_API_KEY — see
    # _CARTO_API_KEY below.
    #
    # CARTO serves this as 256px Web Mercator XYZ tiles, not a single
    # bbox->image export, so _build_basemap_image() fetches every tile
    # touching _radar_bbox()'s lat/lon box at a zoom level chosen to roughly
    # match the requested pixel width, stitches them with Pillow, and crops
    # to the exact bbox before resizing to w x h. Web Mercator's vertical
    # scale differs slightly from the plain equirectangular bbox the NOAA
    # radar frame itself is rendered in (bboxSR=4326, no reprojection), but
    # over the ~280-mile-wide window this app uses (_radar_bbox's default
    # half_miles=140) that difference is a small fraction of one percent —
    # not visible layered under a semi-transparent radar overlay. Requires
    # Pillow (`pip install Pillow`), same dependency the old approach added.
    #
    # Two styles, selected by ?style=:
    #   (default)  dark_all      — labels included (city/state names),
    #              used by the modal's larger Live Radar view where labels
    #              are legible and wanted.
    #   plain      dark_nolabels — the SAME borders/coastline/water/roads,
    #              label layer off, used by the small header mini-preview
    #              where text is illegible at 40x30px and was just noise.
    _CARTO_TILE_URL = "https://{s}.basemaps.cartocdn.com/{style}/{z}/{x}/{y}.png"
    _CARTO_SUBDOMAINS = ["a", "b", "c", "d"]
    _CARTO_HEADERS = {"User-Agent": "personal-dashboard contact@example.com", "Accept": "image/png,*/*"}
    _CARTO_MAX_TILES = 30  # sanity cap — a bad zoom pick should error, not fetch hundreds of tiles
    # CARTO's anonymous (no-key) raster tile endpoint now returns a valid,
    # correctly-typed PNG even without a key — it just has a repeated
    # "API key required" watermark stamped over the cartography instead of
    # the actual state/coastline/road lines. That's why this silently
    # "worked" (no fetch error, no code bug) while still visually showing
    # no lines. A free key removes the watermark; get one (no CARTO account
    # needed, ~1 minute) at https://carto.com/basemaps/apikey and set
    # CARTO_API_KEY. Runs without one — same graceful-when-unset pattern as
    # FMP_API_KEY above — but the tiles will carry that watermark until set.
    # Resolved via _load_carto_api_key() (env var, then carto_api_key.txt
    # fallback — see that function's docstring) rather than os.environ.get
    # directly, since the env-var-only path has been the actual recurring
    # failure point in practice, not this fetch/cache logic below it.
    _CARTO_API_KEY, _CARTO_API_KEY_SOURCE = _load_carto_api_key()

    @staticmethod
    def _lonlat_to_tilepx(lon, lat, zoom):
        """Fractional (x, y) pixel coordinates in this zoom's full tile
        mosaic (256px tiles, standard OSM/Web Mercator slippy-map
        convention: x grows eastward from the antimeridian, y grows
        southward from the north pole)."""
        import math
        lat = max(min(lat, 85.05112878), -85.05112878)  # Web Mercator's valid range
        n = 2.0 ** zoom
        x = (lon + 180.0) / 360.0 * n * 256.0
        lat_rad = math.radians(lat)
        y = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n * 256.0
        return x, y

    @staticmethod
    def _pick_carto_zoom(lon_min, lon_max, target_w):
        """Smallest zoom whose tile grid gives at least target_w pixels
        across the bbox's longitude span, so the final crop is a downscale
        (sharp) rather than an upscale (blurry). Capped to CARTO's
        documented 0-20 zoom range."""
        span = max(lon_max - lon_min, 1e-6)
        for z in range(2, 19):
            px_span = span / 360.0 * (2 ** z) * 256.0
            if px_span >= target_w:
                return z
        return 18

    @classmethod
    def _fetch_carto_tile(cls, z, x, y, style):
        """Fetch one 256x256 tile, trying each subdomain in turn. Raises
        if all of them fail."""
        last_err = None
        for sd in cls._CARTO_SUBDOMAINS:
            url = cls._CARTO_TILE_URL.format(s=sd, style=style, z=z, x=x, y=y)
            if cls._CARTO_API_KEY:
                url += f"?key={cls._CARTO_API_KEY}"
            try:
                req = urllib.request.Request(url, headers=cls._CARTO_HEADERS)
                data, ct = _fetch_with_deadline(req, ssl_ctx, 5, 8)
                if not data or len(data) < 100:
                    raise ValueError(f"tile {z}/{x}/{y}: empty response")
                if not (ct or '').startswith('image/'):
                    raise ValueError(f"tile {z}/{x}/{y}: non-image response (Content-Type={ct!r})")
                return data
            except Exception as e:
                last_err = e
                continue
        raise last_err or RuntimeError(f"tile {z}/{x}/{y} unavailable from any CARTO subdomain")

    @classmethod
    def _build_basemap_image(cls, lat, lon, w, h, carto_style):
        """Fetch and stitch the CARTO tiles covering _radar_bbox(lat,lon,w,h),
        crop to the exact bbox, resize to (w, h), and boost contrast/clarity.
        Returns PNG bytes."""
        from PIL import Image, ImageEnhance
        bbox = cls._radar_bbox(lat, lon, w, h)  # (lon_min, lat_min, lon_max, lat_max)
        zoom = cls._pick_carto_zoom(bbox[0], bbox[2], w)
        # y is inverted vs lat (Web Mercator y grows southward), so the
        # NORTH edge (lat_max) maps to the SMALLER y pixel coordinate.
        x0_px, y0_px = cls._lonlat_to_tilepx(bbox[0], bbox[3], zoom)  # top-left    (lon_min, lat_max)
        x1_px, y1_px = cls._lonlat_to_tilepx(bbox[2], bbox[1], zoom)  # bottom-right(lon_max, lat_min)
        tx0, ty0 = int(x0_px // 256), int(y0_px // 256)
        tx1, ty1 = int(x1_px // 256), int(y1_px // 256)
        n_tiles_side = int(2 ** zoom)
        tile_count = (tx1 - tx0 + 1) * (ty1 - ty0 + 1)
        if tile_count < 1 or tile_count > cls._CARTO_MAX_TILES:
            raise ValueError(f"basemap would need {tile_count} tiles at zoom {zoom} — bbox/zoom math is off")
        canvas = Image.new("RGB", ((tx1 - tx0 + 1) * 256, (ty1 - ty0 + 1) * 256))
        for ty in range(ty0, ty1 + 1):
            if not (0 <= ty < n_tiles_side):
                continue  # off the top/bottom of the mosaic — never happens for a US radar location
            for tx in range(tx0, tx1 + 1):
                wrapped_tx = tx % n_tiles_side  # antimeridian wrap, harmless no-op for US locations
                tile_data = cls._fetch_carto_tile(zoom, wrapped_tx, ty, carto_style)
                tile_img = Image.open(io.BytesIO(tile_data)).convert("RGB")
                canvas.paste(tile_img, ((tx - tx0) * 256, (ty - ty0) * 256))
        crop_box = (
            int(round(x0_px - tx0 * 256)),
            int(round(y0_px - ty0 * 256)),
            int(round(x1_px - tx0 * 256)),
            int(round(y1_px - ty0 * 256)),
        )
        cropped = canvas.crop(crop_box)
        resized = cropped.resize((w, h), Image.LANCZOS)
        # v2.9.54 — CARTO's Dark Matter tiles are legitimately dark (that's
        # the whole point of the style — this app's own dark theme is why
        # it was picked over a light basemap in the first place), but at
        # their as-served contrast the state/coastline borders, minor roads,
        # and city labels sit only ~30-75 RGB levels above the near-black
        # background — enough to exist, not enough to read comfortably,
        # reported as "almost too dark to see the details that are already
        # on the map." This isn't a wrong-layer bug like the old Esri saga
        # above (the cartography itself is correct and complete, per
        # v2.9.43/44) — it's a pure contrast/clarity tweak.
        #
        # Applied here (not via a client-side CSS filter) so it's baked into
        # the cached PNG once per basemap fetch rather than recomputed by
        # the browser on every paint, and so both surfaces that use this
        # same function — the modal's labeled dark_all map and the header's
        # unlabeled dark_nolabels mini-preview — get identical, consistent
        # treatment automatically, matching "similar" treatment as asked
        # for both.
        #
        # Contrast pivots around the image's own mean pixel value, and a
        # Dark Matter tile's mean sits close to its near-black background
        # (only a small fraction of pixels are line/label foreground) — so
        # increasing contrast pushes the already-bright foreground (borders,
        # roads, labels, water) UP more than it pushes the background down,
        # which is the opposite of what a flat brightness boost would do
        # (that would wash out the background too and fight the dark theme
        # instead of preserving it). Brightness is nudged up slightly on
        # top, and sharpness compensates for the softening the LANCZOS
        # resize above introduces on the thin one-pixel lines this basemap
        # is mostly made of. Factors were chosen by measuring exact pixel
        # values against a synthetic tile built from Dark Matter's real
        # documented palette (background/water/border/road/label swatches),
        # not eyeballed: at these settings the background samples a couple
        # RGB levels DARKER than the original (contrast pushes the
        # below-mean background down, not up — deepens the dark theme
        # slightly rather than washing it out), while border lines roughly
        # double their separation from the background (+64 -> +107 delta on
        # the red channel in that test), minor roads go from barely-there
        # (+34) to clearly present (+57), and labels go from readable-if-
        # you-look (+117) to comfortably legible (+176) — a real, measured
        # improvement in exactly the details reported as too hard to see,
        # with the background if anything reinforcing the dark theme rather
        # than lightening it.
        resized = ImageEnhance.Contrast(resized).enhance(1.40)
        resized = ImageEnhance.Brightness(resized).enhance(1.08)
        resized = ImageEnhance.Sharpness(resized).enhance(1.25)
        out = io.BytesIO()
        resized.save(out, format="PNG")
        return out.getvalue()

    def handle_radar_basemap(self):
        """Proxy a static basemap image for the same location/size a radar
        frame is requested at, so it can be layered underneath.

        IMPORTANT: every response below sends Cache-Control: no-store, even
        though this server already has its own internal cache (via
        _mempool_cache, keyed by lat/lon/size/style AND whether a CARTO key
        is configured — see cache_key below). That's deliberate, not an
        oversight: a long browser-side cache (this used to send
        `public, max-age=86400`) caused a real bug — a browser that loaded
        a watermarked tile before CARTO_API_KEY was set would keep serving
        that same broken image from ITS OWN cache for a full day,
        completely unaffected by fixing the key, restarting the proxy, or
        even reverting the whole file. No-store makes the server (whose own
        cache already self-invalidates correctly on key changes) the single
        source of truth, instead of two independent caches that can
        disagree.
        """
        params = parse_qs(urlparse(self.path).query)
        try:
            lat = float(params.get('lat', [None])[0])
            lon = float(params.get('lon', [None])[0])
        except (TypeError, ValueError):
            self.send_error(400, "lat and lon are required")
            return
        try:
            w = max(60, min(int(params.get('w', ['300'])[0] or 300), 900))
            h = max(60, min(int(params.get('h', ['220'])[0] or 220), 700))
        except ValueError:
            w, h = 300, 220
        style = (params.get('style', [''])[0] or '').strip().lower()
        plain = style == 'plain'
        carto_style = "dark_nolabels" if plain else "dark_all"
        source_tag = f"carto-{carto_style}"

        # cache key versioned (":v4:") so this fix isn't masked by a stale
        # 24h-cached watermarked image from before CARTO_API_KEY was set —
        # also folds in whether a key is actually configured right now, so
        # setting/unsetting CARTO_API_KEY self-invalidates immediately
        # instead of serving a stale watermarked (or stale keyed) tile for
        # up to a day after the env var changes.
        keyed_tag = 'keyed' if self._CARTO_API_KEY else 'unkeyed'
        # v2.9.54: bumped v4->v5 so the new contrast/brightness/sharpness
        # enhancement isn't masked by yesterday's already-cached (dimmer)
        # basemap PNGs sitting under the old key for up to 24h.
        cache_key = f"radar-basemap:{'plain' if plain else 'street'}:v5:{keyed_tag}:{round(lat,2)}:{round(lon,2)}:{w}x{h}"
        cached = _mempool_cache_get(cache_key, ttl=86400)
        if cached:
            data, ct, _src = cached
            self.send_response(200)
            self.send_header("Content-Type", ct or "image/png")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
            return

        try:
            data = self._build_basemap_image(lat, lon, w, h, carto_style)
            ct = "image/png"
            _mempool_cache_set(cache_key, data, ct, source_tag)
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            hint = " — run: pip install Pillow" if isinstance(e, ImportError) else ""
            print(f"[radar] basemap fetch failed for lat={lat} lon={lon}: {e}{hint}", flush=True)
            # Stale cache beats an error — a day-old basemap is still
            # correct (streets/borders don't move), unlike a stale radar frame.
            stale = _mempool_cache_get_stale(cache_key)
            if stale:
                print(f"[radar] basemap: serving stale cache for lat={lat} lon={lon}", flush=True)
                data, ct, _src = stale
                self.send_response(200)
                self.send_header("Content-Type", ct or "image/png")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": f"basemap unavailable: {e}"}).encode())


    # ── HRRR future radar (composite reflectivity model forecast) ──
    #
    # Companion to /radar-frame (past+live) and the disk archive above —
    # this covers the FUTURE side of the same timeline. /radar-future-meta
    # is cheap (schedule math only, no HRRR fetch) so the frontend can
    # check availability and get the exact target times before committing
    # to the heavier /radar-future-frame calls.
    def handle_radar_future_meta(self):
        if not HRRR_AVAILABLE:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "available": False,
                "reason": "HRRR dependencies not installed on this proxy (pygrib/numpy)",
            }).encode())
            return
        params = parse_qs(urlparse(self.path).query)
        try:
            lat = float(params.get('lat', [None])[0])
            lon = float(params.get('lon', [None])[0])
        except (TypeError, ValueError):
            self.send_error(400, "lat and lon are required")
            return
        now_dt = datetime.now(timezone.utc)
        for run_dt in _hrrr_candidate_runs(now_dt):
            targets = _hrrr_future_targets(run_dt, now_dt, total_minutes=180)
            if targets:
                self.json_response({
                    "available": True,
                    "frames": [int(t['valid_dt'].timestamp() * 1000) for t in targets],
                    "run_init_ms": int(run_dt.timestamp() * 1000),
                    "source": "NOAA HRRR (High-Resolution Rapid Refresh) — composite reflectivity model forecast",
                    "disclaimer": "Model forecast, not an observation — accuracy naturally decreases further out. Updates roughly hourly as new HRRR runs publish.",
                })
                return
        self.json_response({"available": False, "reason": "no usable HRRR run schedule could be built"})

    def handle_radar_future_frame(self):
        """Serve one HRRR-forecast PNG for a specific target time. All
        target times for a location are computed together (one run's worth
        of fetches) and cached; a per-location lock means concurrent
        requests for different steps of the SAME forecast share one
        computation instead of each triggering a redundant set of NOMADS
        fetches."""
        if not HRRR_AVAILABLE:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "HRRR forecast not available on this proxy"}).encode())
            return
        params = parse_qs(urlparse(self.path).query)
        try:
            lat = float(params.get('lat', [None])[0])
            lon = float(params.get('lon', [None])[0])
            t_ms = int(params.get('t', [None])[0])
        except (TypeError, ValueError):
            self.send_error(400, "lat, lon and t are required")
            return
        try:
            w = max(60, min(int(params.get('w', ['300'])[0] or 300), 900))
            h = max(60, min(int(params.get('h', ['220'])[0] or 220), 700))
        except ValueError:
            w, h = 300, 220

        cache_key = f"hrrr-future:{round(lat,2)}:{round(lon,2)}:{w}x{h}"
        result = _hrrr_cache_get(cache_key)
        if result is None:
            lock = _hrrr_compute_lock(cache_key)
            with lock:
                result = _hrrr_cache_get(cache_key)
                if result is None:
                    try:
                        result = _compute_hrrr_future(lat, lon, w, h)
                        _hrrr_cache_set(cache_key, result)
                    except Exception as e:
                        print(f"[hrrr] compute failed for lat={lat} lon={lon}: {e}", flush=True)
                        self.send_response(502)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": f"HRRR forecast unavailable: {e}"}).encode())
                        return

        # Match the closest computed frame within a tolerance, rather than
        # requiring an exact millisecond match — the frontend gets exact
        # valid_ms values from /radar-future-meta so this should normally
        # be exact, but a tolerance avoids a hard failure over a rounding
        # difference between requests.
        match = None
        for f in result['frames']:
            if abs(f['valid_ms'] - t_ms) < 8 * 60 * 1000:
                match = f
                break
        if not match:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "requested time is not in this run's forecast schedule"}).encode())
            return

        data = match['png']
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=600")
        self.send_header("X-HRRR-Run-Init-Ms", str(result['run_init_ms']))
        self.send_header("X-HRRR-Valid-Ms", str(match['valid_ms']))
        self.end_headers()
        self.wfile.write(data)

    # ── Yahoo Finance quoteSummary (no-crumb approach, works from home IPs) ──

    # ── Yahoo Finance with proper crumb acquisition ───────────────────────────

    # ── Financial data via SEC EDGAR + Stockanalysis (no API key, no rate limits) ──

    def _fetch_json(self, url, headers=None, timeout=15):
        """Simple JSON fetch helper."""
        h = {"User-Agent": "Mozilla/5.0 (compatible; personal-dashboard/1.0)", "Accept": "application/json"}
        if headers:
            h.update(headers)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=timeout) as r:
            return json.loads(r.read())

    def _get_cik(self, sym):
        """Look up SEC CIK for a ticker symbol."""
        # company_tickers.json covers stocks AND ETFs
        tickers = self._fetch_json(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "personal-dashboard contact@example.com"}
        )
        sym_upper = sym.upper()
        for entry in tickers.values():
            if entry.get("ticker", "").upper() == sym_upper:
                return str(entry["cik_str"]).zfill(10)
        return None

    def _edgar_facts(self, cik):
        """Fetch company facts from SEC EDGAR."""
        return self._fetch_json(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            headers={"User-Agent": "personal-dashboard contact@example.com"}
        )

    # Fallback chains: if primary concept has no data, try these alternates in order
    CONCEPT_FALLBACKS = {
        "Revenues": [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "SalesRevenueGoodsNet",
            "RevenuesNetOfInterestExpense",
            "RevenueFromContractWithCustomerProductAndServiceExcludingAssessedTax",
            "SubscriptionAndCirculationRevenue",
            "LicenseAndServiceRevenue",
            "ServiceRevenue",
            "ProductRevenue",
            "SoftwareLicenseRevenue",
        ],
        "GrossProfit": [
            "GrossProfitLoss",
        ],
        "SellingGeneralAndAdministrativeExpense": [
            "GeneralAndAdministrativeExpense",
            "SellingAndMarketingExpense",
            "SellingExpense",
        ],
    }

    def _get_tagged_values(self, facts, concept, unit="USD", n=8, annual=False):
        """Extract recent values for an XBRL concept, sorted newest-first.
        Falls back to alternate XBRL concepts if the primary returns no data."""
        concepts_to_try = [concept] + self.CONCEPT_FALLBACKS.get(concept, [])
        entries = None
        for c in concepts_to_try:
            try:
                candidate = facts["facts"]["us-gaap"][c]["units"][unit]
                if candidate:
                    entries = candidate
                    break
            except (KeyError, TypeError):
                continue
        if not entries:
            return []
        # Filter to 10-Q (quarterly) or 10-K (annual), dedupe by end date
        form = "10-K" if annual else "10-Q"

        def _filter_entries(ents, strict_period=True):
            seen = {}
            for e in ents:
                if e.get("form") != form:
                    continue
                end = e.get("end", "")
                start = e.get("start", "")
                # For annual: only keep full-year periods (end-start >= 340 days)
                if annual and strict_period and start and end:
                    try:
                        import datetime as dt_mod
                        d_end = dt_mod.date.fromisoformat(end)
                        d_start = dt_mod.date.fromisoformat(start)
                        days = (d_end - d_start).days
                        if days < 340:
                            continue  # skip quarterly/transitional filings in 10-K
                    except Exception:
                        pass
                # Pick the most recent filing for each period end
                if end not in seen or e.get("filed", "") > seen[end].get("filed", ""):
                    seen[end] = e
            return sorted(seen.values(), key=lambda x: x["end"], reverse=True)

        out = _filter_entries(entries, strict_period=True)

        # If annual and the most recent known period is more than ~15 months old,
        # retry without the 340-day filter — some companies file annual revenue
        # as a shorter stub period when changing fiscal year or company structure
        if annual and out:
            import datetime as dt_mod
            most_recent = dt_mod.date.fromisoformat(out[0]["end"])
            today = dt_mod.date.today()
            if (today - most_recent).days > 450:
                out_relaxed = _filter_entries(entries, strict_period=False)
                if out_relaxed and out_relaxed[0]["end"] > out[0]["end"]:
                    out = out_relaxed

        ends_found = [x["end"] for x in out[:n]]
        return out[:n]

    def _raw(self, val, fmt=None):
        if val is None: return None
        return {"raw": float(val), "fmt": fmt or str(val)}

    def _period_label(self, entry, annual=False):
        """Make a readable period label from an XBRL entry."""
        end = entry.get("end","")
        if annual:
            return end[:4]
        # Quarter: derive from end date
        try:
            import datetime
            d = datetime.date.fromisoformat(end)
            q = (d.month - 1) // 3 + 1
            return f"{d.year}Q{q}"
        except:
            return end[:7]

    def _build_stmt_rows(self, concept_map, facts, annual=False, n=4):
        """
        concept_map: list of (yf_field_name, xbrl_concept, unit)
        Returns list of dicts keyed by yf_field_name, plus 'endDate' and 'label'.
        Uses majority-vote on end dates to avoid stray outlier periods.
        """
        import datetime
        from collections import Counter
        raw_data = {}
        end_votes = Counter()  # count how many concepts have data for each end date
        for yf_key, concept, unit in concept_map:
            rows = self._get_tagged_values(facts, concept, unit=unit, n=n*2, annual=annual)
            raw_data[yf_key] = {r["end"]: r["val"] for r in rows}
            for end in raw_data[yf_key]:
                end_votes[end] += 1

        # Only keep end dates that have data for at least 2 concepts (majority vote)
        # This eliminates stray dates from concepts with unusual filing periods
        min_votes = max(2, len(concept_map) // 4)
        valid_ends = [end for end, count in end_votes.items() if count >= min_votes]
        ends = sorted(valid_ends, reverse=True)[:n]

        stmts = []
        for end in ends:
            try:
                ts = int(datetime.datetime.strptime(end, "%Y-%m-%d").timestamp())
            except:
                ts = 0
            s = {"endDate": {"raw": ts, "fmt": end}}
            for yf_key, _, _ in concept_map:
                v = raw_data[yf_key].get(end)
                s[yf_key] = self._raw(v) if v is not None else None
            # Derive totalRevenue from grossProfit + costOfRevenue when missing.
            # Some companies (e.g. MSTR post-rebrand) omit the Revenues XBRL tag
            # in their annual 10-K but do file GrossProfit and CostOfRevenue.
            if s.get("totalRevenue") is None:
                gp = s.get("grossProfit")
                cor = s.get("costOfRevenue")
                if gp is not None and cor is not None:
                    derived = gp["raw"] + cor["raw"]
                    s["totalRevenue"] = self._raw(derived)
                elif gp is not None:
                    # CostOfRevenue also missing — use GrossProfit as floor estimate
                    # only if we have no revenue at all (better than a blank)
                    s["totalRevenue"] = self._raw(gp["raw"])
            stmts.append(s)
        return stmts

    def handle_financials(self):
        params = parse_qs(urlparse(self.path).query)
        sym = params.get('sym', [''])[0].strip().upper()
        cat = params.get('cat', [''])[0].strip().lower()
        if not sym:
            self.send_error(400, "Missing sym"); return

        KNOWN_ETFS = {
            'QQQ','SPY','IVV','VOO','VTI','VGT','VUG','VIG','VYM','VXUS','VEA','VWO',
            'BND','AGG','GLD','SLV','IAU','TLT','HYG','LQD','ARKK','ARKG','ARKW',
            'ARKF','ARKQ','XLK','XLF','XLE','XLV','XLU','XLI','XLB','XLP','XLY','XLRE',
            'IWM','IWF','IWD','IJH','IJR','EFA','EEM','VNQ','SCHD','JEPI','JEPQ','SPHD',
            'DGRO','NOBL','DIVO','QYLD','RYLD','XYLD','PFFD','PFF','GOVT','TIPS',
            'BNDX','BSV','BIV','BLV','VCIT','VCSH','VMBS','MBB','EMB','USHY','SHYG',
        }
        is_etf = cat in ('etf', 'mutualfund') or sym in KNOWN_ETFS

        try:
            if is_etf:
                result = self._etf_holdings(sym, cat)
                self.json_response({"quoteSummary": {"result": [result], "error": None}})
            else:
                result = self._stock_financials(sym, cat)
                self.json_response({"quoteSummary": {"result": [result], "error": None}})
        except Exception as e:
            import traceback
            print(f"[Financials] {sym} exception: {traceback.format_exc()}")
            self.json_response({"quoteSummary": {"result": None, "error": str(e)}})

    def _etf_holdings(self, sym, cat):
        """Fetch ETF top holdings. Uses provider APIs + SEC EDGAR N-PORT as fallback."""
        holdings = []

        # ── Source 1: iShares/BlackRock JSON API (reliable, official) ──────────────
        ISHARES_IDS = {
            "IVV":"239726","AGG":"239458","EFA":"239623","EEM":"239637","HYG":"239565",
            "LQD":"239566","TLT":"239454","IWM":"239710","IJH":"239763","IJR":"239774",
            "IWF":"239730","IWD":"239712","MBB":"239453","GOVT":"239468","USHY":"288700",
        }
        if not holdings and sym in ISHARES_IDS:
            try:
                fund_id = ISHARES_IDS[sym]
                url = (f"https://www.ishares.com/us/products/{fund_id}/"
                       f"fund.ajax.getHoldings.json?fileType=json&dataType=fund&startRow=0&endRow=25")
                data = self._fetch_json(url, headers={
                    "Referer": "https://www.ishares.com/",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                })
                for row in (data.get("aaData") or [])[:25]:
                    ticker = (row[0] or "").strip()
                    name   = (row[1] or "").strip()
                    try:
                        pct = float(str(row[5] or "0").replace("%","").replace(",",""))
                    except Exception:
                        pct = 0.0
                    if ticker or name:
                        holdings.append({
                            "symbol": ticker, "holdingName": name,
                            "holdingPercent": {"raw": pct/100, "fmt": f"{pct:.2f}%"},
                        })
                print(f"[ETF] iShares: {len(holdings)} for {sym}")
            except Exception as e:
                print(f"[ETF] iShares failed for {sym}: {e}")

        # ── Source 2: Invesco JSON API (QQQ, QQQM, RSP, SQQQ, etc.) ─────────────
        INVESCO_SLUGS = {
            "QQQ":"qqq","QQQM":"qqqm","RSP":"rsp","SQQQ":"sqqq","TQQQ":"tqqq",
            "ARKK":"arkk","ARKG":"arkg","ARKW":"arkw","ARKF":"arkf","ARKQ":"arkq",
        }
        if not holdings and sym in INVESCO_SLUGS:
            try:
                slug = INVESCO_SLUGS[sym]
                url  = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0/{slug}/0/ALL/ALL"
                req  = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.invesco.com/",
                })
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=12) as r:
                    raw = r.read().decode("utf-8", errors="replace")
                data = json.loads(raw)
                rows = data if isinstance(data, list) else (data.get("holdings") or data.get("data") or [])
                for h in rows[:25]:
                    pct = float(h.get("weighting") or h.get("weight") or h.get("percentage") or 0)
                    holdings.append({
                        "symbol": h.get("ticker") or h.get("symbol") or "",
                        "holdingName": h.get("name") or h.get("secDesc") or "",
                        "holdingPercent": {"raw": pct/100, "fmt": f"{pct:.2f}%"},
                    })
                print(f"[ETF] Invesco: {len(holdings)} for {sym}")
            except Exception as e:
                print(f"[ETF] Invesco failed for {sym}: {e}")

        # ── Source 3: Vanguard investor API ──────────────────────────────────────
        VANGUARD_SYMS = {"VTI","VGT","VUG","VIG","VYM","VOO","VXUS","VEA","VWO",
                         "BND","BNDX","VNQ","VCIT","VCSH","VMBS","VGK","VPL","SCHD"}
        if not holdings and sym in VANGUARD_SYMS:
            try:
                url = (f"https://investor.vanguard.com/investment-products/etfs/"
                       f"profile/api/{sym}/portfolio-holding-details")
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://investor.vanguard.com/investment-products/etfs/profile/{sym}",
                    "Origin": "https://investor.vanguard.com",
                })
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=12) as r:
                    data = json.loads(r.read())
                rows = (data.get("holdingDetails", {}).get("equityHoldings") or
                        data.get("holdingDetails", {}).get("bondHoldings") or
                        data.get("equityHoldings") or data.get("holdings") or [])
                for h in rows[:25]:
                    pct = float(h.get("percentWeight") or h.get("pctWeight") or h.get("weight") or 0)
                    holdings.append({
                        "symbol": h.get("ticker") or h.get("symbol") or "",
                        "holdingName": h.get("longName") or h.get("name") or h.get("secDesc") or "",
                        "holdingPercent": {"raw": pct/100, "fmt": f"{pct:.2f}%"},
                    })
                print(f"[ETF] Vanguard: {len(holdings)} for {sym}")
            except Exception as e:
                print(f"[ETF] Vanguard failed for {sym}: {e}")

        # ── Source 4: SEC EDGAR N-PORT (official filings, always free, no rate limits) ──
        if not holdings:
            try:
                holdings = self._etf_nport(sym)
            except Exception as e:
                print(f"[ETF] N-PORT failed for {sym}: {e}")

        print(f"[ETF] {sym}: {len(holdings)} holdings total")
        return {
            "_cat": cat,
            "quoteType": {"quoteType": "ETF"},
            "topHoldings": {
                "holdings": holdings,
                "stockPosition": {"raw": 0.0, "fmt": "0%"},
                "bondPosition":  {"raw": 0.0, "fmt": "0%"},
                "cashPosition":  {"raw": 0.0, "fmt": "0%"},
                "otherPosition": {"raw": 0.0, "fmt": "0%"},
            }
        }

    def _etf_nport(self, sym):
        """Fetch ETF holdings via SEC EDGAR: first try N-PORT, then stockanalysis HTML."""
        holdings = []

        # Try stockanalysis.com HTML table scrape (fast, no auth needed)
        try:
            req = urllib.request.Request(
                f"https://stockanalysis.com/etf/{sym.lower()}/holdings/",
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://stockanalysis.com/etf/",
                }
            )
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=12) as r:
                html = r.read().decode("utf-8", errors="replace")

            # Parse the holdings table - stockanalysis renders an HTML <table>
            # Find the table with holdings data
            table_m = re.search(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
            if table_m:
                rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_m.group(1), re.DOTALL)
                for row in rows[1:26]:  # skip header
                    cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                    if len(cells) >= 3:
                        # cols: rank, name(link), symbol, weight%, shares, value
                        raw_name = re.sub(r'<[^>]+>', '', cells[1] if len(cells) > 1 else '').strip()
                        raw_sym  = re.sub(r'<[^>]+>', '', cells[2] if len(cells) > 2 else '').strip()
                        raw_pct  = re.sub(r'<[^>]+>', '', cells[3] if len(cells) > 3 else '').strip()
                        raw_pct  = raw_pct.replace('%','').replace(',','').strip()
                        try:
                            pct = float(raw_pct)
                        except Exception:
                            continue
                        if (raw_sym or raw_name) and pct > 0:
                            holdings.append({
                                "symbol": raw_sym,
                                "holdingName": raw_name,
                                "holdingPercent": {"raw": pct/100, "fmt": f"{pct:.2f}%"},
                            })
            if holdings:
                print(f"[ETF] stockanalysis HTML: {len(holdings)} for {sym}")
                return holdings
        except Exception as e:
            print(f"[ETF] stockanalysis HTML failed for {sym}: {e}")

        # Fall back to SEC N-PORT filing
        try:
            cik = self._get_cik(sym)
            if not cik:
                print(f"[ETF N-PORT] No CIK for {sym}")
                return []
            cik_str = str(int(cik))  # strip leading zeros for URL path

            sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            req = urllib.request.Request(sub_url, headers={"User-Agent": "personal-dashboard contact@example.com"})
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                subs = json.loads(r.read())

            filings = subs.get("filings", {}).get("recent", {})
            forms   = filings.get("form", [])
            accnums = filings.get("accessionNumber", [])

            nport_idx = next((i for i, f in enumerate(forms) if f in ("N-PORT-P", "N-PORT")), None)
            if nport_idx is None:
                print(f"[ETF N-PORT] No N-PORT filing for {sym}")
                return []

            accnum     = accnums[nport_idx]
            accnum_nd  = accnum.replace("-", "")
            idx_url    = f"https://www.sec.gov/Archives/edgar/data/{cik_str}/{accnum_nd}/{accnum}-index.json"
            req = urllib.request.Request(idx_url, headers={"User-Agent": "personal-dashboard contact@example.com"})
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                idx = json.loads(r.read())

            xml_file = next(
                (d["name"] for d in idx.get("directory", {}).get("item", [])
                 if d.get("name","").endswith(".xml")), None
            )
            if not xml_file:
                return []

            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_str}/{accnum_nd}/{xml_file}"
            req = urllib.request.Request(xml_url, headers={"User-Agent": "personal-dashboard contact@example.com"})
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=15) as r:
                xml = r.read().decode("utf-8", errors="replace")

            inv_blocks = re.findall(r'<invstOrSec>(.*?)</invstOrSec>', xml, re.DOTALL)

            def xt(block, tag):
                m = re.search(rf'<{tag}>(.*?)</{tag}>', block)
                return m.group(1).strip() if m else ""

            total_m = re.search(r'<netAssets>([\d.]+)</netAssets>', xml)
            total_assets = float(total_m.group(1)) if total_m else 0

            raw = []
            for blk in inv_blocks:
                name  = xt(blk, "name")
                ticker = xt(blk, "ticker")
                pct_s  = xt(blk, "pctVal")
                val_s  = xt(blk, "valUSD") or xt(blk, "val")
                try:
                    pct = float(pct_s) if pct_s else 0
                    val = float(val_s) if val_s else 0
                except Exception:
                    pct, val = 0, 0
                if name and (pct > 0 or val > 0):
                    raw.append({"symbol": ticker, "holdingName": name, "pct": pct, "val": val})

            raw.sort(key=lambda x: x["val"] if x["val"] else x["pct"], reverse=True)
            for h in raw[:25]:
                p = h["pct"]
                holdings.append({
                    "symbol": h["symbol"],
                    "holdingName": h["holdingName"],
                    "holdingPercent": {"raw": p/100, "fmt": f"{p:.2f}%"},
                })
            print(f"[ETF N-PORT] {sym}: {len(holdings)} from SEC filing")
        except Exception as e:
            print(f"[ETF N-PORT] {sym} error: {e}")

        return holdings
    def _stock_financials(self, sym, cat):
        """Fetch stock financials from SEC EDGAR (free, official data)."""
        import concurrent.futures

        # Step 1: get CIK
        cik = self._get_cik(sym)
        if not cik:
            raise ValueError(f"Could not find SEC CIK for {sym}")
        print(f"[Financials STOCK] {sym} CIK={cik}")

        # Step 2: fetch company facts
        facts = self._edgar_facts(cik)

        # Step 3: build financial statements
        # Income statement concepts
        income_concepts_q = [
            ("totalRevenue",              "Revenues",                   "USD"),
            ("costOfRevenue",             "CostOfRevenue",              "USD"),
            ("grossProfit",               "GrossProfit",                "USD"),
            ("researchDevelopment",       "ResearchAndDevelopmentExpense", "USD"),
            ("sellingGeneralAdministrative", "SellingGeneralAndAdministrativeExpense", "USD"),
            ("operatingIncome",           "OperatingIncomeLoss",        "USD"),
            ("netIncome",                 "NetIncomeLoss",              "USD"),
        ]
        # Add EPS - shares separate
        eps_concepts = [
            ("basicEPS",   "EarningsPerShareBasic",   "USD/shares"),
            ("dilutedEPS", "EarningsPerShareDiluted", "USD/shares"),
        ]

        balance_concepts = [
            ("cash",                     "CashAndCashEquivalentsAtCarryingValue", "USD"),
            ("shortTermInvestments",     "ShortTermInvestments",                  "USD"),
            ("netReceivables",           "AccountsReceivableNetCurrent",          "USD"),
            ("totalCurrentAssets",       "AssetsCurrent",                         "USD"),
            ("totalAssets",              "Assets",                                "USD"),
            ("totalCurrentLiabilities",  "LiabilitiesCurrent",                   "USD"),
            ("longTermDebt",             "LongTermDebt",                          "USD"),
            ("totalLiabilities",         "Liabilities",                           "USD"),
            ("totalStockholderEquity",   "StockholdersEquity",                    "USD"),
            ("retainedEarnings",         "RetainedEarningsAccumulatedDeficit",    "USD"),
        ]

        cashflow_concepts = [
            ("netIncome",                            "NetIncomeLoss",                                          "USD"),
            ("depreciation",                         "DepreciationDepletionAndAmortization",                   "USD"),
            ("totalCashFromOperatingActivities",     "NetCashProvidedByUsedInOperatingActivities",             "USD"),
            ("capitalExpenditures",                  "PaymentsToAcquirePropertyPlantAndEquipment",             "USD"),
            ("totalCashflowsFromInvestingActivities","NetCashProvidedByUsedInInvestingActivities",             "USD"),
            ("dividendsPaid",                        "PaymentsOfDividends",                                    "USD"),
            ("totalCashFromFinancingActivities",     "NetCashProvidedByUsedInFinancingActivities",             "USD"),
            ("changeInCash",                         "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect", "USD"),
        ]

        def build(concepts, annual=False, n=4):
            return self._build_stmt_rows(concepts, facts, annual=annual, n=n)

        # Run quarterly and annual builds
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            f_inc_q  = ex.submit(build, income_concepts_q + eps_concepts, False, 5)
            f_inc_a  = ex.submit(build, income_concepts_q + eps_concepts, True,  4)
            f_bal_q  = ex.submit(build, balance_concepts,  False, 5)
            f_bal_a  = ex.submit(build, balance_concepts,  True,  4)
            f_cf_q   = ex.submit(build, cashflow_concepts, False, 5)
            f_cf_a   = ex.submit(build, cashflow_concepts, True,  4)
            inc_q, inc_a = f_inc_q.result(), f_inc_a.result()
            bal_q, bal_a = f_bal_q.result(), f_bal_a.result()
            cf_q,  cf_a  = f_cf_q.result(),  f_cf_a.result()

        # Build EPS chart data from income quarterly
        quarterly_eps = []
        for s in reversed(inc_q):
            actual_eps = s.get("basicEPS") or s.get("dilutedEPS")
            if actual_eps:
                label = self._period_label({"end": s["endDate"]["fmt"]})
                quarterly_eps.append({
                    "date":     label,
                    "actual":   actual_eps,
                    "estimate": None,
                })

        # Build annual EPS — use actual per-share values, NOT net income
        annual_eps = []
        for s in reversed(inc_a):
            actual_eps = s.get("basicEPS") or s.get("dilutedEPS")
            if actual_eps:
                annual_eps.append({
                    "date":     s["endDate"]["fmt"][:4],
                    "actual":   actual_eps,
                    "estimate": None,
                })

        # Revenue/earnings chart data
        def earn_rows(stmts, annual=False):
            rows = []
            for s in reversed(stmts):
                end = s["endDate"]["fmt"]
                rev = s.get("totalRevenue")
                net = s.get("netIncome")
                label = end[:4] if annual else self._period_label({"end": end})
                rows.append({"date": label, "revenue": rev, "earnings": net})
            return rows

        result = {
            "_cat": cat,
            "quoteType": {"quoteType": "EQUITY"},
            "earnings": {
                "earningsChart": {
                    "quarterly": quarterly_eps[-8:],
                    "yearly":    annual_eps[-8:],
                    "currentQuarterEstimate": None,
                },
                "financialsChart": {
                    "quarterly": earn_rows(inc_q),
                    "yearly":    earn_rows(inc_a, annual=True),
                },
            },
            "incomeStatementHistoryQuarterly": {"incomeStatementHistory": inc_q},
            "incomeStatementHistory":          {"incomeStatementHistory": inc_a},
            "balanceSheetHistoryQuarterly":    {"balanceSheetStatements": bal_q},
            "balanceSheetHistory":             {"balanceSheetStatements": bal_a},
            "cashflowStatementHistoryQuarterly": {"cashflowStatements": cf_q},
            "cashflowStatementHistory":          {"cashflowStatements": cf_a},
            "topHoldings": {"holdings": []},
        }
        print(f"[Financials STOCK] {sym}: inc_q={len(inc_q)}, bal_q={len(bal_q)}, cf_q={len(cf_q)}, eps={len(quarterly_eps)}")
        return result


    def send_error_json(self, obj):
        body = json.dumps(obj).encode()
        self.wfile.write(body)

    def handle_asset_news(self):
        """Fetch recent news for an asset symbol via Yahoo Finance RSS."""
        params = parse_qs(urlparse(self.path).query)
        sym = params.get('sym', [''])[0].strip()
        if not sym:
            self.send_response(400); self.end_headers(); return
        try:
            from urllib.parse import quote as uq
            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={uq(sym)}&region=US&lang=en-US"
            req = urllib.request.Request(rss_url, headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as r:
                raw = r.read().decode('utf-8', errors='replace')
            items = []
            for item in re.findall(r'<item>(.*?)</item>', raw, re.S):
                def tag(t, it=item):
                    m = re.search(fr'<{t}[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{t}>', it, re.S)
                    return m.group(1).strip() if m else ''
                title = tag('title')
                link_m = re.search(r'<link/>\s*(https?://[^\s<]+)', item, re.S)
                link = link_m.group(1).strip() if link_m else tag('link')
                pub = tag('pubDate')
                desc = re.sub(r'<[^>]+>', '', tag('description'))[:200]
                if title:
                    items.append({'title': title, 'link': link, 'pub': pub, 'desc': desc})
                if len(items) >= 12: break
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({'items': items, 'sym': sym}).encode())
        except Exception as e:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({'items': [], 'error': str(e)}).encode())

    def handle_primal_stats(self):
        """
        Fetch follower/following counts using Primal's WebSocket cache API.
        This is the same method Primal and Damus use internally.
        Falls back to nostr.band REST API if WebSocket fails.
        """
        params = parse_qs(urlparse(self.path).query)
        pubkey = params.get('pubkey', [''])[0].strip()
        if not pubkey:
            self.send_response(400); self.end_headers(); return
        import json as _json
        import socket
        import base64
        import hashlib
        import struct
        import threading

        def ws_primal_stats(pubkey, timeout=8):
            """
            Connect to Primal's cache WebSocket and request user_profile_stats.
            Tries cache0, cache1, cache2 in parallel — returns first that works.
            """
            import os as _os
            import threading as _threading

            def try_host(host):
                # Primal moved the cache WS endpoint from /v1 to /cache — but individual
                # cache hosts have been seen answering on only one of the two paths, so
                # try /cache first and fall back to /v1 (and vice versa) before giving
                # up on the host. Without the fallback, one wrong-path guess silently
                # zeroed out the most accurate follower-count source.
                primary = '/cache' if host.endswith('primal.net') else '/v1'
                secondary = '/v1' if primary == '/cache' else '/cache'
                r = _try_host_path(host, primary)
                if r.get('followers_count') or r.get('follows_count'):
                    return r
                r2 = _try_host_path(host, secondary)
                for k in ('followers_count', 'follows_count'):
                    if (r2.get(k) or 0) > (r.get(k) or 0):
                        r[k] = r2[k]
                return r

            def _try_host_path(host, path):
                port = 443
                key = base64.b64encode(_os.urandom(16)).decode()
                handshake = (
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    f"Upgrade: websocket\r\n"
                    f"Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {key}\r\n"
                    f"Sec-WebSocket-Version: 13\r\n"
                    f"Origin: https://primal.net\r\n"
                    f"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36\r\n\r\n"
                )
                try:
                    raw_sock = socket.create_connection((host, port), timeout=timeout)
                    sock = ssl_ctx.wrap_socket(raw_sock, server_hostname=host)
                    sock.sendall(handshake.encode())
                    resp = b''
                    while b'\r\n\r\n' not in resp:
                        chunk = sock.recv(1024)
                        if not chunk: break
                        resp += chunk
                    if b'101' not in resp:
                        sock.close()
                        print(f"[Primal WS] {host}: upgrade failed: {resp[:100]}")
                        return {}

                    def ws_send(sock, data):
                        payload = data.encode('utf-8')
                        mask_key = b'\x00\x00\x00\x00'
                        length = len(payload)
                        if length < 126:
                            header = bytes([0x81, 0x80 | length]) + mask_key
                        elif length < 65536:
                            header = bytes([0x81, 0xFE]) + struct.pack('>H', length) + mask_key
                        else:
                            header = bytes([0x81, 0xFF]) + struct.pack('>Q', length) + mask_key
                        masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
                        sock.sendall(header + masked)

                    def ws_recv_frame(sock):
                        hdr = b''
                        while len(hdr) < 2: hdr += sock.recv(2 - len(hdr))
                        opcode = hdr[0] & 0x0F
                        masked = (hdr[1] & 0x80) != 0
                        length = hdr[1] & 0x7F
                        if length == 126:
                            ext = b''
                            while len(ext) < 2: ext += sock.recv(2 - len(ext))
                            length = struct.unpack('>H', ext)[0]
                        elif length == 127:
                            ext = b''
                            while len(ext) < 8: ext += sock.recv(8 - len(ext))
                            length = struct.unpack('>Q', ext)[0]
                        if masked:
                            mk = b''
                            while len(mk) < 4: mk += sock.recv(4 - len(mk))
                        data = b''
                        while len(data) < length:
                            chunk = sock.recv(min(4096, length - len(data)))
                            if not chunk: break
                            data += chunk
                        if masked:
                            data = bytes(b ^ mk[i % 4] for i, b in enumerate(data))
                        return opcode, data.decode('utf-8', errors='replace')

                    req_id = 'stats-' + pubkey[:8]
                    # Send user_follower_count FIRST — this gives the true count directly.
                    # user_profile_stats is a secondary source.
                    msg = _json.dumps(["REQ", req_id, {"cache": ["user_follower_count", {"pubkey": pubkey}]}])
                    ws_send(sock, msg)
                    req_id2 = req_id + 'ps'
                    msg2 = _json.dumps(["REQ", req_id2, {"cache": ["user_profile_stats", {"pubkey": pubkey}]}])
                    ws_send(sock, msg2)

                    stats = {}
                    sock.settimeout(timeout)
                    try:
                        for _ in range(50):
                            opcode, text = ws_recv_frame(sock)
                            if opcode == 8: break
                            if opcode != 1: continue
                            try:
                                m = _json.loads(text)
                            except:
                                continue
                            if not isinstance(m, list) or len(m) < 2:
                                continue
                            if m[0] == 'EOSE':
                                # Track which REQ subscriptions have completed
                                eose_set = stats.setdefault('_eose', set())
                                if len(m) > 1: eose_set.add(m[1])
                                both_eose = req_id in eose_set and req_id2 in eose_set
                                if both_eose or (stats.get('followers_count') and stats.get('follows_count')):
                                    break
                                continue
                            if m[0] == 'EVENT' and len(m) >= 3:
                                ev = m[2]
                                if not isinstance(ev, dict): continue
                                if ev.get('kind') == 10000133:
                                    try:
                                        content_raw = ev.get('content', '{}')
                                        content = _json.loads(content_raw) if isinstance(content_raw, str) else content_raw
                                        # user_follower_count returns {"count": N}
                                        # user_profile_stats returns {"followers_count": N, "follows_count": N, ...}
                                        cnt = content.get('count')
                                        if cnt and int(cnt) > 0:
                                            stats['followers_count'] = int(cnt)
                                            print(f"[Primal WS] {host} user_follower_count for {pubkey[:8]}: {cnt}")
                                        fc = content.get('followers_count') or content.get('follower_count') or 0
                                        fwc = content.get('follows_count') or content.get('following_count') or 0
                                        if fc and int(fc) > stats.get('followers_count', 0): stats['followers_count'] = int(fc)
                                        if fwc: stats['follows_count'] = int(fwc)
                                        print(f"[Primal WS] {host} got kind:10000133 for {pubkey[:8]}: content={_json.dumps(content)[:200]}")
                                        if stats.get('followers_count') and stats.get('follows_count'):
                                            break
                                    except Exception as pe:
                                        print(f"[Primal WS] parse error: {pe}")
                    except socket.timeout:
                        print(f"[Primal WS] {host}: timeout after {timeout}s, stats so far: {stats}")

                    stats.pop('_eose', None)  # remove internal tracking key before returning
                    print(f"[Primal WS] {host} final stats for {pubkey[:8]}: {stats}")
                    try: sock.close()
                    except: pass
                    return stats
                except Exception as e:
                    print(f"[Primal WS] {host}: connection error: {e}")
                    return {}

            # Try all cache servers in parallel, return first non-empty result
            results = [{}, {}, {}]
            hosts = ['cache0.primal.net', 'cache1.primal.net', 'cache2.primal.net']
            threads = [_threading.Thread(target=lambda i=i,h=h: results.__setitem__(i, try_host(h)), daemon=True)
                       for i,h in enumerate(hosts)]
            for t in threads: t.start()
            # Wait up to timeout for first good result
            deadline = _time.time() + timeout
            while _time.time() < deadline:
                for r in results:
                    if r.get('followers_count'):
                        return r
                _time.sleep(0.1)
            # Return best result even if incomplete
            return max(results, key=lambda r: r.get('followers_count') or 0)

        def nostr_band_stats(pubkey):
            """Fetch from nostr.band REST API — logs all fields to diagnose follower count cap."""
            try:
                req = urllib.request.Request(
                    f"https://api.nostr.band/v0/stats/profile/{pubkey}",
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
                )
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=7) as r:
                    data = _json.loads(r.read().decode('utf-8', errors='replace'))
                nb = data.get('stats', {}).get(pubkey, {})
                if not nb:
                    nb = data.get('stats', {})
                # Log EVERYTHING so we can see what nostr.band actually returns
                print(f"[nostr.band] raw stats for {pubkey[:8]}: {_json.dumps(nb)[:500]}")
                result = {}
                for fld in ('followers_pubkey_count', 'followers_count', 'follower_count',
                            'pub_followers_pubkey_count', 'pub_following_count', 'new_followers_count'):
                    v = nb.get(fld)
                    if v is not None and int(v) > 0:
                        result['followers_count'] = int(v)
                        print(f"[nostr.band] followers via '{fld}' = {v} for {pubkey[:8]}")
                        break
                for fld in ('pub_following_pubkey_count', 'follows_count', 'following_count',
                            'following_pubkey_count', 'follows_pubkey_count'):
                    v = nb.get(fld)
                    if v is not None and int(v) > 0:
                        result['follows_count'] = int(v)
                        break
                return result
            except Exception as e:
                print(f"[nostr.band] failed for {pubkey[:8]}: {e}")
                return {}

        def primal_rest_stats(pubkey):
            """
            Try Primal's REST API endpoint for profile stats.
            This is the simplest approach — no WebSocket needed.
            """
            try:
                # Primal's undocumented but stable REST endpoint
                body = _json.dumps(["user_profile_stats", {"pubkey": pubkey}]).encode()
                req = urllib.request.Request(
                    "https://api.primal.net/v1",
                    data=body,
                    headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
                )
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as r:
                    raw = r.read().decode('utf-8', errors='replace')
                data = _json.loads(raw)
                print(f"[Primal REST v1] raw response for {pubkey[:8]}: {raw[:300]}")
                # Response is array of Nostr events; find kind 10000133
                result = {}
                items = data if isinstance(data, list) else []
                for item in items:
                    if isinstance(item, dict) and item.get('kind') == 10000133:
                        try:
                            c = _json.loads(item.get('content', '{}'))
                            print(f"[Primal REST v1] kind:10000133 content keys: {list(c.keys())}")
                            # Try all possible field names
                            for fld in ('followers_count','follower_count','followersCount','followers'):
                                if c.get(fld):
                                    result['followers_count'] = int(c[fld])
                                    break
                            for fld in ('follows_count','following_count','followingCount','following'):
                                if c.get(fld):
                                    result['follows_count'] = int(c[fld])
                                    break
                        except Exception as pe:
                            print(f"[Primal REST v1] parse error: {pe}")
                if result:
                    print(f"[Primal REST v1] stats for {pubkey[:8]}: {result}")
                else:
                    print(f"[Primal REST v1] no kind:10000133 found for {pubkey[:8]}, items: {len(items)}, kinds: {[i.get('kind') for i in items[:5] if isinstance(i,dict)]}")
                return result
            except Exception as e:
                print(f"[Primal REST v1] failed for {pubkey[:8]}: {e}")
                return {}

        def primal_rest_stats_v2(pubkey):
            """
            Try Primal's user_follower_count endpoint.
            Also try their newer API format used by primal.net web app.
            """
            result = {}
            
            # Attempt 1: user_follower_count via api.primal.net/v1
            try:
                body = _json.dumps(["user_follower_count", {"pubkey": pubkey}]).encode()
                req = urllib.request.Request(
                    "https://api.primal.net/v1",
                    data=body,
                    headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
                )
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as r:
                    raw = r.read().decode('utf-8', errors='replace')
                print(f"[Primal REST v2] user_follower_count for {pubkey[:8]}: {raw[:200]}")
                data = _json.loads(raw)
                items = data if isinstance(data, list) else []
                for item in items:
                    if isinstance(item, dict):
                        c = item.get('content')
                        if isinstance(c, str):
                            try: c = _json.loads(c)
                            except: pass
                        if isinstance(c, dict):
                            for fld in ('count','followers_count','follower_count'):
                                if c.get(fld):
                                    result['followers_count'] = int(c[fld])
                                    break
                        elif isinstance(c, (int, float)) and c:
                            result['followers_count'] = int(c)
                        # Also check top-level content that IS the count
                        if not result.get('followers_count'):
                            cnt = item.get('cnt') or item.get('count')
                            if cnt: result['followers_count'] = int(cnt)
            except Exception as e:
                print(f"[Primal REST v2] user_follower_count failed for {pubkey[:8]}: {e}")

            if result:
                print(f"[Primal REST v2] stats for {pubkey[:8]}: {result}")
            return result

        try:
            stats = {}

            # Run all four sources in parallel — nostr.band REST, Primal REST v1, Primal REST v2 (alt), Primal WebSocket
            import threading as _t
            nb_result = {}
            pr_result = {}
            pr2_result = {}
            ws_result = {}

            def _nb():
                try: nb_result.update(nostr_band_stats(pubkey))
                except: pass

            def _pr():
                try: pr_result.update(primal_rest_stats(pubkey))
                except: pass

            def _pr2():
                try: pr2_result.update(primal_rest_stats_v2(pubkey))
                except: pass

            def _ws():
                try: ws_result.update(ws_primal_stats(pubkey, timeout=9))
                except: pass

            # NIP-45 COUNT via relay.nostr.band WebSocket — exact count, no cap
            nip45_result = {}
            def _nip45():
                try:
                    import os as _os2
                    nip45_key = base64.b64encode(_os2.urandom(16)).decode()
                    nip45_handshake = (
                        f"GET / HTTP/1.1\r\n"
                        f"Host: relay.nostr.band\r\n"
                        f"Upgrade: websocket\r\n"
                        f"Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Key: {nip45_key}\r\n"
                        f"Sec-WebSocket-Version: 13\r\n"
                        f"Origin: https://nostr.band\r\n"
                        f"User-Agent: Mozilla/5.0\r\n\r\n"
                    )
                    raw = socket.create_connection(('relay.nostr.band', 443), timeout=8)
                    sock2 = ssl_ctx.wrap_socket(raw, server_hostname='relay.nostr.band')
                    sock2.sendall(nip45_handshake.encode())
                    resp2 = b''
                    while b'\r\n\r\n' not in resp2:
                        c2 = sock2.recv(1024)
                        if not c2: break
                        resp2 += c2
                    if b'101' not in resp2:
                        sock2.close(); return
                    sub_id = 'cnt-' + pubkey[:8]
                    count_msg = _json.dumps(['COUNT', sub_id, {'kinds': [3], '#p': [pubkey]}])
                    payload = count_msg.encode('utf-8')
                    mk = b'\x00\x00\x00\x00'
                    ln = len(payload)
                    if ln < 126: hdr2 = bytes([0x81, 0x80|ln]) + mk
                    elif ln < 65536: hdr2 = bytes([0x81, 0xFE]) + struct.pack('>H', ln) + mk
                    else: hdr2 = bytes([0x81, 0xFF]) + struct.pack('>Q', ln) + mk
                    sock2.sendall(hdr2 + bytes(b ^ mk[i%4] for i,b in enumerate(payload)))
                    sock2.settimeout(7)
                    for _ in range(10):
                        hdr3 = b''
                        while len(hdr3) < 2: hdr3 += sock2.recv(2-len(hdr3))
                        opc = hdr3[0] & 0x0F
                        ln2 = hdr3[1] & 0x7F
                        if ln2 == 126:
                            ext2 = b''
                            while len(ext2) < 2: ext2 += sock2.recv(2-len(ext2))
                            ln2 = struct.unpack('>H', ext2)[0]
                        elif ln2 == 127:
                            ext2 = b''
                            while len(ext2) < 8: ext2 += sock2.recv(8-len(ext2))
                            ln2 = struct.unpack('>Q', ext2)[0]
                        dat2 = b''
                        while len(dat2) < ln2:
                            c3 = sock2.recv(min(4096, ln2-len(dat2)))
                            if not c3: break
                            dat2 += c3
                        if opc != 1: break
                        m2 = _json.loads(dat2.decode('utf-8', errors='replace'))
                        if isinstance(m2, list) and m2[0] == 'COUNT' and len(m2) >= 3:
                            cnt2 = m2[2].get('count') if isinstance(m2[2], dict) else None
                            if cnt2 is not None:
                                nip45_result['followers_count'] = int(cnt2)
                                print(f"[NIP-45 COUNT] relay.nostr.band: {pubkey[:8]} has {cnt2} followers")
                            break
                        elif isinstance(m2, list) and m2[0] in ('EOSE', 'NOTICE'):
                            break
                    sock2.close()
                except Exception as e:
                    print(f"[NIP-45 COUNT] failed for {pubkey[:8]}: {e}")

            threads = [_t.Thread(target=f, daemon=True) for f in (_nb, _pr, _pr2, _ws, _nip45)]
            for t in threads: t.start()
            # nostr.band and Primal REST are fast (HTTP); WS takes longer
            threads[0].join(timeout=8)
            threads[1].join(timeout=8)
            threads[2].join(timeout=8)
            threads[3].join(timeout=10)
            threads[4].join(timeout=8)

            # Take the HIGHEST follower count from any source.
            # nip45_result is DELIBERATELY excluded from the merge: relay.nostr.band's
            # NIP-45 COUNT response is capped (500), which corrupts a highest-wins merge
            # — the client already removed it for the same reason. It's kept above purely
            # as a diagnostic log line.
            def _best_count(key):
                best = 0
                for src in (nb_result, pr_result, pr2_result, ws_result):
                    try:
                        v = int(src.get(key) or 0)
                        if v > best:
                            best = v
                    except: pass
                return best or None

            fc = _best_count('followers_count')
            fwc = _best_count('follows_count')
            if fc: stats['followers_count'] = fc
            if fwc: stats['follows_count'] = fwc

            print(f"[primal-stats] {pubkey[:8]}: followers={stats.get('followers_count')} "
                  f"following={stats.get('follows_count')} "
                  f"(nb={nb_result.get('followers_count')} "
                  f"pr={pr_result.get('followers_count')} "
                  f"pr2={pr2_result.get('followers_count')} "
                  f"ws={ws_result.get('followers_count')} "
                  f"nip45={nip45_result.get('followers_count')})")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(_json.dumps(stats).encode())
        except Exception as e:
            print(f"[primal-stats] error: {e}")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(_json.dumps({}).encode())

    def handle_primal_notes(self):
        """
        Fetch user notes (posts + replies) via Primal's cache server.
        Returns all kind:1 events for the given pubkey as a JSON array.
        This is used specifically to find replies for prolific posters like TFTC
        where relay-based queries with limit:N miss old replies.
        """
        params = parse_qs(urlparse(self.path).query)
        pubkey = params.get('pubkey', [''])[0].strip()
        notes_type = params.get('type', ['replies'])[0]  # 'replies' or 'posts'
        if not pubkey:
            self.send_response(400); self.end_headers(); return
        import json as _json
        import socket, base64, struct, os as _os, threading as _threading

        def fetch_primal_notes(pubkey, notes_type='replies', timeout=10):
            """Fetch user notes from Primal cache via WebSocket."""
            # For posts: use Primal cache servers first (support "feed" cache type for profile posts)
            # then standard NIP-01 relays as fallbacks
            # For replies: use Primal cache servers (support user_replies cache type)
            if notes_type == 'posts':
                hosts = ['cache0.primal.net', 'relay.damus.io', 'nos.lol']
                relay_path = '/v1'  # cache0 uses /v1; relay.damus.io and nos.lol use /
            else:
                hosts = ['cache0.primal.net', 'cache1.primal.net', 'cache2.primal.net']
                relay_path = '/v1'
            all_events = []
            events_lock = _threading.Lock()

            def try_host(host):
                # Primal cache servers use /v1 path; standard relays use /
                path = '/cache' if host.endswith('primal.net') else '/'
                port = 443
                key = base64.b64encode(_os.urandom(16)).decode()
                handshake = (
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    f"Upgrade: websocket\r\n"
                    f"Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {key}\r\n"
                    f"Sec-WebSocket-Version: 13\r\n"
                    f"Origin: https://primal.net\r\n"
                    f"User-Agent: Mozilla/5.0\r\n\r\n"
                )
                try:
                    raw_sock = socket.create_connection((host, port), timeout=timeout)
                    sock = ssl_ctx.wrap_socket(raw_sock, server_hostname=host)
                    sock.sendall(handshake.encode())
                    resp = b''
                    while b'\r\n\r\n' not in resp:
                        chunk = sock.recv(1024)
                        if not chunk: break
                        resp += chunk
                    if b'101' not in resp:
                        sock.close(); return []

                    def ws_send(s, data):
                        payload = data.encode('utf-8')
                        mk = b'\x00\x00\x00\x00'
                        ln = len(payload)
                        if ln < 126: hdr = bytes([0x81, 0x80|ln]) + mk
                        elif ln < 65536: hdr = bytes([0x81, 0xFE]) + struct.pack('>H', ln) + mk
                        else: hdr = bytes([0x81, 0xFF]) + struct.pack('>Q', ln) + mk
                        s.sendall(hdr + bytes(b ^ mk[i%4] for i,b in enumerate(payload)))

                    def ws_recv(s):
                        hdr = b''
                        while len(hdr) < 2: hdr += s.recv(2-len(hdr))
                        opcode = hdr[0] & 0x0F
                        masked = (hdr[1] & 0x80) != 0
                        ln = hdr[1] & 0x7F
                        if ln == 126:
                            ext = b''
                            while len(ext) < 2: ext += s.recv(2-len(ext))
                            ln = struct.unpack('>H', ext)[0]
                        elif ln == 127:
                            ext = b''
                            while len(ext) < 8: ext += s.recv(8-len(ext))
                            ln = struct.unpack('>Q', ext)[0]
                        if masked:
                            mk2 = b''
                            while len(mk2) < 4: mk2 += s.recv(4-len(mk2))
                        data = b''
                        while len(data) < ln:
                            chunk = s.recv(min(4096, ln-len(data)))
                            if not chunk: break
                            data += chunk
                        if masked: data = bytes(b ^ mk2[i%4] for i,b in enumerate(data))
                        return opcode, data.decode('utf-8', errors='replace')

                    req_id = 'notes-' + pubkey[:8]
                    if notes_type == 'replies':
                        # Primal cache: user_replies returns replies made BY this user
                        msg = _json.dumps(["REQ", req_id, {"cache": ["user_replies", {"pubkey": pubkey, "limit": 200}]}])
                    elif host in ('cache0.primal.net', 'cache1.primal.net', 'cache2.primal.net'):
                        # Primal cache "feed" with pubkey = posts authored by this specific user
                        # This is exactly what primal.net uses on profile pages
                        msg = _json.dumps(["REQ", req_id, {"cache": ["feed", {"pubkey": pubkey, "limit": 200}]}])
                    else:
                        # Standard NIP-01 REQ for normal relays
                        msg = _json.dumps(["REQ", req_id, {"kinds": [1], "authors": [pubkey], "limit": 200}])
                    ws_send(sock, msg)

                    local_events = []
                    sock.settimeout(timeout)
                    try:
                        for _ in range(6000):  # enough for limit:5000 + metadata events
                            opcode, text = ws_recv(sock)
                            if opcode == 8: break
                            if opcode != 1: continue
                            try:
                                m = _json.loads(text)
                                if not isinstance(m, list) or len(m) < 2: continue
                                if m[0] == 'EOSE': break
                                if m[0] == 'EVENT' and len(m) >= 3:
                                    ev = m[2]
                                    if isinstance(ev, dict) and ev.get('kind') == 1 and ev.get('pubkey') == pubkey:
                                        local_events.append(ev)
                            except: pass
                    except socket.timeout:
                        print(f"[Primal notes] {host}: timeout, got {len(local_events)} events")
                    finally:
                        try: sock.close()
                        except: pass
                    return local_events
                except Exception as e:
                    print(f"[Primal notes] {host}: error: {e}")
                    return []

            results = [[], [], []]
            threads = [_threading.Thread(target=lambda i=i, h=h: results.__setitem__(i, try_host(h)), daemon=True)
                       for i, h in enumerate(hosts)]
            for t in threads: t.start()
            deadline = import_time() + timeout
            for t in threads:
                remaining = max(0.1, deadline - import_time())
                t.join(timeout=remaining)
            # Return the largest result set
            return max(results, key=len)

        def import_time():
            import time; return time.time()

        try:
            events = fetch_primal_notes(pubkey, notes_type)
            print(f"[primal-notes] {pubkey[:8]} type={notes_type}: got {len(events)} events")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(_json.dumps(events).encode())
        except Exception as e:
            print(f"[primal-notes] error: {e}")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(_json.dumps([]).encode())

    def address_string(self):
        # Override to skip reverse DNS lookup — prevents 15s hangs on macOS
        return self.client_address[0]

    def log_message(self, format, *args):
        print(f"[Monitor Proxy] {self.client_address[0]} - {format % args}")

class ThreadingHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    """Handle each request in its own thread — prevents Yahoo/news fetches from blocking each other."""
    daemon_threads = True

if __name__ == "__main__":
    print(f"")
    print(f"  🖥️  Monitor The Situation — Proxy Server")
    print(f"  ─────────────────────────────────────────")
    print(f"  Dashboard → http://127.0.0.1:{PORT}/")
    print(f"  Serving files from: {SERVE_DIR}")
    print(f"  Supports: Yahoo Finance · Multi-category News · Miner API · BTC Hashrate")
    print(f"  Financial data: Financial Modeling Prep (set FMP_API_KEY env var)")
    if ProxyHandler._CARTO_API_KEY:
        _carto_status = f"keyed, via {ProxyHandler._CARTO_API_KEY_SOURCE}"
    else:
        _carto_status = ("no key set — tiles show a watermark. Get one free at "
                          "https://carto.com/basemaps/apikey, then either set CARTO_API_KEY "
                          "or save it to a carto_api_key.txt file next to proxy.py")
    print(f"  Radar basemap: CARTO ({_carto_status})")
    print(f"  Press Ctrl+C to stop")
    print(f"")
    with ThreadingHTTPServer(("127.0.0.1", PORT), ProxyHandler) as httpd:
        httpd.serve_forever()
