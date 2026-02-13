"""
app.py — Weather Station Poller Backend
Polls NWS api.weather.gov every 5 min for all settlement airports.
Caches locally. Parses T-groups on synoptic obs.
Tracks wind history for kill-switch detection.
Serves dashboard + JSON API.

Deploy on Render: gunicorn -w 1 -k gevent (single worker, gevent,
so the background poller thread stays alive).
"""

# ── CRITICAL: monkey-patch BEFORE anything else imports ──────────────────────
from gevent import monkey
monkey.patch_all()

import requests, json, threading, time, math, os
from flask import Flask, jsonify, send_from_directory
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

app = Flask(__name__, static_folder="static")

# ─── STATION CONFIG ──────────────────────────────────────────────────────────
# Settlement airports + upstream stations used for kill-switch detection.
# KILG = Wilmington DE (upstream for PHL back-door fronts)
# KPAE = Paine Field / Everett (upstream for SEA convergence zone detection)

STATIONS = {
    "KLAX": {"city": "Los Angeles",   "tz": "America/Los_Angeles", "upstream": None,   "lat": 33.94, "lon": -118.41, "wfo": "LOX"},
    "KLAS": {"city": "Las Vegas",     "tz": "America/Los_Angeles", "upstream": None,   "lat": 36.08, "lon": -115.15, "wfo": "VEF"},
    "KPHX": {"city": "Phoenix",       "tz": "America/Phoenix",     "upstream": None,   "lat": 33.43, "lon": -112.01, "wfo": "PSR"},
    "KSFO": {"city": "San Francisco", "tz": "America/Los_Angeles", "upstream": None,   "lat": 37.62, "lon": -122.37, "wfo": "MTR"},
    "KSEA": {"city": "Seattle",       "tz": "America/Los_Angeles", "upstream": "KPAE", "lat": 47.45, "lon": -122.30, "wfo": "SEW"},
    "KAUS": {"city": "Austin",        "tz": "America/Chicago",     "upstream": None,   "lat": 30.19, "lon": -97.66,  "wfo": "EWX"},
    "KDFW": {"city": "Dallas",        "tz": "America/Chicago",     "upstream": None,   "lat": 32.89, "lon": -97.04,  "wfo": "FWD"},
    "KHOU": {"city": "Houston",       "tz": "America/Chicago",     "upstream": None,   "lat": 29.64, "lon": -95.27,  "wfo": "HGX"},
    "KSAT": {"city": "San Antonio",   "tz": "America/Chicago",     "upstream": None,   "lat": 29.53, "lon": -98.46,  "wfo": "EWX"},
    "KOKC": {"city": "Oklahoma City", "tz": "America/Chicago",     "upstream": None,   "lat": 35.39, "lon": -97.60,  "wfo": "OUN"},
    "KDEN": {"city": "Denver",        "tz": "America/Denver",      "upstream": None,   "lat": 39.86, "lon": -104.67, "wfo": "BOU"},
    "KBOS": {"city": "Boston",        "tz": "America/New_York",    "upstream": None,   "lat": 42.36, "lon": -71.00,  "wfo": "BOX"},
    "KNYC": {"city": "New York",      "tz": "America/New_York",    "upstream": None,   "lat": 40.78, "lon": -73.97,  "wfo": "OKX"},
    "KPHL": {"city": "Philadelphia",  "tz": "America/New_York",    "upstream": "KILG", "lat": 39.87, "lon": -75.24,  "wfo": "PHI"},
    "KDCA": {"city": "Washington DC", "tz": "America/New_York",    "upstream": None,   "lat": 38.85, "lon": -77.04,  "wfo": "LWX"},
    "KMDW": {"city": "Chicago",       "tz": "America/Chicago",     "upstream": None,   "lat": 41.78, "lon": -87.75,  "wfo": "LOT"},
    "KMSP": {"city": "Minneapolis",   "tz": "America/Chicago",     "upstream": None,   "lat": 44.88, "lon": -93.22,  "wfo": "MPX"},
    "KMSY": {"city": "New Orleans",   "tz": "America/Chicago",     "upstream": None,   "lat": 29.99, "lon": -90.25,  "wfo": "LIX"},
    "KATL": {"city": "Atlanta",       "tz": "America/New_York",    "upstream": None,   "lat": 33.64, "lon": -84.43,  "wfo": "FFC"},
    "KMIA": {"city": "Miami",         "tz": "America/New_York",    "upstream": None,   "lat": 25.79, "lon": -80.29,  "wfo": "MFL"},
}

# Canonical card order (matches notebook stations dict) — frontend iterates this
CARD_ORDER = ["KLAX","KLAS","KPHX","KSFO","KSEA","KAUS","KDFW","KHOU","KSAT","KOKC","KDEN","KBOS","KNYC","KPHL","KDCA","KMDW","KMSP","KMSY","KATL","KMIA"]

# ─── KALSHI TICKER PREFIXES ──────────────────────────────────────────────────
KALSHI_PREFIXES = {
    "KLAX": "KXHIGHLAX",  "KLAS": "KXHIGHTLV",  "KPHX": "KXHIGHTPHX",
    "KSFO": "KXHIGHTSFO", "KSEA": "KXHIGHTSEA", "KAUS": "KXHIGHAUS",
    "KDEN": "KXHIGHDEN",  "KBOS": "KXHIGHTBOS", "KNYC": "KXHIGHNY",
    "KPHL": "KXHIGHPHIL", "KDCA": "KXHIGHTDC",  "KMDW": "KXHIGHCHI",
    "KMSP": "KXHIGHTMIN", "KMSY": "KXHIGHTNOLA","KATL": "KXHIGHTATL",
    "KMIA": "KXHIGHMIA",
}

# ─── ON-DEMAND CACHES (TTL-based, avoid hammering external APIs) ─────────────
_scan_cache   = {"data": None, "ts": 0}   # unified scan  — 15 min TTL
_afd_cache    = {"data": {},   "ts": 0}   # AFD text      — 30 min TTL
_kalshi_cache = {"data": None, "ts": 0}   # Kalshi prices — 5 min TTL

# Upstream-only stations (not displayed as cards, but polled for kill-switch logic)
UPSTREAM_ONLY = {"KILG", "KPAE"}

# All stations to poll (settlement + upstream)
ALL_POLL = set(STATIONS.keys()) | UPSTREAM_ONLY

NWS_HEADERS = {"User-Agent": "(weathertrading.app, ops@weathertrading.app)"}
POLL_INTERVAL   = 300   # 5 minutes
WIND_HISTORY_MAX = 12   # 12 × 5 min = 1 hour of wind history

# ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
cache = {}
for sid in ALL_POLL:
    cache[sid] = {
        "latest":       None,   # most recent obs (any type)
        "last_tgroup":  None,   # most recent synoptic with T-group
        "wind_history": [],     # [{dir, mph, ts}, ...]
        "daily_max_f":  None,   # highest T-group temp today
        "daily_max_ts": None,
        "last_polled":  None,
    }

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def c_to_f(c):
    return round(c * 1.8 + 32, 1) if c is not None else None

def parse_tgroup(raw_metar: str):
    """
    T-group format: T[s1][ttt][s2][ddd]
      s = 0 positive, 1 negative
      ttt/ddd = tenths of °C (3 digits, zero-padded)
    Returns (temp_c, dew_c) or (None, None).
    """
    if not raw_metar:
        return None, None
    for token in raw_metar.split():
        if token.startswith("T") and len(token) == 9:
            try:
                t_sign = -1 if token[1] == "1" else 1
                t_val  = int(token[2:5]) / 10.0 * t_sign
                d_sign = -1 if token[5] == "1" else 1
                d_val  = int(token[6:9]) / 10.0 * d_sign
                return t_val, d_val
            except (ValueError, IndexError):
                pass
    return None, None

def parse_obs(props: dict) -> dict:
    """Normalize one NWS observation into our schema."""
    ts_str = props.get("timestamp", "")

    raw = props.get("rawMessage", "") or ""
    tg_temp_c, tg_dew_c = parse_tgroup(raw)

    # Rounded values from API (whole °C on AUTO, decimal on synoptic)
    temp_c_api = props.get("temperature", {}).get("value")
    dew_c_api  = props.get("dewpoint",    {}).get("value")

    # T-group wins if present; otherwise use API value
    temp_c = tg_temp_c if tg_temp_c is not None else temp_c_api
    dew_c  = tg_dew_c  if tg_dew_c  is not None else dew_c_api

    wind_dir     = props.get("windDirection", {}).get("value")
    wind_spd_kmh = props.get("windSpeed",    {}).get("value")
    wind_mph     = round(wind_spd_kmh * 0.621371, 1) if wind_spd_kmh else 0.0

    rh = props.get("relativeHumidity", {}).get("value")

    baro_pa = props.get("barometricPressure", {}).get("value")
    baro_mb = round(baro_pa / 100.0, 2) if baro_pa else None

    slp_pa  = props.get("seaLevelPressure", {}).get("value")
    slp_mb  = round(slp_pa / 100.0, 2) if slp_pa else None

    sky = props.get("textDescription", "")
    has_tgroup = tg_temp_c is not None

    return {
        "timestamp_utc": ts_str,
        "temp_c":        temp_c,
        "temp_f":        c_to_f(temp_c),
        "dew_c":         dew_c,
        "dew_f":         c_to_f(dew_c),
        "wind_dir":      wind_dir,
        "wind_mph":      wind_mph,
        "rh":            rh,
        "baro_mb":       baro_mb,
        "slp_mb":        slp_mb,
        "sky":           sky,
        "has_tgroup":    has_tgroup,
        "raw_metar":     raw if raw else None,
    }

def local_time_str(utc_iso: str, tz_name: str) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(utc_iso)
        local = dt.astimezone(ZoneInfo(tz_name))
        return local.strftime("%-I:%M %p %Z")
    except Exception:
        return utc_iso

def wind_sector_label(degrees) -> str:
    if degrees is None:
        return "VRB"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(degrees / 22.5) % 16]

def angle_delta(a, b):
    """Shortest angular distance between two bearings (0-180)."""
    if a is None or b is None:
        return None
    d = abs(a - b)
    return d if d <= 180 else 360 - d

# ─── POLLER ──────────────────────────────────────────────────────────────────

def poll_station(station_id: str):
    """
    1) Always GET /latest (cache-friendly, permissive rate limit).
    2) If last_tgroup is >65 min old, also GET /observations?limit=3
       to pick up the most recent synoptic T-group.
    """
    c = cache[station_id]

    # ── /latest ──
    try:
        url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
        resp = requests.get(url, headers=NWS_HEADERS, timeout=12)
        resp.raise_for_status()
        obs = parse_obs(resp.json()["properties"])
        c["latest"] = obs
        c["last_polled"] = datetime.now(timezone.utc).isoformat()

        # Wind history
        c["wind_history"].append({
            "dir": obs["wind_dir"],
            "mph": obs["wind_mph"],
            "ts":  obs["timestamp_utc"],
        })
        c["wind_history"] = c["wind_history"][-WIND_HISTORY_MAX:]

        # If this obs itself is a synoptic (has T-group), update tracking
        if obs["has_tgroup"]:
            c["last_tgroup"] = obs
            if obs["temp_f"] is not None:
                if c["daily_max_f"] is None or obs["temp_f"] > c["daily_max_f"]:
                    c["daily_max_f"]  = obs["temp_f"]
                    c["daily_max_ts"] = obs["timestamp_utc"]

    except Exception as e:
        print(f"[POLL] {station_id} /latest error: {e}", flush=True)
        return

    # ── /observations (only if we need a fresher T-group) ──
    if not obs["has_tgroup"]:
        now_utc = datetime.now(timezone.utc)
        need_fetch = True
        last_tg = c.get("last_tgroup")
        if last_tg and last_tg.get("timestamp_utc"):
            try:
                age_min = (now_utc - datetime.fromisoformat(last_tg["timestamp_utc"])).total_seconds() / 60
                if age_min < 65:
                    need_fetch = False
            except Exception:
                pass

        if need_fetch:
            try:
                time.sleep(2)  # gentle spacing before second call
                url2 = f"https://api.weather.gov/stations/{station_id}/observations?limit=3"
                resp2 = requests.get(url2, headers=NWS_HEADERS, timeout=12)
                resp2.raise_for_status()
                for feat in resp2.json().get("features", []):
                    candidate = parse_obs(feat["properties"])
                    if candidate["has_tgroup"]:
                        c["last_tgroup"] = candidate
                        if candidate["temp_f"] is not None:
                            if c["daily_max_f"] is None or candidate["temp_f"] > c["daily_max_f"]:
                                c["daily_max_f"]  = candidate["temp_f"]
                                c["daily_max_ts"] = candidate["timestamp_utc"]
                        break
            except Exception as e:
                print(f"[POLL] {station_id} /observations error: {e}", flush=True)

def poller_loop():
    """Daemon thread: staggered initial poll, then every POLL_INTERVAL."""
    # Staggered startup — 2s between each station
    for sid in ALL_POLL:
        poll_station(sid)
        time.sleep(2)

    last_reset_date = datetime.now(timezone.utc).date()

    while True:
        time.sleep(POLL_INTERVAL)

        # Reset daily maxes at UTC midnight
        today = datetime.now(timezone.utc).date()
        if today != last_reset_date:
            for s in cache:
                cache[s]["daily_max_f"]  = None
                cache[s]["daily_max_ts"] = None
            last_reset_date = today

        # Poll everything with 1.5s stagger
        for sid in ALL_POLL:
            poll_station(sid)
            time.sleep(1.5)

# ─── WIND TREND / KILL-SWITCH ANALYSIS ──────────────────────────────────────

def analyze_wind_trend(station_id: str) -> dict:
    """
    Compare last two wind readings for shift detection.
    Compare first vs last in history for hour-long regime change.
    """
    history = cache[station_id].get("wind_history", [])
    if len(history) < 2:
        return {
            "current_sector": wind_sector_label(
                cache[station_id].get("latest", {}).get("wind_dir") if cache[station_id].get("latest") else None
            ),
            "shift_detected": False,
            "regime_change":  False,
            "readings":       len(history),
        }

    latest_dir = history[-1]["dir"]
    prev_dir   = history[-2]["dir"]

    delta_recent = angle_delta(latest_dir, prev_dir)
    shift_40     = (delta_recent is not None and delta_recent >= 40)

    delta_hour   = angle_delta(latest_dir, history[0]["dir"])
    regime_60    = (delta_hour is not None and delta_hour >= 60)

    return {
        "current_sector": wind_sector_label(latest_dir),
        "current_deg":    latest_dir,
        "prev_deg":       prev_dir,
        "delta_recent":   delta_recent,
        "shift_detected": shift_40,
        "regime_change":  regime_60,
        "readings":       len(history),
    }

def get_upstream_wind(station_id: str) -> dict | None:
    """If station has an upstream sensor, return its current wind."""
    info = STATIONS.get(station_id, {})
    up = info.get("upstream")
    if not up or up not in cache:
        return None
    up_latest = cache[up].get("latest")
    if not up_latest:
        return None
    return {
        "station":  up,
        "wind_dir": up_latest["wind_dir"],
        "sector":   wind_sector_label(up_latest["wind_dir"]),
        "wind_mph": up_latest["wind_mph"],
        "ts":       up_latest["timestamp_utc"],
    }

# ─── FLASK ROUTES ────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    """Render health check."""
    return "OK", 200

@app.route("/api/snapshot")
def api_snapshot():
    """
    Full snapshot of all settlement airports.
    Dashboard polls this every 30s.
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    out = {"pulled_at_utc": now_utc, "stations": {}, "card_order": CARD_ORDER}

    for sid in CARD_ORDER:
        info = STATIONS[sid]
        c       = cache[sid]
        latest  = c.get("latest")      or {}
        tgroup  = c.get("last_tgroup") or {}
        wt      = analyze_wind_trend(sid)
        upstream = get_upstream_wind(sid)

        out["stations"][sid] = {
            # Meta
            "city":           info["city"],
            "tz":             info["tz"],
            # Latest obs (5-min AUTO or synoptic)
            "obs_time_local": local_time_str(latest.get("timestamp_utc"), info["tz"]),
            "obs_time_utc":   latest.get("timestamp_utc"),
            "temp_f":         latest.get("temp_f"),
            "dew_f":          latest.get("dew_f"),
            "wind_dir":       latest.get("wind_dir"),
            "wind_sector":    wind_sector_label(latest.get("wind_dir")),
            "wind_mph":       latest.get("wind_mph"),
            "rh":             latest.get("rh"),
            "baro_mb":        latest.get("baro_mb"),
            "sky":            latest.get("sky"),
            # T-group precision (hourly synoptic only)
            "tgroup": {
                "temp_f":     tgroup.get("temp_f"),
                "dew_f":      tgroup.get("dew_f"),
                "time_local": local_time_str(tgroup.get("timestamp_utc"), info["tz"]),
                "time_utc":   tgroup.get("timestamp_utc"),
            },
            # Daily high (T-group based)
            "daily_max_f":    c.get("daily_max_f"),
            "daily_max_time": local_time_str(c.get("daily_max_ts"), info["tz"]),
            # Wind analysis
            "wind_trend":     wt,
            # Upstream station (if applicable)
            "upstream":       upstream,
            # Health
            "last_polled_utc": c.get("last_polled"),
        }

    return jsonify(out)

@app.route("/api/station/<station_id>")
def api_station_detail(station_id):
    """Single station with full wind history array."""
    if station_id not in STATIONS:
        return jsonify({"error": "unknown station"}), 404
    info = STATIONS[station_id]
    c    = cache[station_id]

    return jsonify({
        "station":      station_id,
        "city":         info["city"],
        "latest":       c.get("latest"),
        "last_tgroup":  c.get("last_tgroup"),
        "daily_max_f":  c.get("daily_max_f"),
        "wind_history": [
            {
                "time_local": local_time_str(w["ts"], info["tz"]),
                "dir":        w["dir"],
                "sector":     wind_sector_label(w["dir"]),
                "mph":        w["mph"],
            }
            for w in c.get("wind_history", [])
        ],
        "wind_trend":   analyze_wind_trend(station_id),
        "upstream":     get_upstream_wind(station_id),
        "last_polled":  c.get("last_polled"),
    })

# ─── ON-DEMAND: UNIFIED MODEL SCAN ──────────────────────────────────────────
# Mirrors notebook Cell 2: Open-Meteo for ECMWF/GFS, NWS /forecast for official high.
# NBM not available on Open-Meteo — we pull the NWS point forecast which blends NBM.

def _fetch_openmeteo_high(lat, lon, tz, target_date, model_key):
    """Pull today's max from Open-Meteo for one model."""
    MODEL_MAP = {"ecmwf": "ecmwf_ifs", "gfs": "gfs_global"}
    om_model = MODEL_MAP.get(model_key)
    if not om_model:
        return None
    try:
        r = requests.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m", "models": om_model,
            "temperature_unit": "fahrenheit", "timezone": tz,
            "forecast_days": 7,
        }, timeout=10)
        r.raise_for_status()
        body = r.json()
        # Open-Meteo may key the column as "temperature_2m" or "temperature_2m_{model}"
        key = [k for k in body["hourly"] if "temperature_2m" in k][0]
        temps = [t for t, ti in zip(body["hourly"][key], body["hourly"]["time"])
                 if ti.startswith(target_date) and t is not None]
        return max(temps) if temps else None
    except Exception as e:
        print(f"[SCAN] Open-Meteo {model_key} error: {e}", flush=True)
        return None

def _fetch_nws_forecast_high(lat, lon, target_date):
    """NWS /points → /forecast → daytime high for target_date. This IS the NBM-blended number."""
    try:
        pt = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                          headers=NWS_HEADERS, timeout=8)
        pt.raise_for_status()
        fc_url = pt.json()["properties"]["forecast"]
        fc = requests.get(fc_url, headers=NWS_HEADERS, timeout=8)
        fc.raise_for_status()
        for period in fc.json().get("properties", {}).get("periods", []):
            if target_date in period["startTime"] and "night" not in period["name"].lower():
                return float(period["temperature"])
        return None
    except Exception as e:
        print(f"[SCAN] NWS forecast error ({lat},{lon}): {e}", flush=True)
        return None

def _build_scan(target_date):
    """Full scan: 12 cities × (NWS, ECMWF, GFS) + computed mean."""
    rows = []
    for sid in CARD_ORDER:
        info = STATIONS[sid]
        lat, lon, tz = info["lat"], info["lon"], info["tz"]

        nws  = _fetch_nws_forecast_high(lat, lon, target_date)
        ecmwf = _fetch_openmeteo_high(lat, lon, tz, target_date, "ecmwf")
        gfs   = _fetch_openmeteo_high(lat, lon, tz, target_date, "gfs")

        # Mean of whatever came back (NWS counts as NBM-blend)
        vals = [v for v in [nws, ecmwf, gfs] if v is not None]
        mean = round(sum(vals) / len(vals), 1) if vals else None

        rows.append({
            "station": sid,
            "city":    info["city"],
            "nws":     nws,
            "ecmwf":   ecmwf,
            "gfs":     gfs,
            "mean":    mean,
        })
        time.sleep(0.8)   # gentle rate-limit spacing
    return rows

@app.route("/api/scan")
def api_scan():
    """Unified multi-model scan. TTL = 15 min. Pass ?date=YYYY-MM-DD to override."""
    global _scan_cache
    target_date = requests.args.get("date") if hasattr(requests, "args") else None
    # Flask request object
    from flask import request as flask_req
    target_date = flask_req.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    now = time.time()
    if _scan_cache["data"] and (now - _scan_cache["ts"]) < 900:   # 15 min
        return jsonify({"date": target_date, "stations": _scan_cache["data"], "cached": True})

    data = _build_scan(target_date)
    _scan_cache["data"] = data
    _scan_cache["ts"]   = now
    return jsonify({"date": target_date, "stations": data, "cached": False})

# ─── ON-DEMAND: AREA FORECAST DISCUSSIONS ───────────────────────────────────
# Mirrors notebook Cell 4.  Fetches latest AFD text from each WFO.

def _fetch_afd(station_id, wfo):
    """Pull latest AFD .DISCUSSION... snippet for one WFO."""
    try:
        list_r = requests.get(
            f"https://api.weather.gov/products/types/AFD/locations/{wfo}",
            headers=NWS_HEADERS, timeout=10)
        list_r.raise_for_status()
        latest_url = list_r.json()["@graph"][0]["@id"]

        prod_r = requests.get(latest_url, headers=NWS_HEADERS, timeout=10)
        prod_r.raise_for_status()
        full_text = prod_r.json().get("productText", "")

        # Extract .DISCUSSION... section
        if ".DISCUSSION..." in full_text:
            snippet = full_text.split(".DISCUSSION...")[1].split("&&")[0].strip()
        else:
            snippet = full_text[:2000]

        return {"station": station_id, "wfo": wfo, "text": snippet, "error": None}
    except Exception as e:
        return {"station": station_id, "wfo": wfo, "text": None, "error": str(e)}

@app.route("/api/afd")
def api_afd():
    """All 12 AFDs. TTL = 30 min. Pass ?station=KLAX for single."""
    global _afd_cache
    from flask import request as flask_req
    single = flask_req.args.get("station")

    now = time.time()

    if single:
        # Single-station fetch — always fresh (no TTL)
        info = STATIONS.get(single)
        if not info:
            return jsonify({"error": "unknown station"}), 404
        return jsonify(_fetch_afd(single, info["wfo"]))

    # Full set — TTL cached
    if _afd_cache["data"] and (now - _afd_cache["ts"]) < 1800:  # 30 min
        return jsonify({"stations": _afd_cache["data"], "cached": True})

    results = {}
    for sid in CARD_ORDER:
        results[sid] = _fetch_afd(sid, STATIONS[sid]["wfo"])
        time.sleep(1.0)   # NWS rate-limit spacing
    _afd_cache["data"] = results
    _afd_cache["ts"]   = now
    return jsonify({"stations": results, "cached": False})

# ─── ON-DEMAND: KALSHI MARKET PRICES ────────────────────────────────────────
# Mirrors notebook Cell 5.  Public /events endpoint — no API key needed for reads.

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def _fetch_kalshi_event(event_ticker):
    """GET one event and return its markets with yes_ask prices."""
    try:
        r = requests.get(f"{KALSHI_BASE}/events/{event_ticker}", timeout=10)
        if r.status_code != 200:
            return None
        event = r.json().get("event", {})
        markets = event.get("markets", [])
        out = []
        for m in markets:
            out.append({
                "ticker": m.get("ticker"),
                "title":  m.get("title"),
                "yes_ask": m.get("yes_ask"),
                "yes_bid": m.get("yes_bid"),
            })
        return out
    except Exception as e:
        print(f"[KALSHI] {event_ticker} error: {e}", flush=True)
        return None

@app.route("/api/kalshi")
def api_kalshi():
    """Live Kalshi weather market prices for today. TTL = 5 min."""
    global _kalshi_cache
    from flask import request as flask_req
    target_date_str = flask_req.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    now = time.time()
    if _kalshi_cache["data"] and (now - _kalshi_cache["ts"]) < 300:  # 5 min
        return jsonify({"date": target_date_str, "cities": _kalshi_cache["data"], "cached": True})

    # Build Kalshi date token: 26FEB03 style
    try:
        dt = datetime.strptime(target_date_str, "%Y-%m-%d")
        date_token = dt.strftime("%y%b%d").upper()   # e.g. "26FEB03"
    except Exception:
        return jsonify({"error": "bad date format, use YYYY-MM-DD"}), 400

    cities = {}
    for sid in CARD_ORDER:
        prefix = KALSHI_PREFIXES.get(sid)
        if not prefix:
            continue
        event_ticker = f"{prefix}-{date_token}"
        markets = _fetch_kalshi_event(event_ticker)
        cities[sid] = {
            "city":         STATIONS[sid]["city"],
            "event_ticker": event_ticker,
            "markets":      markets,   # None if event doesn't exist yet
        }
        time.sleep(0.3)

    _kalshi_cache["data"] = cities
    _kalshi_cache["ts"]   = now
    return jsonify({"date": target_date_str, "cities": cities, "cached": False})

# ─── HERBIE STUB ─────────────────────────────────────────────────────────────
# Herbie downloads multi-GB GRIB files — not viable on a $7 Render instance.
# Endpoint exists so the dashboard can display a clear message.

@app.route("/api/herbie")
def api_herbie():
    return jsonify({
        "available": False,
        "message":   "Herbie HRRR GRIB downloads require Colab. Run notebook Cell 3 there and paste results here, or use the /api/scan endpoint for Open-Meteo model highs.",
    })

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

# ─── STARTUP ─────────────────────────────────────────────────────────────────
# Do NOT start the poller at import time — gevent may not be fully patched yet.
# Instead, start it on the first incoming request (guaranteed post-fork, post-patch).

_poller_started = False

@app.before_request
def _start_poller_once():
    global _poller_started
    if not _poller_started:
        _poller_started = True
        t = threading.Thread(target=poller_loop, daemon=True, name="nws-poller")
        t.start()
        print("[STARTUP] Poller thread launched.", flush=True)

if __name__ == "__main__":
    # Dev mode fallback (don't use in production)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
