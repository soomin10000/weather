#!/usr/bin/env python3
"""Forecast-vs-reality log, the groundwork for local bias correction.

Every hour, each forecast source's next 72 hours go into SQLite with their lead
time; every 30 minutes, METAR observations from the airports around us go in
alongside. After a few weeks there's enough to see whether the forecasts run
consistently warm/cold/windy here, by lead time and hour of day.

    python3 forecast_log.py report     # error by source and lead time vs Heathrow
"""
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = os.path.join(os.path.expanduser('~/.local/share/weather'), 'forecast_log.db')
LONDON = ZoneInfo('Europe/London')
MAX_LEAD_H = 72
FORECAST_EVERY = 3600
OBS_EVERY = 1800
GARDEN_HOST = os.environ.get('GARDEN_HOST', '192.168.1.174')   # Ecowitt GW1200A, local API
GARDEN_FROM = '2026-10-06T17:00Z'   # remote sensor moved outdoors ~15:00Z; earlier readings are indoor

# Weather Underground / Weather Company. A PWS-tier key reaches the daily forecast only
# (hourly is 401). Optional: without the key the source is skipped. Env only, no default.
WU_KEY = os.environ.get('WUNDERGROUND_API_KEY')
WU_URL = ('https://api.weather.com/v3/wx/forecast/daily/5day?geocode={},{}'
          '&format=json&units=h&language=en-GB&apiKey={}')
BLEND_SOURCES = ['openmeteo', 'metoffice', 'bpf']
BLEND_MIN_N = 48    # scored hours needed per source and lead bucket before we trust its history

# Keyless METAR feed. Airports ring us: Heathrow NW 19 km, Biggin Hill E 19 km
# (183 m up, runs cooler), London City NE 25 km (urban), Gatwick S 25 km.
# UK METARs give whole degrees only; the rounding averages out over many samples.
METAR_URL = 'https://aviationweather.gov/api/data/metar?ids={}&format=json'
STATIONS = ['EGLL', 'EGKB', 'EGLC', 'EGKK']
KT_TO_MPH = 1.15078

SCHEMA = """
CREATE TABLE IF NOT EXISTS forecasts (
    source     TEXT NOT NULL,     -- openmeteo | metoffice | bpf
    issued_at  INTEGER NOT NULL,  -- unix time we fetched it
    valid_utc  TEXT NOT NULL,     -- 'YYYY-MM-DDTHH:00Z'
    lead_h     INTEGER NOT NULL,
    temp REAL, temp_p10 REAL, temp_p90 REAL,
    pop REAL, p1 REAL,            -- % chance of rain (>0.1 mm/h where known), % >1 mm/h (bpf only)
    wind REAL, gust REAL,         -- mph
    code INTEGER,                 -- WMO
    PRIMARY KEY (source, issued_at, valid_utc)
);
CREATE TABLE IF NOT EXISTS observations (
    station   TEXT NOT NULL,
    obs_utc   TEXT NOT NULL,      -- report time, 'YYYY-MM-DDTHH:MMZ'
    temp REAL, dewpoint REAL,     -- °C
    wind REAL, gust REAL,         -- mph
    pressure REAL,                -- hPa
    wx TEXT,                      -- METAR weather group, e.g. '-RA', 'SHRA', 'DZ'
    raining INTEGER,              -- 1 if wx has rain/drizzle/showers
    raw TEXT,
    PRIMARY KEY (station, obs_utc)
);
CREATE TABLE IF NOT EXISTS garden_log (   -- Ecowitt GW1200A: outdoor WN32 + the gateway's own sensor in the shed
    obs_utc TEXT PRIMARY KEY,
    garden_temp REAL, garden_hum REAL, shed_temp REAL, shed_hum REAL, pressure REAL
);
CREATE TABLE IF NOT EXISTS daily_forecasts (   -- one row per source, issue and local day
    source TEXT NOT NULL, issued_at INTEGER NOT NULL, day TEXT NOT NULL,   -- day 'YYYY-MM-DD' London
    tmax REAL, tmin REAL, pop_day REAL, pop_night REAL, qpf REAL, wind REAL, phrase TEXT,
    PRIMARY KEY (source, issued_at, day)
);
CREATE INDEX IF NOT EXISTS forecasts_valid ON forecasts (valid_utc);
"""

_lock = threading.Lock()
_last = {}   # source -> last time we logged it


def _db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    con = sqlite3.connect(DB, timeout=10)
    con.executescript(SCHEMA)
    return con


def _utc(local):
    """'2026-09-27T15:00' London -> '2026-09-27T14:00Z'"""
    t = datetime.fromisoformat(local).replace(tzinfo=LONDON)
    return t.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00Z')


def log_forecast(source, rows, now=None):
    """rows: dicts with 'time' (London local, Open-Meteo style) and any of the
    forecast columns. Throttled to once an hour per source; never raises."""
    now = now or time.time()
    if now - _last.get(source, 0) < FORECAST_EVERY:
        return
    try:
        out = []
        for r in rows:
            valid = _utc(r['time'])
            lead = round((datetime.fromisoformat(valid.replace('Z', '+00:00')).timestamp() - now) / 3600)
            if 0 <= lead <= MAX_LEAD_H:
                out.append((source, int(now), valid, lead, r.get('temp'), r.get('temp_p10'), r.get('temp_p90'),
                            r.get('pop'), r.get('p1'), r.get('wind'), r.get('gust'), r.get('code')))
        with _lock, _db() as con:
            con.executemany('INSERT OR IGNORE INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', out)
        _last[source] = now
    except Exception as e:
        print(f'forecast log ({source}) failed: {e}')


def cols(block, mapping):
    """Open-Meteo-layout arrays -> row dicts, renaming keys via mapping {ours: theirs}."""
    if not block or not block.get('time'):
        return []
    return [{k: (block.get(v) or [None] * len(block['time']))[i] for k, v in [('time', 'time'), *mapping.items()]}
            for i in range(len(block['time']))]


def fetch_metars():
    req = urllib.request.Request(METAR_URL.format(','.join(STATIONS)),
                                 headers={'User-Agent': 'local-weather/1.0'})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def fetch_garden():
    """Poll the Ecowitt GW1200A's local API. Returns the outdoor WN32 ('garden') and the
    gateway's own sensor ('shed', it lives in the shed), or None if unreachable."""
    with urllib.request.urlopen(f'http://{GARDEN_HOST}/get_livedata_info', timeout=10) as r:
        d = json.loads(r.read())
    common = {c['id']: c for c in d.get('common_list', [])}
    def num(v):
        try:
            return float(str(v).split()[0].rstrip('%'))
        except (ValueError, IndexError):
            return None   # '--' when a sensor isn't reporting
    wh25 = (d.get('wh25') or [{}])[0]
    return {
        'garden': {'temp': num(common.get('0x02', {}).get('val')), 'humidity': num(common.get('0x07', {}).get('val')),
                   'dewpoint': num(common.get('0x03', {}).get('val'))},
        'shed': {'temp': num(wh25.get('intemp')), 'humidity': num(wh25.get('inhumi'))},
        'pressure': num(wh25.get('rel')),
        'time': int(time.time()),
    }


def log_garden(g=None):
    """Store the garden station's current reading as station GARDEN. Never raises."""
    try:
        g = g or fetch_garden()
        if g['garden']['temp'] is None:
            return 0
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%MZ')
        with _lock, _db() as con:
            before = con.total_changes
            con.execute('INSERT OR IGNORE INTO observations (station, obs_utc, temp, dewpoint, pressure, raining) '
                        'VALUES (?,?,?,?,?,0)', ('GARDEN', now, g['garden']['temp'], g['garden']['dewpoint'], g['pressure']))
            con.execute('INSERT OR IGNORE INTO garden_log VALUES (?,?,?,?,?,?)',
                        (now, g['garden']['temp'], g['garden']['humidity'], g['shed']['temp'], g['shed']['humidity'], g['pressure']))
            return con.total_changes - before
    except Exception as e:
        print(f'garden poll failed: {e}')
        return 0


def log_observations():
    """Never raises; returns how many new reports were stored."""
    try:
        rows = []
        for m in fetch_metars():
            wx = m.get('wxString') or ''
            mph = lambda kt: None if kt is None else round(kt * KT_TO_MPH, 1)
            rows.append((m['icaoId'], m['reportTime'][:16] + 'Z', m.get('temp'), m.get('dewp'),
                         mph(m.get('wspd') if isinstance(m.get('wspd'), (int, float)) else None),
                         mph(m.get('wgst')), m.get('altim'), wx,
                         int(any(k in wx for k in ('RA', 'DZ', 'SH'))), m.get('rawOb')))
        with _lock, _db() as con:
            before = con.total_changes
            con.executemany('INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
            return con.total_changes - before
    except Exception as e:
        print(f'observation log failed: {e}')
        return 0


def fetch_wunderground(lat, lon):
    url = WU_URL.format(lat, lon, WU_KEY)
    req = urllib.request.Request(url, headers={'User-Agent': 'local-weather/1.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def log_wunderground(lat, lon):
    """Daily forecast into daily_forecasts. Never raises; returns rows stored. The error
    text is the HTTP code only, so the key in the URL can't reach the logs."""
    try:
        d = fetch_wunderground(lat, lon)
        dp = d['daypart'][0]
        now = int(time.time())
        rows = []
        for i, t in enumerate(d['validTimeLocal']):
            day, night = 2 * i, 2 * i + 1
            # today's day-part goes null once the afternoon is under way; use the max we have
            rows.append(('wunderground', now, t[:10], d['temperatureMax'][i], d['temperatureMin'][i],
                         dp['precipChance'][day], dp['precipChance'][night], d['qpf'][i],
                         dp['windSpeed'][day] if dp['windSpeed'][day] is not None else dp['windSpeed'][night],
                         dp['wxPhraseLong'][day] or dp['wxPhraseLong'][night]))
        with _lock, _db() as con:
            before = con.total_changes
            con.executemany('INSERT OR IGNORE INTO daily_forecasts VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
            return con.total_changes - before
    except urllib.error.HTTPError as e:
        print(f'wunderground failed: HTTP {e.code}')
    except Exception as e:
        print(f'wunderground failed: {type(e).__name__}')
    return 0


def _bucket(lead):
    return 0 if lead < 6 else 1 if lead < 24 else 2


def blend_weights(con):
    """Per source and lead bucket: (bias, weight, n) from how each source's temperature
    matched the outdoor garden sensor. Bias is forecast minus observed, so it gets subtracted.
    Until a source has BLEND_MIN_N scored hours it gets no bias correction and weight 1."""
    garden = {h: t for h, t in con.execute(
        "SELECT strftime('%Y-%m-%dT%H:00Z', obs_utc, '+30 minutes'), AVG(garden_temp) FROM garden_log "
        'WHERE obs_utc >= ? AND garden_temp IS NOT NULL GROUP BY 1', (GARDEN_FROM,))}
    errs = {}
    for src, lead, valid, temp in con.execute(
            'SELECT source, lead_h, valid_utc, temp FROM forecasts WHERE source IN (%s) AND temp IS NOT NULL'
            % ','.join('?' * len(BLEND_SOURCES)), BLEND_SOURCES):
        if valid in garden:
            errs.setdefault((src, _bucket(lead)), []).append((valid, temp - garden[valid]))
    out = {}
    for b in range(3):
        stats = {}
        for src in BLEND_SOURCES:
            pairs = errs.get((src, b), [])
            e = [x for _, x in pairs]
            n = len({v for v, _ in pairs})   # distinct hours: successive issues of one hour aren't independent
            if n >= BLEND_MIN_N:
                bias = sum(e) / len(e)
                var = sum((x - bias) ** 2 for x in e) / len(e)
                stats[src] = (bias, 1 / max(var, 0.25), n)   # floor: 0.5 C of noise is the sensor's own
            else:
                stats[src] = (0.0, 1.0, n)
        total = sum(w for _, w, _ in stats.values())
        for src, (bias, w, n) in stats.items():
            out[(src, b)] = (bias, w / total, n)
    return out


def blend(log=False):
    """Blended hourly temperature from the latest issue of each source, bias-corrected
    and weighted by recent accuracy. Returns {'hourly': [[valid_utc, temp]...], 'weights': ...}."""
    try:
        now = time.time()
        with _lock, _db() as con:
            w = blend_weights(con)
            hours = {}
            for src in BLEND_SOURCES:
                issued = con.execute('SELECT MAX(issued_at) FROM forecasts WHERE source=?', (src,)).fetchone()[0]
                if not issued or now - issued > 6 * 3600:
                    continue
                for valid, temp in con.execute(
                        'SELECT valid_utc, temp FROM forecasts WHERE source=? AND issued_at=? AND temp IS NOT NULL',
                        (src, issued)):
                    t = datetime.fromisoformat(valid.replace('Z', '+00:00')).timestamp()
                    if t < now - 1800:
                        continue
                    lead = round((t - now) / 3600)
                    bias, wt, _ = w[(src, _bucket(lead))]
                    hours.setdefault(valid, []).append((temp - bias, wt, lead))
            out = []
            for valid in sorted(hours):
                v = hours[valid]
                tw = sum(x[1] for x in v)
                out.append((valid, round(sum(x[0] * x[1] for x in v) / tw, 2), v[0][2]))
            if log and out and now - _last.get('blend', 0) >= FORECAST_EVERY:
                con.executemany('INSERT OR IGNORE INTO forecasts (source, issued_at, valid_utc, lead_h, temp) '
                                "VALUES ('blend', ?, ?, ?, ?)",
                                [(int(now), v, lead, t) for v, t, lead in out if 0 <= lead <= MAX_LEAD_H])
                _last['blend'] = now
        return {'hourly': [[v, t] for v, t, _ in out],
                'weights': {f'{s}/{["0-5h", "6-23h", "24h+"][b]}': {'bias': round(x[0], 2), 'weight': round(x[1], 2), 'n': x[2]}
                            for (s, b), x in w.items()}}
    except Exception as e:
        print(f'blend failed: {e}')
        return {'hourly': [], 'weights': {}}


def history(hours=72):
    """Series for the page's charts, as [epoch_ms, value...] rows, oldest first. Never raises."""
    ms = lambda iso: int(datetime.fromisoformat(iso.replace('Z', '+00:00')).timestamp() * 1000)
    since = datetime.fromtimestamp(time.time() - hours * 3600, timezone.utc).strftime('%Y-%m-%dT%H:%MZ')
    try:
        with _lock, _db() as con:
            garden = [[ms(t), *r] for t, *r in con.execute(
                'SELECT obs_utc, garden_temp, garden_hum, shed_temp, shed_hum, pressure FROM garden_log '
                'WHERE obs_utc >= ? ORDER BY obs_utc', (since,))]
            # what Open-Meteo predicted for each hour, from its latest run (shortest lead) before that hour
            fc = [[ms(t), v] for t, v in con.execute(
                "SELECT valid_utc, temp FROM (SELECT valid_utc, temp, MIN(lead_h) FROM forecasts "
                "WHERE source='openmeteo' AND lead_h >= 0 AND valid_utc >= ? AND valid_utc <= strftime('%Y-%m-%dT%H:%MZ','now') "
                'AND temp IS NOT NULL GROUP BY valid_utc) ORDER BY valid_utc', (since,))]
        return {'garden': garden, 'forecast': fc}
    except Exception as e:
        print(f'history failed: {e}')
        return {'garden': [], 'forecast': []}


def report(station='EGLL'):
    """Mean error (forecast minus observed) and mean absolute error by source and
    lead time, matching each forecast hour to the report nearest the top of the hour."""
    con = _db()
    n_f = con.execute('SELECT COUNT(*) FROM forecasts').fetchone()[0]
    n_o = con.execute('SELECT COUNT(*) FROM observations WHERE station=?', (station,)).fetchone()[0]
    span = con.execute('SELECT MIN(obs_utc), MAX(obs_utc) FROM observations').fetchone()
    print(f'{n_f} forecast rows, {n_o} {station} reports, observations {span[0]} to {span[1]}\n')
    q = """
    WITH obs AS (   -- one report per hour: the :50/:20 METAR rounds to the nearest hour
        SELECT strftime('%Y-%m-%dT%H:00Z', obs_utc, '+10 minutes') AS hour, AVG(temp) AS temp,
               AVG(wind) AS wind, MAX(raining) AS raining
        FROM observations WHERE station = ? GROUP BY hour)
    SELECT f.source,
           CASE WHEN f.lead_h < 6 THEN '0-5h' WHEN f.lead_h < 24 THEN '6-23h'
                WHEN f.lead_h < 48 THEN '24-47h' ELSE '48-72h' END AS lead,
           COUNT(*), AVG(f.temp - o.temp), AVG(ABS(f.temp - o.temp)),
           AVG(f.wind - o.wind),
           AVG(CASE WHEN o.raining THEN 1.0 ELSE 0 END) * 100, AVG(f.pop)
    FROM forecasts f JOIN obs o ON o.hour = f.valid_utc
    WHERE f.temp IS NOT NULL AND o.temp IS NOT NULL
    GROUP BY f.source, lead ORDER BY f.source, MIN(f.lead_h)"""
    rows = con.execute(q, (station,)).fetchall()
    if not rows:
        print('Nothing to compare yet: forecasts need their valid hours to pass first.')
        return
    print(f"{'source':10} {'lead':7} {'n':>6} {'temp bias':>10} {'temp MAE':>9} {'wind bias':>10} {'rained%':>8} {'fc pop%':>8}")
    for s, lead, n, bias, mae, wbias, rained, pop in rows:
        f = lambda v, d=1: '—' if v is None else f'{v:+.{d}f}'
        print(f"{s:10} {lead:7} {n:>6} {f(bias):>10} {mae:>9.2f} {f(wbias):>10} {rained:>8.0f} {pop if pop is None else round(pop):>8}")
    print('\nBias = forecast minus observed; positive means the forecast runs high.'
          f'\n{station} is ~19 km from home; compare stations before blaming the forecast.')


if __name__ == '__main__':
    if sys.argv[1:2] == ['report']:
        report(sys.argv[2] if len(sys.argv) > 2 else 'EGLL')
    elif sys.argv[1:2] == ['garden']:
        print(fetch_garden())
        print(log_garden(), 'new reports')
    elif sys.argv[1:2] == ['obs']:
        print(log_observations(), 'new reports')
    else:
        print(__doc__)
