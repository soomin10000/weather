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
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DB = os.path.join(os.path.expanduser('~/.local/share/weather'), 'forecast_log.db')
LONDON = ZoneInfo('Europe/London')
MAX_LEAD_H = 72
FORECAST_EVERY = 3600
OBS_EVERY = 1800

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
    elif sys.argv[1:2] == ['obs']:
        print(log_observations(), 'new reports')
    else:
        print(__doc__)
