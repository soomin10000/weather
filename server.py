#!/usr/bin/env python3
"""Local weather — fetches current conditions from Open-Meteo and serves a dashboard."""
import json
import os
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, 'config.json')

OPEN_METEO = 'https://api.open-meteo.com/v1/forecast'
CURRENT_VARS = ','.join([
    'temperature_2m',
    'relative_humidity_2m',
    'apparent_temperature',
    'precipitation',
    'rain',
    'weather_code',
    'surface_pressure',
    'wind_speed_10m',
    'wind_direction_10m',
    'wind_gusts_10m',
    'uv_index',
    'is_day',
])
HOURLY_VARS = ','.join([
    'temperature_2m',
    'precipitation_probability',
    'weather_code',
    'wind_gusts_10m',
    'is_day',
])
DAILY_VARS = ','.join([
    'weather_code',
    'temperature_2m_max',
    'temperature_2m_min',
    'precipitation_probability_max',
    'precipitation_sum',
    'wind_gusts_10m_max',
    'sunrise',
    'sunset',
])

# WMO weather interpretation codes
WMO_DESCRIPTIONS = {
    0: 'Clear sky',
    1: 'Mainly clear', 2: 'Partly cloudy', 3: 'Overcast',
    45: 'Fog', 48: 'Depositing rime fog',
    51: 'Light drizzle', 53: 'Moderate drizzle', 55: 'Dense drizzle',
    61: 'Slight rain', 63: 'Moderate rain', 65: 'Heavy rain',
    71: 'Slight snow', 73: 'Moderate snow', 75: 'Heavy snow',
    77: 'Snow grains',
    80: 'Slight showers', 81: 'Moderate showers', 82: 'Violent showers',
    85: 'Slight snow showers', 86: 'Heavy snow showers',
    95: 'Thunderstorm', 96: 'Thunderstorm with hail', 99: 'Thunderstorm with heavy hail',
}

# Met Office Weather DataHub, Site Specific (Global Spot). Optional: without a
# key the page runs on Open-Meteo alone. Key comes from the environment only.
METOFFICE_URL = 'https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/'
METOFFICE_KEY = os.environ.get('METOFFICE_API_KEY')
# Two calls (hourly + daily) per fetch; every 30 min = 96/day, well under the free cap
METOFFICE_INTERVAL = 1800

# Keyless public RSS of National Severe Weather Warnings, by region
WARNINGS_URL = 'https://www.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/{}'
WARNINGS_INTERVAL = 900

LONDON = ZoneInfo('Europe/London')
MS_TO_MPH = 2.23694

# Met Office significant weather code -> (nearest WMO code, Met Office wording).
# The page's themes, emoji and jokes all key off WMO, so we translate here.
MO_CODES = {
    -1: (51, 'Trace rain'),
    0: (0, 'Clear night'), 1: (0, 'Sunny day'),
    2: (2, 'Partly cloudy'), 3: (2, 'Partly cloudy'),
    5: (45, 'Mist'), 6: (45, 'Fog'),
    7: (3, 'Cloudy'), 8: (3, 'Overcast'),
    9: (80, 'Light rain shower'), 10: (80, 'Light rain shower'),
    11: (53, 'Drizzle'), 12: (61, 'Light rain'),
    13: (81, 'Heavy rain shower'), 14: (81, 'Heavy rain shower'),
    15: (65, 'Heavy rain'),
    16: (85, 'Sleet shower'), 17: (85, 'Sleet shower'), 18: (71, 'Sleet'),
    19: (81, 'Hail shower'), 20: (81, 'Hail shower'), 21: (77, 'Hail'),
    22: (85, 'Light snow shower'), 23: (85, 'Light snow shower'), 24: (71, 'Light snow'),
    25: (86, 'Heavy snow shower'), 26: (86, 'Heavy snow shower'), 27: (75, 'Heavy snow'),
    28: (95, 'Thunder shower'), 29: (95, 'Thunder shower'), 30: (95, 'Thunder'),
}

_state = {'data': None, 'forecast': None, 'metoffice': None, 'warnings': None,
          'error': None, 'fetched_at': None}
_lock = threading.Lock()


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def fetch_weather(cfg):
    params = (
        f"latitude={cfg['latitude']}"
        f"&longitude={cfg['longitude']}"
        f"&current={CURRENT_VARS}"
        f"&hourly={HOURLY_VARS}"
        f"&daily={DAILY_VARS}"
        f"&forecast_days=7"
        f"&wind_speed_unit=mph"
        f"&timezone=Europe%2FLondon"
    )
    url = f"{OPEN_METEO}?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'local-weather/1.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _get(url, headers=None, timeout=15):
    h = {'User-Agent': 'local-weather/1.0'}
    h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return r.read()


def fetch_metoffice(cfg, timesteps):
    q = urllib.parse.urlencode({
        'latitude': cfg['latitude'], 'longitude': cfg['longitude'],
        'includeLocationName': 'true', 'excludeParameterMetadata': 'true',
    })
    raw = _get(f'{METOFFICE_URL}{timesteps}?{q}',
               {'apikey': METOFFICE_KEY, 'accept': 'application/json'})
    return json.loads(raw)['features'][0]['properties']


def _local(iso_z):
    """'2026-09-27T14:00Z' (UTC) -> '2026-09-27T15:00' London, Open-Meteo's format."""
    t = datetime.fromisoformat(iso_z.replace('Z', '+00:00'))
    return t.astimezone(LONDON).strftime('%Y-%m-%dT%H:%M')


def _mph(ms):
    return None if ms is None else round(ms * MS_TO_MPH, 1)


def build_metoffice(hourly, daily, om):
    """Reshape Met Office GeoJSON into the same array layout the page already reads
    from Open-Meteo (same key names), with codes translated to WMO. Sunrise/sunset,
    is_day and rain totals aren't in the Met Office feed, so borrow them from om."""
    omh, omd = om.get('hourly') or {}, om.get('daily') or {}
    is_day = dict(zip(omh.get('time', []), omh.get('is_day', [])))
    om_day = {k: dict(zip(omd.get('time', []), omd.get(k, [])))
              for k in ('sunrise', 'sunset', 'precipitation_sum')}

    H = {k: [] for k in ('time', 'temperature_2m', 'precipitation_probability',
                         'weather_code', 'is_day', 'description')}
    rows = []
    for ts in hourly.get('timeSeries', []):
        t = _local(ts['time'])
        wmo, desc = MO_CODES.get(ts.get('significantWeatherCode'), (None, None))
        H['time'].append(t)
        H['temperature_2m'].append(ts.get('screenTemperature'))
        H['precipitation_probability'].append(ts.get('probOfPrecipitation'))
        H['weather_code'].append(wmo)
        H['is_day'].append(is_day.get(t, 1))
        H['description'].append(desc)
        rows.append((t, ts, wmo, desc))

    # Current conditions = the hour we're in (Met Office hourly starts a little in the past)
    now = datetime.now(LONDON).strftime('%Y-%m-%dT%H:00')
    cur = next((r for r in reversed(rows) if r[0] <= now), rows[0] if rows else None)
    current = None
    if cur:
        t, ts, wmo, desc = cur
        mslp = ts.get('mslp')
        current = {
            'time': t,
            'temperature_2m': ts.get('screenTemperature'),
            'apparent_temperature': ts.get('feelsLikeTemperature'),
            'relative_humidity_2m': ts.get('screenRelativeHumidity'),
            'weather_code': wmo,
            'description': desc,
            'wind_speed_10m': _mph(ts.get('windSpeed10m')),
            'wind_direction_10m': ts.get('windDirectionFrom10m'),
            'wind_gusts_10m': _mph(ts.get('windGustSpeed10m')),
            'uv_index': ts.get('uvIndex'),
            'surface_pressure': None if mslp is None else round(mslp / 100, 1),  # Pa -> hPa
            'rain': ts.get('totalPrecipAmount'),
            'visibility': ts.get('visibility'),
            'precipitation_probability': ts.get('probOfPrecipitation'),
        }

    # Daily starts with yesterday. A day's low is the night *before* it (the morning
    # minimum), matching Open-Meteo's calendar-day min that the page was built on.
    today = datetime.now(LONDON).strftime('%Y-%m-%d')
    days = daily.get('timeSeries', [])
    D = {k: [] for k in ('time', 'weather_code', 'description', 'temperature_2m_max',
                         'temperature_2m_min', 'precipitation_probability_max',
                         'precipitation_sum', 'sunrise', 'sunset')}
    for i, ts in enumerate(days):
        date = ts['time'][:10]
        if date < today:
            continue
        code = ts.get('daySignificantWeatherCode')
        if code is None:
            code = ts.get('nightSignificantWeatherCode')
        wmo, desc = MO_CODES.get(code, (None, None))
        lo = days[i - 1].get('nightMinScreenTemperature') if i else None
        if lo is None:
            lo = ts.get('nightMinScreenTemperature')
        pops = [p for p in (ts.get('dayProbabilityOfPrecipitation'),
                            ts.get('nightProbabilityOfPrecipitation')) if p is not None]
        D['time'].append(date)
        D['weather_code'].append(wmo)
        D['description'].append(desc)
        D['temperature_2m_max'].append(ts.get('dayMaxScreenTemperature'))
        D['temperature_2m_min'].append(lo)
        D['precipitation_probability_max'].append(max(pops) if pops else None)
        for k in ('precipitation_sum', 'sunrise', 'sunset'):
            D[k].append(om_day[k].get(date))
    # The last Met Office day often lacks a daytime max; drop half-empty tail days
    while D['time'] and (D['temperature_2m_max'][-1] is None or D['weather_code'][-1] is None):
        for v in D.values():
            v.pop()

    loc = (hourly.get('location') or {}).get('name')
    return {'current': current, 'hourly': H, 'daily': D, 'location': loc,
            'model_run': hourly.get('modelRunDate')}


def fetch_warnings(region):
    root = ET.fromstring(_get(WARNINGS_URL.format(region)))
    out = []
    for it in root.iter('item'):
        title = (it.findtext('title') or '').strip()
        desc = (it.findtext('description') or '').strip()
        # The description usually restates the title before the useful part
        if title and desc.startswith(title):
            desc = desc[len(title):].lstrip(' :-')
        level = next((c for c in ('red', 'amber', 'yellow') if c in title.lower()), 'yellow')
        out.append({
            'title': title,
            'description': desc,
            'link': (it.findtext('link') or '').strip(),
            'published': (it.findtext('pubDate') or '').strip(),
            'level': level,
        })
    return out


def poll_loop():
    mo_next = warn_next = 0
    om_forecast = {}
    while True:
        cfg = load_config()
        interval = cfg.get('poll_interval', 600)
        try:
            raw = fetch_weather(cfg)
            current = raw.get('current', {})
            code = current.get('weather_code', -1)
            current['description'] = WMO_DESCRIPTIONS.get(code, f'Code {code}')
            current['location_name'] = cfg.get('location_name', '')
            current['elevation'] = raw.get('elevation')
            om_forecast = {'hourly': raw.get('hourly'), 'daily': raw.get('daily')}
            with _lock:
                _state['data'] = current
                # Kept apart from 'data' so home_menu's index card payload is unchanged
                _state['forecast'] = om_forecast
                _state['error'] = None
                _state['fetched_at'] = time.time()
        except Exception as e:
            with _lock:
                _state['error'] = str(e)
                _state['fetched_at'] = time.time()
            interval = 60

        now = time.time()
        if METOFFICE_KEY and now >= mo_next:
            try:
                mo = build_metoffice(fetch_metoffice(cfg, 'hourly'),
                                     fetch_metoffice(cfg, 'daily'), om_forecast)
                mo['fetched_at'] = now
                with _lock:
                    _state['metoffice'] = mo
                mo_next = now + METOFFICE_INTERVAL
            except Exception as e:
                print(f'Met Office fetch failed: {e}')
                with _lock:
                    # Keep serving the last good forecast; just flag the failure
                    if _state['metoffice']:
                        _state['metoffice']['error'] = str(e)
                # Back off hard on auth/quota errors; 15 min otherwise keeps a failing
                # day under 2 x 96 = 192 calls, inside the free 360/day cap
                code = getattr(e, 'code', None)
                mo_next = now + (3600 if code in (401, 403, 429) else 900)

        if now >= warn_next:
            try:
                w = fetch_warnings(cfg.get('warnings_region', 'se'))
                with _lock:
                    _state['warnings'] = w
                warn_next = now + WARNINGS_INTERVAL
            except Exception as e:
                print(f'Warnings fetch failed: {e}')
                warn_next = now + 300

        time.sleep(interval)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(fmt % args)

    def do_GET(self):
        if self.path == '/':
            self._file('index.html', 'text/html; charset=utf-8')
        elif self.path == '/apple-touch-icon.png':
            self._file('apple-touch-icon.png', 'image/png')
        elif self.path == '/api/weather':
            with _lock:
                payload = dict(_state)
            self._json(payload)
        else:
            self.send_error(404)

    def _file(self, name, ct):
        path = os.path.join(BASE, name)
        try:
            with open(path, 'rb') as f:
                body = f.read()
            self.send_response(200)
            self.send_header('Content-Type', ct)
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_error(404)

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    cfg = load_config()
    port = cfg.get('server_port', 8186)
    threading.Thread(target=poll_loop, daemon=True).start()
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'Weather running on http://0.0.0.0:{port}')
    srv.serve_forever()
