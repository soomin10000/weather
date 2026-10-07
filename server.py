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
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo

import forecast_log

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
    'wind_speed_10m',
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

# Met Office Blended Probabilistic Forecast (BPF): percentile ranges and threshold
# probabilities for one UK site. Separate subscription, separate key. The free plan
# is 55 calls/day and each fetch is 2 calls, so hourly = 48/day. The last good
# response is cached on disk so restarts don't spend quota.
BPF_URL = ('https://data.hub.api.metoffice.gov.uk/mo-blended-prob-forecast-feature-svc/2.0.0'
           '/collections/{}/instances/blended/locations/{}')
BPF_KEY = os.environ.get('METOFFICE_BPF_KEY')
BPF_INTERVAL = 3600
BPF_CACHE = os.path.join(os.path.expanduser('~/.cache/weather'), 'bpf.json')

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

_state = {'garden': None, 'data': None, 'forecast': None, 'metoffice': None, 'warnings': None, 'bpf': None,
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
                         'weather_code', 'is_day', 'description', 'wind_speed_10m', 'wind_gusts_10m')}
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
        H['wind_speed_10m'].append(_mph(ts.get('windSpeed10m')))
        H['wind_gusts_10m'].append(_mph(ts.get('windGustSpeed10m')))
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


def fetch_bpf(site):
    h = {'apikey': BPF_KEY, 'accept': 'application/prs.coverage+json'}
    return {c: json.loads(_get(BPF_URL.format(c, site), h, timeout=60))
            for c in ('uk-spot-percentiles', 'uk-spot-probabilities')}


def _bpf_series(doc):
    """CoverageJSON -> {param: (times, labels, rows)} where rows[i] is the list of
    values across percentiles/thresholds at times[i]. The ranges declare their shape
    as [percentiles, t] but the values are actually t-major; checked against the
    percentiles coming out monotonic."""
    out = {}
    for cov in doc['coverages']:
        axes = cov['domain']['axes']
        for name, r in cov['ranges'].items():
            times = axes['t']['values']
            lab_ax = next(a for a in r['axisNames'] if a != 't')
            labels = axes[lab_ax]['values']
            m, v = len(labels), r['values']
            out[name] = (times, labels, [v[i * m:(i + 1) * m] for i in range(len(times))])
    return out


def build_bpf(raw):
    """Boil the two ~1-2 MB coverage documents down to what the page draws:
    hourly 10/50/90th percentile temperature, rain chances at three intensities,
    gusts, lightning and weather code for ~5 days; daily high/low ranges and
    rain-total chances out to where the data ends."""
    P = _bpf_series(raw['uk-spot-percentiles'])
    Q = _bpf_series(raw['uk-spot-probabilities'])

    def pct(name, want, conv):
        times, labels, rows = P[name]
        idx = [labels.index(w) for w in want]
        return {t: [None if row[j] is None else conv(row[j]) for j in idx] for t, row in zip(times, rows)}

    def prob(name, want):
        times, labels, rows = Q[name]
        idx = [labels.index(w) for w in want]
        return {t: [None if row[j] is None else round(row[j] * 100) for j in idx] for t, row in zip(times, rows)}

    K = lambda v: round(v - 273.15, 1)
    temp = pct('airTemperature1p5m', ['10', '50', '90'], K)
    gust = pct('windSpeedOfGust10mMaximumPt01h', ['50', '90'], _mph)
    code = pct('weatherCodePt01h', ['50'], int)
    # Rain in the hour: >0.1 mm (any), >1 mm (proper), >4 mm (heavy)
    rain = prob('probabilityOfLweThicknessOfPrecipitationAmountAboveThresholdSumPt01h',
                ['>1.0E-4', '>0.001', '>0.004'])
    zap = prob('probabilityOfNumberOfLightningFlashesPerUnitAreaInVicinityAboveThreshold15000mSumPt01h',
               ['>0.0'])

    H = {k: [] for k in ('time', 't10', 't50', 't90', 'weather_code', 'description',
                         'pop', 'p1', 'p4', 'gust50', 'gust90', 'lightning')}
    for t in sorted(code):   # the hourly weather code sets the ~5-day hourly span
        c = code[t][0]
        wmo, text = MO_CODES.get(c, (None, None))
        H['time'].append(_local(t))
        for k, v in zip(('t10', 't50', 't90'), temp.get(t, [None] * 3)):
            H[k].append(v)
        H['weather_code'].append(wmo)
        H['description'].append(text)
        for k, v in zip(('pop', 'p1', 'p4'), rain.get(t, [None] * 3)):
            H[k].append(v)
        for k, v in zip(('gust50', 'gust90'), gust.get(t, [None] * 2)):
            H[k].append(v)
        H['lightning'].append(zap.get(t, [None])[0])

    # Daily values come from rolling windows ending at time t. For each local date:
    # high = 12 h max window ending nearest 21:00, low = 12 h min window ending
    # nearest 09:00, rain = 24 h total window ending nearest the following midnight.
    def nearest(series, target):
        best = None
        for t in series:
            dt = abs((datetime.fromisoformat(t.replace('Z', '+00:00')) - target).total_seconds())
            if dt <= 5400 and (best is None or dt < best[0]):
                best = (dt, t)
        return series[best[1]] if best else None

    tmax = pct('airTemperature1p5mMaximumPt12h', ['10', '50', '90'], K)
    tmin = pct('airTemperature1p5mMinimumPt12h', ['10', '50', '90'], K)
    mm = pct('lweThicknessOfPrecipitationAmountSumPt24h', ['25', '50', '75', '90'],
             lambda v: round(v * 1000, 1))
    # Chance the day's total passes 1 mm (a proper wet day) and 8 mm (a soaking)
    wet = prob('probabilityOfLweThicknessOfPrecipitationAmountAboveThresholdSumPt24h',
               ['>0.001', '>0.008'])
    dates = sorted({_local(t)[:10] for t in tmax})
    D = {k: [] for k in ('time', 'hi10', 'hi50', 'hi90', 'lo10', 'lo50', 'lo90',
                         'rain1', 'rain8', 'mm25', 'mm50', 'mm75', 'mm90', 'lightning')}
    for d in dates:
        day = datetime.fromisoformat(d).replace(tzinfo=LONDON)
        hi = nearest(tmax, day.replace(hour=21))
        lo = nearest(tmin, day.replace(hour=9))
        if not hi or not lo:
            continue
        w, m = nearest(wet, day + timedelta(days=1)), nearest(mm, day + timedelta(days=1))
        D['time'].append(d)
        for k, v in zip(('hi10', 'hi50', 'hi90'), hi):
            D[k].append(v)
        for k, v in zip(('lo10', 'lo50', 'lo90'), lo):
            D[k].append(v)
        for k, v in zip(('rain1', 'rain8'), w or [None] * 2):
            D[k].append(v)
        for k, v in zip(('mm25', 'mm50', 'mm75', 'mm90'), m or [None] * 4):
            D[k].append(v)
        z = [v[0] for t, v in zap.items() if _local(t)[:10] == d and v[0] is not None]
        D['lightning'].append(max(z) if z else None)
    return {'hourly': H, 'daily': D}


def _load_bpf_cache():
    try:
        with open(BPF_CACHE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def poll_loop():
    mo_next = warn_next = obs_next = wu_next = blend_next = 0
    om_forecast = {}
    cached = _load_bpf_cache()
    if cached:
        _state['bpf'] = cached
    bpf_next = cached['fetched_at'] + BPF_INTERVAL if cached else 0
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
            forecast_log.log_forecast('openmeteo', forecast_log.cols(raw.get('hourly'), {
                'temp': 'temperature_2m', 'pop': 'precipitation_probability', 'wind': 'wind_speed_10m',
                'gust': 'wind_gusts_10m', 'code': 'weather_code'}))
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
                forecast_log.log_forecast('metoffice', forecast_log.cols(mo['hourly'], {
                    'temp': 'temperature_2m', 'pop': 'precipitation_probability', 'wind': 'wind_speed_10m',
                    'gust': 'wind_gusts_10m', 'code': 'weather_code'}))
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

        site = cfg.get('bpf_site')
        if BPF_KEY and site and now >= bpf_next:
            try:
                bpf = build_bpf(fetch_bpf(site))
                bpf.update(fetched_at=now, site=site)
                with _lock:
                    _state['bpf'] = bpf
                os.makedirs(os.path.dirname(BPF_CACHE), exist_ok=True)
                with open(BPF_CACHE + '.tmp', 'w') as f:
                    json.dump(bpf, f)
                os.replace(BPF_CACHE + '.tmp', BPF_CACHE)
                bpf_next = now + BPF_INTERVAL
                forecast_log.log_forecast('bpf', forecast_log.cols(bpf['hourly'], {
                    'temp': 't50', 'temp_p10': 't10', 'temp_p90': 't90', 'pop': 'pop', 'p1': 'p1',
                    'gust': 'gust50', 'code': 'weather_code'}))
            except Exception as e:
                print(f'BPF fetch failed: {e}')
                with _lock:
                    if _state['bpf']:
                        _state['bpf']['error'] = str(e)
                # 55/day is tight: back off 3 h on auth/quota errors, 30 min otherwise
                code = getattr(e, 'code', None)
                bpf_next = now + (10800 if code in (401, 403, 429) else 1800)

        if forecast_log.WU_KEY and now >= wu_next:
            # 5-day daily forecast; hourly refresh is plenty, and a failure just retries in 30 min
            wu_next = now + (3600 if forecast_log.log_wunderground(cfg['latitude'], cfg['longitude']) else 1800)
        if now >= blend_next:
            forecast_log.blend(log=True)
            blend_next = now + 3600

        try:
            g = forecast_log.fetch_garden()
            with _lock:
                _state['garden'] = g
        except Exception as e:
            print(f'garden poll failed: {e}')   # keep the last reading; the page ages it out
            g = None
        if now >= obs_next:
            if g:
                forecast_log.log_garden(g)
            forecast_log.log_observations()
            obs_next = now + forecast_log.OBS_EVERY

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
        elif self.path == '/api/blend':
            self._json(forecast_log.blend())
        elif self.path.startswith('/api/history'):
            q = parse_qs(urlparse(self.path).query)
            try:
                hours = min(max(int(q.get('hours', ['72'])[0]), 1), 24 * 14)
            except ValueError:
                hours = 72
            self._json(forecast_log.history(hours))
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
