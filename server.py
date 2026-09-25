#!/usr/bin/env python3
"""Local weather — fetches current conditions from Open-Meteo and serves a dashboard."""
import json
import os
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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

_state = {'data': None, 'forecast': None, 'error': None, 'fetched_at': None}
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


def poll_loop():
    while True:
        try:
            cfg = load_config()
            raw = fetch_weather(cfg)
            current = raw.get('current', {})
            code = current.get('weather_code', -1)
            current['description'] = WMO_DESCRIPTIONS.get(code, f'Code {code}')
            current['location_name'] = cfg.get('location_name', '')
            current['elevation'] = raw.get('elevation')
            with _lock:
                _state['data'] = current
                # Kept apart from 'data' so home_menu's index card payload is unchanged
                _state['forecast'] = {'hourly': raw.get('hourly'), 'daily': raw.get('daily')}
                _state['error'] = None
                _state['fetched_at'] = time.time()
            interval = cfg.get('poll_interval', 600)
        except Exception as e:
            with _lock:
                _state['error'] = str(e)
                _state['fetched_at'] = time.time()
            interval = 60
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
