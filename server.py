#!/usr/bin/env python3
"""Local weather — fetches current conditions from Open-Meteo and serves a dashboard."""
import json
import os
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

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

# WMO weather interpretation codes
WMO_DESCRIPTIONS = {
    0: 'Bloody lovely out there',
    1: 'Pretty decent, actually', 2: 'Make your bloody mind up, sky', 3: 'Utterly grey bollocks',
    45: 'Can\'t see shit', 48: 'Freezing foggy bastard',
    51: 'Spitting like a grumpy camel', 53: 'Drizzling its arse off', 55: 'Soaked to the bloody bone',
    61: 'Pissing it down a bit', 63: 'Properly pissing it down', 65: 'Absolutely pissing it down',
    71: 'Snowing like a bastard', 73: 'Snowing its arse off', 75: 'Holy shit, it\'s snowing',
    77: 'Snow grains — what the hell is that',
    80: 'Brief shower, you miserable git', 81: 'Shower, get inside you muppet', 82: 'Shower from actual hell',
    85: 'Snow shower, sodding typical', 86: 'Heavy snow shower, absolute nightmare',
    95: 'Thunderstorm — what did we do to deserve this', 96: 'Thunderstorm with bloody hail', 99: 'Thunderstorm with heavy hail — we\'re all fucked',
}

_state = {'data': None, 'error': None, 'fetched_at': None}
_lock = threading.Lock()


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def fetch_weather(cfg):
    params = (
        f"latitude={cfg['latitude']}"
        f"&longitude={cfg['longitude']}"
        f"&current={CURRENT_VARS}"
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
    srv = HTTPServer(('0.0.0.0', port), Handler)
    print(f'Weather running on http://0.0.0.0:{port}')
    srv.serve_forever()
