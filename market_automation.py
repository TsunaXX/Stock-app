"""Local signal guards and bounded public-data maintenance; no broker polling."""

import hashlib
import json
import math
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd


class IntradayBackground:
    """One serial server timer per connected browser; no Streamlit state or writes."""

    def __init__(self, alive, disconnected_grace=120):
        self.alive, self.disconnected_grace = alive, disconnected_grace
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.jobs = {}
        self.closed = False
        self.thread = threading.Thread(target=self._run, name='intraday-background', daemon=True)
        self.thread.start()

    def configure(self, room, signature, rows, seconds, update, ready, release):
        with self.lock:
            prior = self.jobs.get(room)
            if prior and prior['signature'] == signature:
                return
            if prior:
                prior['cancelled'] = True
                if not prior['running']:
                    self._release(prior)
            self.jobs[room] = {'signature': signature, 'rows': rows.copy(deep=True),
                               'seconds': seconds, 'update': update, 'ready': ready,
                               'release': release, 'next': 0, 'result': None,
                               'running': False, 'cancelled': False}
        self.wake.set()

    def result(self, room):
        with self.lock:
            value = self.jobs.get(room, {}).get('result')
            return {**value, 'rows': value['rows'].copy(deep=True)} if value else None

    def remove(self, room):
        with self.lock:
            job = self.jobs.pop(room, None)
            if job:
                job['cancelled'] = True
                if not job['running']:
                    self._release(job)
        self.wake.set()

    @staticmethod
    def _release(job):
        try:
            job['release']()
        except Exception:
            pass  # Logout may already have closed this quote connection.

    def close(self):
        with self.lock:
            self.closed = True
            for room in list(self.jobs):
                self.remove(room)
        self.wake.set()

    def _run(self):
        disconnected = None
        while not self.closed:
            try:
                connected = self.alive()
            except Exception:
                connected = False
            if connected:
                disconnected = None
            else:
                disconnected = disconnected or time.monotonic()
                if time.monotonic() - disconnected >= self.disconnected_grace:
                    self.close()
                    break
            with self.lock:
                jobs = list(self.jobs.values())
            for job in jobs:
                with self.lock:
                    if self.closed or job['cancelled'] or time.monotonic() < job['next']:
                        continue
                    job['running'] = True
                started = time.monotonic()
                rows, count, error, outside = job['rows'], 0, None, False
                try:
                    outside = not job['ready']()
                    if not outside:
                        rows, count = job['update'](rows.copy(deep=True))
                except Exception as exc:
                    error = type(exc).__name__
                checked = datetime.now().astimezone().isoformat()
                with self.lock:
                    job['running'] = False
                    if job['cancelled']:
                        self._release(job)
                        continue
                    prior = job['result'] or {}
                    changed = count and data_version(rows.to_dict('records')) != prior.get('version')
                    job['result'] = {'rows': rows, 'count': count, 'error': error,
                                     'outside': outside, 'checked_at': checked,
                                     'updated_at': checked if changed and not error else prior.get('updated_at'),
                                     'version': data_version(rows.to_dict('records')),
                                     'duration': time.monotonic() - started}
                    job['rows'] = rows
                    job['next'] = time.monotonic() + (max(1, job['seconds']) if outside else job['seconds'])
            self.wake.wait(0.25)
            self.wake.clear()


def finite_number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def vwap_guard(bars, price, vwap, tick, previous_side=None):
    """Use this session's cached minutes, including the forming minute immediately."""
    price, vwap, tick = map(finite_number, (price, vwap, tick))
    if price is None or vwap is None or tick is None or tick <= 0:
        return {'side': None, 'ready': False, 'reason': '等待均價資料'}
    buffer = tick * 2
    slope, crosses, recent_high, recent_low = None, 0, None, None
    if isinstance(bars, pd.DataFrame) and not bars.empty:
        required = {'High', 'Low', 'Close', 'Volume'}
        if required.issubset(bars.columns):
            data = bars.sort_index().copy()
            data = data.apply(pd.to_numeric, errors='coerce').dropna(subset=list(required))
            data = data[data['Volume'] >= 0]
            if not data.empty and data['Volume'].sum() > 0:
                typical = (data['High'] + data['Low'] + data['Close']) / 3
                average = (typical * data['Volume']).cumsum() / data['Volume'].cumsum().replace(0, np.nan)
                recent = data.loc[data.index >= data.index[-1] - pd.Timedelta(minutes=10)].tail(11)
                previous = data['Close'].shift()
                ranges = pd.concat([data['High'] - data['Low'],
                                    (data['High'] - previous).abs(),
                                    (data['Low'] - previous).abs()], axis=1).max(axis=1)
                volatility = finite_number(ranges.loc[recent.index].median())
                buffer = max(buffer, (volatility or 0) * .15)
                reference = recent.iloc[:-1] if len(recent) > 1 else recent
                recent_high, recent_low = float(reference['High'].max()), float(reference['Low'].min())
                avg = average.loc[recent.index].dropna()
                if len(avg) >= 3:
                    slope_window = avg.tail(6)
                    minutes = (slope_window.index[-1] - slope_window.index[0]).total_seconds() / 60
                    if minutes > 0:
                        slope = (float(slope_window.iloc[-1]) - float(slope_window.iloc[0])) / minutes
                    deviations = recent.loc[avg.index, 'Close'] - avg
                    signs = np.sign(deviations[deviations.abs() >= tick * .5])
                    crosses = int((signs.diff().abs() == 2).sum())
    # Schmitt trigger: retain the side inside the band, but never admit a new entry there.
    side = 'long' if price > vwap + buffer else ('short' if price < vwap - buffer else previous_side)
    outside = abs(price - vwap) > buffer
    flat = slope is not None and abs(slope) <= max(tick * .1, buffer / 10)
    choppy = crosses >= 3 and flat
    # A clear escape from the whole recent range releases the chop veto immediately.
    escaped = (recent_high is not None and price > recent_high + buffer) or (
        recent_low is not None and price < recent_low - buffer)
    if choppy and not escaped:
        reason = '均價反覆穿越，等待脫離震盪區'
    elif not outside:
        reason = '均價緩衝區內，等待有效突破'
    elif slope is not None and ((side == 'long' and slope < -buffer / 5)
                                or (side == 'short' and slope > buffer / 5)):
        reason = '均價斜率反向，暫不進場'
    else:
        reason = ''
    return {'side': side, 'ready': not reason, 'reason': reason,
            'buffer': buffer, 'slope': slope, 'crosses': crosses}


class BackgroundJobs:
    """Two workers shared by the app; same-key jobs never overlap, reads never wait."""

    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='public-maintenance')
        self.lock = threading.RLock()
        self.jobs = {}

    def poll(self, key, loader, interval=1800):
        with self.lock:
            job = self.jobs.get(key)
            now = time.monotonic()
            if job is None or (job['future'].done() and now - job['completed'] >= interval):
                if job is None and len(self.jobs) >= 16:
                    completed = [k for k, v in self.jobs.items() if v['future'].done()]
                    if not completed:
                        return None
                    self.jobs.pop(completed[0])
                prior = job.get('result') if job else None
                job = self.jobs[key] = {'future': self.pool.submit(loader),
                                       'result': prior, 'completed': float('inf'), 'consumed': False}
            if job['future'].done() and not job['consumed']:
                try:
                    job['result'] = job['future'].result()
                except Exception:
                    pass  # Last successful value survives and retries use the same backoff.
                job['completed'] = now
                job['consumed'] = True
            return job['result']


def data_version(value):
    def safe(item):
        if isinstance(item, dict):
            return {str(k): safe(v) for k, v in item.items()}
        if isinstance(item, (list, tuple)):
            return [safe(v) for v in item]
        return item
    return hashlib.sha256(json.dumps(safe(value), sort_keys=True, ensure_ascii=False,
                                     default=str).encode('utf-8')).hexdigest()


def merge_company_sections(snapshot, sections, prefer_existing=False):
    """Preserve per-company successes on partial failures and keep calendar selection."""
    merged = dict(snapshot)
    for label, incoming in sections.items():
        if not isinstance(incoming, dict) or not isinstance(incoming.get('events'), list):
            continue
        prior = snapshot.get(label, {})
        old_events = prior.get('events', [])
        # Public announcements/ex-dates are rolling feeds: retain historical events.
        def event_key(e):
            if label == 'us_revenue':
                revenue = e.get('revenue', {})
                return str((e.get('ticker') or revenue.get('ticker'), revenue.get('period_end')))
            month = e.get('revenue', {}).get('revenue_month')
            return str(e.get('event_id') or (e.get('ticker'), month) if month else
                       e.get('event_id') or (e.get('ticker'), e.get('date'), e.get('title')))
        events = {event_key(e): e for e in old_events}
        for event in incoming['events']:
            key = event_key(event)
            incoming_stamp = str(event.get('data_asof') or '')
            existing_stamp = str(events.get(key, {}).get('data_asof') or '')
            if incoming_stamp < existing_stamp or (
                    prefer_existing and key in events and incoming_stamp == existing_stamp):
                continue
            events[key] = event
        merged[label] = {**prior, **incoming, 'events': list(events.values())[-500:]}
    labels = ('earnings', 'taiwan_revenue', 'us_revenue', 'disclosures', 'financials', 'dividends')
    merged['events'] = [e for label in labels for e in merged.get(label, {}).get('events', [])]
    return merged


MACRO_TITLES = {
    'ECONOMICS:USCPI': 'CPI', 'ECONOMICS:USNFP': '大非農',
    'ECONOMICS:USGDPQQ': 'GDP', 'ECONOMICS:USCPCEPIMM': '核心 PCE',
    'ECONOMICS:USCPCEPIAC': '核心 PCE', 'ECONOMICS:USBCOI': 'ISM 製造業',
    'ECONOMICS:USADP': '小非農 ADP', 'ECONOMICS:USIR': 'FOMC',
    'ECONOMICS:USICJC': '初領失業金',
}


def macro_results(rows):
    results = {}
    for row in rows:
        label = MACRO_TITLES.get(row.get('ticker'))
        if label is None:
            label = {'adp employment change': '小非農 ADP',
                     'initial jobless claims': '初領失業金',
                     'fed interest rate decision': 'FOMC'}.get(str(row.get('title', '')).lower())
        if label is None:
            continue
        try:
            stamp = pd.Timestamp(row['date'])
            stamp = stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp
            stamp = stamp.tz_convert('Asia/Taipei')
        except (KeyError, TypeError, ValueError):
            continue
        result = {k: finite_number(row.get(k)) for k in ('actual', 'forecast', 'previous')}
        result.update({'release_at': stamp.isoformat(), 'unit': str(row.get('unit') or row.get('scale') or ''),
                       'indicator': str(row.get('title', '')), 'source': 'TradingView Economic Calendar',
                       'source_url': str(row.get('source_url', ''))})
        results.setdefault((label, stamp.date().isoformat()), []).append(result)
    return results


def merge_macro_results(previous, incoming):
    merged = dict(previous)
    for key, rows in incoming.items():
        known = {row.get('indicator'): row for row in previous.get(key, [])}
        for row in rows:
            name = row.get('indicator')
            if row.get('actual') is not None or name not in known:
                known[name] = row
        merged[key] = list(known.values())
    return merged


def attach_macro_results(sources, results, now):
    updated = {}
    for label, events in sources.items():
        updated[label] = []
        for event in events:
            event = dict(event)
            matching = results.get((label, str(event.get('date', ''))), [])
            old = event.get('release_results', [])
            # Missing new values must not erase a previously published result.
            by_name = {r.get('indicator'): r for r in old}
            for result in matching:
                if result['actual'] is not None or result.get('indicator') not in by_name:
                    by_name[result.get('indicator')] = result
            release_results = list(by_name.values())
            if release_results:
                event['release_results'] = release_results
                pieces = []
                for result in release_results:
                    actual, forecast = result['actual'], result['forecast']
                    released = pd.Timestamp(result['release_at']) <= pd.Timestamp(now)
                    state = '已公布' if actual is not None and released else ('待補結果' if released else '待公布')
                    comparison = ('高於預期' if actual > forecast else ('低於預期' if actual < forecast else '符合預期')) if actual is not None and forecast is not None and released else '預期未取得'
                    value = lambda v: f'{v:g}{result["unit"]}' if v is not None else '未取得'
                    pieces.append(f'{result["indicator"]}：{state}｜實際 {value(actual) if released else "待公布"}／預期 {value(forecast)}／前值 {value(result["previous"])}｜{comparison}')
                event['detail'] = str(event.get('detail', '')).split('；公布結果：')[0] + '；公布結果：' + '；'.join(pieces) + '（TradingView 備援）'
            updated[label].append(event)
    return updated


def index_scenario(plan, price):
    price = finite_number(price)
    if price is None:
        return '等待即時行情'
    support, resistance = plan['support'], plan['resistance']
    zone = plan.get('zone_points', 0)
    invalid = plan.get('invalidation')
    direction = plan.get('direction')
    if invalid is not None and ((direction == '偏多' and price <= invalid) or
                                (direction == '偏空' and price >= invalid)):
        return '原情境失效／停損條件已碰觸'
    if price > resistance + zone:
        return '突破壓力，觀察延續'
    if price < support - zone:
        return '跌破支撐，觀察延續'
    if abs(price - support) <= zone:
        return '接近支撐，觀察承接'
    if abs(price - resistance) <= zone:
        return '接近壓力，觀察受壓'
    return '支撐壓力區間內，等待'
