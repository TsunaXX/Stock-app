import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from lxml.etree import XMLSyntaxError
import math
import time
import threading
import os
import itertools
import json
import re
import html
from urllib.parse import urljoin
from types import SimpleNamespace
from datetime import datetime, time as dt_time, timedelta, date
from email.utils import parsedate_to_datetime
import pytz
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import io
import twstock
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import gc
import logging
import tempfile
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import streamlit.components.v1 as components
import pdfplumber
import fitz  # PyMuPDF 用於將 PDF 轉為圖片
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    NoAlertPresentException,
    TimeoutException,
    UnexpectedAlertPresentException,
    WebDriverException,
)

logger = logging.getLogger(__name__)


def _configure_yfinance_runtime_cache():
    """Keep yfinance's SQLite caches writable and isolated per app process."""
    try:
        cache_dir = os.path.join(
            tempfile.gettempdir(), f"stock_app_yfinance_{os.getpid()}"
        )
        os.makedirs(cache_dir, exist_ok=True)
        yf.set_tz_cache_location(cache_dir)
        return cache_dir
    except (AttributeError, OSError):
        return ""


_YFINANCE_CACHE_DIR = _configure_yfinance_runtime_cache()

# 引入 yahoo_fin 與 shioaji
try:
    import yahoo_fin.stock_info as si
except ImportError:
    si = None

sj_import_error = None
try:
    import shioaji as sj
except BaseException as exc:
    if isinstance(exc, (KeyboardInterrupt, SystemExit)):
        raise
    # Shioaji contains native extensions. A missing/incompatible Linux wheel may
    # raise OSError or a native panic instead of ImportError; the rest of the
    # dashboard must remain available in that case.
    sj = None
    sj_import_error = type(exc).__name__


def get_app_secret(key, default=None):
    """Read an optional Streamlit secret without crashing branch/local deployments."""
    try:
        value = st.secrets.get(key, None)
        if value not in (None, ''):
            return value
    except Exception:
        pass
    # Render and other Git-backed deployments commonly provide secrets as
    # runtime environment variables instead of a checked-in secrets.toml.
    return os.environ.get(str(key), default)


# ==========================================
# Shioaji 即時行情共享串流
# ==========================================
@st.cache_resource(show_spinner=False)
def get_market_stream_registry():
    """Keep quote callbacks alive across Streamlit reruns without touching session_state."""
    return {}, threading.RLock()


def _stream_number(value, default=None):
    """Convert Decimal/scalar/level-one list values to a finite float."""
    if isinstance(value, (list, tuple, np.ndarray, pd.Series)):
        value = value[0] if len(value) else None
    try:
        result = float(value)
        return result if np.isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _stream_datetime(value):
    if value is None:
        return datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
    if isinstance(value, tuple):
        try:
            value = datetime(*value)
        except (TypeError, ValueError):
            value = None
    try:
        parsed = pd.Timestamp(value)
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert('Asia/Taipei').tz_localize(None)
        return parsed.to_pydatetime()
    except (TypeError, ValueError, AttributeError):
        return datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)


def _stream_contract_codes(contract):
    codes = []
    for field in ('code', 'target_code', 'symbol'):
        value = str(getattr(contract, field, '') or '').strip()
        if value and value not in codes:
            codes.append(value)
    return codes


def _stream_state(api):
    registry, registry_lock = get_market_stream_registry()
    api_key = id(api)
    with registry_lock:
        state = registry.get(api_key)
        if state is None:
            state = {
                'lock': threading.RLock(),
                'quotes': {},
                'aliases': {},
                'subscriptions': {},
                'snapshot_retry_after': {},
                'callbacks_installed': False,
                'errors': [],
            }
            registry[api_key] = state
    return state


def _remember_stream_error(state, message):
    with state['lock']:
        state['errors'] = (state['errors'] + [str(message)])[-8:]


def _stream_payload(api, payload, security_type):
    """Copy one quote payload into the shared cache; callbacks stay intentionally light."""
    if payload is None:
        return
    state = _stream_state(api)
    code = str(getattr(payload, 'code', '') or '').strip()
    if not code:
        return

    updated_at = _stream_datetime(getattr(payload, 'datetime', None))
    values = {
        'code': code,
        'security_type': security_type,
        'updated_at': updated_at,
        'source': 'stream',
    }
    scalar_fields = (
        'open', 'high', 'low', 'close', 'avg_price', 'amount', 'total_amount',
        'amount_sum', 'volume', 'total_volume', 'vol_sum', 'reference',
        'underlying_price', 'price_chg', 'pct_chg', 'change_price', 'change_rate',
        'bid_side_total_vol', 'ask_side_total_vol',
    )
    for field in scalar_fields:
        number = _stream_number(getattr(payload, field, None))
        if number is not None:
            values[field] = number

    for field in ('bid_price', 'ask_price', 'bid_volume', 'ask_volume'):
        raw = getattr(payload, field, None)
        if raw is not None:
            try:
                values[field] = list(raw)
            except TypeError:
                values[field] = [raw]

    bid = _stream_number(values.get('bid_price'))
    ask = _stream_number(values.get('ask_price'))
    bid_volume = _stream_number(values.get('bid_volume'))
    ask_volume = _stream_number(values.get('ask_volume'))
    if bid is not None:
        values['buy_price'] = bid
    if ask is not None:
        values['sell_price'] = ask
    if bid_volume is not None:
        values['buy_volume'] = bid_volume
    if ask_volume is not None:
        values['sell_volume'] = ask_volume

    close = values.get('close')
    change = values.get('price_chg', values.get('change_price'))
    reference = values.get('reference')
    if reference is None and close is not None and change is not None and close - change > 0:
        reference = close - change
        values['reference'] = reference
    if change is None and close is not None and reference is not None:
        change = close - reference
    if change is not None:
        values['change_price'] = change
    if close is not None and reference is not None and reference > 0:
        values['change_rate'] = (close - reference) / reference * 100

    with state['lock']:
        previous = state['quotes'].get(code, {})
        previous.update(values)
        state['quotes'][code] = previous
        for requested, target in list(state['aliases'].items()):
            if target == code:
                state['quotes'][requested] = previous


def _install_stream_callbacks(api):
    state = _stream_state(api)
    with state['lock']:
        if state['callbacks_installed']:
            return True
        # Claim installation before registering so concurrent Streamlit workers do not
        # overwrite each other's callbacks.
        state['callbacks_installed'] = True

    callback_target = api if any(
        callable(getattr(api, name, None))
        for name in ('set_on_quote_stk_v1_callback', 'set_on_quote_fop_v1_callback', 'set_on_quote_idx_v1_callback')
    ) else getattr(api, 'quote', None)
    if callback_target is None:
        _remember_stream_error(state, '此 Shioaji 版本沒有 Quote v1 callback')
        with state['lock']:
            state['callbacks_installed'] = False
        return False

    installed = 0

    def register(name, security_type):
        nonlocal installed
        setter = getattr(callback_target, name, None)
        if not callable(setter):
            return

        def callback(*args):
            _stream_payload(api, args[-1] if args else None, security_type)

        try:
            setter(callback)
            installed += 1
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            _remember_stream_error(state, f'{name}: {exc}')

    register('set_on_quote_stk_v1_callback', 'STK')
    register('set_on_quote_fop_v1_callback', 'FOP')
    # Index streaming is available in Shioaji 1.7+; older versions simply keep
    # using the one-time snapshot seed for IX0001.
    register('set_on_quote_idx_v1_callback', 'IND')
    if installed == 0:
        _remember_stream_error(state, '無法註冊任何 Quote v1 callback')
        with state['lock']:
            state['callbacks_installed'] = False
        return False
    return True


def _shioaji_quote_type():
    quote_type = getattr(sj, 'QuoteType', None) if sj is not None else None
    if quote_type is None and sj is not None:
        quote_type = getattr(getattr(sj, 'constant', None), 'QuoteType', None)
    return getattr(quote_type, 'Quote', 'quote')


def _unsubscribe_market_stream(api, state, subscription_key, metadata):
    contract = metadata.get('contract')
    if contract is None:
        return
    target = api if callable(getattr(api, 'unsubscribe', None)) else getattr(api, 'quote', None)
    method = getattr(target, 'unsubscribe', None)
    unsubscribed = False
    if callable(method):
        try:
            method(contract, quote_type=_shioaji_quote_type())
            unsubscribed = True
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            _remember_stream_error(state, f'unsubscribe {subscription_key}: {exc}')
    else:
        _remember_stream_error(state, f'unsubscribe {subscription_key}: 此版本無取消訂閱方法')
    if unsubscribed:
        with state['lock']:
            state['subscriptions'].pop(subscription_key, None)


def ensure_market_stream_subscription(api, contract):
    """Subscribe one visible contract once, recycling old subscriptions near the 200 limit."""
    if api is None or contract is None:
        return False
    _install_stream_callbacks(api)
    state = _stream_state(api)
    codes = _stream_contract_codes(contract)
    if not codes:
        return False
    requested = codes[0]
    target_code = codes[1] if len(codes) > 1 else requested
    subscription_key = requested
    now_mono = time.monotonic()
    with state['lock']:
        state['aliases'][requested] = target_code
        existing = state['subscriptions'].get(subscription_key)
        if existing:
            if existing.get('status') == 'active':
                existing['last_used'] = now_mono
                return True
            if now_mono < existing.get('retry_after', 0):
                return False
            state['subscriptions'].pop(subscription_key, None)
        state['subscriptions'][subscription_key] = {
            'contract': contract, 'last_used': now_mono, 'status': 'pending'
        }
        active = [
            (key, value) for key, value in state['subscriptions'].items()
            if key != subscription_key and value.get('status') == 'active'
        ]

    # Shioaji allows at most 200 subscriptions. Keep headroom for broker/system
    # subscriptions and recycle contracts that have not been displayed recently.
    if len(active) >= 180:
        oldest_key, oldest = min(active, key=lambda item: item[1].get('last_used', 0))
        if now_mono - oldest.get('last_used', 0) >= 60:
            _unsubscribe_market_stream(api, state, oldest_key, oldest)
        else:
            with state['lock']:
                state['subscriptions'][subscription_key].update({
                    'status': 'capacity', 'retry_after': now_mono + 60
                })
            return False

    target = api if callable(getattr(api, 'subscribe', None)) else getattr(api, 'quote', None)
    method = getattr(target, 'subscribe', None)
    if not callable(method):
        with state['lock']:
            state['subscriptions'][subscription_key].update({
                'status': 'error', 'retry_after': now_mono + 60
            })
        _remember_stream_error(state, '此 Shioaji 版本沒有 subscribe')
        return False
    try:
        method(contract, quote_type=_shioaji_quote_type())
        with state['lock']:
            state['subscriptions'][subscription_key]['status'] = 'active'
        return True
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        with state['lock']:
            state['subscriptions'][subscription_key].update({
                'status': 'error', 'retry_after': now_mono + 60
            })
        _remember_stream_error(state, f'{requested}: {exc}')
        return False


def _stream_store_snapshot(api, contract, snapshot):
    if snapshot is None:
        return
    state = _stream_state(api)
    requested_codes = _stream_contract_codes(contract)
    snapshot_code = str(getattr(snapshot, 'code', '') or '').strip()
    fields = (
        'open', 'high', 'low', 'close', 'avg_price', 'amount', 'total_amount',
        'amount_sum', 'volume', 'total_volume', 'vol_sum', 'reference',
        'underlying_price', 'change_price', 'change_rate', 'buy_price', 'sell_price',
        'buy_volume', 'sell_volume',
    )
    values = {
        'code': snapshot_code or (requested_codes[0] if requested_codes else ''),
        'updated_at': datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None),
        'source': 'snapshot_seed',
    }
    for field in fields:
        number = _stream_number(getattr(snapshot, field, None))
        if number is not None:
            values[field] = number
    with state['lock']:
        for code in set(requested_codes + ([snapshot_code] if snapshot_code else [])):
            previous = state['quotes'].get(code, {})
            previous.update(values)
            state['quotes'][code] = previous


def _stream_quote_for_contract(api, contract):
    state = _stream_state(api)
    codes = _stream_contract_codes(contract)
    with state['lock']:
        for code in codes:
            quote = state['quotes'].get(code)
            if quote:
                return SimpleNamespace(**quote.copy())
            target = state['aliases'].get(code)
            quote = state['quotes'].get(target) if target else None
            if quote:
                return SimpleNamespace(**quote.copy())
    return None


def _is_contract_stream_session(contract, now_tw):
    """Avoid polling the fallback while the relevant exchange is closed."""
    if now_tw.weekday() >= 5:
        return False
    codes = _stream_contract_codes(contract)
    code = codes[0].upper() if codes else ''
    security_text = str(getattr(contract, 'security_type', '')).upper()
    is_fop = (
        'FUT' in security_text or 'OPT' in security_text
        or hasattr(contract, 'delivery_month') or hasattr(contract, 'option_right')
    )
    if code.startswith('IX') or not is_fop:
        return dt_time(9, 0) <= now_tw.time() <= dt_time(13, 35)
    current_time = now_tw.time()
    return (
        dt_time(8, 45) <= current_time <= dt_time(13, 45)
        or current_time >= dt_time(15, 0)
        or current_time < dt_time(5, 0)
    )


def get_stream_quotes(api, contracts, snapshot_fallback=True):
    """Return quotes in input order: stream first, throttled snapshot for cold start/failure."""
    contracts = list(contracts or [])
    if api is None or not contracts:
        return []
    for contract in contracts:
        ensure_market_stream_subscription(api, contract)

    results = [_stream_quote_for_contract(api, contract) for contract in contracts]
    fallback_indices = []
    state = _stream_state(api)
    now_mono = time.monotonic()
    now_tw = datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
    if snapshot_fallback:
        with state['lock']:
            for index, (contract, quote) in enumerate(zip(contracts, results)):
                codes = _stream_contract_codes(contract)
                key = codes[0] if codes else str(index)
                is_missing = quote is None
                is_stale_quote = False
                if quote is not None:
                    updated_at = _stream_datetime(getattr(quote, 'updated_at', None))
                    is_stale_quote = (
                        (now_tw - updated_at).total_seconds() >= 30
                        and _is_contract_stream_session(contract, now_tw)
                    )
                if (is_missing or is_stale_quote) and now_mono >= state['snapshot_retry_after'].get(key, 0):
                    state['snapshot_retry_after'][key] = now_mono + 30
                    fallback_indices.append(index)

    if fallback_indices:
        missing_contracts = [contracts[index] for index in fallback_indices]
        try:
            snapshots = api.snapshots(missing_contracts) or []
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            snapshots = []
        snapshots_by_code = {
            str(getattr(snapshot, 'code', '')): snapshot for snapshot in snapshots
            if getattr(snapshot, 'code', None)
        }
        # A single invalid/rolling contract can make a mixed snapshot batch
        # return fewer rows. Retry only the missing contracts so one bad symbol
        # does not make the whole futures table look disconnected.
        retry_contracts = missing_contracts if len(missing_contracts) <= 30 else []
        for contract in retry_contracts:
            contract_codes = _stream_contract_codes(contract)
            if any(code in snapshots_by_code for code in contract_codes):
                continue
            try:
                single = api.snapshots([contract]) or []
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                single = []
            if single:
                snapshot = single[0]
                snapshot_code = str(getattr(snapshot, 'code', '') or '')
                if snapshot_code:
                    snapshots_by_code[snapshot_code] = snapshot
        for index, contract in zip(fallback_indices, missing_contracts):
            contract_codes = _stream_contract_codes(contract)
            snapshot = next(
                (snapshots_by_code[code] for code in contract_codes if code in snapshots_by_code),
                None,
            )
            if snapshot is None:
                continue
            _stream_store_snapshot(api, contract, snapshot)
            results[index] = _stream_quote_for_contract(api, contract)
    return results


def merge_stream_quote_into_intraday(df, quote, interval):
    """Overlay the latest stream price on the unfinished intraday candle."""
    if df is None or df.empty or quote is None or interval not in ('1m', '5m', '15m', '60m'):
        return df
    price = _stream_number(getattr(quote, 'close', None))
    if price is None or price <= 0:
        return df
    updated_at = _stream_datetime(getattr(quote, 'updated_at', None))
    now_tw = datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
    # A cached closing quote must never open a synthetic candle after the session.
    if abs((now_tw - updated_at).total_seconds()) > 180:
        return df

    minutes = {'1m': 1, '5m': 5, '15m': 15, '60m': 60}[interval]
    bucket = pd.Timestamp(updated_at).floor(f'{minutes}min')
    result = df.copy()
    if result.index.tz is not None:
        result.index = result.index.tz_localize(None)
    last_index = pd.Timestamp(result.index[-1])
    if bucket < last_index - pd.Timedelta(minutes=minutes):
        return result

    if bucket < last_index + pd.Timedelta(minutes=minutes):
        target = last_index
        result.at[target, 'Close'] = price
        result.at[target, 'High'] = max(float(result.at[target, 'High']), price)
        result.at[target, 'Low'] = min(float(result.at[target, 'Low']), price)
        return result

    latest_volume = _stream_number(getattr(quote, 'volume', None), 0) or 0
    new_row = pd.DataFrame([{
        'Open': price, 'High': price, 'Low': price, 'Close': price, 'Volume': latest_volume
    }], index=[bucket])
    return pd.concat([result, new_row]).sort_index()


def get_market_stream_status(api):
    if api is None:
        return {'subscriptions': 0, 'stream_quotes': 0, 'errors': []}
    state = _stream_state(api)
    with state['lock']:
        streamed_codes = {
            value.get('code') for value in state['quotes'].values()
            if value.get('source') == 'stream' and value.get('code')
        }
        return {
            'subscriptions': sum(
                value.get('status') == 'active' for value in state['subscriptions'].values()
            ),
            'stream_quotes': len(streamed_codes),
            'errors': list(state['errors']),
        }


def clear_market_stream(api):
    """Unsubscribe and forget one disconnected API object's quote cache."""
    if api is None:
        return
    registry, registry_lock = get_market_stream_registry()
    with registry_lock:
        state = registry.get(id(api))
    if state is not None:
        with state['lock']:
            subscriptions = list(state['subscriptions'].items())
        for subscription_key, metadata in subscriptions:
            _unsubscribe_market_stream(api, state, subscription_key, metadata)
    with registry_lock:
        registry.pop(id(api), None)
# ==========================================
# 新增: 全域行事曆與權證判斷函數
# ==========================================
@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_official_market_holidays(year):
    """Fetch future TWSE closure dates so expiry logic is not frozen at 2026."""
    try:
        response = requests.get(
            f"https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=json&queryYear={year - 1911}",
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("stat") != "ok":
            return {}
        holidays = {}
        for row in payload.get("data", []):
            if len(row) < 2:
                continue
            parsed = pd.to_datetime(row[0], errors="coerce")
            title = str(row[1]).strip()
            if pd.isna(parsed) or any(word in title for word in ("開始交易", "最後交易日", "封關日")):
                continue
            holidays[(parsed.month, parsed.day)] = title or "市場無交易"
        return holidays
    except (requests.RequestException, ValueError, TypeError, KeyError):
        return {}


def get_holidays(year):
    h = {}
    if year == 2025:
         h.update({
             (1, 1): "元旦", (1, 27): "春節", (1, 28): "春節", (1, 29): "春節", (1, 30): "春節", (1, 31): "春節",
             (2, 3): "春節", (2, 28): "228紀念日", (4, 3): "兒童節", (4, 4): "清明節",
             (5, 1): "勞動節", (5, 30): "端午節", (10, 6): "中秋節", (10, 10): "國慶日"
         })
    if year == 2026:
        h.update({
            (1, 1): "元旦", (2, 11): "封關日", (2, 12): "市場無交易", (2, 13): "市場無交易",
            (2, 14): "春節", (2, 15): "春節", (2, 16): "春節", (2, 17): "春節",
            (2, 18): "春節", (2, 19): "春節", (2, 20): "春節", (2, 21): "春節", (2, 22): "春節",
            (2, 27): "和平紀念日(補)", (2, 28): "和平紀念日",
            (4, 3): "兒童節(補)", (4, 4): "兒童節", (4, 5): "清明節", (4, 6): "清明節(補)",
            (5, 1): "勞動節", (6, 19): "端午節", (9, 25): "中秋節", (9, 28): "教師節",
            (10, 9): "國慶日(補)", (10, 10): "國慶日", (10, 25): "光復節", (10, 26): "光復節(補)",
            (12, 25): "行憲紀念日"
        })
    if not h:
        h.update(fetch_official_market_holidays(year))
    return h

def is_market_closed_func(d_date):
    if d_date.weekday() >= 5: return True
    h_dict = get_holidays(d_date.year)
    name = h_dict.get((d_date.month, d_date.day), "")
    if name and name != "封關日": return True
    return False


def adjust_to_next_market_day(value):
    """Return the next exchange business day, including the supplied date."""
    adjusted = pd.Timestamp(value).date()
    while is_market_closed_func(adjusted):
        adjusted += timedelta(days=1)
    return adjusted

def get_futures_trading_date(now_dt):
    """Map Taiwan local time to the active futures trading date."""
    trading_date = pd.Timestamp(now_dt.date())
    if now_dt.time() >= dt_time(15, 0):
        trading_date += pd.Timedelta(days=1)
    while is_market_closed_func(trading_date.date()):
        trading_date += pd.Timedelta(days=1)
    return trading_date.normalize()


def get_taiex_contract(api):
    """Return the TAIEX contract across supported Shioaji contract layouts."""
    if api is None:
        return None

    # Shioaji 1.7+ uses the exchange index code IX0001.  Older applications
    # may still expose the legacy Contracts tree, so keep narrowly-scoped
    # compatibility fallbacks without ever substituting another market source.
    try:
        contract = api.contracts.get("IX0001")
        if contract is not None:
            return contract
    except (AttributeError, KeyError, TypeError):
        pass

    try:
        legacy_contracts = api.Contracts
        for group_name in ("Indexs", "Indices"):
            group = getattr(legacy_contracts, group_name, None)
            tse = getattr(group, "TSE", None)
            if tse is None:
                continue
            for code in ("IX0001", "001", "TSE01"):
                try:
                    contract = tse[code]
                except (KeyError, TypeError, AttributeError):
                    contract = getattr(tse, code, None)
                if contract is not None:
                    return contract
    except AttributeError:
        pass
    return None


@st.cache_data(ttl=60 * 60 * 6, max_entries=48, show_spinner=False)
def fetch_twse_taiex_month(month_text, refresh_bucket=None):
    """Fetch one official TWSE TAIEX month; failures are not cached."""
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    response = requests.get(
        'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST',
        params={'response': 'json', 'date': str(month_text)},
        headers=headers,
        timeout=(3, 7),
    )
    response.raise_for_status()
    payload = response.json()
    if str(payload.get('stat', '')).upper() != 'OK':
        raise ValueError(f"TWSE index status: {payload.get('stat')}")
    records = []
    for row in payload.get('data', []):
        if len(row) < 5:
            continue
        date_parts = str(row[0]).strip().split('/')
        if len(date_parts) != 3:
            continue
        trade_date = pd.Timestamp(
            year=int(date_parts[0]) + 1911,
            month=int(date_parts[1]), day=int(date_parts[2]),
        )
        values = [float(str(value).replace(',', '').strip()) for value in row[1:5]]
        records.append({
            'ts': trade_date, 'Open': values[0], 'High': values[1],
            'Low': values[2], 'Close': values[3], 'Volume': np.nan,
        })
    if not records:
        raise ValueError('TWSE index month returned no rows')
    return records


def fetch_twse_taiex_daily_history(lookback_days=180):
    """Fetch official TAIEX history in parallel, reusing successful months."""
    tz_tw = pytz.timezone('Asia/Taipei')
    end_date = pd.Timestamp(datetime.now(tz_tw).date())
    start_date = end_date - pd.Timedelta(days=lookback_days)
    months = list(pd.period_range(
        start=start_date.to_period('M'), end=end_date.to_period('M'), freq='M',
    ))
    records = []

    def fetch_month(month):
        try:
            is_current_month = month == end_date.to_period('M')
            refresh_bucket = int(time.time() // 180) if is_current_month else None
            return fetch_twse_taiex_month(
                month.start_time.strftime('%Y%m%d'), refresh_bucket,
            )
        except (requests.RequestException, ValueError, TypeError, KeyError):
            return []

    with ThreadPoolExecutor(max_workers=min(4, len(months))) as executor:
        for month_records in executor.map(fetch_month, months):
            records.extend(month_records)

    if not records:
        return pd.DataFrame()
    result = pd.DataFrame(records)
    result = result[(result['ts'] >= start_date) & (result['ts'] <= end_date)]
    if result.empty:
        return pd.DataFrame()
    return result.drop_duplicates(subset=['ts'], keep='last').set_index('ts').sort_index()


@st.cache_data(ttl=60 * 60 * 24 * 30, max_entries=400, show_spinner=False)
def fetch_twse_market_turnover(trade_date):
    """Return one official TWSE total-market turnover value in hundred-million TWD."""
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    try:
        response = requests.get(
            'https://www.twse.com.tw/exchangeReport/MI_INDEX',
            params={'response': 'json', 'date': trade_date, 'type': 'MS'},
            headers=headers,
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get('stat') not in (None, 'OK'):
            return np.nan
        for table in [payload] + list(payload.get('tables', [])):
            fields = table.get('fields', [])
            data = table.get('data', [])
            amount_idx = next(
                (index for index, field in enumerate(fields) if '成交金額' in str(field)),
                None,
            )
            if amount_idx is None:
                continue
            for row in data:
                if len(row) <= amount_idx:
                    continue
                label = ''.join(str(value).strip() for value in row[:amount_idx])
                if not any(keyword in label for keyword in ('證券合計', '市場總計', '總計')):
                    continue
                raw_amount = str(row[amount_idx]).replace(',', '').replace('元', '').strip()
                amount = float(raw_amount)
                if amount > 0:
                    return amount / 100_000_000
    except (requests.RequestException, ValueError, TypeError, KeyError):
        pass
    return np.nan


def fetch_twse_market_turnovers(trading_dates):
    """Return official daily TWSE market turnover in hundred-million TWD."""
    def parse_turnover(trade_date):
        return trade_date, fetch_twse_market_turnover(trade_date)

    date_list = list(trading_dates)
    if not date_list:
        return {}
    with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS if 'ANALYSIS_MAX_WORKERS' in globals() else 2) as executor:
        pairs = list(executor.map(parse_turnover, date_list))
    return dict(pairs)


def merge_taiex_history_with_shioaji(twse_df, shioaji_df, include_turnover=False):
    """Keep official index OHLC while retaining available Shioaji turnover data."""
    if twse_df.empty:
        return shioaji_df
    result = twse_df.copy()
    if shioaji_df is not None and not shioaji_df.empty:
        shared_dates = result.index.intersection(shioaji_df.index)
        for column in ('Volume', 'Amount'):
            if column in shioaji_df.columns:
                result.loc[shared_dates, column] = shioaji_df.loc[shared_dates, column]

    if include_turnover:
        turnover_dates = tuple(result.index[-70:].strftime('%Y%m%d'))
        turnovers = fetch_twse_market_turnovers(turnover_dates)
        if turnovers:
            result['Volume'] = [turnovers.get(ts.strftime('%Y%m%d'), value) for ts, value in result['Volume'].items()]
            result.attrs['volume_unit'] = '億'
    return result

def get_near_month_futures_settlement(reference_dt=None):
    """回傳台指與股票期貨近月契約的第三個星期三結算日。"""
    tz_tw = pytz.timezone('Asia/Taipei')
    now_tw = reference_dt or datetime.now(tz_tw)
    if isinstance(now_tw, date) and not isinstance(now_tw, datetime):
        now_tw = datetime.combine(now_tw, dt_time.min)
    if now_tw.tzinfo is None:
        now_tw = tz_tw.localize(now_tw)
    else:
        now_tw = now_tw.astimezone(tz_tw)

    def third_wednesday(year, month):
        wednesdays = [
            day for week in calendar.monthcalendar(year, month)
            if (day := week[calendar.WEDNESDAY]) != 0
        ]
        return date(year, month, wednesdays[2])

    settlement_date = adjust_to_next_market_day(
        third_wednesday(now_tw.year, now_tw.month)
    )
    # 到期日一般交易於 13:30 結束；之後提醒下一個近月契約。
    if now_tw.date() > settlement_date or (
        now_tw.date() == settlement_date and now_tw.time() >= dt_time(13, 30)
    ):
        next_month = now_tw.month + 1
        next_year = now_tw.year
        if next_month == 13:
            next_month, next_year = 1, next_year + 1
        settlement_date = adjust_to_next_market_day(
            third_wednesday(next_year, next_month)
        )

    return settlement_date

def is_warrant(code):
    c = str(code)
    if c.startswith('00'): return False
    return len(c) > 4

# ==========================================
# 網路同步行事曆資料來源（僅供「股市行事曆」分頁使用）
# ==========================================
CALENDAR_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/json,text/calendar;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}
TWSE_HOLIDAY_URL = "https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=json&queryYear={roc_year}"
TWSE_NEWS_URL = "https://www.twse.com.tw/news/newsList?response=json"
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BLS_CPI_URL = "https://www.bls.gov/schedule/news_release/cpi.htm"
BLS_EMPLOYMENT_URL = "https://www.bls.gov/schedule/news_release/empsit.htm"
BLS_YEAR_SCHEDULE_URL = "https://www.bls.gov/schedule/{year}/home.htm"
BLS_RELEASE_ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
BLS_CPS_CALENDAR_URL = "https://www.bls.gov/cps/publications/release-calendar.htm"
ADP_EMPLOYMENT_DATA_URL = "https://adpemploymentreport.com/ner_production.json"
ADP_EMPLOYMENT_PAGE_URL = "https://adpemploymentreport.com/"
TRADINGVIEW_CALENDAR_URL = "https://economic-calendar.tradingview.com/events"
BEA_RELEASE_SCHEDULE_URL = "https://www.bea.gov/news/schedule/full"
ISM_RELEASE_CALENDAR_URL = "https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/"


def _calendar_get(url):
    """取得公開行事曆來源；單次短截止，失敗由快取／手動更新接手。"""
    try:
        response = requests.get(
            url, headers=CALENDAR_HTTP_HEADERS, timeout=(3, 8),
        )
        response.raise_for_status()
        return response
    except requests.RequestException:
        return None


def _twse_announcement_detail(row, fallback):
    """僅在偵測到全市場休市時讀取公告 PDF，補足休市原因。"""
    for identifier in row[4:2:-1]:
        if not identifier:
            continue
        response = _calendar_get(f"https://www.twse.com.tw/staticFiles/news/news/tsecnews/{identifier}.pdf")
        if not response or not response.content.startswith(b"%PDF"):
            continue
        try:
            document = fitz.open(stream=response.content, filetype="pdf")
            text_value = " ".join(page.get_text("text") for page in document)
            document.close()
            if text_value.strip():
                return re.sub(r"\s+", " ", text_value).strip()
        except (RuntimeError, ValueError):
            continue
    return fallback


def _taiwan_time_from_eastern(year, month, day, hour, minute=0):
    eastern = pytz.timezone("US/Eastern")
    taipei = pytz.timezone("Asia/Taipei")
    return eastern.localize(datetime(year, month, day, hour, minute)).astimezone(taipei)


@st.cache_data(ttl=60 * 60 * 6, max_entries=12, show_spinner=False)
def fetch_tradingview_us_calendar(year):
    """免 API Key 的美國經濟日曆備援；保留原始資料來源欄位供畫面標示。"""
    headers = {
        **CALENDAR_HTTP_HEADERS,
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/economic-calendar/",
    }
    # 單次回傳上限為 2,000 筆，整年查詢會在年中被截斷，因此分季並行取得。
    quarter_ranges = [(1, 3), (4, 6), (7, 9), (10, 12)]

    def fetch_quarter(month_range):
        start_month, end_month = month_range
        end_day = calendar.monthrange(year, end_month)[1]
        params = {
            "from": f"{year}-{start_month:02}-01T00:00:00.000Z",
            "to": f"{year}-{end_month:02}-{end_day:02}T23:59:59.999Z",
            "countries": "US",
        }
        try:
            response = requests.get(
                TRADINGVIEW_CALENDAR_URL, params=params, headers=headers,
                timeout=(3, 9),
            )
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("result", []) if isinstance(payload, dict) else []
            return rows if isinstance(rows, list) else []
        except (requests.RequestException, ValueError, TypeError):
            return []

    combined = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        for quarter_rows in executor.map(fetch_quarter, quarter_ranges):
            for row in quarter_rows:
                row_key = str(row.get("id") or f"{row.get('title')}|{row.get('date')}")
                combined[row_key] = row
    return sorted(combined.values(), key=lambda row: str(row.get("date", "")))


def _tradingview_macro_events(year, event_type):
    """將 TradingView 的高影響美國總經事件轉為台灣日期與時間。"""
    events, seen = [], set()
    for row in fetch_tradingview_us_calendar(year):
        title = str(row.get("title", "")).strip()
        indicator = str(row.get("indicator", "")).strip()
        ticker = str(row.get("ticker", "")).strip()
        if event_type == "cpi":
            matched = ticker == "ECONOMICS:USCPI" or (
                title.upper() == "CPI" and indicator.lower() == "consumer price index cpi"
            )
            event_title = "美國 CPI 公布"
        elif event_type == "nfp":
            matched = title.lower() == "non farm payrolls" and ticker == "ECONOMICS:USNFP"
            event_title = "美國大非農／失業率"
        elif event_type == "gdp":
            matched = title.lower().startswith("gdp growth rate") and ticker == "ECONOMICS:USGDPQQ"
            event_title = "美國 GDP 公布"
        elif event_type == "core_pce":
            matched = title.lower() in {
                "core pce price index mom", "core pce price index yoy",
            } and ticker in {
                "ECONOMICS:USCPCEPIMM", "ECONOMICS:USCPCEPIAC",
            }
            event_title = "美國核心 PCE 公布"
        elif event_type == "ism_manufacturing":
            matched = title.lower() == "ism manufacturing pmi" and ticker == "ECONOMICS:USBCOI"
            event_title = "美國 ISM 製造業指數"
        else:
            return []
        if not matched:
            continue
        try:
            release_dt = pd.Timestamp(row.get("date"))
            if release_dt.tzinfo is None:
                release_dt = release_dt.tz_localize("UTC")
            taiwan_dt = release_dt.tz_convert("Asia/Taipei")
        except (TypeError, ValueError):
            continue
        if taiwan_dt.year != year or taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        original_source = str(row.get("source", "BLS")).strip() or "BLS"
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": f"TradingView 免費經濟日曆備援；原始發布單位：{original_source}。",
            "closed": False,
            "temporary": False,
            "source": "TradingView Economic Calendar",
            "impact": "high",
        })
    return events


def _merge_macro_events(fallback_events, official_events):
    """同日同類事件以官方排程覆蓋備援來源，並保留官方頁尚未列出的歷史日期。"""
    merged = {
        str(event.get("date", "")): event
        for event in fallback_events or [] if event.get("date")
    }
    for event in official_events or []:
        if event.get("date"):
            merged[str(event["date"])] = event
    return sorted(merged.values(), key=lambda event: (str(event.get("date", "")), str(event.get("title", ""))))


@st.cache_data(ttl=60 * 60 * 6, max_entries=1, show_spinner=False)
def fetch_bea_release_schedule_html():
    """集中快取 BEA 發布排程，避免 GDP 與核心 PCE 各下載一次。"""
    response = _calendar_get(BEA_RELEASE_SCHEDULE_URL)
    return response.text if response and "Access Denied" not in response.text else ""


def _parse_bea_release_schedule(page_html, year, release_type):
    """解析 BEA 官方 GDP／Personal Income and Outlays 發布排程。"""
    if not page_html:
        return []
    soup = BeautifulSoup(page_html, "html.parser")
    if not re.search(fr"\bYear\s+{year}\b", soup.get_text(" ", strip=True), re.I):
        return []
    month_lookup = {name: index for index, name in enumerate(
        ["January", "February", "March", "April", "May", "June",
         "July", "August", "September", "October", "November", "December"], 1
    )}
    date_pattern = re.compile(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})$",
        re.I,
    )
    time_pattern = re.compile(r"^(\d{1,2}):(\d{2})\s*(AM|PM)$", re.I)
    event_title = "美國 GDP 公布" if release_type == "gdp" else "美國核心 PCE 公布"
    events, seen = [], set()
    # Direct class lookup is substantially faster than compiling three CSS
    # selectors for every row on BEA's large schedule page.
    for row in soup.find_all("tr"):
        release_title_node = row.find(class_="release-title")
        release_date_node = row.find(class_="release-date")
        scheduled_date_node = row.find(class_="scheduled-date")
        release_time_node = (
            scheduled_date_node.find("small") if scheduled_date_node else None
        )
        if not (release_title_node and release_date_node and release_time_node):
            continue
        release_title = release_title_node.get_text(" ", strip=True)
        if release_type == "gdp":
            matched = bool(re.match(r"^GDP\s*\(", release_title, re.I))
        elif release_type == "core_pce":
            matched = release_title.lower().startswith("personal income and outlays")
        else:
            return []
        if not matched:
            continue
        date_match = date_pattern.match(release_date_node.get_text(" ", strip=True))
        time_match = time_pattern.match(release_time_node.get_text(" ", strip=True))
        if not (date_match and time_match):
            continue
        month_name, day = date_match.groups()
        hour, minute, meridiem = time_match.groups()
        hour = int(hour) % 12 + (12 if meridiem.upper() == "PM" else 0)
        try:
            taiwan_dt = _taiwan_time_from_eastern(
                year, month_lookup[month_name.title()], int(day), hour, int(minute)
            )
        except (ValueError, pytz.AmbiguousTimeError, pytz.NonExistentTimeError):
            continue
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": f"BEA 官方發布排程：{release_title}；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "U.S. Bureau of Economic Analysis",
            "impact": "high",
        })
    return events


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_bea_gdp_events(year):
    """GDP 以 BEA 官方排程為準，TradingView 補足官方頁未保留的歷史發布日期。"""
    official_events = _parse_bea_release_schedule(fetch_bea_release_schedule_html(), year, "gdp")
    current_year = datetime.now(pytz.timezone('Asia/Taipei')).year
    if official_events and year >= current_year:
        return sorted(official_events, key=lambda event: str(event.get('date', '')))
    fallback_events = _tradingview_macro_events(year, "gdp")
    return _merge_macro_events(fallback_events, official_events)


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_bea_core_pce_events(year):
    """核心 PCE 隨 BEA Personal Income and Outlays 發布，並以 TradingView 補歷史日期。"""
    official_events = _parse_bea_release_schedule(fetch_bea_release_schedule_html(), year, "core_pce")
    current_year = datetime.now(pytz.timezone('Asia/Taipei')).year
    if official_events and year >= current_year:
        return sorted(official_events, key=lambda event: str(event.get('date', '')))
    fallback_events = _tradingview_macro_events(year, "core_pce")
    return _merge_macro_events(fallback_events, official_events)


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_ism_manufacturing_events(year):
    """擷取 ISM 製造業 PMI；僅接受明確的 ISM Manufacturing PMI 主指標。"""
    official_events = []
    # ISM 頁面可能對伺服器回傳驗證頁；保留已由官方 2026 日曆核對的完整日期，補足年底尚未進入備援 API 的月份。
    verified_official_dates = {
        2026: [(1, 5), (2, 2), (3, 2), (4, 1), (5, 1), (6, 1),
               (7, 1), (8, 3), (9, 1), (10, 1), (11, 2), (12, 1)],
    }
    response = None if year in verified_official_dates else _calendar_get(ISM_RELEASE_CALENDAR_URL)
    soup = BeautifulSoup(response.text, "html.parser") if response else BeautifulSoup("", "html.parser")
    month_lookup = {name: index for index, name in enumerate(
        ["January", "February", "March", "April", "May", "June",
         "July", "August", "September", "October", "November", "December"], 1
    )}
    seen = set()
    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        month_match = re.search(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
            cells[0].get_text(" ", strip=True), re.I,
        )
        day_match = re.search(r"\b(\d{1,2})\b", cells[1].get_text(" ", strip=True))
        if not month_match or not day_match or int(month_match.group(2)) != year:
            continue
        taiwan_dt = _taiwan_time_from_eastern(
            year, month_lookup[month_match.group(1).title()], int(day_match.group(1)), 10
        )
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        official_events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"美國 ISM 製造業指數（{taiwan_dt:%H:%M}）",
            "detail": "ISM 官方發布日曆；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "Institute for Supply Management",
            "impact": "high",
        })
    for month, day in verified_official_dates.get(year, []):
        taiwan_dt = _taiwan_time_from_eastern(year, month, day, 10)
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        official_events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"美國 ISM 製造業指數（{taiwan_dt:%H:%M}）",
            "detail": "ISM 2026 官方發布日曆；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "Institute for Supply Management",
            "impact": "high",
        })
    if official_events:
        return sorted(official_events, key=lambda event: str(event.get('date', '')))
    return _tradingview_macro_events(year, "ism_manufacturing")


@st.cache_data(ttl=60 * 60 * 6, max_entries=12, show_spinner=False)
def fetch_twse_holiday_events(year):
    """同步臺灣證券交易所的年度開休市日，保留官方名稱與說明。"""
    response = _calendar_get(TWSE_HOLIDAY_URL.format(roc_year=year - 1911))
    if not response:
        return []
    try:
        payload = response.json()
        if payload.get("stat") != "ok":
            return []
        events = []
        for row in payload.get("data", []):
            if len(row) < 2:
                continue
            event_date = pd.to_datetime(row[0], errors="coerce")
            if pd.isna(event_date):
                continue
            title = str(row[1]).strip()
            detail = str(row[2]).strip() if len(row) > 2 else ""
            # TWSE 年度表列出的「開始交易／最後交易日」可交易；其餘列皆為休市或非交易日。
            is_closed = not any(word in title for word in ("開始交易", "最後交易日", "封關日"))
            events.append({
                "date": event_date.date().isoformat(),
                "title": title,
                "detail": detail,
                "closed": is_closed,
                "temporary": False,
                "source": "TWSE 年度開休市日期",
            })
        return events
    except (ValueError, TypeError, KeyError):
        return []


@st.cache_data(ttl=60 * 15, max_entries=1, show_spinner=False)
def fetch_twse_temporary_closure_events():
    """從證交所最新公告辨識已宣布的突發休市（颱風、天災等）。"""
    response = _calendar_get(TWSE_NEWS_URL)
    if not response:
        return []
    try:
        rows = response.json().get("data", [])
    except (ValueError, AttributeError):
        return []

    events = []
    seen = set()
    date_pattern = re.compile(r"(\d{2,3})年\s*(\d{1,2})月\s*(\d{1,2})日")
    keywords = ("休市", "停止交易", "暫停交易", "市場無交易")
    for row in rows:
        title = str(row[1]).strip() if len(row) > 1 else ""
        if not title or not any(keyword in title for keyword in keywords):
            continue
        # 排除「個別上市公司暫停交易」；只處理證交所的全市場交易狀態。
        if not any(scope in title for scope in ("集中交易市場", "證券市場", "全市場")):
            continue
        # 年度開休市表的公告不是突發事件，避免覆蓋層重複顯示。
        if "開休市日期" in title:
            continue
        # 連假期間的「自 X 日至 Y 日休市」屬年度排程，交由開休市表處理。
        if re.search(r"\d{1,2}月\s*\d{1,2}日\s*至", title):
            continue
        match = date_pattern.search(title)
        if not match:
            continue
        roc_year, month, day = map(int, match.groups())
        actual_year = roc_year + 1911 if roc_year < 1911 else roc_year
        try:
            closure_date = date(actual_year, month, day)
        except ValueError:
            continue
        if closure_date in seen:
            continue
        seen.add(closure_date)
        announcement_detail = _twse_announcement_detail(row, title)
        if "颱風" in announcement_detail or "天然災害" in announcement_detail:
            reason = "颱風／天然災害"
        elif "地震" in announcement_detail:
            reason = "地震"
        elif "豪雨" in announcement_detail:
            reason = "豪雨"
        else:
            reason = "證交所公告"
        events.append({
            "date": closure_date.isoformat(),
            "title": f"⚠️ 台股突發休市｜{reason}",
            "detail": announcement_detail[:220],
            "closed": True,
            "temporary": True,
            "source": "TWSE 最新公告",
        })
    return events


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_fomc_events(year):
    """由 Fed 官方會議日程轉成台灣時間的利率決議時間。"""
    response = _calendar_get(FOMC_CALENDAR_URL)
    if not response:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    heading = soup.find(string=re.compile(fr"{year}\s+FOMC Meetings", re.I))
    if not heading:
        return []
    heading_tag = heading.find_parent(["h2", "h3", "h4", "h5"])
    if not heading_tag:
        return []
    month_numbers = {name: index for index, name in enumerate(
        ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1
    )}
    pattern = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*[-–]\s*(\d{1,2})", re.I)
    events = []
    for sibling in heading_tag.find_all_next():
        if sibling.name in ("h2", "h3", "h4", "h5") and sibling is not heading_tag:
            break
        if sibling.name != "div":
            continue
        text_value = sibling.get_text(" ", strip=True)
        match = pattern.match(text_value)
        if not match:
            continue
        month = month_numbers[match.group(1).title()]
        decision_day = int(match.group(3))
        taiwan_dt = _taiwan_time_from_eastern(year, month, decision_day, 14)
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"FOMC 利率決議（{taiwan_dt:%H:%M}）",
            "detail": f"Fed 官方會議：{match.group(1)} {match.group(2)}–{decision_day}；以台灣時間顯示公布時點。",
            "closed": False,
            "temporary": False,
            "source": "Federal Reserve",
        })
    return events


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_bls_cpi_events(year):
    """CPI 以免金鑰經濟日曆為主，BLS 官方來源作交叉備援。"""
    verified_events = _verified_bls_2026_events(year, "cpi")
    if verified_events:
        # BLS frequently rate-limits cloud servers.  The complete 2026 official
        # schedule is already verified below, so retrying three blocked BLS
        # pages adds roughly ten seconds without improving the result.
        return verified_events
    fallback_events = _tradingview_macro_events(year, "cpi")
    official_events = _parse_bls_release_ics(
        _calendar_get(BLS_RELEASE_ICS_URL), year, "Consumer Price Index", "美國 CPI 公布"
    )
    if not official_events:
        official_events = _parse_bls_year_schedule(
            _calendar_get(BLS_YEAR_SCHEDULE_URL.format(year=year)), year,
            "Consumer Price Index", "美國 CPI 公布",
        )
    if not official_events:
        official_events = _parse_official_release_schedule(
            _calendar_get(BLS_CPI_URL), year, "美國 CPI 公布", "U.S. Bureau of Labor Statistics"
        )
    if official_events:
        return sorted(official_events, key=lambda event: str(event.get('date', '')))
    return _tradingview_macro_events(year, "ism_manufacturing")


def _parse_official_release_schedule(response, year, event_title, source):
    """解析美國官方發布排程的日期與美東時間，統一換算成台灣時間。"""
    if not response or "Access Denied" in response.text:
        return []
    text_value = BeautifulSoup(response.text, "html.parser").get_text(" ", strip=True)
    month_lookup = {name: index for index, name in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1
    )}
    pattern = re.compile(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s*(\d{1,2}),\s*(\d{4})\s*(\d{1,2}):(\d{2})\s*(AM|PM)",
        re.I,
    )
    events, seen = [], set()
    for match in pattern.finditer(text_value):
        month_name, day, event_year, hour, minute, meridiem = match.groups()
        if int(event_year) != year:
            continue
        hour = int(hour) % 12 + (12 if meridiem.upper() == "PM" else 0)
        taiwan_dt = _taiwan_time_from_eastern(year, month_lookup[month_name.title()], int(day), hour, int(minute))
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": f"{source} 官方排程；以台灣時間顯示。",
            "closed": False,
            "temporary": False,
            "source": source,
            "impact": "high",
        })
    return events


def _parse_bls_year_schedule(response, year, release_name, event_title):
    """解析 BLS 年度總表；此頁比個別新聞稿排程頁更穩定。"""
    if not response or "Access Denied" in response.text:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    month_lookup = {name: index for index, name in enumerate(
        ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1
    )}
    date_pattern = re.compile(
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
        r"(\d{1,2}),\s*(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)",
        re.I,
    )
    events, seen = [], set()
    row_texts = [row.get_text(" ", strip=True) for row in soup.select("tr")]
    if not row_texts:
        row_texts = [text.strip() for text in soup.stripped_strings]
    for row_text in row_texts:
        if release_name.lower() not in row_text.lower():
            continue
        match = date_pattern.search(row_text)
        if not match:
            continue
        month_name, day, event_year, hour, minute, meridiem = match.groups()
        if int(event_year) != year:
            continue
        hour = int(hour) % 12 + (12 if meridiem.upper() == "PM" else 0)
        taiwan_dt = _taiwan_time_from_eastern(
            year, month_lookup[month_name.title()], int(day), hour, int(minute)
        )
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": "BLS 官方年度發布總表；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "U.S. Bureau of Labor Statistics（年度總表）",
            "impact": "high",
        })
    return events


def _parse_bls_release_ics(response, year, summary_pattern, event_title):
    """從 BLS 官方 iCalendar 擷取指定新聞稿。"""
    if not response:
        return []
    events, seen = [], set()
    for block in re.split(r"BEGIN:VEVENT", response.text, flags=re.I)[1:]:
        unfolded = re.sub(r"\r?\n[ \t]", "", block)
        if not re.search(rf"SUMMARY[^:]*:.*(?:{summary_pattern})", unfolded, re.I):
            continue
        match = re.search(r"DTSTART([^:]*):(\d{8})(?:T(\d{6}))?(Z?)", unfolded, re.I)
        if not match:
            continue
        _attributes, date_digits, time_digits, utc_suffix = match.groups()
        try:
            release_dt = datetime.strptime(date_digits + (time_digits or "083000"), "%Y%m%d%H%M%S")
            if utc_suffix.upper() == "Z":
                taiwan_dt = pytz.utc.localize(release_dt).astimezone(pytz.timezone("Asia/Taipei"))
            else:
                taiwan_dt = pytz.timezone("US/Eastern").localize(release_dt).astimezone(
                    pytz.timezone("Asia/Taipei")
                )
        except (ValueError, pytz.AmbiguousTimeError, pytz.NonExistentTimeError):
            continue
        if taiwan_dt.year != year or taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": "BLS 官方 iCalendar；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "U.S. Bureau of Labor Statistics（iCalendar）",
            "impact": "high",
        })
    return events


def _verified_bls_2026_events(year, release_type):
    """BLS 官網阻擋伺服器請求時，補入已核對的 2026 完整官方排程。"""
    verified_dates = {
        "cpi": [(1, 13), (2, 13), (3, 11), (4, 10), (5, 12), (6, 10),
                (7, 14), (8, 12), (9, 11), (10, 14), (11, 10), (12, 10)],
        "employment": [(1, 9), (2, 11), (3, 6), (4, 3), (5, 8), (6, 5),
                       (7, 2), (8, 7), (9, 4), (10, 2), (11, 6), (12, 4)],
    }
    if year != 2026 or release_type not in verified_dates:
        return []
    event_title = "美國 CPI 公布" if release_type == "cpi" else "美國大非農／失業率"
    events = []
    for month, day in verified_dates[release_type]:
        taiwan_dt = _taiwan_time_from_eastern(year, month, day, 8, 30)
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"{event_title}（{taiwan_dt:%H:%M}）",
            "detail": "BLS 2026 官方發布排程；已由美東時間換算為台灣時間。",
            "closed": False,
            "temporary": False,
            "source": "U.S. Bureau of Labor Statistics（2026 官方排程）",
            "impact": "high",
        })
    return events


def _parse_bls_employment_ics(response, year):
    """由 BLS 官方 iCalendar 備援解析 Employment Situation。"""
    return _parse_bls_release_ics(
        response, year, "Employment Situation", "美國大非農／失業率"
    )


def _parse_bls_cps_employment_calendar(response, year):
    """BLS 主排程與 ICS 都被阻擋時，使用 CPS 官方發布日曆交叉備援。"""
    if not response:
        return []
    text_value = BeautifulSoup(response.text, 'html.parser').get_text(' ', strip=True)
    month_lookup = {name: index for index, name in enumerate(
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], 1
    )}
    date_pattern = re.compile(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s*(\d{1,2}),\s*(\d{4})",
        re.I,
    )
    events, seen = [], set()
    for match in date_pattern.finditer(text_value):
        if int(match.group(3)) != year or 'Employment Situation' not in text_value[match.end():match.end() + 180]:
            continue
        taiwan_dt = _taiwan_time_from_eastern(
            year, month_lookup[match.group(1).title()], int(match.group(2)), 8, 30
        )
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            'date': taiwan_dt.date().isoformat(),
            'title': f"美國大非農／失業率（{taiwan_dt:%H:%M}）",
            'detail': 'BLS CPS 官方發布日曆備援；以台灣時間顯示。',
            'closed': False, 'temporary': False,
            'source': 'U.S. Bureau of Labor Statistics（CPS）', 'impact': 'high',
        })
    return events


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_bls_employment_events(year):
    """BLS Employment Situation：市場俗稱大非農，同時公布失業率。"""
    verified_events = _verified_bls_2026_events(year, "employment")
    if verified_events:
        # Same as CPI: use the already verified full-year official schedule and
        # avoid serial retries against BLS from Streamlit's cloud IP.
        return verified_events
    fallback_events = _tradingview_macro_events(year, "nfp")
    official_events = _parse_bls_employment_ics(_calendar_get(BLS_RELEASE_ICS_URL), year)
    if not official_events:
        official_events = _parse_bls_year_schedule(
            _calendar_get(BLS_YEAR_SCHEDULE_URL.format(year=year)), year,
            "Employment Situation", "美國大非農／失業率",
        )
    if not official_events:
        official_events = _parse_official_release_schedule(
            _calendar_get(BLS_EMPLOYMENT_URL), year,
            "美國大非農／失業率", "U.S. Bureau of Labor Statistics",
        )
    if not official_events:
        official_events = _parse_bls_cps_employment_calendar(_calendar_get(BLS_CPS_CALENDAR_URL), year)
    return _merge_macro_events(fallback_events, official_events)


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def fetch_adp_employment_events(year):
    """由 ADP 官方資料端點取得每月小非農日期，固定 08:15 ET 發布。"""
    official_fallback_dates = {
        # ADP 2026 年官方 Calendar；補回端點會因日期已過而移除的歷史月份。
        2026: [(1, 7), (2, 4), (3, 4), (4, 1), (5, 6), (6, 3), (7, 1),
               (8, 5), (9, 2), (9, 30), (11, 4), (12, 2)],
    }
    report_date_texts = []
    response = (
        None if year in official_fallback_dates
        else _calendar_get(ADP_EMPLOYMENT_DATA_URL)
    )

    if response:
        try:
            payload = response.json()
            # futureReports 先列月度 NER，分隔文字後才是 weekly NER pulse；不可遞迴收集全部日期。
            for report in payload.get("futureReports", []) if isinstance(payload, dict) else []:
                report_date = str(report.get("reportDate", "")).strip() if isinstance(report, dict) else ""
                if "weekly NER pulse" in report_date:
                    break
                if report_date:
                    report_date_texts.append(report_date)
        except (TypeError, ValueError, AttributeError):
            pass
    # JSON 欄位改版或只保留未來日期時，再讀 ADP 官方首頁的 Calendar 文字。
    page_response = (
        _calendar_get(ADP_EMPLOYMENT_PAGE_URL)
        if year not in official_fallback_dates and not report_date_texts
        else None
    )
    if page_response:
        page_text = BeautifulSoup(page_response.text, 'html.parser').get_text(' ', strip=True)
        calendar_match = re.search(
            r"Upcoming Reports:\s*(.*?)(?:Upcoming reports\s*\(weekly|$)",
            page_text, re.I | re.S,
        )
        if calendar_match:
            report_date_texts.append(calendar_match.group(1))
    month_lookup = {name: index for index, name in enumerate(
        ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1
    )}
    pattern = re.compile(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})",
        re.I,
    )
    parsed_dates = []
    for report_text in report_date_texts:
        for match in pattern.finditer(str(report_text)):
            month_name, day, event_year = match.groups()
            if int(event_year) == year:
                parsed_dates.append((month_lookup[month_name.title()], int(day)))
    parsed_dates.extend(official_fallback_dates.get(year, []))

    events, seen = [], set()
    for month, day in parsed_dates:
        taiwan_dt = _taiwan_time_from_eastern(year, month, day, 8, 15)
        if taiwan_dt.date() in seen:
            continue
        seen.add(taiwan_dt.date())
        events.append({
            "date": taiwan_dt.date().isoformat(),
            "title": f"美國小非農 ADP（{taiwan_dt:%H:%M}）",
            "detail": "ADP National Employment Report 官方 Calendar；以台灣時間顯示。",
            "closed": False,
            "temporary": False,
            "source": "ADP Research Institute",
            "impact": "high",
        })
    return sorted(events, key=lambda event: str(event.get("date", "")))


def _us_weekly_claims_release_date(year, month, day):
    """週四遇主要聯邦假日時，依慣例提前至週三；最終仍以 DOL 公告為準。"""
    release_date = date(year, month, day)
    thanksgiving = date(year, 11, 1)
    thanksgiving += timedelta(days=(calendar.THURSDAY - thanksgiving.weekday()) % 7 + 21)
    thursday_holidays = {
        date(year, 1, 1),
        date(year, 6, 19),
        date(year, 7, 4),
        thanksgiving,
        date(year, 12, 25),
    }
    return release_date - timedelta(days=1) if release_date in thursday_holidays else release_date


@st.cache_data(ttl=60 * 60 * 12, max_entries=12, show_spinner=False)
def build_us_initial_claims_events(year):
    """建立美國勞工部每週四 08:30 ET 的初領失業金預定發布時間。"""
    events = []
    # 同時計算下一年第一週，讓元旦遇週四而提前到 12/31 的事件歸在實際發布年份。
    for scheduled_year in (year, year + 1):
        for month in range(1, 13):
            for week in calendar.monthcalendar(scheduled_year, month):
                day = week[calendar.THURSDAY]
                if not day:
                    continue
                release_date = _us_weekly_claims_release_date(scheduled_year, month, day)
                taiwan_dt = _taiwan_time_from_eastern(
                    release_date.year, release_date.month, release_date.day, 8, 30
                )
                if taiwan_dt.year != year:
                    continue
                scheduled_date = date(scheduled_year, month, day)
                holiday_note = "；遇聯邦假日，預定提前至週三" if release_date != scheduled_date else ""
                events.append({
                    "date": taiwan_dt.date().isoformat(),
                    "title": f"美國初領失業金人數（{taiwan_dt:%H:%M}）",
                    "detail": f"U.S. Department of Labor 例行週度發布時間{holiday_note}；最終以官方公告為準。",
                    "closed": False,
                    "temporary": False,
                    "source": "U.S. Department of Labor",
                    "impact": "routine",
                })
    return events


def _calendar_business_days(year, month, closed_dates=()):
    """建立指定月份的工作日；closed_dates 可帶入交易所正式與突發休市日。"""
    closed = set(closed_dates or ())
    return [
        date(year, month, day)
        for day in range(1, calendar.monthrange(year, month)[1] + 1)
        if date(year, month, day).weekday() < 5 and date(year, month, day) not in closed
    ]


def build_us_quadruple_witching_events(year):
    """建立美股季月第三個週五的四巫日；遇美股休市則提前至前一交易日。"""
    events = []
    for month in (3, 6, 9, 12):
        expiration_date = _nth_weekday(year, month, calendar.FRIDAY, 3)
        scheduled_date = expiration_date
        while expiration_date.weekday() >= 5 or expiration_date in _us_market_holidays(expiration_date.year):
            expiration_date -= timedelta(days=1)
        holiday_note = "；原第三個週五遇美股休市，提前至前一交易日" if expiration_date != scheduled_date else ""
        events.append({
            "date": expiration_date.isoformat(),
            "title": "美股四巫日",
            "detail": f"美股股指期貨、股指選擇權與個股選擇權集中到期{holiday_note}。",
            "closed": False,
            "temporary": False,
            "source": "Cboe expiration calendar",
            "impact": "high",
        })
    return events


def build_sgx_ftse_taiwan_settlement_events(year, taiwan_closed_dates=()):
    """依 SGX 規則，以每月倒數第二個臺灣交易日建立富台指期結算事件。"""
    events = []
    for month in range(1, 13):
        business_days = _calendar_business_days(year, month, taiwan_closed_dates)
        if len(business_days) < 2:
            continue
        settlement_date = business_days[-2]
        events.append({
            "date": settlement_date.isoformat(),
            "title": "富台指期結算（TWN）",
            "detail": "SGX FTSE Taiwan Index Futures：契約月倒數第二個臺灣交易日為最後交易／結算日。",
            "closed": False,
            "temporary": False,
            "source": "Singapore Exchange（SGX）",
            "impact": "high",
        })
    return events


def build_msci_quarterly_rebalance_events(year, taiwan_closed_dates=()):
    """將 MSCI 2／5／8／11 月指數檢討標在當月最後臺灣交易日收盤。"""
    events = []
    review_names = {2: "2月季度檢討", 5: "5月半年度檢討", 8: "8月季度檢討", 11: "11月半年度檢討"}
    for month in review_names:
        business_days = _calendar_business_days(year, month, taiwan_closed_dates)
        if not business_days:
            continue
        implementation_date = business_days[-1]
        events.append({
            "date": implementation_date.isoformat(),
            "title": "MSCI 季調生效（收盤後）",
            "detail": f"MSCI {review_names[month]}：依方法論於該月最後交易日收盤實施，次一交易日生效。",
            "closed": False,
            "temporary": False,
            "source": "MSCI Index Review",
            "impact": "high",
        })
    return events


EARNINGS_TICKER_ALIASES = {
    "META": ("META", "Meta Platforms（Facebook）"),
    "FACEBOOK": ("META", "Meta Platforms（Facebook）"),
    "FB": ("META", "Meta Platforms（Facebook）"),
    "GOOGLE": ("GOOGL", "Alphabet（Google）"),
    "ALPHABET": ("GOOGL", "Alphabet（Google）"),
    "GOOG": ("GOOGL", "Alphabet（Google）"),
    "GOOGL": ("GOOGL", "Alphabet（Google）"),
    "TESLA": ("TSLA", "Tesla"),
    "TSLA": ("TSLA", "Tesla"),
    "TSMC": ("2330.TW", "台積電（TSMC）"),
    "TAIWAN SEMICONDUCTOR": ("2330.TW", "台積電（TSMC）"),
    "APPLE": ("AAPL", "Apple"),
    "AMAZON": ("AMZN", "Amazon"),
    "MICROSOFT": ("MSFT", "Microsoft"),
    "NVIDIA": ("NVDA", "NVIDIA"),
}


def resolve_earnings_ticker(user_input):
    """將公司俗稱、台股代碼或 Yahoo 代碼轉成可查詢的財報標的。"""
    raw = str(user_input).strip()
    normalized = re.sub(r"\s+", " ", raw.upper())
    if normalized in EARNINGS_TICKER_ALIASES:
        ticker, display_name = EARNINGS_TICKER_ALIASES[normalized]
        return {"input": raw, "display_name": display_name, "candidates": (ticker,)}

    # 台股公司中文名稱與代碼使用本機名單；純數字先查上市，再嘗試上櫃。
    try:
        code_map, name_map = load_local_stock_names()
    except Exception:
        code_map, name_map = {}, {}
    if raw in name_map:
        code = name_map[raw]
        return {"input": raw, "display_name": f"{raw}（{code}）", "candidates": (f"{code}.TW", f"{code}.TWO")}
    if normalized.isdigit() and len(normalized) in (4, 5, 6):
        display_name = code_map.get(normalized, normalized)
        return {"input": raw, "display_name": f"{display_name}（{normalized}）", "candidates": (f"{normalized}.TW", f"{normalized}.TWO")}

    # 已輸入台股市場尾碼時仍補上公司名稱；其他市場尾碼則不改寫。
    if normalized.endswith((".TW", ".TWO")):
        code = normalized.split(".", 1)[0]
        display_name = code_map.get(code, code)
        return {"input": raw, "display_name": f"{display_name}（{code}）", "candidates": (normalized,)}
    # 其他輸入視為 Yahoo Finance 可識別的美股代碼。
    return {"input": raw, "display_name": normalized, "candidates": (normalized,)}


def _format_earnings_time(raw_datetime):
    """將 Yahoo 的日期／時間轉為台灣時間，並標明美股公布時段。"""
    parsed = pd.to_datetime(raw_datetime, errors="coerce")
    if pd.isna(parsed):
        return None, None

    has_explicit_time = bool(parsed.hour or parsed.minute or parsed.second)
    if not has_explicit_time:
        return parsed.date(), "公布時間待公司公告"

    eastern = pytz.timezone("US/Eastern")
    taipei = pytz.timezone("Asia/Taipei")
    if parsed.tzinfo is None:
        eastern_dt = eastern.localize(parsed.to_pydatetime())
    else:
        eastern_dt = parsed.tz_convert(eastern)
    taiwan_dt = eastern_dt.astimezone(taipei)
    eastern_hour = eastern_dt.hour + eastern_dt.minute / 60
    if eastern_hour >= 16:
        session = "美股收盤後"
    elif eastern_hour < 9.5:
        session = "美股盤前"
    else:
        session = "美股盤中"
    return taiwan_dt.date(), f"{session}（台灣時間 {taiwan_dt:%m/%d %H:%M}）"


def _get_earnings_dates(ticker_object):
    """優先採 Yahoo 財報行事曆的帶時間資料，無資料時退回 calendar 欄位。"""
    try:
        earnings_table = ticker_object.get_earnings_dates(limit=8)
        if isinstance(earnings_table, pd.DataFrame) and not earnings_table.empty:
            return list(earnings_table.index)
    except Exception:
        pass
    try:
        calendar_data = ticker_object.calendar
        dates = calendar_data.get("Earnings Date", []) if isinstance(calendar_data, dict) else []
        return list(dates) if isinstance(dates, (list, tuple, pd.Series, np.ndarray)) else [dates]
    except Exception:
        return []


@st.cache_data(ttl=60 * 60 * 6, max_entries=24, show_spinner=False)
def fetch_earnings_events(inputs):
    """查詢指定公司財報日，回傳事件與未找到日期的輸入，避免靜默漏顯示。"""
    events, resolved, missing = [], [], []
    today = datetime.now(pytz.timezone("Asia/Taipei")).date()
    for user_input in inputs:
        item = resolve_earnings_ticker(user_input)
        candidate_events = []
        selected_ticker = None
        for ticker in item["candidates"]:
            for earnings_date in _get_earnings_dates(yf.Ticker(ticker)):
                event_date, time_label = _format_earnings_time(earnings_date)
                if event_date is None or event_date < today:
                    continue
                candidate_events.append((event_date, time_label))
            if candidate_events:
                selected_ticker = ticker
                break
        if not candidate_events:
            missing.append(f"{item['input']} → {item['display_name']}（尚無 Yahoo 財報日期）")
            continue

        resolved.append(f"{item['input']} → {item['display_name']}（{selected_ticker}）")
        seen_dates = set()
        for event_date, time_label in candidate_events:
            if event_date in seen_dates:
                continue
            seen_dates.add(event_date)
            events.append({
                "date": event_date.isoformat(),
                "title": f"{item['display_name']} 財報預估日",
                "detail": f"Yahoo Finance｜{time_label}；日期可能為預估值，請以公司公告為準。",
                "closed": False,
                "temporary": False,
                "source": "Yahoo Finance",
                "ticker": selected_ticker,
                "market": (
                    "台股" if re.fullmatch(r"\d{4,6}\.(?:TW|TWO)", str(selected_ticker or ""), re.I)
                    else "美股"
                ),
            })
    return {"events": events, "resolved": resolved, "missing": missing}


TWSE_MONTHLY_REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TPEX_MONTHLY_REVENUE_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
MOPS_MONTHLY_REVENUE_PAGE_URL = "https://mopsov.twse.com.tw/mops/web/t05st10_ifrs"
MOPS_MONTHLY_REVENUE_QUERY_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t05st10_ifrs"


def _to_number(value):
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _blank_display_text(value):
    """將資料表空值統一轉成空白，避免顯示 None／nan／NaT。"""
    if value is None:
        return ''
    if isinstance(value, str):
        text = value.strip()
        return '' if text.lower() in {'none', 'nan', 'nat', '<na>', 'null'} else text
    try:
        missing = pd.isna(value)
        if isinstance(missing, (bool, np.bool_)) and missing:
            return ''
    except (TypeError, ValueError):
        pass
    return str(value)


def _compact_number_text(value):
    """以最短有效格式顯示數值；空值維持空白。"""
    number = _to_number(value)
    if number is None or not math.isfinite(number):
        return ''
    return format(number, '.12g')


def _format_compact_number(value, max_decimals=2, signed=False, thousands=True):
    """保留必要小數並移除尾端 0，供操作計畫卡片顯示。"""
    number = _to_number(value)
    if number is None or not math.isfinite(number):
        return '—'
    sign = '+' if signed else ''
    separator = ',' if thousands else ''
    text_value = f"{number:{sign}{separator}.{max(0, int(max_decimals))}f}"
    if '.' in text_value:
        text_value = text_value.rstrip('0').rstrip('.')
    return text_value


# Pandas Styler／Streamlit 表格若未另外指定格式，也統一去除浮點尾端 0。
pd.options.display.float_format = lambda value: _format_compact_number(value, 8)


def _content_column_width(values, min_width=44, max_width=180, full_content=False):
    """只依儲存格內容估算欄寬，不讓較長的欄位標題撐出大段空白。"""
    try:
        series = pd.Series(values).dropna().astype(str)
    except (TypeError, ValueError):
        return min_width
    if series.empty:
        return min_width
    lengths = series.map(lambda text: sum(2 if ord(char) > 127 else 1 for char in text))
    content_length = (
        float(lengths.max())
        if full_content or len(lengths) <= 4 else float(lengths.quantile(0.95))
    )
    return max(min_width, min(max_width, int(content_length * 7 + 12)))


def compact_table_column_config(frame, min_width=44, max_width=180):
    """建立內容優先的緊湊表格欄寬，數值欄同步移除尾端 0。"""
    raw_frame = getattr(frame, "data", frame)
    if not isinstance(raw_frame, pd.DataFrame):
        return {}
    config = {}
    for column in raw_frame.columns:
        width = _content_column_width(raw_frame[column], min_width, max_width)
        if pd.api.types.is_numeric_dtype(raw_frame[column]):
            config[column] = st.column_config.NumberColumn(format="%.12g", width=width)
        else:
            config[column] = st.column_config.TextColumn(width=width)
    return config


def render_index_plan_metric_cards(items):
    """指數計畫主數值採較小字級；補充風險資訊維持原尺寸。"""
    cards = ''.join(
        "<div class='index-plan-main-card'>"
        f"<div class='index-plan-main-label'>{html.escape(str(label))}</div>"
        f"<div class='index-plan-main-value'>{html.escape(str(value))}</div>"
        "</div>"
        for label, value in items
    )
    st.markdown(
        "<style>"
        ".index-plan-main-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1rem;margin:.2rem 0 .75rem}"
        ".index-plan-main-card{text-align:center;min-width:0;padding:.15rem .35rem .45rem;border-bottom:1px solid rgba(128,128,128,.28)}"
        ".index-plan-main-label{font-size:13px;font-weight:650;color:#f4f6f8;white-space:nowrap}"
        ".index-plan-main-value{font-size:27px;line-height:1.22;font-weight:650;color:#f7f8fa;white-space:nowrap}"
        "@media(max-width:700px){.index-plan-main-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:.55rem}.index-plan-main-value{font-size:23px}}"
        "</style>"
        f"<div class='index-plan-main-grid'>{cards}</div>",
        unsafe_allow_html=True,
    )


def render_index_level_metric(container, label, value):
    """指數計畫頂端價位採與即時微台相近的緊湊字級。"""
    container.markdown(
        "<div style='line-height:1.2;text-align:left'>"
        f"<div style='font-size:13px;font-weight:650;color:#f4f6f8'>{html.escape(str(label))}</div>"
        f"<div style='font-size:25px;font-weight:650;color:#f7f8fa;margin-top:5px;white-space:nowrap'>{html.escape(str(value))}</div>"
        "</div>",
        unsafe_allow_html=True,
    )


def _roc_compact_date(value):
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 7:
        return None
    try:
        return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:]))
    except ValueError:
        return None


def _signed_percent(value):
    number = _to_number(value)
    return "--" if number is None else f"{_format_compact_number(number, 2, signed=True)}%"


def _latest_completed_roc_month():
    """回傳依月營收申報期限可期待的最新民國年月。"""
    today = datetime.now(pytz.timezone("Asia/Taipei")).date()
    last_day = today.replace(day=1) - timedelta(days=1)
    # 月營收通常在次月 10 日前公告；10 日以前維持前一個已完整公告月份，
    # 11 日起才要求前一個曆月，避免把仍可用的最新資料誤判為過期。
    if today.day <= 10:
        last_day = last_day.replace(day=1) - timedelta(days=1)
    return last_day.year - 1911, last_day.month


def _previous_roc_month(roc_year, month):
    if month == 1:
        return roc_year - 1, 12
    return roc_year, month - 1


def _roc_month_text(roc_year, month):
    return f"{int(roc_year):03d}{int(month):02d}"


@st.cache_data(ttl=60 * 60 * 4, max_entries=128, show_spinner=False)
def fetch_mops_company_monthly_revenue(code, roc_year, month, market_type):
    """補查 MOPS 單一公司月營收，處理 TWSE 彙總 OpenAPI 更新落後的公告空窗。"""
    payload = {
        "step": "1",
        "firstin": "1",
        "off": "1",
        "queryName": "co_id",
        "inpuType": "co_id",
        # MOPS 單一公司查詢可同時辨識上市與上櫃；傳 sii／otc 會使部分代號
        # 在資料彙總更新前查不到最新月份。
        "TYPEK": "all",
        "isnew": "false",
        "co_id": str(code),
        "year": str(roc_year),
        "month": f"{int(month):02d}",
    }
    headers = {
        **CALENDAR_HTTP_HEADERS,
        "Referer": MOPS_MONTHLY_REVENUE_PAGE_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        with requests.Session() as session:
            session.headers.update(CALENDAR_HTTP_HEADERS)
            session.get(MOPS_MONTHLY_REVENUE_PAGE_URL, timeout=12)
            response = session.post(
                MOPS_MONTHLY_REVENUE_QUERY_URL,
                data=payload,
                headers=headers,
                timeout=12,
            )
            response.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    expected_month = f"民國{roc_year}年{int(month):02d}月"
    if expected_month not in page_text or "本資料由" not in page_text:
        return None

    values = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
            if len(cells) < 2:
                continue
            label, value = cells[0], cells[1]
            if label in {"本月", "去年同期", "增減百分比", "本年累計", "去年累計"}:
                values.setdefault(label, []).append(value)
    required_labels = {"本月", "去年同期", "增減百分比", "本年累計", "去年累計"}
    if not required_labels.issubset(values):
        return None

    company_match = re.search(r"\)(.+?)\s*公司提供", page_text)
    note_match = re.search(
        r"備註\s*/\s*營收變化原因說明\s*[:：]?\s*(.*?)(?=\s*1\.各項增減百分比資訊|$)",
        page_text,
    )
    return {
        "公司代號": str(code),
        "公司名稱": company_match.group(1).strip() if company_match else str(code),
        "資料年月": _roc_month_text(roc_year, month),
        "營業收入-當月營收": values["本月"][0],
        "營業收入-去年當月營收": values["去年同期"][0],
        "營業收入-去年同月增減(%)": values["增減百分比"][0],
        "累計營業收入-當月累計營收": values["本年累計"][0],
        "累計營業收入-去年累計營收": values["去年累計"][0],
        "累計營業收入-前期比較增減(%)": values["增減百分比"][1] if len(values["增減百分比"]) > 1 else None,
        "備註": note_match.group(1).strip() if note_match else "-",
        "_mops_direct": True,
        # MOPS 單一公司頁不提供出表日期，採系統首次偵測到資料的台灣日期標示。
        "_report_date": datetime.now(pytz.timezone("Asia/Taipei")).date().isoformat(),
    }


def _signed_percent_arrow(value):
    """以臺股慣例顯示漲紅跌綠所搭配的方向箭頭。"""
    number = _to_number(str(value).replace('%', ''))
    if number is None:
        return '—'
    arrow = '↑' if number > 0 else ('↓' if number < 0 else '→')
    return f"{arrow} {_format_compact_number(number, 2, signed=True)}%"


def _percent_badge_html(value, font_size=14):
    """產生可放入精簡市場卡的紅漲綠跌百分比。"""
    number = _to_number(str(value).replace('%', ''))
    if number is None:
        return "<span style='color:#9e9e9e;'>—</span>"
    color = '#ff4b4b' if number > 0 else ('#00c853' if number < 0 else '#9e9e9e')
    return (
        f"<span style='color:{color};font-size:{font_size}px;font-weight:800;'>"
        f"{_signed_percent_arrow(number)}</span>"
    )


def _score_color(value):
    """將 0–100 分映射為低分綠、高分紅的連續色階。"""
    score = _to_number(value)
    if score is None:
        return '#9e9e9e'
    ratio = min(100, max(0, score)) / 100
    if ratio <= 0.5:
        # 綠 → 黃：低分一眼辨識為偏弱。
        progress = ratio / 0.5
        red = round(255 * progress)
        green = round(200 + (179 - 200) * progress)
        blue = round(83 * (1 - progress))
    else:
        # 黃 → 紅：分數越高，紅色越明確。
        progress = (ratio - 0.5) / 0.5
        red = 255
        green = round(179 + (75 - 179) * progress)
        blue = round(75 * progress)
    return f'#{red:02x}{green:02x}{blue:02x}'


def _thousand_currency(value):
    number = _to_number(value)
    if number is None:
        return "--"
    amount_ntd = int(round(number * 1000))
    trillions, remainder = divmod(abs(amount_ntd), 1_000_000_000_000)
    hundred_millions, remainder = divmod(remainder, 100_000_000)
    ten_thousands = remainder // 10_000
    sign = "-" if amount_ntd < 0 else ""
    parts = []
    if trillions:
        parts.append(f"{trillions:,}兆")
    if hundred_millions or parts:
        parts.append(f"{hundred_millions:,}億")
    parts.append(f"{ten_thousands:,}萬元")
    return sign + "".join(parts)


def _percent_color(value):
    number = _to_number(str(value).replace("%", ""))
    if number is None:
        return "#B0BEC5"
    return "#FF5252" if number >= 0 else "#00E676"


def _revenue_metric_html(label, value, change_text):
    """以較緊湊的營收卡顯示箭頭增減；正紅、負綠符合台股慣例。"""
    match = re.search(r"[+-]?\d+(?:\.\d+)?", str(change_text))
    number = float(match.group()) if match else None
    arrow = "↑" if number is not None and number >= 0 else "↓" if number is not None else "–"
    color = _percent_color(str(number)) if number is not None else "#B0BEC5"
    return (
        "<div class='revenue-metric-card'>"
        f"<div class='revenue-metric-label'>{html.escape(label)}</div>"
        f"<div class='revenue-metric-value'>{html.escape(value)}</div>"
        f"<div class='revenue-metric-delta' style='color:{color};'>{arrow} {html.escape(change_text)}</div>"
        "</div>"
    )


def _usd_currency(value):
    number = _to_number(value)
    if number is None:
        return "--"
    if abs(number) >= 1_000_000_000:
        return f"US${_format_compact_number(number / 1_000_000_000, 2)}B"
    if abs(number) >= 1_000_000:
        return f"US${_format_compact_number(number / 1_000_000, 2)}M"
    return f"US${number:,.0f}"


@st.cache_data(ttl=60 * 60 * 4, max_entries=1, show_spinner=False)
def fetch_twse_monthly_revenue_rows():
    """取得 MOPS 最新上市與上櫃公司月營收彙總表。"""
    rows = []
    for url in (TWSE_MONTHLY_REVENUE_URL, TPEX_MONTHLY_REVENUE_URL):
        response = _calendar_get(url)
        if not response:
            continue
        try:
            data = response.json()
            if isinstance(data, list):
                rows.extend(data)
        except ValueError:
            continue
    return rows


def select_latest_monthly_revenue_rows(rows):
    """Keep only each company's newest reported ROC month from mixed official feeds."""
    latest_rows = {}
    for source_row in rows or []:
        if not isinstance(source_row, dict):
            continue
        code = str(source_row.get("公司代號", "")).strip()
        month = str(source_row.get("資料年月", "")).strip()
        if not code or not re.fullmatch(r"\d{5}", month):
            continue
        current = latest_rows.get(code)
        if current is None or month > str(current.get("資料年月", "")).strip():
            latest_rows[code] = source_row
    return latest_rows


@st.cache_data(ttl=60 * 60 * 4, max_entries=100, show_spinner=False)
def fetch_finmind_monthly_revenue_rows(code, start_year):
    """Fetch one company's monthly revenue from FinMind as a secondary public source."""
    try:
        response = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={
                "dataset": "TaiwanStockMonthRevenue",
                "data_id": str(code),
                "start_date": f"{int(start_year):04d}-01-01",
            },
            headers={"User-Agent": "Mozilla/5.0 (compatible; StockApp/1.0)"},
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        return rows if isinstance(rows, list) else []
    except (requests.RequestException, ValueError, TypeError):
        return []


def build_finmind_monthly_revenue_row(records, code, target_year, target_month, company_name):
    """Convert FinMind's TWD records to the MOPS thousand-TWD row used by the calendar."""
    valid_records = [record for record in records or [] if isinstance(record, dict)]

    def find_record(year, month):
        return next((
            record for record in reversed(valid_records)
            if int(_to_number(record.get("revenue_year")) or 0) == int(year)
            and int(_to_number(record.get("revenue_month")) or 0) == int(month)
        ), None)

    current = find_record(target_year, target_month)
    if not current:
        eligible = [
            record for record in valid_records
            if (
                int(_to_number(record.get("revenue_year")) or 0),
                int(_to_number(record.get("revenue_month")) or 0),
            ) <= (int(target_year), int(target_month))
        ]
        current = max(
            eligible,
            key=lambda record: (
                int(_to_number(record.get("revenue_year")) or 0),
                int(_to_number(record.get("revenue_month")) or 0),
            ),
            default=None,
        )
    if not current:
        return None
    selected_year = int(_to_number(current.get("revenue_year")) or 0)
    selected_month = int(_to_number(current.get("revenue_month")) or 0)
    previous_year, previous_month = (
        (selected_year - 1, 12) if selected_month == 1 else (selected_year, selected_month - 1)
    )
    previous = find_record(previous_year, previous_month)
    last_year = find_record(selected_year - 1, selected_month)

    def revenue_thousands(record):
        value = _to_number(record.get("revenue")) if record else None
        return value / 1000 if value is not None else None

    current_revenue = revenue_thousands(current)
    previous_revenue = revenue_thousands(previous)
    last_year_revenue = revenue_thousands(last_year)

    def change_percent(value, comparison):
        if value is None or comparison in (None, 0):
            return None
        return (value - comparison) / comparison * 100

    current_ytd = sum(
        revenue_thousands(record) or 0
        for record in valid_records
        if int(_to_number(record.get("revenue_year")) or 0) == selected_year
        and 1 <= int(_to_number(record.get("revenue_month")) or 0) <= selected_month
    )
    last_year_ytd = sum(
        revenue_thousands(record) or 0
        for record in valid_records
        if int(_to_number(record.get("revenue_year")) or 0) == selected_year - 1
        and 1 <= int(_to_number(record.get("revenue_month")) or 0) <= selected_month
    )
    report_date = str(current.get("create_time") or current.get("date") or "")[:10]
    return {
        "公司代號": str(code),
        "公司名稱": str(company_name),
        "資料年月": _roc_month_text(selected_year - 1911, selected_month),
        "營業收入-當月營收": current_revenue,
        "營業收入-上月營收": previous_revenue,
        "營業收入-去年當月營收": last_year_revenue,
        "營業收入-上月比較增減(%)": change_percent(current_revenue, previous_revenue),
        "營業收入-去年同月增減(%)": change_percent(current_revenue, last_year_revenue),
        "累計營業收入-當月累計營收": current_ytd,
        "累計營業收入-去年累計營收": last_year_ytd,
        "累計營業收入-前期比較增減(%)": change_percent(current_ytd, last_year_ytd),
        "備註": "FinMind 月營收 API 備援",
        "_report_date": report_date,
        "_source": "FinMind TaiwanStockMonthRevenue",
    }


def select_cnyes_revenue_announcement_date(items, code, revenue_year, revenue_month):
    """Return the earliest matching public revenue-report date in the release window.

    MOPS is the value source.  This only supplies a date when a public report names
    both the company code and the reported month; it never replaces MOPS revenue.
    """
    try:
        report_year, report_month = int(revenue_year), int(revenue_month)
        window_year, window_month = (
            (report_year + 1, 1) if report_month == 12 else (report_year, report_month + 1)
        )
        window_start = date(window_year, window_month, 1)
        window_end = date(window_year, window_month, 15)
    except (TypeError, ValueError):
        return None

    matched_dates = []
    code_pattern = re.compile(rf"(?<!\d){re.escape(str(code))}(?!\d)")
    month_pattern = re.compile(rf"{report_month}\s*月")
    taipei = pytz.timezone("Asia/Taipei")
    for item in items or []:
        if not isinstance(item, dict):
            continue
        text = " ".join(
            html.unescape(str(item.get(field, "")))
            for field in ("title", "content", "summary", "keyword")
        )
        if "營收" not in text or not code_pattern.search(text) or not month_pattern.search(text):
            continue
        timestamp = _to_number(item.get("publishAt") or item.get("publish_at"))
        if timestamp is None:
            continue
        try:
            published_date = datetime.fromtimestamp(float(timestamp), taipei).date()
        except (OSError, OverflowError, TypeError, ValueError):
            continue
        if window_start <= published_date <= window_end:
            matched_dates.append(published_date)
    return min(matched_dates).isoformat() if matched_dates else None


def extract_cnyes_search_items(payload):
    """Support both the former data.items and current items.data envelopes."""
    if not isinstance(payload, dict):
        return []
    data_container = payload.get('data')
    if isinstance(data_container, dict):
        rows = data_container.get('items')
        if isinstance(rows, list):
            return rows
        if isinstance(rows, dict) and isinstance(rows.get('data'), list):
            return rows['data']
    item_container = payload.get('items')
    if isinstance(item_container, list):
        return item_container
    if isinstance(item_container, dict) and isinstance(item_container.get('data'), list):
        return item_container['data']
    return []


@st.cache_data(ttl=60 * 60 * 4, max_entries=300, show_spinner=False)
def fetch_cnyes_revenue_announcement_date(code, revenue_year, revenue_month):
    """Find a corroborating public release date without using its revenue value."""
    items = []
    for page in range(1, 4):
        page_items = None
        for _attempt in range(2):
            try:
                response = requests.get(
                    "https://api.cnyes.com/media/api/v1/search/news",
                    params={"q": str(code), "page": page},
                    headers={"User-Agent": "Mozilla/5.0 (compatible; StockApp/1.0)"},
                    timeout=10,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    continue
                page_items = extract_cnyes_search_items(payload)
                break
            except (requests.RequestException, ValueError, TypeError):
                continue
        if not page_items:
            break
        items.extend(page_items)
    return select_cnyes_revenue_announcement_date(items, code, revenue_year, revenue_month)


def select_google_news_revenue_announcement_date(
    rss_content, code, company_name, revenue_year, revenue_month
):
    """Select the earliest matching monthly-revenue date from Google News RSS."""
    try:
        report_year, report_month = int(revenue_year), int(revenue_month)
        window_year, window_month = (
            (report_year + 1, 1) if report_month == 12 else (report_year, report_month + 1)
        )
        window_start = date(window_year, window_month, 1)
        window_end = date(window_year, window_month, 15)
    except (TypeError, ValueError):
        return None

    soup = BeautifulSoup(rss_content or b"", "xml")
    month_pattern = re.compile(rf"{report_month}\s*月")
    code_pattern = re.compile(rf"(?<!\d){re.escape(str(code))}(?!\d)")
    company_text = str(company_name or "").strip()
    taipei = pytz.timezone("Asia/Taipei")
    matched_dates = []
    for item in soup.find_all("item"):
        title_node, date_node = item.find("title"), item.find("pubDate")
        title = html.unescape(title_node.get_text(" ", strip=True)) if title_node else ""
        if "營收" not in title or not month_pattern.search(title):
            continue
        if not code_pattern.search(title) and (not company_text or company_text not in title):
            continue
        try:
            published_date = parsedate_to_datetime(
                date_node.get_text(strip=True)
            ).astimezone(taipei).date()
        except (AttributeError, TypeError, ValueError):
            continue
        if window_start <= published_date <= window_end:
            matched_dates.append(published_date)
    return min(matched_dates).isoformat() if matched_dates else None


def choose_revenue_announcement_date(
    finmind_date=None, cnyes_date=None, google_news_date=None,
    revenue_year=None, revenue_month=None,
):
    """Choose a release date with FinMind primary and news only as fallback."""
    release_window = None
    try:
        report_year, report_month = int(revenue_year), int(revenue_month)
        next_year, next_month = (
            (report_year + 1, 1) if report_month == 12 else (report_year, report_month + 1)
        )
        release_window = (
            date(next_year, next_month, 1), date(next_year, next_month, 15),
        )
    except (TypeError, ValueError):
        pass
    for source, value in (
        ('FinMind 收錄日', finmind_date),
        ('鉅亨網直接營收報導', cnyes_date),
        ('Google News 營收報導', google_news_date),
    ):
        text = str(value or '').strip()
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
            try:
                parsed = date.fromisoformat(text)
            except ValueError:
                continue
            if release_window and not release_window[0] <= parsed <= release_window[1]:
                continue
            return parsed.isoformat(), source
    return None, ''


@st.cache_data(ttl=60 * 60 * 4, max_entries=300, show_spinner=False)
def fetch_google_news_revenue_announcement_date(
    code, company_name, revenue_year, revenue_month
):
    """Use an independent public-news feed as a second date source."""
    response_content = None
    for _attempt in range(2):
        try:
            response = requests.get(
                "https://news.google.com/rss/search",
                params={
                    "q": f"{code} {int(revenue_month)}月營收",
                    "hl": "zh-TW",
                    "gl": "TW",
                    "ceid": "TW:zh-Hant",
                },
                headers={"User-Agent": "Mozilla/5.0 (compatible; StockApp/1.0)"},
                timeout=10,
            )
            response.raise_for_status()
            response_content = response.content
            break
        except (requests.RequestException, TypeError, ValueError):
            continue
    if not response_content:
        return None
    return select_google_news_revenue_announcement_date(
        response_content, code, company_name, revenue_year, revenue_month
    )


@st.cache_data(ttl=60 * 60 * 4, max_entries=24, show_spinner=False)
def fetch_taiwan_monthly_revenue_events(inputs):
    """依追蹤清單產生台股最新月營收事件，必要時由 MOPS 單一公司資料補齊。"""
    input_by_code = {}
    for user_input in inputs:
        item = resolve_earnings_ticker(user_input)
        for ticker in item["candidates"]:
            match = re.fullmatch(r"(\d{4,6})\.(?:TW|TWO)", ticker)
            if match:
                input_by_code.setdefault(match.group(1), item)
                break
    if not input_by_code:
        return {"events": [], "missing": []}

    revenue_rows = fetch_twse_monthly_revenue_rows()
    rows_by_code = select_latest_monthly_revenue_rows(revenue_rows)
    target_roc_year, target_month = _latest_completed_roc_month()
    target_month_text = _roc_month_text(target_roc_year, target_month)
    previous_roc_year, previous_month = _previous_roc_month(target_roc_year, target_month)
    previous_month_text = _roc_month_text(previous_roc_year, previous_month)
    events, missing = [], []
    for code, item in input_by_code.items():
        row = rows_by_code.get(code)
        row_month = str(row.get("資料年月", "")).strip() if row else ""
        finmind_records = fetch_finmind_monthly_revenue_rows(
            code, target_roc_year + 1911 - 1
        )
        finmind_row = build_finmind_monthly_revenue_row(
            finmind_records,
            code,
            target_roc_year + 1911,
            target_month,
            item["display_name"],
        )
        finmind_report_date = finmind_row.get("_report_date") if finmind_row else None
        direct_row = None
        if not row or row_month < target_month_text:
            for market_type in ("sii", "otc", "all"):
                direct_row = fetch_mops_company_monthly_revenue(
                    code, target_roc_year, target_month, market_type
                )
                if direct_row:
                    break
            if direct_row:
                previous_row = None
                finmind_month = str(finmind_row.get("資料年月", "")) if finmind_row else ""
                if finmind_row and finmind_month == str(direct_row.get("資料年月", "")):
                    direct_row["營業收入-上月營收"] = finmind_row.get("營業收入-上月營收")
                    direct_row["營業收入-上月比較增減(%)"] = finmind_row.get(
                        "營業收入-上月比較增減(%)"
                    )
                elif row and str(row.get("資料年月", "")).strip() == previous_month_text:
                    previous_row = row
                else:
                    previous_row = None
                    for market_type in ("sii", "otc", "all"):
                        previous_row = fetch_mops_company_monthly_revenue(
                            code, previous_roc_year, previous_month, market_type
                        )
                        if previous_row:
                            break
                if not direct_row.get("營業收入-上月營收"):
                    previous_revenue = previous_row.get("營業收入-當月營收") if previous_row else None
                    direct_row["營業收入-上月營收"] = previous_revenue
                    current_number = _to_number(direct_row["營業收入-當月營收"])
                    previous_number = _to_number(previous_revenue)
                    if current_number is not None and previous_number not in (None, 0):
                        direct_row["營業收入-上月比較增減(%)"] = (
                            (current_number - previous_number) / previous_number * 100
                        )

        # 數值優先採 MOPS；若 MOPS 彙總尚未更新則採最新 FinMind。MOPS 單一
        # 公司頁未提供確切出表日，因此保留原本「系統偵測日」的誠實標示，不把
        # FinMind 收錄時間誤稱為官方公告日。
        candidates = [candidate for candidate in (row, direct_row, finmind_row) if candidate]
        if candidates:
            row = max(
                candidates,
                key=lambda candidate: (
                    str(candidate.get("資料年月", "")),
                    2 if candidate.get("_mops_direct") else (1 if not candidate.get("_source") else 0),
                ),
            )
        if not row:
            missing.append(f"{item['display_name']}（目前官方月營收彙總表未提供）")
            continue
        report_date_value = row.get("_report_date")
        try:
            report_date = date.fromisoformat(str(report_date_value)) if report_date_value else None
        except ValueError:
            report_date = None
        report_date = report_date or _roc_compact_date(row.get("出表日期"))
        revenue_month = str(row.get("資料年月", ""))
        if not report_date or not revenue_month:
            missing.append(f"{item['display_name']}（官方資料日期格式異常）")
            continue
        revenue_roc_year = int(revenue_month[:3])
        revenue_month_number = int(revenue_month[-2:])
        company = str(row.get("公司名稱", item["display_name"])).strip()
        finmind_month = str(finmind_row.get("資料年月", "")) if finmind_row else ""
        finmind_date_candidate = (
            finmind_report_date if finmind_month == revenue_month else None
        )
        cnyes_report_date = None
        google_report_date = None
        public_report_date, public_report_source = choose_revenue_announcement_date(
            finmind_date_candidate, revenue_year=revenue_roc_year + 1911,
            revenue_month=revenue_month_number,
        )
        if not public_report_date:
            cnyes_report_date = fetch_cnyes_revenue_announcement_date(
                code, revenue_roc_year + 1911, revenue_month_number
            )
            public_report_date, public_report_source = choose_revenue_announcement_date(
                cnyes_date=cnyes_report_date, revenue_year=revenue_roc_year + 1911,
                revenue_month=revenue_month_number,
            )
        if not public_report_date:
            google_report_date = fetch_google_news_revenue_announcement_date(
                code, company, revenue_roc_year + 1911, revenue_month_number
            )
            public_report_date, public_report_source = choose_revenue_announcement_date(
                google_news_date=google_report_date,
                revenue_year=revenue_roc_year + 1911,
                revenue_month=revenue_month_number,
            )
        if public_report_date:
            report_date = date.fromisoformat(public_report_date)
        mom = _signed_percent(row.get("營業收入-上月比較增減(%)"))
        yoy = _signed_percent(row.get("營業收入-去年同月增減(%)"))
        fallback_note = (
            f"最新 {int(target_month_text[-2:])} 月尚無資料，遞補顯示 {revenue_month_number} 月；"
            if revenue_month < target_month_text else ""
        )
        revenue_data = {
            "company": company,
            "code": code,
            "revenue_month": revenue_month,
            "report_date": report_date.isoformat(),
            "current_month": row.get("營業收入-當月營收"),
            "previous_month": row.get("營業收入-上月營收"),
            "last_year_month": row.get("營業收入-去年當月營收"),
            "mom": mom,
            "yoy": yoy,
            "ytd": row.get("累計營業收入-當月累計營收"),
            "last_year_ytd": row.get("累計營業收入-去年累計營收"),
            "ytd_yoy": _signed_percent(row.get("累計營業收入-前期比較增減(%)")),
            "note": str(row.get("備註", "-")).strip(),
            "date_source": (
                f"公告日期來源（採 {public_report_source}）" if public_report_date
                else "系統偵測日（MOPS 未提供單一公司公告時間）" if row.get("_mops_direct")
                else "MOPS 彙總資料日期"
            ),
        }
        events.append({
            "date": report_date.isoformat(),
            "title": f"{company} {revenue_month_number}月營收 MOM{mom}／YOY{yoy}",
            "detail": (
                fallback_note
                + f"{revenue_month} 月營收：{_thousand_currency(revenue_data['current_month'])}；"
                + (
                    f"公告日期：{public_report_date}（採 {public_report_source}）；" if public_report_date else
                    "MOPS 單一公司資料（公告日未提供，顯示系統偵測日）；" if row.get("_mops_direct") else ""
                )
                + "點擊事件名稱查看月營收明細。"
            ),
            "closed": False,
            "temporary": False,
            "source": row.get("_source") or (
                "MOPS 單一公司月營收"
                if row.get("_mops_direct") else "TWSE OpenAPI（MOPS 每月營收）"
            ),
            "ticker": code,
            "market": "台股",
            "revenue": revenue_data,
        })
    return {"events": events, "missing": missing}


def _income_statement_revenue(statement):
    """從 Yahoo 財報表取出 Total Revenue 列，兼容不同版本的欄位大小寫。"""
    if not isinstance(statement, pd.DataFrame) or statement.empty:
        return None
    for index in statement.index:
        normalised_index = re.sub(r"[^a-z]", "", str(index).lower())
        if normalised_index in {"totalrevenue", "operatingrevenue"}:
            return statement.loc[index]
    return None


def _growth_percent(current, comparison):
    current_number, comparison_number = _to_number(current), _to_number(comparison)
    if current_number is None or comparison_number in (None, 0):
        return None
    return (current_number - comparison_number) / abs(comparison_number) * 100


@st.cache_data(ttl=60 * 60 * 6, max_entries=24, show_spinner=False)
def fetch_us_revenue_events(inputs):
    """取得美股最新已公告季度與年度營收；美股沒有統一月營收，改以 QoQ／YoY 呈現。"""
    events, missing = [], []
    for user_input in inputs:
        item = resolve_earnings_ticker(user_input)
        ticker = next((symbol for symbol in item["candidates"] if not re.fullmatch(r"\d{4,6}\.(?:TW|TWO)", symbol)), None)
        if not ticker:
            continue
        try:
            ticker_obj = yf.Ticker(ticker)
            quarterly_row = _income_statement_revenue(ticker_obj.quarterly_income_stmt)
            annual_row = _income_statement_revenue(ticker_obj.income_stmt)
            if quarterly_row is None:
                missing.append(f"{item['display_name']}（尚無可用季度營收資料）")
                continue
            quarter_columns = sorted(quarterly_row.index, reverse=True)
            quarter_values = [(pd.Timestamp(column), quarterly_row[column]) for column in quarter_columns if pd.notna(quarterly_row[column])]
            if not quarter_values:
                missing.append(f"{item['display_name']}（季度營收欄位為空）")
                continue
            period_end, quarter_revenue = quarter_values[0]
            previous_quarter = quarter_values[1][1] if len(quarter_values) > 1 else None
            expected_year_ago = period_end - pd.DateOffset(years=1)
            year_ago_candidates = [
                (abs((candidate_date - expected_year_ago).days), candidate_value)
                for candidate_date, candidate_value in quarter_values[1:]
                if abs((candidate_date - expected_year_ago).days) <= 45
            ]
            year_ago_quarter = min(year_ago_candidates, default=(None, None))[1]
            qoq_value = _growth_percent(quarter_revenue, previous_quarter)
            yoy_value = _growth_percent(quarter_revenue, year_ago_quarter)
            annual_values = []
            if annual_row is not None:
                annual_columns = sorted(annual_row.index, reverse=True)
                annual_values = [(pd.Timestamp(column), annual_row[column]) for column in annual_columns if pd.notna(annual_row[column])]
            annual_revenue = annual_values[0][1] if annual_values else None
            previous_annual = annual_values[1][1] if len(annual_values) > 1 else None
            annual_yoy_value = _growth_percent(annual_revenue, previous_annual)
            qoq = "--" if qoq_value is None else f"{_format_compact_number(qoq_value, 2, signed=True)}%"
            yoy = "--" if yoy_value is None else f"{_format_compact_number(yoy_value, 2, signed=True)}%"
            annual_yoy = "--" if annual_yoy_value is None else f"{_format_compact_number(annual_yoy_value, 2, signed=True)}%"
            revenue_data = {
                "company": item["display_name"],
                "ticker": ticker,
                "period_end": period_end.date().isoformat(),
                "quarter_revenue": quarter_revenue,
                "previous_quarter": previous_quarter,
                "year_ago_quarter": year_ago_quarter,
                "qoq": qoq,
                "yoy": yoy,
                "annual_revenue": annual_revenue,
                "previous_annual": previous_annual,
                "annual_yoy": annual_yoy,
            }
            events.append({
                "date": period_end.date().isoformat(),
                "title": f"{item['display_name']} 季營收（期末）QoQ{qoq}／YOY{yoy}",
                "detail": f"最新已公告財報期間截至 {period_end:%Y/%m/%d}；點擊事件名稱查看季度及年度營收。",
                "closed": False,
                "temporary": False,
                "source": "Yahoo Finance（季度／年度營收）",
                "revenue": revenue_data,
            })
        except Exception:
            missing.append(f"{item['display_name']}（查詢季度／年度營收失敗）")
    return {"events": events, "missing": missing}




def empty_company_event_snapshot():
    return {
        "updated_at": "",
        "tickers": "",
        "events": [],
        "earnings": {"events": [], "resolved": [], "missing": []},
        "taiwan_revenue": {"events": [], "missing": []},
        "us_revenue": {"events": [], "missing": []},
        # MOPS 單一公司月營收沒有可供程式驗證的公告時間；使用者校正後保留於
        # 快照，後續重新同步也會套回相同的公司／營收月份。
        "revenue_date_overrides": {},
    }


def normalize_company_event_snapshot(saved):
    """Migrate legacy flat company events and rebuild a consistent cross-device snapshot."""
    normalized = empty_company_event_snapshot()
    if not isinstance(saved, dict):
        return normalized
    normalized['updated_at'] = str(saved.get('updated_at', '') or '')
    normalized['tickers'] = str(saved.get('tickers', '') or '')
    saved_overrides = saved.get('revenue_date_overrides', {})
    if isinstance(saved_overrides, dict):
        normalized['revenue_date_overrides'] = {
            str(key): str(value)
            for key, value in saved_overrides.items()
            if re.fullmatch(r'\d{4,6}:\d{5}', str(key))
            and re.fullmatch(r'\d{4}-\d{2}-\d{2}', str(value))
        }
    for section in ('earnings', 'taiwan_revenue', 'us_revenue'):
        section_value = saved.get(section, {})
        if isinstance(section_value, dict):
            normalized[section] = dict(section_value)
        if not isinstance(normalized[section].get('events'), list):
            normalized[section]['events'] = []

    def event_key(event):
        return (
            str(event.get('date', '')), str(event.get('title', '')),
            str(event.get('ticker', '')), str(event.get('source', '')),
        )

    section_events = {
        section: list(normalized[section].get('events', []))
        for section in ('earnings', 'taiwan_revenue', 'us_revenue')
    }
    section_keys = {
        section: {event_key(event) for event in events if isinstance(event, dict)}
        for section, events in section_events.items()
    }
    unclassified_events = []
    for event in saved.get('events', []) if isinstance(saved.get('events'), list) else []:
        if not isinstance(event, dict):
            continue
        title = str(event.get('title', ''))
        source = str(event.get('source', ''))
        market = str(event.get('market', ''))
        ticker = str(event.get('ticker', ''))
        if '月營收' in title or 'MOPS' in source or '每月營收' in source:
            section = 'taiwan_revenue'
            event.setdefault('market', '台股')
        elif '財報' in title or source == 'Yahoo Finance':
            if not market:
                market = '台股' if re.fullmatch(r'\d{4,6}\.(?:TW|TWO)', ticker, re.I) else '美股'
                event.setdefault('market', market)
            section = 'earnings'
        elif market == '美股' or '營收' in title:
            section = 'us_revenue'
        else:
            unclassified_events.append(event)
            continue
        key = event_key(event)
        if key not in section_keys[section]:
            section_events[section].append(event)
            section_keys[section].add(key)

    all_events, all_keys = [], set()
    for section in ('earnings', 'taiwan_revenue', 'us_revenue'):
        normalized[section]['events'] = section_events[section]
        for event in section_events[section]:
            if not isinstance(event, dict):
                continue
            key = event_key(event)
            if key not in all_keys:
                all_events.append(event)
                all_keys.add(key)
    for event in unclassified_events:
        key = event_key(event)
        if key not in all_keys:
            all_events.append(event)
            all_keys.add(key)
    normalized['events'] = all_events
    return normalized


def apply_revenue_announcement_date_overrides(snapshot, overrides=None):
    """Apply exact user-confirmed monthly-revenue dates and keep them across syncs."""
    normalized = normalize_company_event_snapshot(snapshot)
    merged_overrides = dict(normalized.get('revenue_date_overrides', {}))
    for key, value in (overrides or {}).items():
        key_text, value_text = str(key), str(value)
        if re.fullmatch(r'\d{4,6}:\d{5}', key_text) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value_text):
            merged_overrides[key_text] = value_text

    revenue_events = []
    for event in normalized.get('taiwan_revenue', {}).get('events', []):
        updated_event = dict(event)
        revenue = dict(updated_event.get('revenue', {}))
        override_key = f"{str(updated_event.get('ticker', ''))}:{str(revenue.get('revenue_month', ''))}"
        exact_date = merged_overrides.get(override_key)
        if exact_date:
            updated_event['date'] = exact_date
            revenue['report_date'] = exact_date
            revenue['date_source'] = '使用者校正的實際公告日'
            updated_event['revenue'] = revenue
            base_detail = str(updated_event.get('detail', '')).split('；實際公告日校正：', 1)[0]
            updated_event['detail'] = f"{base_detail}；實際公告日校正：{exact_date}。"
        revenue_events.append(updated_event)

    normalized['taiwan_revenue'] = {
        **normalized.get('taiwan_revenue', {}),
        'events': revenue_events,
    }
    normalized['revenue_date_overrides'] = merged_overrides
    normalized['events'] = (
        list(normalized.get('earnings', {}).get('events', []))
        + revenue_events
        + list(normalized.get('us_revenue', {}).get('events', []))
    )
    return normalize_company_event_snapshot(normalized)


def parse_calendar_event_date(value):
    """Parse calendar dates without letting browser/UTC conversion move the day."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or '').strip()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
        try:
            return date.fromisoformat(text)
        except ValueError:
            return None
    try:
        parsed = pd.Timestamp(value)
        if pd.isna(parsed):
            return None
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert('Asia/Taipei')
        return parsed.date()
    except (TypeError, ValueError, AttributeError):
        return None


def load_company_event_snapshot(
    force=False
):
    """
    公司營收／財報／重大事件獨立快照。

    不再從股票 strategy scope 取得。
    """
    local_snapshot = empty_company_event_snapshot()

    try:
        local_snapshot = normalize_company_event_snapshot(
            _read_json_cache_file(
                COMPANY_EVENT_SNAPSHOT_FILE
            )
        )
    except Exception:
        local_snapshot = (
            empty_company_event_snapshot()
        )

    if (
        not force
        and st.session_state.get(
            '_company_event_cloud_loaded',
            False
        )
    ):
        return local_snapshot

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    if not gsheet_api_url:
        st.session_state[
            '_company_event_cloud_loaded'
        ] = True

        return local_snapshot

    remote_payload, remote_error = (
        _fetch_remote_scope(
            gsheet_api_url,
            GOOGLE_SCOPE_COMPANY,
            timeout=8,
        )
    )

    if isinstance(
        remote_payload,
        dict
    ):
        remote_snapshot = (
            normalize_company_event_snapshot(
                remote_payload
            )
        )

        local_time = pd.Timestamp(
            local_snapshot.get(
                'updated_at',
                ''
            )
        )

        remote_time = pd.Timestamp(
            remote_snapshot.get(
                'updated_at',
                ''
            )
        )

        use_remote = False

        try:
            if (
                remote_snapshot.get(
                    'events'
                )
                and (
                    pd.isna(
                        local_time
                    )
                    or (
                        pd.notna(
                            remote_time
                        )
                        and remote_time
                        >= local_time
                    )
                )
            ):
                use_remote = True
        except (
            TypeError,
            ValueError,
        ):
            use_remote = bool(
                remote_snapshot.get(
                    'events'
                )
            )

        if use_remote:
            local_snapshot = (
                remote_snapshot
            )

            try:
                _write_json_atomic(
                    COMPANY_EVENT_SNAPSHOT_FILE,
                    _json_safe(
                        local_snapshot
                    ),
                    indent=2,
                )
            except (
                OSError,
                TypeError,
                ValueError,
            ):
                pass

    st.session_state[
        '_company_event_cloud_loaded'
    ] = True

    return local_snapshot


def save_company_event_snapshot(
    snapshot
):
    """
    公司事件快照獨立保存。

    本機 + Google Sheet company_events。
    """
    normalized = (
        normalize_company_event_snapshot(
            snapshot
        )
    )

    try:
        _write_json_atomic(
            COMPANY_EVENT_SNAPSHOT_FILE,
            _json_safe(
                normalized
            ),
            indent=2,
        )
    except (
        OSError,
        TypeError,
        ValueError,
    ):
        return False

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    if not gsheet_api_url:
        st.session_state[
            '_company_event_cloud_loaded'
        ] = True

        return True

    updated_at = str(
        normalized.get(
            'updated_at',
            ''
        )
    ).strip()

    if not updated_at:
        updated_at = (
            datetime.now(
                pytz.timezone(
                    'Asia/Taipei'
                )
            ).isoformat()
        )

        normalized[
            'updated_at'
        ] = updated_at

    with get_data_cache_sync_lock():
        sync_ok, _ = (
            _save_remote_scope(
                gsheet_api_url,
                GOOGLE_SCOPE_COMPANY,
                normalized,
                updated_at=updated_at,
                timeout=8,
                verify=True,
            )
        )

    st.session_state[
        '_company_event_cloud_loaded'
    ] = bool(sync_ok)

    return sync_ok


COMPANY_SYNC_MAX_TICKERS = 12


def fetch_company_event_sections(ticker_symbols):
    """Fetch independent company sections concurrently with a fixed worker cap."""
    symbols = tuple(dict.fromkeys(
        str(value).strip().upper() for value in ticker_symbols
        if str(value).strip()
    ))[:COMPANY_SYNC_MAX_TICKERS]
    jobs = {
        'earnings': fetch_earnings_events,
        'taiwan_revenue': fetch_taiwan_monthly_revenue_events,
        'us_revenue': fetch_us_revenue_events,
    }
    results = {}
    errors = []
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        future_map = {
            executor.submit(fetcher, symbols): section
            for section, fetcher in jobs.items()
        }
        for future in as_completed(future_map):
            section = future_map[future]
            try:
                result = future.result()
                if not isinstance(result, dict):
                    raise ValueError('資料格式不是物件')
                results[section] = result
            except Exception as exc:
                errors.append(f'{section}: {type(exc).__name__}')
    return results, errors, symbols


def fetch_calendar_base_sources(year):
    """Fetch the two independent TWSE calendar sources concurrently."""
    jobs = {
        'holidays': fetch_twse_holiday_events,
        'temporary': fetch_twse_temporary_closure_events,
    }
    results = {'holidays': [], 'temporary': []}
    errors = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_map = {
            executor.submit(fetcher, year) if label == 'holidays'
            else executor.submit(fetcher): label
            for label, fetcher in jobs.items()
        }
        for future in as_completed(future_map):
            label = future_map[future]
            try:
                results[label] = list(future.result() or [])
            except Exception as exc:
                errors.append(f'{label}: {type(exc).__name__}')
    return results, errors


def fetch_selected_calendar_sources(year, selected_event_types, taiwan_closed_dates):
    """Fetch selected independent calendar feeds with a bounded worker pool."""
    selected = set(selected_event_types or [])
    closed_dates = set(taiwan_closed_dates or set())
    jobs = []

    def add(option, label, fetcher):
        if option in selected:
            jobs.append((label, fetcher))

    add('FOMC 利率決議', 'FOMC', lambda: fetch_fomc_events(year))
    add('美國 CPI', 'CPI', lambda: fetch_bls_cpi_events(year))
    add('美國核心 PCE', '核心 PCE', lambda: fetch_bea_core_pce_events(year))
    add('美國 GDP', 'GDP', lambda: fetch_bea_gdp_events(year))
    add('美國 ISM 製造業指數', 'ISM 製造業', lambda: fetch_ism_manufacturing_events(year))
    add('美國大非農', '大非農', lambda: fetch_bls_employment_events(year))
    add('美國小非農 ADP', '小非農 ADP', lambda: fetch_adp_employment_events(year))
    add('美國初領失業金', '初領失業金', lambda: build_us_initial_claims_events(year))
    add('美股四巫日', '美股四巫日', lambda: build_us_quadruple_witching_events(year))
    add(
        '富台指期結算', '富台指期結算',
        lambda: build_sgx_ftse_taiwan_settlement_events(year, closed_dates),
    )
    add(
        'MSCI 季度調整', 'MSCI 季調',
        lambda: build_msci_quarterly_rebalance_events(year, closed_dates),
    )
    if not jobs:
        return {}, []
    results = {}
    errors_by_label = {}
    with ThreadPoolExecutor(max_workers=min(6, len(jobs))) as executor:
        future_map = {executor.submit(fetcher): label for label, fetcher in jobs}
        for future in as_completed(future_map):
            label = future_map[future]
            try:
                results[label] = list(future.result() or [])
            except Exception as exc:
                results[label] = []
                errors_by_label[label] = f'{label}: {type(exc).__name__}'
    ordered_results = {
        label: results.get(label, []) for label, _fetcher in jobs
    }
    ordered_errors = [
        errors_by_label[label] for label, _fetcher in jobs
        if label in errors_by_label
    ]
    return ordered_results, ordered_errors


def merge_calendar_last_success(current_sources, previous_sources):
    """Keep each calendar feed's last non-empty result on transient failure."""
    previous = previous_sources if isinstance(previous_sources, dict) else {}
    merged = {}
    retained = []
    for label, events in (current_sources or {}).items():
        event_list = list(events or [])
        prior_events = list(previous.get(label, []) or [])
        if event_list:
            merged[label] = event_list
        elif prior_events:
            merged[label] = prior_events
            retained.append(label)
        else:
            merged[label] = []
    return merged, retained


def render_company_event_snapshot(snapshot):
    """在獨立分頁顯示財報與營收明細；此函式不執行任何網路查詢。"""
    earnings_result = snapshot.get("earnings", {})
    taiwan_result = snapshot.get("taiwan_revenue", {})
    us_result = snapshot.get("us_revenue", {})

    taiwan_tab, earnings_tab, us_tab = st.tabs(["🏢 台股月營收", "📅 財報時間", "🌎 美股季度／年度營收"])
    with earnings_tab:
        earnings_events = earnings_result.get("events", [])
        if earnings_events:
            rows = [{
                "日期": event.get("date", ""),
                "公司／事件": event.get("title", ""),
                "公布時間": str(event.get("detail", "")).split("；", 1)[0].replace("Yahoo Finance｜", ""),
            } for event in earnings_events]
            earnings_frame = pd.DataFrame(rows)
            st.dataframe(
                earnings_frame, column_config=compact_table_column_config(earnings_frame),
                hide_index=True, width='stretch',
            )
        else:
            st.info("目前快照沒有可用的財報日期。")
        if earnings_result.get("resolved"):
            st.caption("辨識結果：" + "； ".join(earnings_result["resolved"]))
        if earnings_result.get("missing"):
            st.warning("尚未取得財報日期：" + "； ".join(earnings_result["missing"]))

    with taiwan_tab:
        taiwan_events = taiwan_result.get("events", [])
        if not taiwan_events:
            st.info("目前快照沒有台股月營收；按上方按鈕同步最新資料。")
        else:
            st.caption("資料來源：證交所 OpenAPI；彙總表延遲時改由 MOPS 單一公司頁補查。")
            for event in taiwan_events:
                revenue = event.get("revenue", {})
                st.markdown(f"#### {revenue.get('company', '')}（{revenue.get('code', '')}）｜{revenue.get('revenue_month', '')} 月營收")
                metric_cols = st.columns(3)
                metric_cols[0].markdown(_revenue_metric_html("當月營收", _thousand_currency(revenue.get("current_month")), str(revenue.get("mom", "--")) + " MoM"), unsafe_allow_html=True)
                metric_cols[1].markdown(_revenue_metric_html("去年當月營收", _thousand_currency(revenue.get("last_year_month")), str(revenue.get("yoy", "--")) + " YoY"), unsafe_allow_html=True)
                metric_cols[2].markdown(_revenue_metric_html("本年累計營收", _thousand_currency(revenue.get("ytd")), str(revenue.get("ytd_yoy", "--")) + " 累計 YoY"), unsafe_allow_html=True)
                revenue_frame = pd.DataFrame([{
                    "資料年月": revenue.get("revenue_month", ""),
                    "當月營收": _thousand_currency(revenue.get("current_month")),
                    "上月營收": _thousand_currency(revenue.get("previous_month")),
                    "去年當月營收": _thousand_currency(revenue.get("last_year_month")),
                    "本年累計營收": _thousand_currency(revenue.get("ytd")),
                    "去年累計營收": _thousand_currency(revenue.get("last_year_ytd")),
                    "備註": revenue.get("note", "-"),
                }])
                st.dataframe(
                    revenue_frame, column_config=compact_table_column_config(revenue_frame),
                    hide_index=True, width='stretch',
                )
        if taiwan_result.get("missing"):
            st.info("未取得台股月營收：" + "； ".join(taiwan_result["missing"]))

    with us_tab:
        us_events = us_result.get("events", [])
        if not us_events:
            st.info("目前快照沒有美股季度／年度營收；按上方按鈕同步最新資料。")
        else:
            st.caption("美股沒有統一月營收申報；以下顯示最新已公告財報期間，金額單位為美元。")
            for event in us_events:
                revenue = event.get("revenue", {})
                st.markdown(f"#### {revenue.get('company', '')}（{revenue.get('ticker', '')}）｜財報期間截至 {revenue.get('period_end', '')}")
                metric_cols = st.columns(3)
                metric_cols[0].markdown(_revenue_metric_html("季度營收", _usd_currency(revenue.get("quarter_revenue")), str(revenue.get("qoq", "--")) + " QoQ"), unsafe_allow_html=True)
                metric_cols[1].markdown(_revenue_metric_html("去年同期季營收", _usd_currency(revenue.get("year_ago_quarter")), str(revenue.get("yoy", "--")) + " YoY"), unsafe_allow_html=True)
                metric_cols[2].markdown(_revenue_metric_html("年度營收", _usd_currency(revenue.get("annual_revenue")), str(revenue.get("annual_yoy", "--")) + " 年 YoY"), unsafe_allow_html=True)
                us_revenue_frame = pd.DataFrame([{
                    "季度營收": _usd_currency(revenue.get("quarter_revenue")),
                    "前一季營收": _usd_currency(revenue.get("previous_quarter")),
                    "去年同期季營收": _usd_currency(revenue.get("year_ago_quarter")),
                    "年度營收": _usd_currency(revenue.get("annual_revenue")),
                    "前年度營收": _usd_currency(revenue.get("previous_annual")),
                }])
                st.dataframe(
                    us_revenue_frame, column_config=compact_table_column_config(us_revenue_frame),
                    hide_index=True, width='stretch',
                )
        if us_result.get("missing"):
            st.info("未取得美股營收：" + "； ".join(us_result["missing"]))

# ==========================================
# 永豐 API (Shioaji) 擷取核心
# ==========================================
def fetch_shioaji_data(api, code, interval='1d', lookback_days=10):
    try:
        # ==========================================
        # 1. 暴力破解版：強迫取得「近月合約」物件
        # ==========================================
        contract = None
        is_future = False
        is_index = False
        
        if code in ["^TWII", "加權指數", "TSE", "加權指數(^TWII)"]:
            contract = get_taiex_contract(api)
            is_index = True
        elif code in ["TWF=F", "台指期貨", "TXF", "台指期貨(TWF=F)", "台指(全)", "台指期(全)", "台指期貨(全)"]:
            is_future = True
            try:
                contract = api.Contracts.Futures.TXF.TXFR1
            except AttributeError:
                contract = None

        elif code in ["TMF=F", "微型台指期貨", "TMF", "微型台指", "微型台指期貨(TMF=F)", "微台(全)", "微台期(全)", "微型台指(全)", "微型台指期貨(全)"]:
            is_future = True
            # TMF 是微型台指；MXF 是小型台指。使用 R1 可讓歷史 K 棒依結算日
            # 自動換月，避免用「今天的近月實體合約」回查舊日期而混入不同月份資料。
            contract = api.Contracts.Futures.TMF.TMFR1
        else:
            try:
                contract = api.Contracts.Stocks[code]
            except:
                pass

        if not contract:
            return pd.DataFrame()

        # ==========================================
        # 2. 終極測試：把天數壓到最小極限值
        # ==========================================
        tz_tw = pytz.timezone('Asia/Taipei')
        now = datetime.now(tz_tw)
        end_date = now.strftime("%Y-%m-%d")
        
        if is_future or is_index:
            if interval == '1d':
                # 日 K 會自動分段請求；不可再以 90 天截斷，否則加權指數
                # 無法湊齊費波與 MA60 所需的交易日數。
                actual_lookback = min(lookback_days, 370)
            elif interval in ['1wk', '1mo']:
                actual_lookback = min(lookback_days, 370)
            elif interval == '60m':
                actual_lookback = min(lookback_days, 12)
            elif interval == '15m':
                actual_lookback = min(lookback_days, 5)
            elif interval == '5m':
                actual_lookback = min(lookback_days, 3)
            else:
                actual_lookback = 1
        else:
            actual_lookback = min(lookback_days, 150) if interval in ['1d', '1wk', '1mo'] else 5
            
        start_date = (now - timedelta(days=actual_lookback)).strftime("%Y-%m-%d")

        # 3. 呼叫官方 api.kbars
        kbars_dict = None
        
        # Shioaji 單次 K 棒查詢有日期區間上限；日 K 也必須分段，否則 60 根
        # 費波樣本可能不完整。分 K 使用較短區間以控制單次資料量。
        max_chunk_days = 15 if interval in ['1m', '5m', '15m', '60m'] else 30
        if actual_lookback > max_chunk_days:
            all_ts, all_open, all_high, all_low, all_close, all_vol, all_amount = [], [], [], [], [], [], []
            curr_end = now
            chunks = []
            
            while curr_end > now - timedelta(days=actual_lookback):
                curr_start = curr_end - timedelta(days=max_chunk_days)
                if curr_start < now - timedelta(days=actual_lookback):
                    curr_start = now - timedelta(days=actual_lookback)
                chunks.append((curr_start, curr_end))
                curr_end = curr_start - timedelta(days=1)
            
            for c_start, c_end in reversed(chunks):
                s_str = c_start.strftime("%Y-%m-%d")
                e_str = c_end.strftime("%Y-%m-%d")
                for attempt in range(2):
                    try:
                        k = api.kbars(contract=contract, start=s_str, end=e_str)
                        if k and hasattr(k, 'ts') and len(k.ts) > 0:
                            all_ts.extend(k.ts)
                            all_open.extend(k.Open)
                            all_high.extend(k.High)
                            all_low.extend(k.Low)
                            all_close.extend(k.Close)
                            all_vol.extend(k.Volume)
                            amount = getattr(k, 'Amount', None)
                            if amount is not None:
                                all_amount.extend(amount)
                            break
                        time.sleep(0.3)
                    except:
                        time.sleep(0.3)
            
            if all_ts:
                kbars_dict = {
                    'ts': all_ts, 'Open': all_open, 'High': all_high,
                    'Low': all_low, 'Close': all_close, 'Volume': all_vol
                }
                if len(all_amount) == len(all_ts):
                    kbars_dict['Amount'] = all_amount
        else:
            # 關鍵修正：分K (短天數) 直接單次抓取，不經過複雜的迴圈切割，速度最快且最穩
            for attempt in range(2):
                try:
                    kbars = api.kbars(contract=contract, start=start_date, end=end_date)
                    if kbars and hasattr(kbars, 'ts') and len(kbars.ts) > 0:
                        kbars_dict = {**kbars}
                        amount = getattr(kbars, 'Amount', None)
                        if amount is not None:
                            kbars_dict['Amount'] = amount
                        break
                    time.sleep(0.5)
                except Exception:
                    time.sleep(1.0)
        
        # 4. 依照官方文件轉換成 DataFrame 格式
        if not kbars_dict:
            st.session_state['sj_last_error'] = f"contract={getattr(contract, 'code', contract)} 期間內查無K棒（API回傳0筆，非例外錯誤）"
            return pd.DataFrame()
            
        df = pd.DataFrame(kbars_dict)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # 將時間設為 Index 並確保移除時區資訊以利畫圖
        if df['ts'].dt.tz is not None:
            df['ts'] = df['ts'].dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
        
        df.set_index('ts', inplace=True)
        
        # 確保擁有官方的開高低收量欄位
        agg_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'Amount': 'sum'}
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}

        # 5. K棒週期重取樣
        if interval == '1m':
            pass
        elif interval in ['1d', '1wk', '1mo']:
            if is_future:
                # 1. 取得真實時間的浮點數小時 (例如 08:45 為 8.75) 以判斷是否為日盤
                original_hours = df.index.hour + df.index.minute / 60.0
                
                # 2. 處理期貨夜盤日K對齊 (將 T-1 15:00 ~ T 13:45 平移至 T 日)
                df.index = df.index + pd.Timedelta(hours=9)
                
                # 3. 動態判定開市日：加入「有實際成交量」才算開市 (過濾休市但 API 回傳 0 量假資料的情形)
                is_day_session = (original_hours >= 8.75) & (original_hours <= 13.75) & (df['Volume'] > 0)
                date_series = df.index.normalize()
                dates_with_day_session = date_series[is_day_session].unique()
                
                # 4. 計算現實世界中「當前」的期貨 T 盤日期，作為兜底目標
                now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
                current_t_date = get_futures_trading_date(now_tw)

                # 5. 建立日期映射：將沒有日盤的孤立夜盤往後併入有效的 T 盤
                all_dates = pd.Series(date_series.unique()).sort_values()
                date_mapping = {}
                
                for d in all_dates:
                    if d in dates_with_day_session:
                        date_mapping[d] = d
                    else:
                        future_valid = [fd for fd in all_dates if fd > d and fd in dates_with_day_session]
                        if future_valid:
                            date_mapping[d] = min(future_valid)
                        else:
                            # 找不到未來的日盤 (如目前的最新夜盤)，強制併入當前的「現實 T 盤日期」
                            target_date = current_t_date
                            if d > target_date:  # 防呆機制
                                target_date = d
                                while target_date.dayofweek >= 5:
                                    target_date += pd.Timedelta(days=1)
                            date_mapping[d] = target_date
                
                # 6. 套用動態修正
                df.index = date_series.map(date_mapping) + (df.index - date_series)

            if interval == '1d': df = df.resample('D').agg(agg_dict).dropna()
            elif interval == '1wk': df = df.resample('W-FRI').agg(agg_dict).dropna()
            else: df = df.resample('M').agg(agg_dict).dropna()
            
            if is_future:
                df.index = df.index.normalize()

        else:
            resample_map = {'5m': '5min', '15m': '15min', '60m': '60min'}
            if interval in resample_map:
                df = df.copy()
                if is_future:
                    # 期貨：1分K為「結束時間」，需用 closed='right' 避免整點K棒吸收到上一小時的高低點
                    if interval == '60m':
                        day_mask = (df.index.time > dt_time(8, 45)) & (df.index.time <= dt_time(13, 45))
                        df_day = df[day_mask].resample('60min', closed='right', label='left', offset='45min').agg(agg_dict).dropna()
                        df_night = df[~day_mask].resample('60min', closed='right', label='left').agg(agg_dict).dropna()
                        df = pd.concat([df_day, df_night]).sort_index()
                    else:
                        df = df.resample(resample_map[interval], closed='right', label='left').agg(agg_dict).dropna()
                else:
                    # 個股：開盤第一筆為 09:00:00，若用 closed='right' 會被誤分到上一根空K棒，必須維持 closed='left'
                    df = df.resample(resample_map[interval], closed='left', label='left').agg(agg_dict).dropna()

        # 加權指數的「量」應顯示大盤成交金額。Shioaji K 棒的 Volume 是
        # 原始成交量，不可直接標為「億」；將 Amount 轉為億元後統一供圖表使用。
        if is_index and 'Amount' in df.columns:
            df['Volume'] = df['Amount'].astype(float) / 100_000_000
            df.attrs['volume_unit'] = '億'

        # Historical K bars remain query-based. The unfinished minute candle is
        # overlaid from the live subscription so a rerun no longer needs another
        # kbars/snapshot round trip just to obtain the latest price.
        if interval in ('1m', '5m', '15m', '60m'):
            stream_quote = get_stream_quotes(api, [contract], snapshot_fallback=False)
            df = merge_stream_quote_into_intraday(
                df, stream_quote[0] if stream_quote else None, interval
            )

        # 保留資料實際來源，讓圖表可以驗證商品與連續契約是否正確。
        df.attrs['shioaji_contract_code'] = getattr(contract, 'code', '')
        df.attrs['shioaji_target_code'] = getattr(contract, 'target_code', '')
        return df
    except Exception as e:
        print(f"Shioaji fetch error for {code}: {e}")
        st.session_state['sj_last_error'] = f"{type(e).__name__}: {e}"
        return pd.DataFrame()


def get_cached_fibonacci_kbars(api, code, interval='1d', lookback_days=10):
    """Reuse Fibonacci history while the live stream updates the newest bar.

    Historical kbars are the expensive part of a chart rerun.  The cache is
    deliberately shorter for minute bars, but daily/weekly history can be
    reused for much longer because the current quote is overlaid separately.
    """
    if api is None:
        return pd.DataFrame()
    ttl_by_interval = {
        '1m': 20, '5m': 45, '15m': 90, '60m': 180,
        '1d': 1800, '1wk': 3600, '1mo': 3600,
    }
    ttl = ttl_by_interval.get(interval, 120)
    cache = st.session_state.setdefault('_fibonacci_kbar_cache', {})
    cache_key = (id(api), str(code), str(interval), int(lookback_days))
    cached = cache.get(cache_key)
    now_mono = time.monotonic()
    if cached and now_mono - cached['saved_at'] <= ttl:
        return cached['data'].copy()

    data = fetch_shioaji_data(api, code, interval=interval, lookback_days=lookback_days)
    if not data.empty:
        cache[cache_key] = {'saved_at': now_mono, 'data': data.copy()}
        if len(cache) > 12:
            oldest_key = min(cache, key=lambda key: cache[key]['saved_at'])
            cache.pop(oldest_key, None)
    return data.copy()

# ==========================================
# 費波計算核心函數
# ==========================================
def get_taiwan_tick_size(price):
    if price < 10: return 0.01
    elif price < 50: return 0.05
    elif price < 100: return 0.1
    elif price < 500: return 0.5
    elif price < 1000: return 1
    else: return 5

def round_to_tick(price):
    tick = get_taiwan_tick_size(price)
    p_dec = Decimal(str(price))
    t_dec = Decimal(str(tick))
    rounded = (p_dec / t_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * t_dec
    return float(rounded)

# ==========================================
# 臺灣市場溫度計
# ==========================================
def merge_market_temperature_snapshot(df, api, code):
    """Merge the current Shioaji snapshot into a daily series.

    Futures snapshots after 15:00 are assigned to the following trading date,
    so the thermometer follows the same night-session convention as the chart.
    """
    if df.empty or api is None:
        return df

    try:
        is_future = code == "TWF=F"
        now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
        if code == "^TWII":
            # Do not create a synthetic index bar from a stale snapshot before
            # the cash session opens or on a non-trading day.
            if is_market_closed_func(now_tw.date()) or now_tw.time() < dt_time(9, 0):
                return df
            contract = get_taiex_contract(api)
            if contract is None:
                return df
            trading_date = pd.Timestamp(now_tw.date())
        elif is_future:
            contract = api.Contracts.Futures.TXF.TXFR1
            trading_date = get_futures_trading_date(now_tw)
        else:
            return df

        snapshots = get_stream_quotes(api, [contract])
        if not snapshots:
            return df

        snap = snapshots[0]
        close = float(getattr(snap, 'close', 0) or 0)
        if close <= 0:
            return df

        # 夜盤跨交易日時，快照的官方漲跌基準不一定等於前一根日 K 收盤。
        # 保留券商直接提供的漲跌點數／幅度，避免由未完成日 K 再次推算。
        reference_close = None
        snapshot_change = None
        snapshot_change_pct = None
        try:
            change_price = getattr(snap, 'change_price', None)
            if change_price is not None:
                snapshot_change = float(change_price)
                inferred_reference = close - float(change_price)
                if inferred_reference > 0:
                    reference_close = inferred_reference
            change_rate = getattr(snap, 'change_rate', None)
            if change_rate is not None:
                snapshot_change_pct = float(change_rate)
            if reference_close is None:
                snapshot_reference = float(getattr(snap, 'reference', 0) or 0)
                contract_reference = float(getattr(contract, 'reference', 0) or 0)
                reference_close = snapshot_reference if snapshot_reference > 0 else contract_reference
                if reference_close <= 0:
                    reference_close = None
        except (TypeError, ValueError):
            pass

        open_price = float(getattr(snap, 'open', close) or close)
        high = float(getattr(snap, 'high', close) or close)
        low = float(getattr(snap, 'low', close) or close)
        volume = float(getattr(snap, 'total_volume', 0) or 0)
        trading_date = pd.Timestamp(trading_date).normalize()

        result = df.copy()
        last_date = pd.Timestamp(result.index[-1]).normalize()
        if last_date < trading_date:
            current_bar = pd.DataFrame(
                [{'Open': open_price, 'High': high, 'Low': low, 'Close': close, 'Volume': volume}],
                index=[trading_date]
            )
            result = pd.concat([result, current_bar])
        elif last_date == trading_date:
            result.at[result.index[-1], 'Close'] = close
            result.at[result.index[-1], 'High'] = max(float(result['High'].iloc[-1]), high)
            result.at[result.index[-1], 'Low'] = min(float(result['Low'].iloc[-1]), low)
            result.at[result.index[-1], 'Volume'] = max(float(result['Volume'].iloc[-1]), volume)
        if reference_close is not None:
            result.attrs['market_temperature_reference_close'] = reference_close
        if snapshot_change is not None:
            result.attrs['market_temperature_change'] = snapshot_change
        if snapshot_change_pct is not None:
            result.attrs['market_temperature_change_pct'] = snapshot_change_pct
        return result
    except Exception:
        return df


def fetch_market_temperature_data(code, lookback_days=180):
    """Fetch a daily OHLCV series for the thermometer, preferring Shioaji."""
    source = ""
    df = pd.DataFrame()
    api = st.session_state.get('sj_api')

    if st.session_state.get('sj_logged_in', False) and api is not None:
        df = fetch_shioaji_data(api, code, interval='1d', lookback_days=lookback_days)
        if code == '^TWII':
            twse_df = fetch_twse_taiex_daily_history(lookback_days=lookback_days)
            if not twse_df.empty:
                df = merge_taiex_history_with_shioaji(twse_df, df)
                source = "證交所官方歷史日K + 永豐 Shioaji 即時串流"
        if not df.empty:
            df = merge_market_temperature_snapshot(df, api, code)
            if not source:
                source = "永豐 Shioaji 全盤資料"

    # 加權指數的日 K 優先由證交所官方資料補齊；即使永豐尚未登入，
    # 也不會因資料不足而改用 Yahoo 的不同口徑。
    if df.empty and code == '^TWII':
        twse_df = fetch_twse_taiex_daily_history(lookback_days=lookback_days)
        if not twse_df.empty:
            df = twse_df
            source = "證交所官方歷史日K"

    # 僅未登入永豐時，才允許加權指數以 Yahoo 作為歷史資料備援。登入後
    # 必須維持單一券商資料源，避免快照與歷史 K 棒混用而產生錯誤漲跌。
    if df.empty and code == '^TWII' and not st.session_state.get('sj_logged_in', False):
        try:
            df = yf.Ticker('^TWII').history(period='6mo', interval='1d')
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            if df.index.tz is not None:
                df.index = df.index.tz_convert('Asia/Taipei').tz_localize(None)
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
            source = "Yahoo 歷史資料（未登入永豐）"
        except Exception:
            df = pd.DataFrame()

    temperature_attrs = {
        key: value for key, value in df.attrs.items()
        if key.startswith('market_temperature_')
    }
    result = df.sort_index()
    result.attrs.update(temperature_attrs)
    return result, source


def get_cached_market_temperature_data(code, lookback_days=180, max_age_seconds=180):
    """Reuse slower daily history while still merging the current streamed quote."""
    cache = st.session_state.setdefault('_market_temperature_history_cache', {})
    logged_in = bool(st.session_state.get('sj_logged_in', False))
    api = st.session_state.get('sj_api')
    cache_key = (code, int(lookback_days), logged_in, id(api) if api is not None else None)
    cached = cache.get(cache_key)
    now = time.monotonic()
    if cached and now - cached['saved_at'] <= max_age_seconds:
        df = cached['df'].copy(deep=True)
        df.attrs.update(cached.get('attrs', {}))
        if logged_in and api is not None and not df.empty:
            df = merge_market_temperature_snapshot(df, api, code)
        return df, cached['source']

    df, source = fetch_market_temperature_data(code, lookback_days=lookback_days)
    if not df.empty:
        cache[cache_key] = {
            'saved_at': now,
            'df': df.copy(deep=True),
            'attrs': dict(df.attrs),
            'source': source,
        }
        return df, source
    # A transient upstream failure must not replace the last complete history
    # with an empty dataframe. Stale data stays explicitly labelled.
    if cached and isinstance(cached.get('df'), pd.DataFrame) and not cached['df'].empty:
        stale_df = cached['df'].copy(deep=True)
        stale_df.attrs.update(cached.get('attrs', {}))
        stale_source = str(cached.get('source', '') or '上次成功資料')
        return stale_df, f"{stale_source}（暫時沿用）"
    return df, source


def clear_index_market_data_cache():
    """Clear only index-room history so a failed first load can retry cleanly."""
    st.session_state.pop('_market_temperature_history_cache', None)
    fetch_twse_taiex_month.clear()


def calculate_market_temperature(df):
    """Return a transparent 0-100 trend / momentum temperature score."""
    required = {'High', 'Low', 'Close'}
    if df.empty or not required.issubset(df.columns):
        return None

    data = df.dropna(subset=['High', 'Low', 'Close']).copy()
    if len(data) < 25:
        return None

    close = data['Close'].astype(float)
    high = data['High'].astype(float)
    low = data['Low'].astype(float)
    latest = float(close.iloc[-1])

    window = min(60, len(data))
    range_high = float(high.tail(window).max())
    range_low = float(low.tail(window).min())
    range_score = 50.0 if range_high == range_low else (latest - range_low) / (range_high - range_low) * 100

    change = close.diff()
    gains = change.clip(lower=0).rolling(14, min_periods=10).mean()
    losses = (-change.clip(upper=0)).rolling(14, min_periods=10).mean()
    latest_gain = float(gains.iloc[-1])
    latest_loss = float(losses.iloc[-1])
    if latest_loss == 0 and latest_gain > 0:
        rsi = 100.0
    elif latest_gain == 0 and latest_loss > 0:
        rsi = 0.0
    elif latest_gain == 0 and latest_loss == 0:
        rsi = 50.0
    else:
        relative_strength = latest_gain / latest_loss
        rsi = 100 - 100 / (1 + relative_strength)
    if not np.isfinite(rsi):
        rsi = 50.0

    previous_close = close.shift(1)
    true_range = pd.concat([
        high - low,
        (high - previous_close).abs(),
        (low - previous_close).abs()
    ], axis=1).max(axis=1)
    atr = float(true_range.rolling(14, min_periods=10).mean().iloc[-1])
    ma20 = float(close.rolling(20, min_periods=15).mean().iloc[-1])
    ma60 = float(close.rolling(60, min_periods=20).mean().iloc[-1])
    trend_score = 50.0 if not np.isfinite(atr) or atr <= 0 else 50 + (latest - ma20) / atr * 10

    momentum = 0.0 if len(close) <= 5 else (latest / float(close.iloc[-6]) - 1) * 100
    momentum_score = 50 + momentum * 20
    # 期貨夜盤使用券商快照的官方參考價；未取得快照時才退回前一根日 K。
    reference_close = df.attrs.get('market_temperature_reference_close')
    try:
        reference_close = float(reference_close)
    except (TypeError, ValueError):
        reference_close = None
    previous = reference_close if reference_close is not None and reference_close > 0 else float(close.iloc[-2])
    snapshot_change = df.attrs.get('market_temperature_change')
    snapshot_change_pct = df.attrs.get('market_temperature_change_pct')
    try:
        snapshot_change = float(snapshot_change)
    except (TypeError, ValueError):
        snapshot_change = None
    try:
        snapshot_change_pct = float(snapshot_change_pct)
    except (TypeError, ValueError):
        snapshot_change_pct = None
    change_value = snapshot_change if snapshot_change is not None and np.isfinite(snapshot_change) else latest - previous
    change_pct = snapshot_change_pct if snapshot_change_pct is not None and np.isfinite(snapshot_change_pct) else ((change_value / previous * 100) if previous else 0.0)
    if change_value > 0:
        price_color, price_arrow = "#ff4b4b", "▲"
    elif change_value < 0:
        price_color, price_arrow = "#00c853", "▼"
    else:
        price_color, price_arrow = "#dfe6e9", "◆"

    clip = lambda value: float(np.clip(value, 0, 100))
    score = round(clip(0.40 * range_score + 0.25 * rsi + 0.25 * trend_score + 0.10 * momentum_score))

    # 狀態以溫度區間為主；均線僅作入／出場規則輔助。否則溫度已落在
    # 0 度這類極端空方值時，仍可能因均線排序暫未翻轉而誤顯示盤整。
    bullish = score >= 60
    bearish = score <= 40
    if bullish:
        status, color = "偏多", "#ff4b4b"
        entry = "回測短均線後止穩，可觀察順勢切入；溫度高於 80 時不追價。"
        exit_rule = "跌破 MA20 或溫度回落至 55 以下，應收緊停損／減碼。"
    elif bearish:
        status, color = "偏空", "#00c853"
        entry = "反彈至短均線受壓再觀察；既有多單宜保守，不宜逆勢攤平。"
        exit_rule = "站回 MA20 且溫度回升至 45 以上，應撤除空方／觀望。"
    else:
        status, color = "區間盤整", "#ffc107"
        entry = "等待溫度突破 60 或跌破 40，再配合價格突破確認方向。"
        exit_rule = "區間中段不追價；接近區間兩端才規畫分批停利或停損。"

    return {
        'score': score, 'status': status, 'color': color,
        'entry': entry, 'exit_rule': exit_rule, 'close': latest,
        'rsi': rsi, 'range_score': range_score, 'ma20': ma20,
        'ma60': ma60, 'momentum': momentum, 'updated_at': data.index[-1],
        'change': change_value, 'change_pct': change_pct,
        'reference_close': previous,
        'price_color': price_color, 'price_arrow': price_arrow
    }


def get_futures_intraday_state(api, direction):
    """Return 15-minute confirmation, VWAP and active-session opening range."""
    empty_state = {
        'available': False, 'confirmation_text': "尚未取得 15 分 K；進場前請確認價格在費波區止跌／受壓。",
        'confirmed': False, 'is_up_bar': False, 'is_down_bar': False,
        'bullish_break': False, 'bearish_break': False,
        'vwap': None, 'opening_high': None, 'opening_low': None, 'latest': None,
    }
    if api is None or not st.session_state.get('sj_logged_in', False):
        return empty_state
    try:
        intraday = fetch_shioaji_data(api, 'TWF=F', interval='15m', lookback_days=5)
        intraday = intraday.dropna(subset=['Open', 'High', 'Low', 'Close']).sort_index()
        if len(intraday) < 2:
            return {**empty_state, 'confirmation_text': "15 分 K 資料不足，暫不觸發進場。"}

        now_tw = datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
        index_dates = pd.DatetimeIndex(intraday.index)
        if dt_time(8, 45) <= now_tw.time() <= dt_time(13, 45):
            session_mask = (
                (index_dates.date == now_tw.date())
                & (index_dates.time >= dt_time(8, 45))
                & (index_dates.time <= dt_time(13, 45))
            )
        elif now_tw.time() >= dt_time(15, 0):
            session_mask = (index_dates.date == now_tw.date()) & (index_dates.time >= dt_time(15, 0))
        elif now_tw.time() < dt_time(5, 0):
            previous_date = (now_tw - timedelta(days=1)).date()
            session_mask = (
                ((index_dates.date == previous_date) & (index_dates.time >= dt_time(15, 0)))
                | ((index_dates.date == now_tw.date()) & (index_dates.time < dt_time(5, 0)))
            )
        else:
            session_mask = np.zeros(len(intraday), dtype=bool)

        session = intraday.loc[session_mask].copy()
        if len(session) < 2:
            session = intraday.tail(min(12, len(intraday))).copy()
        last = session.iloc[-1]
        previous_close = float(session['Close'].iloc[-2])
        is_up_bar = float(last['Close']) > float(last['Open']) and float(last['Close']) > previous_close
        is_down_bar = float(last['Close']) < float(last['Open']) and float(last['Close']) < previous_close
        opening_high = float(session['High'].iloc[0])
        opening_low = float(session['Low'].iloc[0])
        latest = float(last['Close'])
        volume = pd.to_numeric(session.get('Volume', pd.Series(0, index=session.index)), errors='coerce').fillna(0)
        typical_price = (session['High'] + session['Low'] + session['Close']) / 3
        vwap = float((typical_price * volume).sum() / volume.sum()) if volume.sum() > 0 else float(session['Close'].mean())
        bullish_break = latest > vwap and latest > opening_high and is_up_bar
        bearish_break = latest < vwap and latest < opening_low and is_down_bar
        if direction == '偏多':
            confirmation_text = "15 分 K 已出現止跌上收確認。" if is_up_bar else "等待 15 分 K 止跌上收確認。"
            confirmed = is_up_bar
        elif direction == '偏空':
            confirmation_text = "15 分 K 已出現受壓下收確認。" if is_down_bar else "等待 15 分 K 受壓下收確認。"
            confirmed = is_down_bar
        else:
            confirmation_text = "方向未明，等待區間邊緣或突破回測確認。"
            confirmed = False
        return {
            'available': True, 'confirmation_text': confirmation_text, 'confirmed': confirmed,
            'is_up_bar': is_up_bar, 'is_down_bar': is_down_bar,
            'bullish_break': bullish_break, 'bearish_break': bearish_break,
            'vwap': vwap, 'opening_high': opening_high, 'opening_low': opening_low, 'latest': latest,
        }
    except Exception:
        return empty_state


def get_cached_futures_intraday_state(api, direction, max_age_seconds=8):
    """Throttle the slower 15-minute K request; the streamed price remains live."""
    cache = st.session_state.setdefault('_trade_plan_intraday_cache', {})
    cache_key = (direction, id(api) if api is not None else None)
    cached = cache.get(cache_key)
    now = time.monotonic()
    if cached and now - cached['saved_at'] <= max_age_seconds:
        return dict(cached['value'])
    value = get_futures_intraday_state(api, direction)
    cache.clear()
    cache[cache_key] = {'saved_at': now, 'value': dict(value)}
    return value


def evaluate_trade_entry_state(
    plan, live_price, live_change, intraday_state, temperature_delta=0.0,
    entry_profile='積極（提早確認）',
):
    """Separate the lagging trend regime from the immediate entry permission."""
    atr = max(float(plan.get('atr', 0) or 0), 1.0)
    shock_ratio = float(live_change or 0) / atr
    direction = plan['direction']
    is_aggressive = str(entry_profile).startswith('積極')
    zone_tolerance = max(float(plan['zone_points']), atr * (0.45 if is_aggressive else 0.25))
    near_entry = abs(float(live_price) - float(plan['entry_level'])) <= zone_tolerance
    bullish_break = bool(intraday_state.get('bullish_break'))
    bearish_break = bool(intraday_state.get('bearish_break'))
    is_up_bar = bool(intraday_state.get('is_up_bar'))
    is_down_bar = bool(intraday_state.get('is_down_bar'))
    intraday_latest = intraday_state.get('latest')
    vwap = intraday_state.get('vwap')
    try:
        intraday_latest = float(intraday_latest)
    except (TypeError, ValueError):
        intraday_latest = float(live_price)
    try:
        vwap = float(vwap)
    except (TypeError, ValueError):
        vwap = None
    bullish_live = is_up_bar or (
        is_aggressive and vwap is not None
        and float(live_price) >= intraday_latest and float(live_price) > vwap
    )
    bearish_live = is_down_bar or (
        is_aggressive and vwap is not None
        and float(live_price) <= intraday_latest and float(live_price) < vwap
    )

    state = {
        'stage': '等待確認', 'permission': '等待進場', 'color': '#ffc107',
        'can_enter': False, 'execution_direction': None,
        'reason': '等待價格接近費波位置，並由 15 分 K 確認方向。',
        'shock_ratio': shock_ratio, 'temperature_delta': float(temperature_delta or 0),
        'entry_level': float(plan['entry_level']), 'invalidation': float(plan['invalidation']),
        'target': float(plan['target']),
    }

    def finalize(result):
        risk = abs(float(result['entry_level']) - float(result['invalidation']))
        reward = abs(float(result['target']) - float(result['entry_level']))
        result['risk_points'] = risk
        result['reward_points'] = reward
        result['rr_ratio'] = (reward / risk) if risk > 0 else None
        return result

    # A large move against the lagging daily regime takes priority over every
    # continuation signal. It prevents an oversold rebound from being sold only
    # because RSI, MA and the 60-day range still read bearish (and vice versa).
    if direction == '偏空' and shock_ratio >= 1.5:
        state.update(
            stage='超跌反彈', permission='禁止追空', color='#ffc107',
            reason='反向上漲已達日 ATR 的 1.5 倍；原偏空趨勢屬落後背景，先等待反彈失敗或重新跌破 VWAP。',
        )
        if bullish_break:
            state['reason'] += ' 目前 15 分 K 已站上 VWAP 與本段開盤區間高點。'
        return finalize(state)
    if direction == '偏多' and shock_ratio <= -1.5:
        state.update(
            stage='過熱回落', permission='禁止追多', color='#ffc107',
            reason='反向下跌已達日 ATR 的 1.5 倍；原偏多趨勢暫停使用，先等待止跌或重新站回 VWAP。',
        )
        if bearish_break:
            state['reason'] += ' 目前 15 分 K 已跌破 VWAP 與本段開盤區間低點。'
        return finalize(state)
    if direction == '偏空' and temperature_delta >= 15 and bullish_break:
        state.update(
            stage='空方快速升溫', permission='暫停做空', color='#ffc107',
            reason='溫度單日快速回升且 15 分 K 站上 VWAP／開盤區間高點，等待反彈失敗後再評估空方。',
        )
        return finalize(state)
    if direction == '偏多' and temperature_delta <= -15 and bearish_break:
        state.update(
            stage='多方快速降溫', permission='暫停做多', color='#ffc107',
            reason='溫度單日快速下降且 15 分 K 跌破 VWAP／開盤區間低點，等待止跌後再評估多方。',
        )
        return finalize(state)

    if direction not in ('偏多', '偏空'):
        lower_edge = float(live_price) <= float(plan['support']) + zone_tolerance
        upper_edge = float(live_price) >= float(plan['resistance']) - zone_tolerance
        if lower_edge and bullish_live:
            state.update(
                stage='區間下緣止跌', permission='允許試多', color='#ff4b4b', can_enter=True,
                execution_direction='偏多', reason='價格位於費波支撐邊緣且盤中價格止跌轉強；採區間反彈，不視為趨勢翻多。',
            )
        elif upper_edge and bearish_live:
            state.update(
                stage='區間上緣受壓', permission='允許試空', color='#00c853', can_enter=True,
                execution_direction='偏空', reason='價格位於費波壓力邊緣且盤中價格受壓轉弱；採區間回落，不視為趨勢翻空。',
                entry_level=float(plan['resistance']),
                invalidation=float(plan['resistance']) + max(float(plan['zone_points']), atr * 0.5),
                target=float(plan['support']),
            )
        elif float(live_price) > float(plan['resistance']) and bullish_break:
            if is_aggressive and shock_ratio < 1.0:
                vwap_stop = vwap if vwap is not None and vwap < float(live_price) else -np.inf
                stop = max(float(plan['resistance']) - atr * 0.45, vwap_stop)
                risk = max(float(live_price) - stop, atr * 0.20)
                state.update(
                    stage='向上突破跟進', permission='允許小部位試多', color='#ff4b4b', can_enter=True,
                    execution_direction='偏多', reason='價格突破費波壓力並站穩 VWAP／開盤區間；漲幅未達 1 ATR，可小部位跟進並嚴守突破失敗停損。',
                    entry_level=float(live_price), invalidation=stop,
                    target=max(float(plan['target']), float(live_price) + risk * 1.3),
                )
            else:
                state.update(stage='向上突破', permission='等待回測', reason='已突破費波壓力；等待回測原壓力不破，避免追在短線過熱處。')
        elif float(live_price) < float(plan['support']) and bearish_break:
            if is_aggressive and shock_ratio > -1.0:
                vwap_stop = vwap if vwap is not None and vwap > float(live_price) else np.inf
                stop = min(float(plan['support']) + atr * 0.45, vwap_stop)
                risk = max(stop - float(live_price), atr * 0.20)
                state.update(
                    stage='向下跌破跟進', permission='允許小部位試空', color='#00c853', can_enter=True,
                    execution_direction='偏空', reason='價格跌破費波支撐並落在 VWAP／開盤區間下方；跌幅未達 1 ATR，可小部位跟進並嚴守跌破失敗停損。',
                    entry_level=float(live_price), invalidation=stop,
                    target=min(float(plan['target']), float(live_price) - risk * 1.3),
                )
            else:
                state.update(stage='向下跌破', permission='等待反抽', reason='已跌破費波支撐；等待反抽原支撐不過，避免追在短線超跌處。')
        else:
            state.update(stage='區間盤整', reason='價格未在區間邊緣形成確認，區間中段不建立方向部位。')
        return finalize(state)

    if direction == '偏多':
        if shock_ratio >= 1.5 or float(live_price) > float(plan['resistance']) + atr * 0.5:
            state.update(stage='多方過熱', permission='禁止追多', reason='漲幅或價格延伸已過大；等待回測費波支撐後再評估。')
        elif bearish_break:
            state.update(stage='多方轉弱', permission='暫停做多', reason='15 分 K 已跌破 VWAP 與本段開盤區間低點，先等待重新站回。')
        elif near_entry and bullish_live:
            state.update(
                stage='多方提前確認' if is_aggressive and not is_up_bar else '多方延續',
                permission='允許小部位進場' if is_aggressive else '允許進場', color='#ff4b4b', can_enter=True,
                execution_direction='偏多',
                reason='價格進入擴大後的費波觀察區，且即時價站上 VWAP 並轉強。' if is_aggressive and not is_up_bar else '價格位於費波支撐觀察區，且 15 分 K 止跌上收。',
            )
    else:
        if shock_ratio <= -1.5 or float(live_price) < float(plan['support']) - atr * 0.5:
            state.update(stage='空方過熱', permission='禁止追空', reason='跌幅或價格延伸已過大；等待反彈至費波壓力後再評估。')
        elif bullish_break:
            state.update(stage='空方反彈轉強', permission='暫停做空', reason='15 分 K 已站上 VWAP 與本段開盤區間高點，先等待反彈失敗。')
        elif near_entry and bearish_live:
            state.update(
                stage='空方提前確認' if is_aggressive and not is_down_bar else '空方延續',
                permission='允許小部位進場' if is_aggressive else '允許進場', color='#00c853', can_enter=True,
                execution_direction='偏空',
                reason='價格進入擴大後的費波觀察區，且即時價跌破 VWAP 並轉弱。' if is_aggressive and not is_down_bar else '價格位於費波壓力觀察區，且 15 分 K 受壓下收。',
            )
    return finalize(state)


def get_trade_risk_level(risk_points, atr):
    """Classify a stop distance against the current daily ATR."""
    atr = max(float(atr or 0), 1.0)
    ratio = float(risk_points or 0) / atr
    if ratio <= 0.5:
        return '低', '#00c853', ratio
    if ratio <= 0.9:
        return '中', '#ffc107', ratio
    return '高', '#ff4b4b', ratio


def get_near_futures_contract(api, product='TMF'):
    """Resolve the live near-month futures contract used by the trade plan."""
    if api is None:
        return None
    try:
        if product == 'TMF':
            return api.Contracts.Futures.TMF.TMFR1
        candidates = [c for c in api.Contracts.Futures.TXF if c.code[-2:] not in ('R1', 'R2') and '/' not in c.code]
        return min(candidates, key=lambda c: getattr(c, 'delivery_date', '999999'))
    except (AttributeError, ValueError):
        return None


def get_live_futures_snapshot(api, product='TMF'):
    """Return an immediately refreshed futures price and official change fields."""
    contract = get_near_futures_contract(api, product)
    if contract is None:
        return None
    try:
        snapshots = get_stream_quotes(api, [contract])
        if not snapshots:
            return None
        snapshot = snapshots[0]
        price = float(getattr(snapshot, 'close', 0) or getattr(snapshot, 'open', 0) or 0)
        if price <= 0:
            return None
        change = getattr(snapshot, 'change_price', None)
        try:
            change = float(change)
        except (TypeError, ValueError):
            change = None
        reference = price - change if change is not None else float(getattr(contract, 'reference', 0) or 0)
        change_pct = getattr(snapshot, 'change_rate', None)
        try:
            change_pct = float(change_pct)
        except (TypeError, ValueError):
            change_pct = ((price - reference) / reference * 100) if reference > 0 else None
        if change is None and reference > 0:
            change = price - reference
        if change is None:
            change = 0.0
        return {
            'price': price, 'change': change, 'change_pct': change_pct or 0.0,
            'color': '#ff4b4b' if change > 0 else ('#00c853' if change < 0 else '#dfe6e9'),
            'arrow': '▲' if change > 0 else ('▼' if change < 0 else '◆'),
            'contract_code': getattr(contract, 'code', 'TMF'),
        }
    except Exception:
        return None


@st.cache_data(ttl=300, max_entries=1, show_spinner=False)
def fetch_taifex_index_margin_map():
    """Read current initial margin requirements from the TAIFEX OpenAPI."""
    margin_map = {}
    try:
        response = requests.get(
            'https://openapi.taifex.com.tw/v1/IndexFuturesAndOptionsMargining',
            headers={'accept': 'application/json', 'If-Modified-Since': 'Mon, 26 Jul 1997 05:00:00 GMT'},
            timeout=8,
        )
        for item in response.json() if response.status_code == 200 else []:
            raw_name = str(item.get('Contract', '')).replace(' ', '')
            raw_margin = str(item.get('InitialMargin', '0')).replace(',', '')
            try:
                margin = float(raw_margin)
            except ValueError:
                continue
            if raw_name in ('TMF', 'MXF') or '微型臺' in raw_name or '微型台' in raw_name:
                margin_map['TMF'] = margin
            elif raw_name == 'TX' or '臺股期貨' in raw_name or '台指期貨' in raw_name:
                margin_map['TX'] = margin
    except (requests.RequestException, ValueError, TypeError):
        pass
    return margin_map


def calculate_short_wave_plan(api, direction):
    """Calculate an intentionally fast 5-minute pullback/breakout plan."""
    if api is None or direction not in ('偏多', '偏空'):
        return None
    try:
        data = fetch_shioaji_data(api, 'TWF=F', interval='5m', lookback_days=3)
        data = data.dropna(subset=['Open', 'High', 'Low', 'Close']).tail(30)
        if len(data) < 15:
            return None
        previous = data['Close'].shift(1)
        atr = float(pd.concat([
            data['High'] - data['Low'],
            (data['High'] - previous).abs(),
            (data['Low'] - previous).abs(),
        ], axis=1).max(axis=1).rolling(14, min_periods=10).mean().iloc[-1])
        if not np.isfinite(atr) or atr <= 0:
            return None
        latest = float(data['Close'].iloc[-1])
        recent = data.tail(6)
        range_low = float(recent['Low'].min())
        range_high = float(recent['High'].max())
        ema_fast = float(data['Close'].ewm(span=5, adjust=False).mean().iloc[-1])
        ema_slow = float(data['Close'].ewm(span=10, adjust=False).mean().iloc[-1])
        previous_bar = data.iloc[-2]
        last_bar = data.iloc[-1]
        volume = pd.to_numeric(data.get('Volume', pd.Series(0, index=data.index)), errors='coerce').fillna(0)
        average_volume = float(volume.tail(20).mean())
        volume_ratio = float(volume.iloc[-1] / average_volume) if average_volume > 0 else 1.0
        zone = max(10.0, atr * 0.35)
        if direction == '偏多':
            entry = min(latest, max(range_low, ema_fast))
            stop = entry - atr * 0.45
            target = max(range_high, entry + atr * 0.85)
            momentum_ready = (
                latest >= ema_fast
                and (latest > float(previous_bar['Close']) or float(last_bar['High']) > float(previous_bar['High']))
            )
            trigger = (
                f"即時價守住 EMA5 約 {ema_fast:,.0f}，或突破前一根高點 {float(previous_bar['High']):,.0f} 即可小部位試多；"
                "不必等待完整 5 分 K 收盤。"
            )
        else:
            entry = max(latest, min(range_high, ema_fast))
            stop = entry + atr * 0.45
            target = min(range_low, entry - atr * 0.85)
            momentum_ready = (
                latest <= ema_fast
                and (latest < float(previous_bar['Close']) or float(last_bar['Low']) < float(previous_bar['Low']))
            )
            trigger = (
                f"即時價受壓於 EMA5 約 {ema_fast:,.0f}，或跌破前一根低點 {float(previous_bar['Low']):,.0f} 即可小部位試空；"
                "不必等待完整 5 分 K 收盤。"
            )
        return {
            'latest': latest, 'entry': entry, 'stop': stop, 'target': target,
            'zone': zone, 'risk': abs(entry - stop), 'reward': abs(target - entry),
            'rr': (abs(target - entry) / abs(entry - stop)) if entry != stop else None,
            'trigger': trigger, 'momentum_ready': momentum_ready,
            'ema_fast': ema_fast, 'ema_slow': ema_slow, 'volume_ratio': volume_ratio,
        }
    except Exception:
        return None


def get_cached_short_wave_plan(api, direction, max_age_seconds=8):
    """Reuse the 5-minute calculation briefly so one refresh does not reload all bars."""
    cache = st.session_state.setdefault('_trade_plan_short_wave_cache', {})
    cache_key = (direction, id(api) if api is not None else None)
    cached = cache.get(cache_key)
    now = time.monotonic()
    if cached and now - cached['saved_at'] <= max_age_seconds:
        return dict(cached['value']) if cached['value'] else None
    value = calculate_short_wave_plan(api, direction)
    cache.clear()
    cache[cache_key] = {'saved_at': now, 'value': dict(value) if value else None}
    return value


def resolve_short_wave_direction(plan, trade_state, intraday_state):
    """Let 5-minute momentum react before the slower daily entry gate."""
    if trade_state.get('can_enter') and trade_state.get('execution_direction') in ('偏多', '偏空'):
        return trade_state['execution_direction'], '主計畫與短波方向一致'
    shock_ratio = float(trade_state.get('shock_ratio', 0) or 0)
    if intraday_state.get('bullish_break') and shock_ratio < 1.25:
        return '偏多', '15 分 K 已突破 VWAP／開盤區間，短波先採多方快進快出'
    if intraday_state.get('bearish_break') and shock_ratio > -1.25:
        return '偏空', '15 分 K 已跌破 VWAP／開盤區間，短波先採空方快進快出'
    if plan.get('direction') in ('偏多', '偏空') and not str(trade_state.get('permission', '')).startswith('禁止'):
        return plan['direction'], '沿用日線方向，但以 EMA5／前根高低點提早觸發'
    return None, '盤中動能尚未形成，暫不建立短波方向'


def get_txo_target_contract_specs(expiry_choice, today=None):
    """Return TAIFEX delivery-month keys and settlement dates for a requested expiry."""
    now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
    today = today or now_tw.date()
    passed_expiry_cutoff = today == now_tw.date() and now_tw.time() >= dt_time(13, 30)

    def next_weekday(weekday):
        days_ahead = (weekday - today.weekday()) % 7
        if days_ahead == 0 and passed_expiry_cutoff:
            days_ahead = 7
        return today + timedelta(days=days_ahead)

    def week_of_month(value):
        return (value.day - 1) // 7 + 1

    def wednesday_spec():
        nominal_expiry = next_weekday(2)
        week = week_of_month(nominal_expiry)
        root_map = {1: 'TX1', 2: 'TX2', 4: 'TX4', 5: 'TX5'}
        # 第三個星期三是月選，不是週選。
        if week == 3:
            nominal_expiry += timedelta(days=7)
            week = week_of_month(nominal_expiry)
        return {
            'root': root_map[week],
            'delivery_month': f"{nominal_expiry:%Y%m}W{week}",
            'expiry': adjust_to_next_market_day(nominal_expiry),
        }

    def friday_spec():
        nominal_expiry = next_weekday(4)
        week = week_of_month(nominal_expiry)
        root_map = {1: 'TXU', 2: 'TXV', 3: 'TXX', 4: 'TXY', 5: 'TXZ'}
        return {
            'root': root_map[week],
            'delivery_month': f"{nominal_expiry:%Y%m}F{week}",
            'expiry': adjust_to_next_market_day(nominal_expiry),
        }

    def monthly_spec():
        year, month = today.year, today.month
        first_day = date(year, month, 1)
        first_wednesday = first_day + timedelta(days=(2 - first_day.weekday()) % 7)
        nominal_expiry = first_wednesday + timedelta(days=14)
        expiry = adjust_to_next_market_day(nominal_expiry)
        if expiry < today or (expiry == today and passed_expiry_cutoff):
            next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
            first_wednesday = next_month + timedelta(days=(2 - next_month.weekday()) % 7)
            nominal_expiry = first_wednesday + timedelta(days=14)
            expiry = adjust_to_next_market_day(nominal_expiry)
        return {
            'root': 'TXO',
            'delivery_month': f"{nominal_expiry:%Y%m}",
            'expiry': expiry,
        }

    if expiry_choice == '週三選':
        return [wednesday_spec()]
    if expiry_choice == '週五選':
        return [friday_spec()]
    if expiry_choice == '月選':
        return [monthly_spec()]
    if expiry_choice == '最近到期':
        return sorted([wednesday_spec(), friday_spec(), monthly_spec()], key=lambda item: item['expiry'])
    return []


def get_txo_option_contracts(api, requested_specs=None):
    """Load only requested TXO contracts first, following the Shioaji contract API."""
    if api is None:
        return [], "永豐 Shioaji 尚未登入"

    attempts = []
    # Shioaji 1.7 documents a filtered options endpoint.  Querying it first avoids
    # downloading the entire TXO contract universe just to find one weekly expiry.
    specs = requested_specs or [{'root': 'TXO', 'delivery_month': None}]
    for spec in specs:
        root = spec['root']
        delivery_month = spec.get('delivery_month')
        try:
            response = requests.get(
                'http://127.0.0.1:8080/api/v1/data/contracts/options',
                params={'root': root, 'delivery_month': delivery_month}, timeout=2,
            )
            if response.status_code == 200:
                payload = response.json()
                if isinstance(payload, dict):
                    payload = payload.get('contracts', payload.get('data', []))
                if isinstance(payload, list) and payload:
                    compact_contracts = []
                    for item in payload:
                        if not isinstance(item, dict) or not item.get('code'):
                            continue
                        try:
                            shioaji_contract = api.contracts.get(item['code'])
                        except Exception:
                            shioaji_contract = None
                        if shioaji_contract is not None:
                            compact_contracts.append(SimpleNamespace(**item, shioaji_contract=shioaji_contract))
                    if compact_contracts:
                        attempts.append((compact_contracts, f"永豐 Shioaji {root} 指定契約 API（{delivery_month}）"))
        except (requests.RequestException, ValueError, TypeError):
            pass
        try:
            filtered_contracts = list(api.contracts.options(root, delivery_month=delivery_month))
            if filtered_contracts:
                attempts.append((filtered_contracts, f"永豐 Shioaji {root} {delivery_month} 契約檔"))
        except Exception:
            # 某些舊版 SDK 不支援 options(..., delivery_month=...)，下面改用完整契約檔篩選。
            pass
    for root in dict.fromkeys(spec['root'] for spec in specs):
        # 官方 Python SDK：每個商品 root 分開查詢，避免把週選誤查成 TXO 月選。
        try:
            attempts.append((list(api.contracts.options(root)), f"永豐 Shioaji {root} 商品根目錄"))
        except Exception:
            pass
        try:
            legacy = getattr(api.Contracts.Options, root)
            raw_contracts = list(legacy)
            attempts.append((raw_contracts, f"永豐 Shioaji {root} 契約檔（相容模式）"))
        except Exception:
            pass

    for raw_contracts, source in attempts:
        contracts = []
        for contract in raw_contracts:
            if isinstance(contract, str):
                try:
                    contract = api.contracts.get(contract)
                except Exception:
                    continue
            try:
                has_strike = getattr(contract, 'strike_price', None) is not None
            except Exception:
                has_strike = False
            if has_strike:
                contracts.append(contract)
        if contracts:
            unique_contracts = {}
            for contract in contracts:
                code = str(getattr(contract, 'code', '') or getattr(contract, 'symbol', '') or id(contract))
                unique_contracts[code] = contract
            return list(unique_contracts.values()), source
    return [], "永豐 Shioaji 未提供 TXO 選擇權契約檔"


def select_txo_expiry(api, expiry_choice):
    """Choose the nearest requested TXO expiry using the contract delivery date.

    Delivery dates are used as the primary filter because they remain stable across
    Shioaji SDK versions, unlike enum/string representations of expiry metadata.
    """
    target_specs = get_txo_target_contract_specs(expiry_choice)
    requested_delivery_months = [item['delivery_month'] for item in target_specs]
    today = datetime.now(pytz.timezone('Asia/Taipei')).date()

    def save_diagnostic(message):
        try:
            st.session_state['txo_contract_diagnostic'] = message
        except Exception:
            pass

    def expiry_date(contract):
        value = getattr(contract, 'delivery_date', getattr(contract, 'last_trading_date', None))
        try:
            return pd.Timestamp(value).date()
        except (TypeError, ValueError):
            return None

    def delivery_month(contract):
        try:
            return str(getattr(contract, 'delivery_month', '')).replace('/', '').replace('-', '').upper()
        except Exception:
            return ''

    def is_monthly(contract, expiry):
        week_value = getattr(contract, 'week_of_month', None)
        normalized = str(getattr(week_value, 'value', week_value)).lower()
        if normalized in ('3', 'third') or normalized.endswith('.third'):
            return True
        return expiry.weekday() == 2 and 15 <= expiry.day <= 21

    # 「最近到期」必須按日期逐一查找。舊邏輯一次載入三種到期別後，只要第一批
    # 沒找到 W2，就可能在後續迴圈直接接受 202608 月選，造成畫面上方與建議契約不一致。
    options = []
    sources = []
    seen_contracts = set()
    for spec in target_specs:
        batch_options, batch_source = get_txo_option_contracts(api, [spec])
        sources.append(batch_source)
        exact_contracts = [
            contract for contract in batch_options
            if delivery_month(contract) == spec['delivery_month']
            or expiry_date(contract) == spec['expiry']
            or (
                spec['root'] != 'TXO'
                and delivery_month(contract) == spec['delivery_month'][:6]
            )
        ]
        if exact_contracts:
            actual_expiries = [expiry_date(contract) for contract in exact_contracts]
            actual_expiries = [value for value in actual_expiries if value is not None]
            selected_expiry = min(actual_expiries) if actual_expiries else spec['expiry']
            selected_contracts = [
                contract for contract in exact_contracts
                if expiry_date(contract) in (None, selected_expiry)
            ]
            save_diagnostic(
                f"{batch_source}｜已取得 {len(selected_contracts)} 筆 "
                f"{selected_expiry:%Y/%m/%d} 到期契約"
            )
            return selected_contracts, selected_expiry, batch_source
        for contract in batch_options:
            contract_key = str(
                getattr(contract, 'code', '')
                or getattr(contract, 'symbol', '')
                or id(contract)
            )
            if contract_key not in seen_contracts:
                options.append(contract)
                seen_contracts.add(contract_key)

    source = '／'.join(dict.fromkeys(sources)) if sources else '永豐 Shioaji 未提供 TXO 選擇權契約檔'

    active = [(contract, expiry_date(contract)) for contract in options]
    active = [(contract, expiry) for contract, expiry in active if expiry is not None and expiry >= today]
    if expiry_choice == '週三選':
        active = [(contract, expiry) for contract, expiry in active if expiry.weekday() == 2 and not is_monthly(contract, expiry)]
    elif expiry_choice == '週五選':
        active = [(contract, expiry) for contract, expiry in active if expiry.weekday() == 4]
    elif expiry_choice == '月選':
        active = [(contract, expiry) for contract, expiry in active if is_monthly(contract, expiry)]
    if not active:
        available_months = sorted({delivery_month(contract) for contract in options if delivery_month(contract)})
        available_text = '、'.join(available_months[:8]) if available_months else '無可辨識交割月份'
        expected_text = '、'.join(requested_delivery_months) if requested_delivery_months else '最近到期'
        save_diagnostic(f"{source}｜共取得 {len(options)} 筆選擇權契約｜預期 {expected_text}｜實際可見：{available_text}")
        return [], None, source

    selected_expiry = min(expiry for _, expiry in active)
    selected_contracts = [contract for contract, expiry in active if expiry == selected_expiry]
    save_diagnostic(f"{source}｜已取得 {len(selected_contracts)} 筆 {selected_expiry:%Y/%m/%d} 到期契約")
    return selected_contracts, selected_expiry, source


def txo_contract_delivery_label(contract, expiry):
    """依實際到期日產生契約月份，修正部分 SDK 將週選只回傳 YYYYMM 的情況。"""
    try:
        normalized = str(getattr(contract, 'delivery_month', '')).replace('/', '').replace('-', '').upper()
    except Exception:
        normalized = ''
    if expiry is None:
        return normalized
    week = (expiry.day - 1) // 7 + 1
    if expiry.weekday() == 2 and not (15 <= expiry.day <= 21):
        return f"{expiry:%Y%m}W{week}"
    if expiry.weekday() == 4:
        return f"{expiry:%Y%m}F{week}"
    return f"{expiry:%Y%m}" if 15 <= expiry.day <= 21 and expiry.weekday() == 2 else (normalized or f"{expiry:%Y%m}")


def txo_right_value(contract):
    right = getattr(contract, 'option_right', '')
    value = str(getattr(right, 'value', right)).upper()
    if value in ('P', 'PUT') or value.endswith('.PUT'):
        return 'P'
    if value in ('C', 'CALL') or value.endswith('.CALL'):
        return 'C'
    return value


def get_txo_snapshot_prices(api, contracts, sides):
    """Read executable-side option prices from Shioaji streaming, falling back to close."""
    prices = [None] * len(contracts)
    try:
        snapshot_contracts = [getattr(contract, 'shioaji_contract', contract) for contract in contracts]
        snapshots = get_stream_quotes(api, snapshot_contracts)
        for index, (snapshot, side) in enumerate(zip(snapshots, sides)):
            field = 'buy_price' if side == 'sell' else 'sell_price'
            price = float(getattr(snapshot, field, 0) or getattr(snapshot, 'close', 0) or 0)
            prices[index] = price if price > 0 else None
    except Exception:
        pass
    return prices


def get_txo_snapshot_quotes(api, contracts):
    """Read executable option quotes and liquidity fields from the shared stream."""
    quotes = []
    try:
        snapshot_contracts = [getattr(contract, 'shioaji_contract', contract) for contract in contracts]
        snapshots = get_stream_quotes(api, snapshot_contracts)
    except Exception:
        snapshots = []
    for index, contract in enumerate(contracts):
        snapshot = snapshots[index] if index < len(snapshots) else None

        def number(field):
            try:
                value = float(getattr(snapshot, field, 0) or 0)
                return value if np.isfinite(value) and value > 0 else None
            except (TypeError, ValueError):
                return None

        bid = number('buy_price')
        ask = number('sell_price')
        last = number('close')
        premium = ask or last
        mid = ((bid + ask) / 2) if bid is not None and ask is not None else premium
        spread = (ask - bid) if bid is not None and ask is not None and ask >= bid else None
        spread_pct = (spread / mid * 100) if spread is not None and mid else None
        volume = number('total_volume') or 0.0
        bid_volume = number('buy_volume') or 0.0
        ask_volume = number('sell_volume') or 0.0
        book_total = bid_volume + ask_volume
        book_balance = (bid_volume - ask_volume) / book_total if book_total > 0 else 0.0
        if ask is None:
            liquidity = '報價不足'
        elif bid is None or spread_pct is None or spread_pct > 25:
            liquidity = '低'
        elif spread_pct <= 8 and volume >= 50:
            liquidity = '高'
        else:
            liquidity = '中'
        quotes.append({
            'contract': contract, 'bid': bid, 'ask': ask, 'last': last,
            'premium': premium, 'spread': spread, 'spread_pct': spread_pct,
            'volume': volume, 'bid_volume': bid_volume, 'ask_volume': ask_volume,
            'book_balance': book_balance, 'liquidity': liquidity,
        })
    return quotes


def _normal_cdf(value):
    return 0.5 * (1.0 + math.erf(float(value) / math.sqrt(2.0)))


def black_scholes_index_option_price(spot, strike, years, volatility, is_call, rate=0.012, dividend_yield=0.0):
    """European index option value used only for clearly-labelled estimates."""
    spot, strike = float(spot), float(strike)
    years, volatility = max(float(years), 1e-8), max(float(volatility), 1e-6)
    root_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate - dividend_yield + 0.5 * volatility ** 2) * years) / (volatility * root_t)
    d2 = d1 - volatility * root_t
    if is_call:
        return spot * math.exp(-dividend_yield * years) * _normal_cdf(d1) - strike * math.exp(-rate * years) * _normal_cdf(d2)
    return strike * math.exp(-rate * years) * _normal_cdf(-d2) - spot * math.exp(-dividend_yield * years) * _normal_cdf(-d1)


def estimate_implied_volatility(market_price, spot, strike, years, is_call):
    """Invert Black-Scholes by bisection; return None when the quote is invalid."""
    if market_price is None or float(market_price) <= 0 or float(spot) <= 0 or float(strike) <= 0:
        return None
    intrinsic = max(float(spot) - float(strike), 0.0) if is_call else max(float(strike) - float(spot), 0.0)
    if float(market_price) + 1e-6 < intrinsic:
        return None
    low, high = 0.01, 5.0
    if black_scholes_index_option_price(spot, strike, years, high, is_call) < float(market_price):
        return None
    for _ in range(70):
        middle = (low + high) / 2
        value = black_scholes_index_option_price(spot, strike, years, middle, is_call)
        if value < float(market_price):
            low = middle
        else:
            high = middle
    return (low + high) / 2


def option_time_to_expiry_years(expiry):
    timezone = pytz.timezone('Asia/Taipei')
    now_tw = datetime.now(timezone)
    expiry_dt = timezone.localize(datetime.combine(expiry, dt_time(13, 30)))
    remaining_seconds = max((expiry_dt - now_tw).total_seconds(), 30 * 60)
    return remaining_seconds / (365.25 * 24 * 60 * 60)


def option_profit_probability(spot, breakeven, years, volatility, is_call, rate=0.012):
    """Risk-neutral terminal probability of finishing beyond the premium breakeven."""
    if breakeven is None or volatility is None or spot <= 0 or breakeven <= 0:
        return None
    root_t = math.sqrt(max(years, 1e-8))
    d2 = (math.log(float(spot) / float(breakeven)) + (rate - 0.5 * volatility ** 2) * years) / (volatility * root_t)
    return _normal_cdf(d2) if is_call else _normal_cdf(-d2)


def _txo_strike_step(contracts, default=50.0):
    strikes = sorted({float(getattr(contract, 'strike_price', 0) or 0) for contract in contracts})
    gaps = [right - left for left, right in zip(strikes, strikes[1:]) if right > left]
    return float(np.median(gaps)) if gaps else float(default)


def evaluate_txo_directional_quality(
    premium, target_pnl, stop_pnl, probability, spread_pct, liquidity, target_reachable,
):
    """Gate quick BC/BP ideas on reachable profit, liquidity and payoff quality.

    ``trade_ready`` remains the normal, stricter entry gate.  ``small_position_ready``
    is deliberately a separate tier: it is allowed to surface a rare quick-trade
    opportunity when the win probability / quote width is less ideal, but it still
    requires a positive target scenario, a reachable breakeven and a minimum
    reward-to-risk ratio.  This prevents a relaxed threshold from turning a losing
    model scenario into a false "high reward" recommendation.
    """
    premium_risk = max(float(premium or 0) * 50 * 0.40, 1.0)
    scenario_risk = max(abs(min(float(stop_pnl or 0), 0.0)), premium_risk)
    payoff_ratio = max(float(target_pnl or 0), 0.0) / scenario_risk
    reasons = []
    if not target_reachable:
        reasons.append('短波目標尚未越過損益兩平')
    if float(target_pnl or 0) <= 0:
        reasons.append('目標情境仍未獲利')
    if payoff_ratio < 1.25:
        reasons.append('目標報酬／風險低於 1.25')
    if probability is None or float(probability) < 0.38:
        reasons.append('模型獲利機率低於 38%')
    if liquidity not in ('高', '中'):
        reasons.append('流動性不足')
    if spread_pct is None or float(spread_pct) > 20:
        reasons.append('買賣價差過寬')

    # 放寬版只用於「小部位試單」提示，不會降低一般單買的 1.25
    # 報酬／風險標準。仍須目標可達、目標情境為正，避免以小搏大被誤解
    # 成「可以接受負損益」。
    relaxed_reasons = []
    if not target_reachable:
        relaxed_reasons.append('短波目標尚未越過損益兩平')
    if float(target_pnl or 0) <= 0:
        relaxed_reasons.append('目標情境仍未獲利')
    if payoff_ratio < 0.90:
        relaxed_reasons.append('目標報酬／風險低於 0.90')
    if probability is None or float(probability) < 0.33:
        relaxed_reasons.append('模型獲利機率低於 33%')
    if liquidity not in ('高', '中'):
        relaxed_reasons.append('流動性不足')
    if spread_pct is None or float(spread_pct) > 25:
        relaxed_reasons.append('買賣價差超過 25%')
    return {
        'trade_ready': not reasons,
        'small_position_ready': not relaxed_reasons,
        'payoff_ratio': payoff_ratio,
        'quality_notes': reasons,
        'small_position_notes': relaxed_reasons,
    }


def rank_txo_directional_candidates(
    contracts, quotes, plan, is_buy_call, moneyness_preference, selected_expiry,
):
    """Compare ITM/ATM/OTM contracts with volatility, liquidity and scenario P/L."""
    spot = float(plan['latest'])
    target = float(plan['target'])
    stop = float(plan['invalidation'])
    years = option_time_to_expiry_years(selected_expiry)
    realized_vol = float(plan.get('realized_volatility', 0.25) or 0.25)
    atm_contract = min(contracts, key=lambda item: abs(float(getattr(item, 'strike_price', 0)) - spot)) if contracts else None
    atm_strike = float(getattr(atm_contract, 'strike_price', spot))
    max_volume = max([float(quote.get('volume', 0) or 0) for quote in quotes] + [1.0])
    rows = []
    for contract, quote in zip(contracts, quotes):
        strike = float(getattr(contract, 'strike_price', 0) or 0)
        if strike == atm_strike:
            moneyness = '平價'
        elif (strike < atm_strike and is_buy_call) or (strike > atm_strike and not is_buy_call):
            moneyness = '價內'
        else:
            moneyness = '價外'
        if moneyness_preference in ('價外', '平價', '價內') and moneyness != moneyness_preference:
            continue
        premium = quote.get('premium')
        if premium is None:
            continue
        implied_vol = estimate_implied_volatility(premium, spot, strike, years, is_buy_call)
        model_vol = implied_vol or realized_vol
        volatility_source = '即時權利金反推 IV' if implied_vol is not None else '20 日歷史波動率替代'
        breakeven = strike + premium if is_buy_call else strike - premium
        probability = option_profit_probability(spot, breakeven, years, model_vol, is_buy_call)
        remaining_after_fast_exit = max(years * 0.65, 30 * 60 / (365.25 * 24 * 60 * 60))
        target_option_price = black_scholes_index_option_price(target, strike, remaining_after_fast_exit, model_vol, is_buy_call)
        stop_option_price = black_scholes_index_option_price(stop, strike, remaining_after_fast_exit, model_vol, is_buy_call)
        target_pnl = (target_option_price - premium) * 50
        stop_pnl = (stop_option_price - premium) * 50
        target_return_pct = (target_option_price - premium) / premium * 100 if premium > 0 else None
        # 使用相同到期時間與同一模型衡量理論價差，避免把到期機率與
        # 到期前目標／停損損益混成一個不一致的「期望值」。
        theoretical_now = black_scholes_index_option_price(
            spot, strike, years, model_vol, is_buy_call,
        )
        model_value_gap = (theoretical_now - premium) * 50
        distance_points = abs(strike - spot)
        target_reachable = target >= breakeven if is_buy_call else target <= breakeven
        liquidity_score = {'高': 1.0, '中': 0.65, '低': 0.25, '報價不足': 0.0}.get(quote['liquidity'], 0.0)
        volume_score = math.sqrt(max(float(quote.get('volume', 0) or 0), 0.0) / max_volume)
        flow_score = float(np.clip((float(quote.get('book_balance', 0) or 0) + 1) / 2, 0, 1))
        capital_efficiency = float(np.clip(max(target_pnl, 0.0) / max(premium * 50, 1.0), 0, 2)) / 2
        probability_score = float(probability or 0.0)
        spread_pct_value = quote.get('spread_pct')
        spread_quality = (
            float(np.clip(1 - float(spread_pct_value) / 30, 0, 1))
            if spread_pct_value is not None else 0.0
        )
        quality = evaluate_txo_directional_quality(
            premium, target_pnl, stop_pnl, probability, spread_pct_value,
            quote['liquidity'], target_reachable,
        )
        payoff_ratio = quality['payoff_ratio']
        payoff_score = float(np.clip(payoff_ratio / 3, 0, 1))
        value_score = float(np.clip((float(model_value_gap or 0) / max(premium * 50, 1.0) + 1) / 2, 0, 1))
        if moneyness == '價外':
            moneyness_score = 0.60 if target_reachable else 0.1
        elif moneyness == '平價':
            moneyness_score = 0.90
        else:
            moneyness_score = 1.0
        score = 100 * (
            0.25 * probability_score + 0.15 * capital_efficiency + 0.14 * liquidity_score
            + 0.10 * spread_quality + 0.09 * volume_score + 0.07 * flow_score
            + 0.12 * payoff_score + 0.03 * value_score + 0.05 * moneyness_score
        )
        rows.append({
            **quote, 'contract': contract, 'strike': strike, 'moneyness': moneyness,
            'distance_points': distance_points, 'distance_pct': distance_points / spot * 100 if spot else 0.0,
            'target_reachable': target_reachable, 'score': score, 'breakeven': breakeven,
            'implied_volatility': implied_vol, 'model_volatility': model_vol,
            'volatility_source': volatility_source, 'model_probability': probability,
            'target_option_price': target_option_price, 'stop_option_price': stop_option_price,
            'target_pnl': target_pnl, 'stop_pnl': stop_pnl,
            'target_return_pct': target_return_pct,
            'model_value_gap': model_value_gap, 'expected_pnl': model_value_gap,
            **quality,
        })
    rows.sort(
        key=lambda row: (
            row['trade_ready'], row.get('small_position_ready', False), row['score']
        ),
        reverse=True,
    )
    return rows


def _txo_contract_strike(contract):
    value = contract.get('strike') if isinstance(contract, dict) else getattr(contract, 'strike_price', None)
    return _safe_number(value)


def _select_txo_spread_hedge(contracts, short_contract, is_bull_put, preferred_width=100):
    """價差只採 50／100 點；預設先找 100 點，缺少時才退回 50 點。"""
    short_strike = _txo_contract_strike(short_contract)
    if short_strike is None:
        return None
    widths = [50] if int(preferred_width) == 50 else [100, 50]
    for width in widths:
        target_strike = short_strike - width if is_bull_put else short_strike + width
        match = next(
            (contract for contract in contracts
             if _txo_contract_strike(contract) is not None
             and abs(_txo_contract_strike(contract) - target_strike) < 0.01),
            None,
        )
        if match is not None:
            return match
    return None


def get_txo_spread_quote(api, plan, expiry_choice, preferred_width=100):
    """Find an OTM defined-risk credit spread for the selected TXO expiry."""
    if api is None or plan is None or plan['direction'] not in ('偏多', '偏空'):
        return None
    expiry_options, selected_expiry, source = select_txo_expiry(api, expiry_choice)
    if not expiry_options:
        return None

    is_bull_put = plan['direction'] == '偏多'
    right = 'P' if is_bull_put else 'C'
    contracts = [contract for contract in expiry_options if txo_right_value(contract) == right]
    spot = plan['latest']
    strike_step = _txo_strike_step(contracts)
    if is_bull_put:
        desired = min(plan['entry_level'] - plan['zone_points'] * 0.5, spot - strike_step)
        short_candidates = [c for c in contracts if float(getattr(c, 'strike_price', 0)) <= desired]
        short_contract = max(short_candidates, key=lambda c: float(c.strike_price)) if short_candidates else None
    else:
        desired = max(plan['entry_level'] + plan['zone_points'] * 0.5, spot + strike_step)
        short_candidates = [c for c in contracts if float(getattr(c, 'strike_price', 0)) >= desired]
        short_contract = min(short_candidates, key=lambda c: float(c.strike_price)) if short_candidates else None
    long_contract = _select_txo_spread_hedge(
        contracts, short_contract, is_bull_put, preferred_width
    ) if short_contract is not None else None
    if short_contract is None or long_contract is None:
        return None

    spread_quotes = get_txo_snapshot_quotes(api, [short_contract, long_contract])
    short_quote, long_quote = spread_quotes
    short_price = short_quote['bid'] or short_quote['last']
    long_price = long_quote['ask'] or long_quote['last']
    width = abs(float(short_contract.strike_price) - float(long_contract.strike_price))
    credit_points = (short_price - long_price) if short_price is not None and long_price is not None else None
    if credit_points is not None and credit_points <= 0:
        credit_points = None
    credit = credit_points * 50 if credit_points is not None else None
    max_loss = width * 50 - credit if credit is not None else width * 50
    dte = (selected_expiry - datetime.now(pytz.timezone('Asia/Taipei')).date()).days
    years = option_time_to_expiry_years(selected_expiry)
    short_mid = (
        (short_quote['bid'] + short_quote['ask']) / 2
        if short_quote['bid'] is not None and short_quote['ask'] is not None else short_quote['premium']
    )
    implied_vol = estimate_implied_volatility(
        short_mid, float(spot), float(short_contract.strike_price), years, not is_bull_put,
    )
    model_vol = implied_vol or float(plan.get('realized_volatility', 0.25) or 0.25)
    breakeven = None
    model_probability = None
    expected_pnl = None
    if credit_points is not None:
        breakeven = (
            float(short_contract.strike_price) - credit_points
            if is_bull_put else float(short_contract.strike_price) + credit_points
        )
        model_probability = option_profit_probability(
            float(spot), breakeven, years, model_vol, is_call=is_bull_put,
        )
        short_strike = float(short_contract.strike_price)
        long_strike = float(long_contract.strike_price)
        short_model_value = black_scholes_index_option_price(
            float(spot), short_strike, years, model_vol, is_call=not is_bull_put,
        )
        long_model_value = black_scholes_index_option_price(
            float(spot), long_strike, years, model_vol, is_call=not is_bull_put,
        )
        theoretical_spread_value = max(0.0, short_model_value - long_model_value)
        expected_pnl = credit - theoretical_spread_value * 50
    return {
        'name': '賣權多頭價差（Bull Put Credit Spread）' if is_bull_put else '買權空頭價差（Bear Call Credit Spread）',
        'right': 'Put' if is_bull_put else 'Call', 'short_contract': short_contract,
        'long_contract': long_contract, 'short_strike': float(short_contract.strike_price),
        'long_strike': float(long_contract.strike_price), 'expiry': selected_expiry,
        'dte': dte, 'short_premium': short_price, 'long_premium': long_price,
        'net_credit_points': credit_points, 'net_credit': credit, 'max_profit': credit,
        'max_loss': max_loss, 'risk_level': '高' if dte <= 1 else ('中高' if dte <= 3 else '中'),
        'breakeven': breakeven, 'model_probability': model_probability,
        'expected_pnl': expected_pnl, 'model_volatility': model_vol,
        'implied_volatility': implied_vol,
        'source': source, 'delivery_month': txo_contract_delivery_label(short_contract, selected_expiry),
    }


def get_txo_directional_quote(api, plan, expiry_choice, moneyness_preference='自動評選'):
    """Compare BC/BP candidates across ITM, ATM and OTM using live quotes."""
    if api is None or plan is None or plan['direction'] not in ('偏多', '偏空'):
        return None
    expiry_options, selected_expiry, source = select_txo_expiry(api, expiry_choice)
    if not expiry_options:
        return None

    is_buy_call = plan['direction'] == '偏多'
    right = 'C' if is_buy_call else 'P'
    contracts = [contract for contract in expiry_options if txo_right_value(contract) == right]
    spot = float(plan['latest'])
    if not contracts:
        return None

    # Cover the live price, stop and target instead of blindly taking only the
    # nearest strikes.  This keeps OTM candidates reachable while bounding API use.
    strike_step = _txo_strike_step(contracts)
    lower_bound = min(spot, float(plan['target']), float(plan['invalidation'])) - strike_step * 2
    upper_bound = max(spot, float(plan['target']), float(plan['invalidation'])) + strike_step * 2
    nearby = [
        contract for contract in contracts
        if lower_bound <= float(getattr(contract, 'strike_price', 0) or 0) <= upper_bound
    ]
    if not nearby:
        nearby = sorted(contracts, key=lambda c: abs(float(c.strike_price) - spot))[:18]
    elif len(nearby) > 24:
        anchors = (spot, float(plan['target']), float(plan['invalidation']))
        nearby = sorted(
            nearby,
            key=lambda c: min(abs(float(c.strike_price) - anchor) for anchor in anchors),
        )[:24]
    quotes = get_txo_snapshot_quotes(api, nearby)
    ranked = rank_txo_directional_candidates(
        nearby, quotes, plan, is_buy_call, moneyness_preference, selected_expiry,
    )
    if not ranked:
        return None
    selected = ranked[0]
    contract = selected['contract']
    premium = selected['premium']
    dte = (selected_expiry - datetime.now(pytz.timezone('Asia/Taipei')).date()).days
    strike = float(contract.strike_price)
    breakeven = selected['breakeven']
    target = float(plan['target'])
    target_intrinsic = max(target - strike, 0.0) if is_buy_call else max(strike - target, 0.0)
    return {
        'name': '單買買權（BC / Buy Call）' if is_buy_call else '單買賣權（BP / Buy Put）',
        'right': 'Call' if is_buy_call else 'Put', 'contract': contract,
        'strike': strike, 'expiry': selected_expiry, 'dte': dte,
        'premium': premium, 'max_loss': premium * 50 if premium is not None else None,
        'risk_level': '高' if dte <= 1 else ('中高' if dte <= 3 else '中'),
        'source': source, 'delivery_month': txo_contract_delivery_label(contract, selected_expiry),
        'premium_basis': '永豐 Shioaji 快照：最佳賣價，缺值時以最後成交價替代',
        'bid': selected['bid'], 'ask': selected['ask'], 'spread': selected['spread'],
        'spread_pct': selected['spread_pct'], 'volume': selected['volume'],
        'liquidity': selected['liquidity'], 'distance_points': selected['distance_points'],
        'distance_pct': selected['distance_pct'], 'moneyness': selected['moneyness'],
        'target_reachable': selected['target_reachable'], 'profile': moneyness_preference,
        'breakeven': breakeven, 'target_intrinsic': target_intrinsic,
        'target_pnl': selected['target_pnl'], 'stop_pnl': selected['stop_pnl'],
        'target_return_pct': selected['target_return_pct'],
        'model_value_gap': selected['model_value_gap'], 'expected_pnl': selected['model_value_gap'],
        'implied_volatility': selected['implied_volatility'],
        'model_volatility': selected['model_volatility'],
        'volatility_source': selected['volatility_source'],
        'model_probability': selected['model_probability'], 'selection_score': selected['score'],
        'trade_ready': selected['trade_ready'], 'payoff_ratio': selected['payoff_ratio'],
        'small_position_ready': selected.get('small_position_ready', False),
        'quality_notes': selected['quality_notes'],
        'small_position_notes': selected.get('small_position_notes', []),
        'alternatives': ranked[:6],
    }


def recommend_txo_strategy(directional_quote, spread_quote, plan):
    """Choose long premium or defined-risk spread from volatility and signal conditions."""
    realized_vol = float(plan.get('realized_volatility', 0.25) or 0.25)
    valid_spread = spread_quote is not None and float(spread_quote.get('max_profit') or 0) > 0
    if directional_quote is None and not valid_spread:
        return {'choice': '等待', 'color': '#ffc107', 'reason': '契約或即時報價不足，無法建立可驗證的選擇權計畫。'}
    if directional_quote is None:
        return {'choice': '價差單', 'color': '#ffc107', 'reason': '單買契約報價不足；僅保留限定風險價差單參考。'}
    implied_vol = float(directional_quote.get('model_volatility', realized_vol) or realized_vol)
    iv_ratio = implied_vol / max(realized_vol, 1e-6)
    dte = int(directional_quote.get('dte', 0) or 0)
    probability = float(directional_quote.get('model_probability', 0) or 0)
    liquidity = directional_quote.get('liquidity')
    if not directional_quote.get('trade_ready', False):
        detail = '、'.join(directional_quote.get('quality_notes') or ['單買條件未達門檻'])
        if directional_quote.get('small_position_ready', False):
            # This branch is intentionally explicit: the relaxed tier is an
            # opportunity to test with a small, predefined loss only—not a
            # replacement for the normal 1.25 R/R entry gate.
            right = directional_quote['right']
            side_color = '#ff4b4b' if plan['direction'] == '偏多' else '#00c853'
            return {
                'choice': f'單買 {right}（小部位試單）',
                'color': side_color,
                'reason': (
                    f"放寬條件後可觀察{directional_quote['moneyness']} {right}；"
                    f"模型獲利機率約 {_format_compact_number(probability * 100, 1)}%，"
                    f"報酬／風險約 {_format_compact_number(directional_quote.get('payoff_ratio'), 2)}。"
                    "這不是一般單買通過，僅限可承受歸零的小部位，仍須 5 分 K 確認。"
                ),
                'iv_ratio': iv_ratio,
                'relaxed': True,
            }
        if valid_spread:
            return {
                'choice': '價差單', 'color': '#ffc107',
                'reason': f'單買暫不採用（{detail}）；改看最大風險已限定的價差單。',
                'iv_ratio': iv_ratio,
            }
        return {
            'choice': '等待', 'color': '#ffc107',
            'reason': f'單買暫不採用：{detail}。等價格、流動性或報酬風險改善後再評估。',
            'iv_ratio': iv_ratio,
        }
    if valid_spread and (iv_ratio >= 1.20 or (dte <= 1 and probability < 0.45)):
        reason = (
            f"隱含／模型波動率約為 20 日實現波動率的 {_format_compact_number(iv_ratio, 2)} 倍，"
            "單買權利金相對偏貴或到期時間過短；限定風險價差較能降低時間價值與波動率回落風險。"
        )
        return {'choice': '價差單', 'color': '#ffc107', 'reason': reason, 'iv_ratio': iv_ratio}
    if probability >= 0.38 and liquidity in ('高', '中') and float(directional_quote.get('payoff_ratio', 0) or 0) >= 1.25:
        reason = (
            f"模型到期獲利機率約 {_format_compact_number(probability * 100, 1)}%，且流動性為{liquidity}；"
            f"目前以{directional_quote['moneyness']} {directional_quote['right']} 快進快出較合適。"
        )
        return {'choice': f"單買 {directional_quote['right']}", 'color': '#ff4b4b' if plan['direction'] == '偏多' else '#00c853', 'reason': reason, 'iv_ratio': iv_ratio}
    return {
        'choice': '等待／小部位', 'color': '#ffc107',
        'reason': '模型獲利機率或即時流動性未達快進快出條件；若仍交易，只採可承受歸零的小部位。',
        'iv_ratio': iv_ratio,
    }


def render_option_metric_cards(title, items):
    """以小字、分組卡片呈現選擇權資訊，避免多排大型 metric 難以掃讀。"""
    cards = ''.join(
        "<div style='background:#10151d;border:1px solid #303845;border-radius:6px;padding:8px 10px;'>"
        f"<div style='font-size:11px;color:#aeb8c5;line-height:1.2'>{html.escape(str(label))}</div>"
        f"<div style='font-size:17px;font-weight:700;color:{color};line-height:1.35;margin-top:2px'>"
        f"{html.escape(str(value))}</div></div>"
        for label, value, color in items
    )
    st.markdown(
        f"<div style='font-size:13px;font-weight:700;color:#dfe6e9;margin:10px 0 5px'>{html.escape(title)}</div>"
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:7px;'>"
        f"{cards}</div>",
        unsafe_allow_html=True,
    )


def _txo_scenario_color(value, positive='#ff4b4b', negative='#00c853'):
    """Return the app's red/green semantic color for a scenario P/L value."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return '#cbd5e1'
    if numeric > 0:
        return positive
    if numeric < 0:
        return negative
    return '#cbd5e1'


def render_txo_scenario_note(option_quote):
    """Render the target/stop scenario with explicit colors and mobile wrapping."""
    if not option_quote or option_quote.get('target_return_pct') is None:
        return
    target_return = _format_compact_number(
        option_quote['target_return_pct'], 2, signed=True,
    )
    target_pnl = _format_compact_number(option_quote['target_pnl'], 0, signed=True)
    stop_pnl = _format_compact_number(option_quote['stop_pnl'], 0, signed=True)
    target_color = _txo_scenario_color(option_quote.get('target_pnl'))
    stop_color = _txo_scenario_color(option_quote.get('stop_pnl'))
    st.markdown(
        "<div style='margin:8px 0;padding:9px 11px;border-left:3px solid #64748b;"
        "background:#111821;border-radius:4px;color:#cbd5e1;font-size:13px;"
        "line-height:1.55;overflow-wrap:anywhere;'>"
        "<span style='color:#94a3b8;'>📌 短波情境（假設剩餘時間約 65%）：</span> "
        f"目標報酬 <strong style='color:{target_color};'>{html.escape(target_return)}%</strong> "
        f"（損益 <strong style='color:{target_color};'>${html.escape(target_pnl)}</strong>）；"
        f"停損損益 <strong style='color:{stop_color};'>${html.escape(stop_pnl)}</strong>。 "
        "未計手續費、稅與波動率即時變化。"
        "</div>",
        unsafe_allow_html=True,
    )


def build_txo_payoff_chart(option_quote, plan, is_spread=False):
    """Build an interactive expiry payoff curve in TWD per option position."""
    if not option_quote or not plan:
        return None
    spot = float(plan.get('latest', 0) or 0)
    target = float(plan.get('target', spot) or spot)
    stop = float(plan.get('invalidation', spot) or spot)
    if spot <= 0:
        return None

    if is_spread:
        credit_points = option_quote.get('net_credit_points')
        if credit_points is None and option_quote.get('net_credit') is not None:
            credit_points = float(option_quote['net_credit']) / 50
        if credit_points is None:
            return None
        short_strike = float(option_quote['short_strike'])
        long_strike = float(option_quote['long_strike'])
        strikes = [short_strike, long_strike]
        is_put = option_quote.get('right') == 'Put'
    else:
        premium = option_quote.get('premium')
        if premium is None:
            return None
        strike = float(option_quote['strike'])
        strikes = [strike]
        is_call = option_quote.get('right') == 'Call'

    key_points = [spot, target, stop, *strikes]
    if option_quote.get('breakeven') is not None:
        key_points.append(float(option_quote['breakeven']))
    span = max(max(key_points) - min(key_points), float(plan.get('atr', 0) or 0) * 2, 400.0)
    lower = math.floor((min(key_points) - span * 0.35) / 50) * 50
    upper = math.ceil((max(key_points) + span * 0.35) / 50) * 50
    underlying = np.linspace(lower, upper, 241)

    if is_spread:
        if is_put:
            payoff_points = (
                float(credit_points)
                - np.maximum(short_strike - underlying, 0)
                + np.maximum(long_strike - underlying, 0)
            )
        else:
            payoff_points = (
                float(credit_points)
                - np.maximum(underlying - short_strike, 0)
                + np.maximum(underlying - long_strike, 0)
            )
    else:
        intrinsic = np.maximum(underlying - strike, 0) if is_call else np.maximum(strike - underlying, 0)
        payoff_points = intrinsic - float(premium)
    payoff_twd = payoff_points * 50

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=underlying, y=payoff_twd, mode='lines', name='到期損益',
        line=dict(color='#dfe6e9', width=2.5),
        hovertemplate='結算指數 %{x:,.0f}<br>損益 $%{y:,.0f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=underlying, y=np.where(payoff_twd >= 0, payoff_twd, np.nan),
        mode='lines', line=dict(color='#ff4b4b', width=3), name='獲利',
        hovertemplate='結算指數 %{x:,.0f}<br>獲利 $%{y:,.0f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=underlying, y=np.where(payoff_twd < 0, payoff_twd, np.nan),
        mode='lines', line=dict(color='#00c853', width=3), name='虧損',
        hovertemplate='結算指數 %{x:,.0f}<br>虧損 $%{y:,.0f}<extra></extra>',
    ))
    fig.add_hline(y=0, line_color='#7f8c8d', line_width=1)
    markers = [
        ('目前', spot, '#29b6f6'), ('目標', target, '#ff4b4b'), ('停損', stop, '#00c853'),
    ]
    if option_quote.get('breakeven') is not None:
        markers.append(('損益兩平', float(option_quote['breakeven']), '#ffb300'))
    # Labels at nearby strikes (especially current/target/stop on a spread)
    # used to share one row and overlap on both desktop and phone widths.
    # Assign horizontal-distance lanes so close labels move to separate rows.
    marker_lanes = []
    annotation_gap = max(span * 0.09, 120.0)
    for label, value, color in sorted(markers, key=lambda item: item[1]):
        lane = next(
            (position for position, last_value in enumerate(marker_lanes)
             if value - last_value >= annotation_gap),
            len(marker_lanes),
        )
        if lane == len(marker_lanes):
            marker_lanes.append(value)
        else:
            marker_lanes[lane] = value
        fig.add_vline(x=value, line_color=color, line_dash='dot', line_width=1.4)
        fig.add_annotation(
            x=value, y=0.98, yref='paper',
            text=f"{label} {value:,.0f}",
            showarrow=True, arrowhead=0, arrowwidth=1.4, arrowcolor=color,
            ax=0, ay=-(28 + lane * 27), xanchor='center', yanchor='bottom',
            bgcolor='rgba(16,21,29,.88)', bordercolor=color, borderwidth=1,
            borderpad=3, font=dict(size=12, color=color),
        )
    fig.update_layout(
        template='plotly_dark', height=430,
        margin=dict(l=45, r=24, t=118 + max(0, len(marker_lanes) - 2) * 18, b=62),
        title=dict(text='到期損益曲線（每口／每組）', font=dict(size=17)),
        xaxis_title='到期結算指數', yaxis_title='損益（元）',
        legend=dict(orientation='h', y=-0.20, x=1, xanchor='right', font=dict(size=11)),
        hovermode='x unified',
    )
    fig.update_xaxes(title_font=dict(size=13), tickfont=dict(size=12), automargin=True)
    fig.update_yaxes(title_font=dict(size=13), tickfont=dict(size=12), automargin=True)
    return fig


def _taifex_number(value):
    """Convert a TAIFEX table cell to float, retaining unavailable values as None."""
    text = str(value).strip().replace(',', '')
    if text in ('', '-', '—'):
        return None
    try:
        return float(text)
    except ValueError:
        return None


@st.cache_data(ttl=60, max_entries=1, show_spinner=False)
def fetch_taifex_txo_daily_quotes():
    """Fetch official TXO daily rows as a fallback when Shioaji contracts are unavailable."""
    try:
        response = requests.get(
            'https://www.taifex.com.tw/cht/3/optDailyMarketExcel?marketCode=1', timeout=15,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        records = []
        for row in soup.find_all('tr'):
            cells = [cell.get_text(' ', strip=True) for cell in row.find_all(['td', 'th'])]
            if len(cells) < 16 or cells[0] != 'TXO':
                continue
            expiry_text = str(cells[2]).strip()
            try:
                expiry = datetime.strptime(expiry_text, '%Y%m%d').date()
            except ValueError:
                continue
            right = 'C' if cells[4].strip().lower() == 'call' else ('P' if cells[4].strip().lower() == 'put' else '')
            strike = _taifex_number(cells[3])
            if not right or strike is None:
                continue
            records.append({
                'delivery_month': str(cells[1]).strip().upper(), 'expiry': expiry,
                'strike': strike, 'right': right,
                'last': _taifex_number(cells[8]), 'bid': _taifex_number(cells[14]),
                'ask': _taifex_number(cells[15]),
            })
        return records
    except (requests.RequestException, ValueError, TypeError):
        return []


def get_taifex_txo_records(expiry_choice):
    """Return the requested expiry from official TAIFEX daily TXO rows."""
    records = fetch_taifex_txo_daily_quotes()
    target_specs = get_txo_target_contract_specs(expiry_choice)
    for target in target_specs:
        matches = [
            record for record in records
            if record['delivery_month'] == target['delivery_month']
            or record['expiry'] == target['expiry']
        ]
        if matches:
            actual_expiry = min(record['expiry'] for record in matches)
            return [record for record in matches if record['expiry'] == actual_expiry], actual_expiry
    if target_specs:
        return [], None
    today = datetime.now(pytz.timezone('Asia/Taipei')).date()
    active = [record for record in records if record['expiry'] >= today]
    if not active:
        return [], None
    expiry = min(record['expiry'] for record in active)
    return [record for record in active if record['expiry'] == expiry], expiry


def get_taifex_txo_directional_quote(plan, expiry_choice):
    """Offer a delayed official-market fallback for a single BC/BP when Shioaji is unavailable."""
    if plan is None or plan['direction'] not in ('偏多', '偏空'):
        return None
    records, expiry = get_taifex_txo_records(expiry_choice)
    if not records:
        return None
    is_buy_call = plan['direction'] == '偏多'
    right = 'C' if is_buy_call else 'P'
    contracts = [record for record in records if record['right'] == right]
    spot = plan['latest']
    if is_buy_call:
        candidates = [record for record in contracts if record['strike'] >= spot]
        contract = min(candidates, key=lambda record: record['strike']) if candidates else None
    else:
        candidates = [record for record in contracts if record['strike'] <= spot]
        contract = max(candidates, key=lambda record: record['strike']) if candidates else None
    if contract is None:
        return None
    premium = contract['ask'] if contract['ask'] is not None else contract['last']
    premium_basis = '期交所最後最佳賣價' if contract['ask'] is not None else '期交所最後成交價（當時無最佳賣價）'
    dte = (expiry - datetime.now(pytz.timezone('Asia/Taipei')).date()).days
    return {
        'name': '單買買權（BC / Buy Call）' if is_buy_call else '單買賣權（BP / Buy Put）',
        'right': 'Call' if is_buy_call else 'Put', 'strike': contract['strike'],
        'expiry': expiry, 'dte': dte, 'premium': premium,
        'max_loss': premium * 50 if premium is not None else None,
        'risk_level': '高' if dte <= 1 else ('中高' if dte <= 3 else '中'),
        'source': '期交所每日選擇權行情備援（非永豐即時快照）',
        'delivery_month': contract['delivery_month'],
        'premium_basis': premium_basis,
    }


def get_taifex_txo_spread_quote(plan, expiry_choice, preferred_width=100):
    """Offer a defined-risk TXO spread from official daily rows when needed."""
    if plan is None or plan['direction'] not in ('偏多', '偏空'):
        return None
    records, expiry = get_taifex_txo_records(expiry_choice)
    if not records:
        return None
    is_bull_put = plan['direction'] == '偏多'
    right = 'P' if is_bull_put else 'C'
    contracts = [record for record in records if record['right'] == right]
    spot = plan['latest']
    if is_bull_put:
        desired = min(plan['invalidation'] - plan['zone_points'], spot - 1)
        short_candidates = [record for record in contracts if record['strike'] <= desired]
        short_contract = max(short_candidates, key=lambda record: record['strike']) if short_candidates else None
    else:
        desired = max(plan['invalidation'] + plan['zone_points'], spot + 1)
        short_candidates = [record for record in contracts if record['strike'] >= desired]
        short_contract = min(short_candidates, key=lambda record: record['strike']) if short_candidates else None
    long_contract = _select_txo_spread_hedge(
        contracts, short_contract, is_bull_put, preferred_width
    ) if short_contract is not None else None
    if short_contract is None or long_contract is None:
        return None
    short_premium = short_contract['bid'] or short_contract['last']
    long_premium = long_contract['ask'] or long_contract['last']
    credit = (short_premium - long_premium) * 50 if short_premium is not None and long_premium is not None else None
    max_loss = abs(short_contract['strike'] - long_contract['strike']) * 50 - credit if credit is not None else abs(short_contract['strike'] - long_contract['strike']) * 50
    dte = (expiry - datetime.now(pytz.timezone('Asia/Taipei')).date()).days
    return {
        'name': '賣權多頭價差（Bull Put Credit Spread）' if is_bull_put else '買權空頭價差（Bear Call Credit Spread）',
        'right': 'Put' if is_bull_put else 'Call', 'short_strike': short_contract['strike'],
        'long_strike': long_contract['strike'], 'expiry': expiry, 'dte': dte,
        'short_premium': short_premium, 'long_premium': long_premium,
        'net_credit': credit, 'max_loss': max_loss,
        'risk_level': '高' if dte <= 1 else ('中高' if dte <= 3 else '中'),
        'source': '期交所每日選擇權行情備援（非永豐即時快照）',
        'delivery_month': short_contract['delivery_month'],
    }


def calculate_index_trade_plan(index_df, index_result, futures_df, futures_result):
    """Build a rule-based TAIEX futures plan from existing thermometer and Fibonacci data."""
    has_futures = futures_result is not None and not futures_df.empty
    has_index = index_result is not None and not index_df.empty
    if not has_futures and not has_index:
        return None

    now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
    is_night_session = now_tw.time() >= dt_time(15, 0) or now_tw.time() < dt_time(5, 0)
    if not has_futures:
        primary_df, primary_result, market_label = index_df, index_result, "加權指數（最近完整交易日）"
        direction = index_result['status']
        alignment_note = "期貨日 K 暫時不足，改用證交所加權指數最近完整日 K 建立假日／休市參考計畫。"
    elif is_night_session or not has_index:
        primary_df, primary_result, market_label = futures_df, futures_result, "臺股期貨（夜盤優先）"
        direction = futures_result['status']
        alignment_note = "夜盤以期貨日夜盤 K 與期貨溫度計判讀。"
    elif index_result['status'] == futures_result['status']:
        primary_df, primary_result, market_label = futures_df, futures_result, "加權／期貨一致"
        direction = futures_result['status']
        alignment_note = f"加權與期貨皆為「{direction}」，可等待費波位置確認。"
    else:
        primary_df, primary_result, market_label = futures_df, futures_result, "加權／期貨分歧"
        direction = "觀望"
        alignment_note = f"加權為「{index_result['status']}」、期貨為「{futures_result['status']}」，暫不建立方向部位。"

    data = primary_df.dropna(subset=['High', 'Low', 'Close']).tail(60).copy()
    if len(data) < 20:
        return None
    latest = float(data['Close'].iloc[-1])
    swing_low = float(data['Low'].min())
    swing_high = float(data['High'].max())
    swing_range = swing_high - swing_low
    if swing_range <= 0:
        return None

    ratios = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
    levels = [swing_low + ratio * swing_range for ratio in ratios]
    support_idx = max((i for i, level in enumerate(levels) if level <= latest), default=0)
    resistance_idx = min((i for i, level in enumerate(levels) if level >= latest), default=len(levels) - 1)
    support = levels[support_idx]
    resistance = levels[resistance_idx]
    prev_close = data['Close'].shift(1)
    true_range = pd.concat([
        data['High'] - data['Low'],
        (data['High'] - prev_close).abs(),
        (data['Low'] - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = float(true_range.rolling(14, min_periods=10).mean().iloc[-1])
    zone_points = max(20.0, (atr * 0.15) if np.isfinite(atr) else 20.0)
    log_returns = np.log(pd.to_numeric(data['Close'], errors='coerce')).diff().dropna()
    realized_volatility = float(log_returns.tail(20).std() * math.sqrt(252)) if len(log_returns) >= 10 else 0.25
    if not np.isfinite(realized_volatility) or realized_volatility <= 0:
        realized_volatility = 0.25
    plan_volume = pd.to_numeric(data.get('Volume', pd.Series(0, index=data.index)), errors='coerce').fillna(0)
    average_volume = float(plan_volume.tail(20).mean())
    latest_volume_ratio = float(plan_volume.iloc[-1] / average_volume) if average_volume > 0 else 1.0

    if direction == '偏多':
        entry_level = support
        invalidation = levels[max(0, support_idx - 1)] if support_idx > 0 else support - max(zone_points, atr * 0.5)
        target = resistance if resistance > entry_level else levels[min(len(levels) - 1, support_idx + 1)]
        if target <= entry_level:
            target = swing_high + swing_range * 0.272
        action = "多方觀察"
        action_color = "#ff4b4b"
        trigger = f"價格回到 {entry_level:,.0f} ± {zone_points:,.0f} 點支撐區，且 15 分 K 止跌上收。"
        option_name = "賣權多頭價差（Bull Put Spread）"
        short_strike = int(math.floor((invalidation - zone_points) / 50) * 50)
        long_strike = short_strike - 100
    elif direction == '偏空':
        entry_level = resistance
        invalidation = levels[min(len(levels) - 1, resistance_idx + 1)] if resistance_idx < len(levels) - 1 else resistance + max(zone_points, atr * 0.5)
        target = support if support < entry_level else levels[max(0, resistance_idx - 1)]
        if target >= entry_level:
            target = max(0.0, swing_low - swing_range * 0.272)
        action = "空方觀察"
        action_color = "#00c853"
        trigger = f"價格反彈到 {entry_level:,.0f} ± {zone_points:,.0f} 點壓力區，且 15 分 K 受壓下收。"
        option_name = "買權空頭價差（Bear Call Spread）"
        short_strike = int(math.ceil((invalidation + zone_points) / 50) * 50)
        long_strike = short_strike + 100
    else:
        entry_level = support
        invalidation = support - max(zone_points, atr * 0.5)
        target = resistance
        action = "等待"
        action_color = "#ffc107"
        trigger = "加權與期貨方向分歧，或溫度為區間盤整；等待價格到區間邊緣再判讀。"
        option_name = "不建立價差部位"
        short_strike = long_strike = None

    valid_directional_plan = (
        direction == '偏多' and invalidation < entry_level < target
    ) or (
        direction == '偏空' and target < entry_level < invalidation
    )
    if direction in ('偏多', '偏空') and not valid_directional_plan:
        direction = '觀望'
        action = '等待'
        action_color = '#ffc107'
        trigger = '目前沒有符合價格順序的有效停損與目標，等待區間重新形成。'
        option_name = '不建立價差部位'
        short_strike = long_strike = None
    risk_points = max(0.0, abs(entry_level - invalidation))
    reward_points = max(0.0, abs(target - entry_level)) if direction != '觀望' else 0.0
    return {
        'market_label': market_label, 'alignment_note': alignment_note,
        'direction': direction, 'action': action, 'action_color': action_color,
        'latest': latest, 'swing_low': swing_low, 'swing_high': swing_high,
        'support': support, 'resistance': resistance, 'entry_level': entry_level,
        'invalidation': invalidation, 'target': target, 'zone_points': zone_points,
        'trigger': trigger, 'atr': atr, 'risk_points': risk_points, 'reward_points': reward_points,
        'realized_volatility': realized_volatility, 'latest_volume_ratio': latest_volume_ratio,
        'rr_ratio': (reward_points / risk_points) if risk_points > 0 else None,
        'micro_risk_1': risk_points * 10, 'micro_risk_2': risk_points * 20,
        'option_name': option_name, 'short_strike': short_strike,
        'long_strike': long_strike, 'max_spread_risk_before_credit': (abs(short_strike - long_strike) * 50) if short_strike is not None else None,
    }


def build_market_temperature_gauge(label, result):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=result['score'],
        number={'suffix': '°', 'font': {'size': 54, 'color': result['color']}},
        title={'text': f"<b>{label}</b><br><br><span style='font-size:18px;color:{result['color']}'>{result['status']}</span>"},
        gauge={
            'shape': 'angular',
            'axis': {'range': [0, 100], 'tickvals': [0, 20, 40, 60, 80, 100], 'tickfont': {'size': 12}},
            'bar': {'color': '#f8f9fa', 'thickness': 0.20},
            'bgcolor': 'rgba(0,0,0,0)',
            'borderwidth': 0,
            'steps': [
                {'range': [0, 20], 'color': '#087f5b'},
                {'range': [20, 40], 'color': '#66bb6a'},
                {'range': [40, 60], 'color': '#c99700'},
                {'range': [60, 80], 'color': '#ff8a80'},
                {'range': [80, 100], 'color': '#d90429'},
            ],
            'threshold': {'line': {'color': result['color'], 'width': 6}, 'thickness': 0.8, 'value': result['score']}
        }
    ))
    fig.update_layout(
        height=330, margin=dict(l=22, r=22, t=88, b=0),
        paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#dfe6e9')
    )
    return fig

def _fibo_trade_price(value, is_futures=False):
    """Round a displayed trade level without applying stock tick rules to futures."""
    value = float(value)
    return float(round(value)) if is_futures else round_to_tick(value)


def _round_fibo_asset_price(value, asset_type):
    if asset_type == 'futures':
        return float(round(float(value)))
    if asset_type == 'index':
        return round(float(value), 2)
    return round_to_tick(value)


def _format_fibo_number(value, decimals=2):
    """Use grouped numbers while dropping insignificant trailing decimal zeroes."""
    return f"{float(value):,.{decimals}f}".rstrip('0').rstrip('.')


def _format_fibo_trade_price(value, asset_type):
    """Format futures as whole points and stocks with their actual tick decimals."""
    rounded = _round_fibo_asset_price(value, asset_type)
    if asset_type == 'futures':
        return f"{rounded:,.0f}"
    if asset_type == 'index':
        return _format_fibo_number(rounded)
    tick = get_taiwan_tick_size(max(float(rounded), 0.01))
    decimals = 2 if tick < 0.1 else (1 if tick < 1 else 0)
    return _format_fibo_number(rounded, decimals)


def _fibo_pivots(data, width=2):
    """Return alternating local swing points, newest point last."""
    highs = data['High'].astype(float).to_numpy()
    lows = data['Low'].astype(float).to_numpy()
    candidates = []
    for i in range(width, len(data) - width):
        high_window = highs[i - width:i + width + 1]
        low_window = lows[i - width:i + width + 1]
        if highs[i] >= high_window.max() and highs[i] > max(high_window[0], high_window[-1]):
            candidates.append((i, 'H', float(highs[i])))
        if lows[i] <= low_window.min() and lows[i] < min(low_window[0], low_window[-1]):
            candidates.append((i, 'L', float(lows[i])))

    pivots = []
    for point in sorted(candidates, key=lambda item: item[0]):
        if pivots and point[1] == pivots[-1][1]:
            is_more_extreme = (point[1] == 'H' and point[2] >= pivots[-1][2]) or (
                point[1] == 'L' and point[2] <= pivots[-1][2]
            )
            if is_more_extreme:
                pivots[-1] = point
        else:
            pivots.append(point)
    return pivots


def build_fibonacci_trade_suggestion(data, range_high, range_low, ticker_code, interval):
    """Create a compact, rule-based Fibonacci / 123 / 2B trade reference.

    The result is deliberately conditional: it presents a level to wait for,
    instead of turning a lagging trend label into an unconditional market order.
    """
    source = data.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()
    if len(source) < 12 or range_high <= range_low:
        return None

    close = float(source['Close'].iloc[-1])
    previous_close = source['Close'].astype(float).shift(1)
    true_range = pd.concat([
        source['High'].astype(float) - source['Low'].astype(float),
        (source['High'].astype(float) - previous_close).abs(),
        (source['Low'].astype(float) - previous_close).abs(),
    ], axis=1).max(axis=1)
    is_futures = ticker_code in ('TWF=F', 'TMF=F')
    asset_type = 'futures' if is_futures else ('index' if str(ticker_code).startswith('^') else 'stock')
    atr = float(true_range.tail(14).mean()) if not true_range.empty else 0.0
    atr_floor = get_taiwan_tick_size(close) if asset_type == 'stock' else 1.0
    atr = max(atr if np.isfinite(atr) else 0.0, (range_high - range_low) * 0.01, atr_floor)
    buffer = max(atr * 0.18, (range_high - range_low) * 0.006, atr_floor)

    ratios = (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0)
    levels = {ratio: range_low + (range_high - range_low) * ratio for ratio in ratios}
    ordered_levels = [levels[ratio] for ratio in ratios]
    below = [price for price in ordered_levels if price <= close]
    above = [price for price in ordered_levels if price >= close]
    support = max(below) if below else range_low
    resistance = min(above) if above else range_high
    lower_support = max([price for price in ordered_levels if price < support], default=range_low)
    upper_resistance = min([price for price in ordered_levels if price > resistance], default=range_high)

    pivots = _fibo_pivots(source.tail(45))
    signal_parts = []
    direction = None
    trigger = None
    invalidation = None

    # 123: a lower high + breakdown confirms bearish reversal; the inverse
    # confirms bullish reversal.  It intentionally requires the final break.
    if len(pivots) >= 3:
        p1, p2, p3 = pivots[-3:]
        if (p1[1], p2[1], p3[1]) == ('H', 'L', 'H') and p3[2] < p1[2]:
            if close < p2[2]:
                direction, trigger, invalidation = 'short', p2[2], p3[2] + buffer
                signal_parts.append('空方 123 確認')
            else:
                signal_parts.append('空方 123 觀察中')
        elif (p1[1], p2[1], p3[1]) == ('L', 'H', 'L') and p3[2] > p1[2]:
            if close > p2[2]:
                direction, trigger, invalidation = 'long', p2[2], p3[2] - buffer
                signal_parts.append('多方 123 確認')
            else:
                signal_parts.append('多方 123 觀察中')

    # 2B: a recent false break must close back inside the preceding swing.
    # This avoids treating only an intrabar wick as a reversal signal.
    recent = source.tail(4)
    prior = source.iloc[-24:-4] if len(source) >= 24 else source.iloc[:-4]
    tolerance = max(buffer, atr * 0.15)
    if not prior.empty and not recent.empty:
        prior_high = float(prior['High'].max())
        prior_low = float(prior['Low'].min())
        if float(recent['High'].max()) > prior_high + tolerance and close < prior_high:
            direction, trigger = 'short', prior_high
            invalidation = max(float(recent['High'].max()), prior_high) + buffer
            signal_parts = ['空方 2B 假突破確認'] + signal_parts
        elif float(recent['Low'].min()) < prior_low - tolerance and close > prior_low:
            direction, trigger = 'long', prior_low
            invalidation = min(float(recent['Low'].min()), prior_low) - buffer
            signal_parts = ['多方 2B 假跌破確認'] + signal_parts

    structural_confirmed = direction in ('long', 'short')
    if direction == 'long':
        entry = close
        stop = invalidation if invalidation is not None else lower_support - buffer
        target_candidates = [price for price in ordered_levels if price > entry + buffer]
        target = target_candidates[0] if target_candidates else entry + atr * 1.5
        action = '反轉偏多，回踩不破可做多'
        color = '#ff4b4b'
        note = '已完成結構確認；不追高，跌回停損即離場。'
    elif direction == 'short':
        entry = close
        stop = invalidation if invalidation is not None else upper_resistance + buffer
        target_candidates = [price for price in reversed(ordered_levels) if price < entry - buffer]
        target = target_candidates[0] if target_candidates else entry - atr * 1.5
        action = '反轉偏空，反彈不過可做空'
        color = '#00c853'
        note = '已完成結構確認；不追殺，突破停損即離場。'
    elif close >= levels[0.618]:
        direction = 'long'
        entry, stop = support, max(lower_support - buffer, range_low - buffer)
        target = upper_resistance if upper_resistance > entry else range_high
        action = '偏多，等回測支撐做多'
        color = '#ff4b4b'
        note = '尚未出現反轉訊號；只在回測支撐止穩時進場。'
    elif close <= levels[0.382]:
        direction = 'short'
        entry, stop = resistance, min(upper_resistance + buffer, range_high + buffer)
        target = lower_support if lower_support < entry else range_low
        action = '偏空，等反彈壓力做空'
        color = '#00c853'
        note = '尚未出現反轉訊號；只在反彈受壓時進場。'
    else:
        long_entry, long_stop, long_target = levels[0.382], levels[0.236] - buffer, levels[0.5]
        short_entry, short_stop, short_target = levels[0.618], levels[0.786] + buffer, levels[0.5]
        long_entry, long_stop, long_target = (
            _round_fibo_asset_price(value, asset_type)
            for value in (long_entry, long_stop, long_target)
        )
        short_entry, short_stop, short_target = (
            _round_fibo_asset_price(value, asset_type)
            for value in (short_entry, short_stop, short_target)
        )
        return {
            'mode': 'range', 'action': '區間盤整，等待邊緣再操作', 'color': '#ffc107',
            'signal': '、'.join(signal_parts) if signal_parts else '未形成 123／2B 確認',
            'note': '價格在 0.382–0.618 洗盤區，中央位置不追價。',
            'long': (long_entry, long_stop, long_target),
            'short': (short_entry, short_stop, short_target),
            'is_futures': is_futures, 'asset_type': asset_type,
        }

    entry, stop, target = (
        _round_fibo_asset_price(value, asset_type)
        for value in (entry, stop, target)
    )
    if asset_type == 'stock' and direction == 'short':
        action = (
            '反轉偏空，反彈不過可減碼；符合融券條件才放空'
            if structural_confirmed else
            '偏空，等反彈壓力減碼；符合融券條件才放空'
        )
    risk = abs(entry - stop)
    reward = abs(target - entry)
    return {
        'mode': direction, 'action': action, 'color': color,
        'signal': '、'.join(dict.fromkeys(signal_parts)) if signal_parts else '費波趨勢判讀',
        'note': note, 'entry': entry, 'stop': stop, 'target': target,
        'risk': risk, 'reward': reward,
        'rr': reward / risk if risk > 0 else None,
        'is_futures': is_futures, 'asset_type': asset_type,
    }


def render_fibonacci_trade_suggestion(suggestion):
    """Render the Fibonacci plan below the chart in a concise, actionable form."""
    if not suggestion:
        return
    header_cols = st.columns([1.25, 2.75], vertical_alignment="top")
    header_cols[0].markdown(
        "<div style='font-size:17px;font-weight:700;white-space:nowrap;margin-top:-5px;'>🧭 費波操作建議 "
        f"<span style='color:{suggestion['color']};'>｜{suggestion['action']}</span></div>",
        unsafe_allow_html=True,
    )
    explanation = f"結構訊號：{suggestion['signal']}；{suggestion['note']}"

    def price_text(value):
        return _format_fibo_trade_price(value, suggestion['asset_type'])

    def amount_note(risk, reward):
        if suggestion['asset_type'] == 'futures':
            return f"微台1口：約 -{risk * 10:,.0f}／+{reward * 10:,.0f}"
        if suggestion['asset_type'] == 'stock':
            return f"現股1張：約 -{risk * 1000:,.0f}／+{reward * 1000:,.0f}"
        return f"風險 {_format_fibo_number(risk)} 點／目標 {_format_fibo_number(reward)} 點"

    if suggestion['mode'] == 'range':
        long_entry, long_stop, long_target = suggestion['long']
        short_entry, short_stop, short_target = suggestion['short']
        long_risk, long_reward = abs(long_entry - long_stop), abs(long_target - long_entry)
        short_risk, short_reward = abs(short_entry - short_stop), abs(short_target - short_entry)
        unit_note = f"（{amount_note(long_risk, long_reward)}）"
        short_unit_note = f"（{amount_note(short_risk, short_reward)}）"
        short_label = '減碼／融券條件' if suggestion['asset_type'] == 'stock' else '做空條件'
        header_cols[1].markdown(
            f"- <span style='color:#ff4b4b;'>做多條件</span>：回測 **{price_text(long_entry)}** 止穩；停損 **{price_text(long_stop)}**；目標 **{price_text(long_target)}** {unit_note}  \n"
            f"- <span style='color:#00c853;'>{short_label}</span>：反彈 **{price_text(short_entry)}** 受壓；停損 **{price_text(short_stop)}**；目標 **{price_text(short_target)}** {short_unit_note}",
            unsafe_allow_html=True,
        )
        st.caption(explanation)
        return

    entry, stop, target = suggestion['entry'], suggestion['stop'], suggestion['target']
    risk, reward = suggestion['risk'], suggestion['reward']
    cols = header_cols[1].columns(5)
    metric_unit = '元' if suggestion['asset_type'] == 'stock' else '點'
    metric_decimals = 2 if suggestion['asset_type'] != 'futures' else 0

    metric_colors = {
        '參考進場': '#40C4FF',
        '停損／出場': '#00E676',
        '第一目標': '#FF5252',
        '預估風險': '#FFB74D',
        '預估獲利': '#FF5252',
    }

    def compact_metric(container, label, value):
        metric_color = metric_colors.get(label, '#F5F5F5')
        container.markdown(
            f"<div style='white-space:nowrap;'>"
            f"<div style='font-size:12px;font-weight:700;color:{metric_color};'>{label}</div>"
            f"<div style='font-size:19px;font-weight:650;line-height:1.25;color:{metric_color};'>"
            f"{value}</div></div>",
            unsafe_allow_html=True,
        )

    compact_metric(cols[0], '參考進場', price_text(entry))
    compact_metric(cols[1], '停損／出場', price_text(stop))
    compact_metric(cols[2], '第一目標', price_text(target))
    compact_metric(cols[3], '預估風險', f"{_format_fibo_number(risk, metric_decimals)} {metric_unit}")
    compact_metric(cols[4], '預估獲利', f"{_format_fibo_number(reward, metric_decimals)} {metric_unit}")
    st.caption(explanation)
    if suggestion['asset_type'] == 'futures':
        st.caption(
            f"微台1口試算：停損約 -{risk * 10:,.0f}；"
            f"目標約 +{reward * 10:,.0f}；風報比 {_format_compact_number(suggestion['rr'], 2)}。"
        )
    elif suggestion['asset_type'] == 'stock':
        st.caption(
            f"現股1張試算：停損價差約 -{risk * 1000:,.0f}；"
            f"目標價差約 +{reward * 1000:,.0f}；風報比 {_format_compact_number(suggestion['rr'], 2)}。"
        )


def fibonacci_initial_y_range(low_price, high_price, padding_ratio=0.05):
    """Start at the core 0–1 range while keeping extension traces zoomable."""
    low_value = float(low_price)
    high_value = float(high_price)
    price_range = high_value - low_value
    if not np.isfinite(price_range) or price_range <= 0:
        return None, None
    padding = price_range * max(0.0, float(padding_ratio))
    return low_value - padding, high_value + padding


def plot_fibonacci_chart(
    symbol, interval, lookback=60, font_size=15, ma_flags=None,
    ma_width=1.5, show_vol=True, advice_container=None,
):
    if ma_flags is None:
        ma_flags = {'5': True, '10': True, '20': True, '60': True}

    code_map_fibo, name_map_fibo = load_local_stock_names()
    
    # 處理輸入(支援名稱或代號)
    raw_input = symbol.strip()
    ticker_code = raw_input
    display_name = raw_input

    # 針對大盤與期貨的特例處理 (支援含有"全"字的自訂輸入)
    if raw_input in ["^TWII", "加權指數", "加權指數(^TWII)", "加權股價指數(TAIEX)"]:
        ticker_code = "^TWII"
        display_name = "加權股價指數(TAIEX)"
    elif raw_input in ["TWF=F", "台指期貨", "臺股期貨", "台指", "小型台指", "台指期貨(TWF=F)", "臺股期貨(TX)", "台指(全)", "台指期(全)", "台指期貨(全)"]:
        ticker_code = "TWF=F"
        display_name = "臺股期貨(TX)"
    elif raw_input in ["TMF=F", "微型台指期貨", "微型臺指期貨", "微台", "微型台指", "微型台指期貨(TMF=F)", "微型臺指期貨(TMF)", "微台(全)", "微台期(全)", "微型台指(全)", "微型台指期貨(全)"]:
        ticker_code = "TMF=F"
        display_name = "微型臺指期貨(TMF)"
    else:
        if "(" in raw_input and raw_input.endswith(")"):
            name_part, code_part = raw_input.rsplit("(", 1)
            ticker_code = code_part[:-1]
            display_name = raw_input
        elif " " in raw_input:
            parts = raw_input.split(" ", 1)
            if parts[0].isdigit() or parts[0].endswith(".TW") or parts[0].endswith(".TWO"):
                ticker_code = parts[0]
                display_name = f"{parts[1]}({parts[0]})"
            elif parts[1].isdigit() or parts[1].endswith(".TW") or parts[1].endswith(".TWO"):
                ticker_code = parts[1]
                display_name = f"{parts[0]}({parts[1]})"
        else:
            if raw_input.isdigit():
                ticker_code = raw_input
                name = code_map_fibo.get(ticker_code, "")
                display_name = f"{name}({ticker_code})" if name else ticker_code
            else:
                if raw_input in name_map_fibo:
                    ticker_code = name_map_fibo[raw_input]
                    display_name = f"{raw_input}({ticker_code})"
                else:
                    ticker_code = raw_input

    ticker = ticker_code if (ticker_code.endswith(".TW") or ticker_code.endswith(".TWO") or ticker_code.startswith("^") or "=" in ticker_code) else f"{ticker_code}.TW"
    period_map = {"1m": "7d", "5m": "30d", "15m": "60d", "60m": "730d", "1d": "2y", "1wk": "2y", "1mo": "5y"}
    is_index = ticker.startswith('^') or 'TWF' in ticker or 'TMF' in ticker
    
    try:
        df = pd.DataFrame()
        raw_code = ticker_code.split('.')[0]
        sj_kbars_used = False
        twse_taiex_used = False
        sj_snap_used = False
        
        twstock_used = False
        explicit_ref_prev_close = None
        
        # 1. 優先使用永豐 API 獲取盤中即時 K 線
        if st.session_state.get('sj_logged_in', False):
            days_needed = {"1m": 3, "5m": 3, "15m": 5, "60m": 12, "1d": 150, "1wk": 730, "1mo": 1825}
            if interval in days_needed:
                req_days = days_needed[interval]
                sj_df = get_cached_fibonacci_kbars(
                    st.session_state.sj_api, raw_code,
                    interval=interval, lookback_days=req_days,
                )
                if not sj_df.empty:
                    df = sj_df
                    sj_kbars_used = True

        # 永豐指數 K 棒在部分帳號／版本只保留近月資料。日 K 的歷史區段
        # 改以證交所官方資料補齊，盤中仍由永豐快照覆寫最新一根。
        if ticker.startswith("^TWII") and interval == "1d":
            twse_df = fetch_twse_taiex_daily_history(lookback_days=180)
            if not twse_df.empty:
                df = merge_taiex_history_with_shioaji(
                    twse_df, df if sj_kbars_used else None, include_turnover=True
                )
                twse_taiex_used = True

        # 已登入時，加權指數必須維持永豐單一來源。若直接退回 Yahoo，
        # 會把不同供應商／不同時間點的 K 棒混入費波與漲跌計算。
        if not sj_kbars_used and not twse_taiex_used and st.session_state.get('sj_logged_in', False) and ticker.startswith("^TWII"):
            st.warning(
                "無法取得永豐加權指數資料，已停止繪圖以避免混用 Yahoo 歷史數據。"
                f" 詳細錯誤：{st.session_state.get('sj_last_error', '無')}"
            )
            return

        # 若永豐未登入或其他商品的永豐資料不足，才退回使用 yfinance。
        if not sj_kbars_used and not twse_taiex_used:
            import time
            for attempt in range(3):
                try:
                    stock_data = yf.Ticker(ticker)
                    df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    if not df.empty:
                        break
                    time.sleep(1) # 若遇 Rate limit 導致空資料，稍待後重試
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1)
                        continue
                    else:
                        break

            # 自動處理上櫃代號
            if (df.empty or 'High' not in df.columns) and ticker.endswith(".TW"):
                ticker_two = ticker.replace(".TW", ".TWO")
                for attempt in range(3):
                    try:
                        stock_data = yf.Ticker(ticker_two)
                        df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.droplevel(1)
                        if not df.empty:
                            ticker = ticker_two 
                            break
                        time.sleep(1)
                    except Exception as e:
                        if attempt < 2:
                            time.sleep(1)
                            continue
                        else:
                            break

           # 期貨異常保護 (移除自動替換加權指數邏輯)
            if (df.empty or 'High' not in df.columns) and (ticker == "TWF=F" or ticker == "TMF=F"):
                sj_status = "已登入" if st.session_state.get('sj_logged_in', False) else "未登入"
                st.warning(f"⚠️ 無法獲取 {display_name} 的資料。診斷：永豐API={sj_status}（{'有取得資料' if sj_kbars_used else '沒取得資料'}）；永豐詳細錯誤：{st.session_state.get('sj_last_error', '無')}。請確保網路連線正常或稍後再試。")
                return
                
            # 將 YF 的個股成交量 (股) 統一轉換為 (張)
            if not df.empty and not is_index and 'Volume' in df.columns:
                df['Volume'] = df['Volume'] / 1000

            # --- 新增：動態過濾歷史 K 線中的未開市日 (如颱風天) ---
            if not df.empty:
                if df.index.tzinfo is not None:
                    df.index = df.index.tz_convert('Asia/Taipei').tz_localize(None)
                
                # 自動判斷未開盤：若日K以上的交易量為 0，視為未開市自動剔除 (不依賴行事曆)
                if interval in ["1d", "1wk", "1mo"]:
                    drop_indices = df[df['Volume'] == 0].index
                    if not drop_indices.empty:
                        df.drop(index=drop_indices, inplace=True)
                
        # 透過共享串流更新圖表最後一筆資料；首次暖機才使用 snapshot。
        if st.session_state.get('sj_logged_in', False) and not df.empty:
            try:
                contract_snap = None
                if ticker.startswith("^TWII"):
                    contract_snap = get_taiex_contract(st.session_state.sj_api)
                elif ticker == "TWF=F":
                    try:
                        contract_snap = min(
                            [c for c in st.session_state.sj_api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code],
                            key=lambda c: c.delivery_date
                        )
                    except (ValueError, AttributeError):
                        contract_snap = st.session_state.sj_api.Contracts.Futures.TXF.TXFR1
                elif ticker == "TMF=F":
                    contract_snap = st.session_state.sj_api.Contracts.Futures.TMF.TMFR1
                else:
                    try: 
                        contract_snap = st.session_state.sj_api.Contracts.Stocks[raw_code]
                    except:
                        try: contract_snap = getattr(st.session_state.sj_api.Contracts.Stocks, raw_code, None)
                        except: pass
                
                if contract_snap:
                    snap = get_stream_quotes(st.session_state.sj_api, [contract_snap])
                    if snap and len(snap) > 0:
                        s = snap[0]
                        # 指數快照有時不提供成交量，close 也可能在開盤瞬間尚未
                        # 寫入；以 open 作為價格備援，避免整段即時更新被略過。
                        rt_price = float(getattr(s, 'close', 0) or getattr(s, 'open', 0) or 0)
                        rt_open = float(getattr(s, 'open', 0) or rt_price)
                        rt_high = float(getattr(s, 'high', 0) or rt_price)
                        rt_low = float(getattr(s, 'low', 0) or rt_price)
                        if ticker.startswith("^TWII"):
                            # 加權指數以成交金額呈現；轉成「億」後須與歷史
                            # K 棒 Amount 的轉換口徑一致。
                            index_turnover = (
                                getattr(s, 'amount_sum', 0)
                                or getattr(s, 'AmountSum', 0)
                                or getattr(s, 'total_amount', 0)
                                or 0
                            )
                            rt_vol = float(index_turnover) / 100_000_000
                        else:
                            rt_vol = float(getattr(s, 'total_volume', 0) or 0)
                        
                        # 擷取永豐快照的正確昨日參考價，避免計算漲跌幅異常
                        try:
                            # 修正：利用快照的 change_price 反推官方真實昨收(日盤基準價)，解決期貨夜盤基準落差
                            change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                            if change_val is not None and rt_price > 0:
                                explicit_ref_prev_close = rt_price - float(change_val)
                            elif hasattr(contract_snap, 'reference') and contract_snap.reference > 0:
                                explicit_ref_prev_close = float(contract_snap.reference)
                        except:
                            pass
                        
                        if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                        
                        tz_tw = pytz.timezone('Asia/Taipei')
                        now_tw_aware = datetime.now(tz_tw)
                        now_dt = now_tw_aware.replace(tzinfo=None)
                        if interval == "1d" and ticker in ["TWF=F", "TMF=F"]:
                            # 15:00 起的夜盤屬於下一個交易日；該未完成日 K
                            # 會在隔日日盤持續累加，不會另開一根日 K。
                            now_dt = get_futures_trading_date(now_tw_aware).to_pydatetime()
                        elif interval in ["1d", "1wk", "1mo"]:
                            now_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                            
                        if rt_price > 0:
                            is_before_open = False
                            current_time = datetime.now(tz_tw).time()
                            if ticker in ["TWF=F", "TMF=F"]:
                                if (dt_time(5, 0) <= current_time < dt_time(8, 45)) or (dt_time(13, 45) <= current_time < dt_time(15, 0)):
                                    is_before_open = True
                            else:
                                if current_time < dt_time(9, 0): is_before_open = True

                            # 新增：臨時狀況休市（如颱風天）攔截，若已過開盤時間且成交量為 0，視為休市不產生新 K 棒
                            is_temporary_closed = False
                            if ticker in ["TWF=F", "TMF=F"]:
                                if (dt_time(8, 50) <= current_time < dt_time(13, 45)) and rt_vol == 0:
                                    is_temporary_closed = True
                                elif (current_time >= dt_time(15, 5) or current_time < dt_time(5, 0)) and rt_vol == 0:
                                    is_temporary_closed = True
                            elif not ticker.startswith("^TWII"):
                                # 加權指數快照的 total_volume 可能固定為 0，不能
                                # 用它判定休市，否則盤中即時 K 棒會被阻擋。
                                if current_time >= dt_time(9, 5) and rt_vol == 0:
                                    is_temporary_closed = True

                            bucket_minutes_map = {'1m': 1, '5m': 5, '15m': 15, '60m': 60}
                            bucket_minutes = bucket_minutes_map.get(interval)
                            if bucket_minutes is not None:
                                is_new_bucket = now_dt >= (df.index[-1] + pd.Timedelta(minutes=bucket_minutes))
                            else:
                                is_new_bucket = df.index[-1] < now_dt  

                            if is_before_open or is_market_closed_func(now_dt.date()) or is_temporary_closed:
                                is_morning_premarket = (current_time < dt_time(9, 0)) if ticker not in ["TWF=F", "TMF=F"] else (dt_time(5, 0) <= current_time < dt_time(8, 45))
                                
                                # 補齊條件：非早晨盤前且「非臨時狀況休市」
                                if df.index[-1].date() < now_dt.date() and not is_market_closed_func(now_dt.date()) and not is_morning_premarket and not is_temporary_closed:
                                    now_dt_naive = now_dt if interval in ["1d", "1wk", "1mo"] else datetime.now(tz_tw).replace(tzinfo=None)
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt_naive])
                                    df = pd.concat([df, new_row])
                                else:
                                    # 未開盤/空窗/假日/臨時休市：只同步收盤價，不可更新高低 (避免扁平快照污染)
                                    ref_high = rt_high if interval in ["1d", "1wk", "1mo"] else rt_price
                                    ref_low = rt_low if interval in ["1d", "1wk", "1mo"] else rt_price
                                    df.at[df.index[-1], 'Close'] = rt_price
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), ref_high)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), ref_low)
                                    if interval in ["1d", "1wk", "1mo"]:
                                        df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                            elif is_new_bucket:
                                # 真正跨入新的一根K棒，才建立新row
                                if interval == "1d":
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                                elif interval in ["1m", "5m", "15m", "60m"]:
                                    same_day_mask = df.index.date == df.index[-1].date()
                                    today_known_vol = float(df.loc[same_day_mask, 'Volume'].sum())
                                    bar_vol = max(0.0, rt_vol - today_known_vol)
                                    new_row = pd.DataFrame([{'Open': rt_price, 'High': rt_price, 'Low': rt_price, 'Close': rt_price, 'Volume': bar_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                                else:
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                            else:
                                # 仍在同一根K棒區間內：保留原本開盤價，只更新累積高低收
                                df.at[df.index[-1], 'Close'] = rt_price
                                if interval in ["1d", "1wk", "1mo"]:
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                                    df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                                else:
                                    # 分K不可套用快照「全盤累計」高低，只能用目前報價當作這根K棒目前已知的高低邊界
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_price)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_price)
                            
                        sj_snap_used = True
            except Exception:
                pass
                
       # twstock 盤後修補方案 (修正: 支援盤中所有週期取得最新即時價格，以修復盤中漲跌幅度與標題顏色)
        if not sj_kbars_used and not sj_snap_used and not df.empty and not is_index:
            try:
                tz_tw = pytz.timezone('Asia/Taipei')
                now_tw = datetime.now(tz_tw)
                today_date = pd.Timestamp(now_tw.date())
                is_post_market = now_tw.time() >= dt_time(15, 0)
                
                rt_price = None
                rt_open = None
                rt_high = None
                rt_low = None
                rt_vol = 0.0

                rt_data = twstock.realtime.get(raw_code)
                if rt_data and rt_data.get('success') and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                    rt_price = float(rt_data['realtime']['latest_trade_price'])
                    rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                    rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                    rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                    rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                
                if is_post_market and (rt_price is None or rt_price == 0):
                    stock = twstock.Stock(raw_code)
                    if len(stock.date) > 0 and stock.date[-1].date() == today_date.date():
                        rt_price = float(stock.price[-1])
                        rt_open = float(stock.open[-1])
                        rt_high = float(stock.high[-1])
                        rt_low = float(stock.low[-1])
                        rt_vol = float(stock.capacity[-1]) / 1000 

                if rt_price not in [None, 0]:
                    if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                    last_hist_date = pd.Timestamp(df.index[-1].date())
                    
                    # 新增：未開盤時間攔截，防止重覆產生K線
                    current_time = datetime.now(tz_tw).time()
                    is_before_open = current_time < dt_time(9, 0)
                    is_temporary_closed = current_time >= dt_time(9, 5) and rt_vol == 0
                    
                    if interval == "1d":
                        if last_hist_date < today_date and not is_market_closed_func(now_tw.date()) and not is_before_open and not is_temporary_closed:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                            df = pd.concat([df, new_row])
                        else:
                            df.at[df.index[-1], 'Close'] = rt_price
                            df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                            df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                            df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                    elif interval in ["1m", "5m", "15m", "60m"]:
                        now_dt_naive = now_tw.replace(tzinfo=None)
                        bucket_minutes = {'1m': 1, '5m': 5, '15m': 15, '60m': 60}[interval]
                        bucket_start = now_dt_naive.replace(
                            minute=(now_dt_naive.minute // bucket_minutes) * bucket_minutes,
                            second=0, microsecond=0,
                        )
                        last_bucket = pd.Timestamp(df.index[-1]).to_pydatetime().replace(
                            minute=(pd.Timestamp(df.index[-1]).minute // bucket_minutes) * bucket_minutes,
                            second=0, microsecond=0,
                        )
                        same_day_mask = df.index.date == now_tw.date()
                        known_volume = float(df.loc[same_day_mask, 'Volume'].sum())
                        if bucket_start > last_bucket and rt_price > 0 and not is_market_closed_func(now_tw.date()) and not is_temporary_closed:
                            bar_volume = max(0.0, rt_vol - known_volume)
                            new_row = pd.DataFrame(
                                [{'Open': rt_price, 'High': rt_price, 'Low': rt_price, 'Close': rt_price, 'Volume': bar_volume}],
                                index=[bucket_start],
                            )
                            df = pd.concat([df, new_row])
                        else:
                            df.at[df.index[-1], 'Close'] = rt_price
                            df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_price)
                            df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_price)
                            previous_bar_volume = known_volume - float(df['Volume'].iloc[-1])
                            df.at[df.index[-1], 'Volume'] = max(
                                float(df['Volume'].iloc[-1]), rt_vol - previous_bar_volume,
                            )
                    else:
                        df.at[df.index[-1], 'Close'] = rt_price
                        df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                        df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                        df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                        if df['Open'].iloc[-1] == 0: df.at[df.index[-1], 'Open'] = rt_open
                    twstock_used = True
            except Exception: pass

    except Exception as e:
        st.error(f"⚠️ 獲取數據失敗: {e}")
        return

    if df.empty or 'High' not in df.columns or 'Low' not in df.columns:
        st.warning(f"無法獲取有效的交易數據 ({ticker}, {interval})，可能是該區間無資料或代號錯誤。")
        return
    
    # 計算均線
    if ma_flags['5']: df['MA5'] = df['Close'].rolling(window=5).mean()
    if ma_flags['10']: df['MA10'] = df['Close'].rolling(window=10).mean()
    if ma_flags['20']: df['MA20'] = df['Close'].rolling(window=20).mean()
    if ma_flags['60']: df['MA60'] = df['Close'].rolling(window=60).mean()

    # 裁切近期 K 棒，加入 60 -> 45 -> 30 的回落機制
    lookbacks_to_try = [60, 45, 30] if lookback == 60 else [lookback]
    
    df_subset = pd.DataFrame()
    for lb in lookbacks_to_try:
        temp_subset = df.tail(lb).copy()
        temp_subset = temp_subset.dropna(subset=['High', 'Low'])
        if not temp_subset.empty:
            h = float(temp_subset['High'].max())
            l = float(temp_subset['Low'].min())
            if h != l:
                df_subset = temp_subset
                high_60 = h
                low_60 = l
                break

    if df_subset.empty:
        temp_subset = df.tail(lookback).copy().dropna(subset=['High', 'Low'])
        if temp_subset.empty:
            st.error(f"該股票 ({ticker}, {interval}) 的近期 K 線資料不完整或為空。")
            return
        else:
            st.warning(f"該股票 ({ticker}, {interval}) 近期高低點相同，無法畫出波段比例。")
            return

    diff = high_60 - low_60
    ratios = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
    asset_type = (
        'futures' if ticker_code in ('TWF=F', 'TMF=F')
        else ('index' if str(ticker_code).startswith('^') else 'stock')
    )
    
    # 費波顏色映射表
    color_map = {
        1.0: "#ff4b4b",
        0.786: "#ff9d00",
        0.618: "#7fff00",
        0.5: "#00ffff",
        0.382: "#1e90ff",
        0.236: "#9370db",
        0.0: "#ffffff"
    }
    
    fmt = '%Y-%m-%d %H:%M:%S'
    x_strings = df_subset.index.strftime(fmt).tolist()
    if interval in ["1d", "1wk", "1mo"]: x_display = df_subset.index.strftime('%Y-%m-%d').tolist()
    else: x_display = df_subset.index.strftime('%m-%d %H:%M').tolist()

    if show_vol and 'Volume' in df_subset.columns:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    else:
        fig = go.Figure()

    # Plotly 的 Candlestick 只支援漲跌兩色，收盤=開盤會被強制當作漲(紅)；
    # 改用三條trace疊加(各自只保留該類別資料、其餘設NaN)，正確呈現紅漲/綠跌/白平盤
    up_mask = df_subset['Close'] > df_subset['Open']
    down_mask = df_subset['Close'] < df_subset['Open']
    flat_mask = df_subset['Close'] == df_subset['Open']

    def _masked(col, mask):
        return df_subset[col].where(mask)

    for mask, color, show_legend in [(up_mask, '#ff4b4b', True), (down_mask, '#00e676', False), (flat_mask, '#ffffff', False)]:
        kline_trace = go.Candlestick(
            x=x_strings, open=_masked('Open', mask), high=_masked('High', mask),
            low=_masked('Low', mask), close=_masked('Close', mask), name="K線",
            increasing=dict(line=dict(color=color), fillcolor=color),
            decreasing=dict(line=dict(color=color), fillcolor=color),
            showlegend=show_legend
        )
        if show_vol and 'Volume' in df_subset.columns: fig.add_trace(kline_trace, row=1, col=1)
        else: fig.add_trace(kline_trace)

    # 繪製均線
    ma_settings = {
        'MA5': ('orange', ma_flags['5']),
        'MA10': ('lightblue', ma_flags['10']),
        'MA20': ('green', ma_flags['20']),
        'MA60': ('yellow', ma_flags['60'])
    }
    for ma_name, (color, is_show) in ma_settings.items():
        if is_show and ma_name in df_subset.columns:
            ma_trace = go.Scatter(x=x_strings, y=df_subset[ma_name], mode='lines', name=ma_name, line=dict(color=color, width=ma_width))
            if show_vol and 'Volume' in df_subset.columns: fig.add_trace(ma_trace, row=1, col=1)
            else: fig.add_trace(ma_trace)

    # 繪製成交量
    if show_vol and 'Volume' in df_subset.columns:
        colors = ['#ff4b4b' if close > open else ('#00e676' if close < open else '#ffffff') for close, open in zip(df_subset['Close'], df_subset['Open'])]
        vol_trace = go.Bar(x=x_strings, y=df_subset['Volume'], name="成交量", marker_color=colors)
        fig.add_trace(vol_trace, row=2, col=1)

    high_idx_str = df_subset['High'].idxmax().strftime(fmt)
    low_idx_str = df_subset['Low'].idxmin().strftime(fmt)
    disp_high = _round_fibo_asset_price(high_60, asset_type)
    disp_low = _round_fibo_asset_price(low_60, asset_type)
    
    target_row = 1 if (show_vol and 'Volume' in df_subset.columns) else None
    target_col = 1 if (show_vol and 'Volume' in df_subset.columns) else None

    fig.add_annotation(x=high_idx_str, y=high_60, text=f"最高:{disp_high:g}", showarrow=True, arrowhead=1, yshift=10, font=dict(color="red", size=font_size), row=target_row, col=target_col)
    fig.add_annotation(x=low_idx_str, y=low_60, text=f"最低:{disp_low:g}", showarrow=True, arrowhead=1, ay=40, font=dict(color="green", size=font_size), row=target_row, col=target_col)

    last_date_str = x_strings[-1]
    first_date_str = x_strings[0]
    
    for r in ratios:
        price = low_60 + r * diff
        rounded_price = _round_fibo_asset_price(price, asset_type)
        line_col = color_map.get(r, "rgba(150, 150, 150, 0.5)")
        
        fig.add_shape(type="line", x0=first_date_str, y0=price, x1=last_date_str, y1=price,
            line=dict(color=line_col, width=1, dash="dash" if r not in [0, 1] else "solid"), row=target_row, col=target_col)
            
        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
        fig.add_annotation(
            x=last_date_str, y=price, text=f"{r_label} ({rounded_price:g})",
            showarrow=False, xanchor="left", xshift=10,
            # Keep the price labels legible on a phone without letting the
            # right-side stack dominate the plotting area.
            font=dict(size=max(10, min(int(font_size), 14)), color=line_col),
            row=target_row, col=target_col,
        )

    y_min_view, y_max_view = fibonacci_initial_y_range(low_60, high_60)
    if pd.isna(y_min_view) or pd.isna(y_max_view): y_min_view, y_max_view = None, None

    interval_display_map = {"1m": "1分K", "5m": "5分K", "15m": "15分K", "60m": "60分K", "1d": "日K", "1wk": "週K", "1mo": "月K"}
    interval_name = interval_display_map.get(interval, interval)
    ticker_suffix = ".TW" if ticker.endswith(".TW") else (".TWO" if ticker.endswith(".TWO") else "")
    
    try:
        last_date_obj = df_subset.index[-1]
        if interval in ["1d", "1wk", "1mo"]:
            date_str = last_date_obj.strftime('%Y/%m/%d')
        else:
            date_str = last_date_obj.strftime('%Y/%m/%d %H:%M')

        op = float(df_subset['Open'].iloc[-1])
        hi = float(df_subset['High'].iloc[-1])
        lo = float(df_subset['Low'].iloc[-1])
        cl = float(df_subset['Close'].iloc[-1])
        vol = float(df_subset['Volume'].iloc[-1]) if 'Volume' in df_subset.columns else 0.0

       # 修正：總漲跌幅必須與「日線級別的昨日收盤價 (日盤)」對比，不能與「上一根分K」對比
        if explicit_ref_prev_close is not None and explicit_ref_prev_close > 0:
            daily_ref_close = explicit_ref_prev_close
        else:
            if interval in ["1d", "1wk", "1mo"]:
                daily_ref_close = float(df['Close'].iloc[-2]) if len(df) > 1 else cl
            else:
                last_date = df.index[-1].date()
                past_df = df[df.index.date < last_date]
                if not past_df.empty:
                    daily_ref_close = float(past_df['Close'].iloc[-1])
                else:
                    daily_ref_close = float(df['Close'].iloc[-2]) if len(df) > 1 else cl

        chg = cl - daily_ref_close
        pct_chg = (chg / daily_ref_close * 100) if daily_ref_close > 0 else 0.0

        chg = round(chg, 2)
        pct_chg = round(pct_chg, 2)
        color = "#ff4b4b" if chg > 0 else ("#00e676" if chg < 0 else "white")
        sign = "+" if chg > 0 else ""

        # K棒內部變色邏輯維持與「上一根K棒收盤價」對比 (解決連續區段跳空落差)
        prev_k_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else op

        def get_ohlc_color(val, ref):
            if val > ref: return "#ff4b4b"
            elif val < ref: return "#00e676"
            return "white"

        # 標題上的開高低收文字，使用上一根K棒收盤價(prev_k_close)變色
        color_op = get_ohlc_color(op, prev_k_close)
        color_hi = get_ohlc_color(hi, prev_k_close)
        color_lo = get_ohlc_color(lo, prev_k_close)
        color_cl = get_ohlc_color(cl, prev_k_close)
        
        # 主名稱與總漲跌幅的顏色，則使用正確的日盤昨收 (daily_ref_close)
        color_main = get_ohlc_color(cl, daily_ref_close)

        if is_index:
            if ticker == '^TWII':
                has_turnover_amount = twse_taiex_used or 'Amount' in df.columns or df.attrs.get('volume_unit') == '億'
                if not has_turnover_amount or vol == 0 or pd.isna(vol):
                    vol_num = "無資料(缺漏)"
                    vol_unit = ""
                else:
                    vol_num = _format_compact_number(vol, 2)
                    vol_unit = " 億"
            else:
                vol_num = f"{vol:,.0f}"
                vol_unit = " 單位(口)"
            price_unit = " 點"
        else:
            vol_num = f"{vol:,.0f}"
            vol_unit = " 張"
            price_unit = " 元"

        disp_title = display_name.replace('(^TWII)', '(TSE)') if ticker == '^TWII' else display_name
        
        # 標題改為各自獨立套用顏色
        title_html = (
            f"<span style='color:{color_main};'>{disp_title}{ticker_suffix}</span> - {interval_name} {date_str} "
            f"開 <span style='color:{color_op};'>{_format_compact_number(op, 2)}</span> "
            f"高 <span style='color:{color_hi};'>{_format_compact_number(hi, 2)}</span> "
            f"低 <span style='color:{color_lo};'>{_format_compact_number(lo, 2)}</span> "
            f"收 <span style='color:{color_cl};'>{_format_compact_number(cl, 2)}</span>{price_unit} "
            f"量 {vol_num}{vol_unit} "
            f"<span style='color:{color_main};'>{_format_compact_number(chg, 2, signed=True)}"
            f"({_format_compact_number(pct_chg, 2, signed=True)}%)</span>"
        )
    except Exception:
        title_html = f"{display_name}{ticker_suffix} - {interval_name}"

    layout_update = dict(
        title=dict(text=title_html, font=dict(size=16)),
        template="plotly_dark",
        height=800 if show_vol else 700,
        margin=dict(l=30, r=82, t=58, b=42),
        showlegend=True,
    )

    if show_vol and 'Volume' in df_subset.columns:
        fig.update_yaxes(title_text="點數", range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False, row=1, col=1)
        fig.update_yaxes(title_text="成交量", fixedrange=False, row=2, col=1)
        fig.update_xaxes(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False, rangeslider_visible=False, row=2, col=1)
        fig.update_xaxes(type='category', showgrid=False, rangeslider_visible=False, showticklabels=False, row=1, col=1) 
    else:
        layout_update.update(
            yaxis_title="點數",
            yaxis=dict(range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False),
            xaxis=dict(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False),
            xaxis_rangeslider_visible=False
        )

    trade_suggestion = build_fibonacci_trade_suggestion(
        df_subset, high_60, low_60, ticker, interval
    )
    if advice_container is None:
        render_fibonacci_trade_suggestion(trade_suggestion)
    else:
        with advice_container:
            render_fibonacci_trade_suggestion(trade_suggestion)

    fig.update_layout(**layout_update)
    st.plotly_chart(fig, width='stretch')
    
    fetch_time_str = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')
    
    if twse_taiex_used:
        data_source_text = "證交所官方歷史日K + 永豐即時串流"
    elif sj_kbars_used:
        contract_code = df.attrs.get('shioaji_contract_code', '')
        target_code = df.attrs.get('shioaji_target_code', '')
        resolved_contract = f" → {target_code}" if target_code else ""
        data_source_text = f"永豐 Shioaji API (K線合約：{contract_code}{resolved_contract})"
    elif sj_snap_used:
        data_source_text = "YF歷史 + 永豐即時串流"
    elif twstock_used:
        data_source_text = "YF歷史 + Twstock即時"
    else:
        data_source_text = "YF 歷史數據"
        
    session_note = "；日K口徑：夜盤 15:00 起＋次日日盤，依查詢時間動態更新" if (sj_kbars_used and interval == '1d' and ticker in ['TWF=F', 'TMF=F']) else ""
    st.caption(f"📊 數據最後更新時間: {fetch_time_str} ({data_source_text}{session_note})")


# ==========================================
# 網路爬蟲加入快取，避免切換分頁時卡死
# ==========================================
@st.cache_data(ttl=3600, max_entries=2, show_spinner=False)
def fetch_fubon_html(url):
    """解決富邦 DJ 拒絕 iframe 連線的問題、處理亂碼與排版"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = 'cp950' 
        html = r.text
        
        # 強制轉換 meta charset 避免瀏覽器以預設編碼解析造成亂碼
        html = re.sub(r'charset=["\']?(big5|utf-8|cp950)["\']?', 'charset=utf-8', html, flags=re.IGNORECASE)
        
        # 注入 base 標籤與 CSS：消除右方空白但保留預設字體大小
        injection = '''
        <base href="https://fubon-ebrokerdj.fbs.com.tw/">
        <meta charset="utf-8">
        <style>
            body { margin: 0 !important; padding: 0 !important; text-align: left; background-color: white;}
            center { text-align: left !important; margin: 0 !important; }
            table { margin: 0 !important; width: 100% !important; max-width: 100% !important; }
            .dj-container, .wrapper { margin: 0 !important; padding: 0 !important; width: 100% !important; }
            a { text-decoration: none; color: #333; }
        </style>
        '''
        html = re.sub(r'<head>', f'<head>{injection}', html, flags=re.IGNORECASE)
        return html
    except Exception as e:
        return f"<html><body><h3>無法載入資料: {e}</h3></body></html>"


def parse_sinopac_report_list(page_html, page_url):
    """Parse current and legacy Sinopac lists without relying on fragile CSS classes."""
    soup = BeautifulSoup(page_html or '', 'html.parser')
    reports, seen = [], set()
    for link_tag in soup.find_all('a', href=True):
        title = link_tag.get_text(' ', strip=True) or str(link_tag.get('title', '')).strip()
        compact_title = re.sub(r'\s+', '', title)
        if '台指期籌碼快訊' not in compact_title:
            continue
        report_url = urljoin(page_url, link_tag.get('href', '').strip())
        if not report_url or report_url in seen:
            continue
        context = link_tag.find_parent(['li', 'tr', 'article', 'div'])
        context_text = context.get_text(' ', strip=True) if context else title
        date_match = re.search(
            r'(202\d)\s*[/-]?\s*(0?[1-9]|1[0-2])\s*[/-]?\s*(0?[1-9]|[12]\d|3[01])(?!\d)',
            f'{title} {context_text}',
        )
        date_str = (
            f'{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}'
            if date_match else '近期發布'
        )
        reports.append({'日期': date_str, 'title': title, 'url': report_url})
        seen.add(report_url)
    reports.sort(key=lambda item: item['日期'], reverse=True)
    return reports


def parse_sinopac_report_markdown(page_text):
    """Parse the read-only text proxy used only when the official page blocks Cloud."""
    reports, seen = [], set()
    pattern = re.compile(
        r'\[\s*台指期籌碼快訊\s*\]\((https://www\.spf\.com\.tw/[^)]+\.pdf)\)'
        r'\s*(202\d)[/-](0?[1-9]|1[0-2])[/-](0?[1-9]|[12]\d|3[01])(?!\d)',
        re.I,
    )
    for match in pattern.finditer(page_text or ''):
        report_url = match.group(1).strip()
        if report_url in seen:
            continue
        reports.append({
            '日期': f'{match.group(2)}-{int(match.group(3)):02d}-{int(match.group(4)):02d}',
            'title': '台指期籌碼快訊',
            'url': report_url,
        })
        seen.add(report_url)
    reports.sort(key=lambda item: item['日期'], reverse=True)
    return reports


@st.cache_data(ttl=600, max_entries=1, show_spinner=False)
def get_report_list():
    """回復大修改前的永豐期貨盤後快訊清單抓取方式。"""
    url = "https://www.spf.com.tw/sinopacSPF/research/list.do?id=1709f20d3ff00000d8e2039e8984ed51"
    base_url = "https://www.spf.com.tw"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        reports = []
        items = soup.select('div.list_news ul li')

        if items:
            for item in items:
                link_tag = item.find('a')
                date_tag = item.find('span', class_='date')
                if not link_tag:
                    continue
                title = link_tag.get_text(strip=True)
                if "台指期籌碼快訊" not in title:
                    continue
                href = link_tag.get('href', '')
                pdf_url = f"{base_url}{href}" if href.startswith('/') else f"{base_url}/sinopacSPF/research/{href}"
                date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                if date_match:
                    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                elif date_tag:
                    date_str = date_tag.get_text(strip=True).replace('/', '-')
                else:
                    date_str = "近期發布"
                reports.append({'日期': date_str, 'title': title, 'url': pdf_url})
        else:
            for link_tag in soup.find_all('a', href=re.compile(r'\.pdf')):
                title = link_tag.get_text(strip=True) or link_tag.get('title', '')
                if "台指期籌碼快訊" not in title:
                    continue
                href = link_tag.get('href', '')
                pdf_url = f"{base_url}{href}" if href.startswith('/') else href
                date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                if date_match:
                    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                else:
                    parent = link_tag.find_parent(['tr', 'li'])
                    date_text = parent.get_text(' ', strip=True) if parent else ''
                    match = re.search(r'202\d[/-]\d{2}[/-]\d{2}', date_text)
                    date_str = match.group().replace('/', '-') if match else "近期發布"
                reports.append({'日期': date_str, 'title': title, 'url': pdf_url})

        unique_reports = []
        seen = set()
        for report in reports:
            if report['url'] not in seen:
                unique_reports.append(report)
                seen.add(report['url'])
        unique_reports.sort(key=lambda report: report['日期'], reverse=True)
        return unique_reports
    except Exception as exc:
        return []

@st.cache_data(ttl=600, max_entries=1, show_spinner=False)
def fetch_and_parse_pdf(pdf_url):
    """下載、解析數值並將 PDF 轉為圖片供直接預覽"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        if not pdf_url.lower().endswith('.pdf'):
            r_inner = requests.get(pdf_url, headers=headers, timeout=10, verify=False)
            r_inner.raise_for_status()
            if r_inner.content.startswith(b'%PDF'):
                pdf_bytes = r_inner.content
            else:
                pdf_bytes = None
            soup_inner = BeautifulSoup(r_inner.text, 'html.parser')
            if pdf_bytes is None:
                for tag in soup_inner.find_all(['a', 'iframe']):
                    link = tag.get('href') or tag.get('src')
                    if link and ('.pdf' in link.lower() or 'download' in link.lower()):
                        pdf_url = urljoin(pdf_url, link)
                        break
        else:
            pdf_bytes = None

        if pdf_bytes is None:
            response = requests.get(pdf_url, headers=headers, timeout=15, verify=False)
            response.raise_for_status()
            pdf_bytes = response.content
        if not pdf_bytes.startswith(b'%PDF'):
            raise ValueError('永豐來源未回傳 PDF')
        
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
            
        ratio_match = re.search(r"散戶小台多空比[:：]\s*([-+]?[\d\.]+)%", text)
        ratio = ratio_match.group(1) if ratio_match else "N/A"
        
        images = []
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            # 最多轉譯前 2 頁；直接輸出壓縮 JPEG，避免 PNG 與 PIL
            # 中間副本在 Streamlit rerun 時同時佔用大量記憶體。
            for page_idx in range(min(len(doc), 2)):
                page = doc[page_idx]
                pix = page.get_pixmap(dpi=100)
                images.append(pix.tobytes('jpeg', jpg_quality=84))
            doc.close()  
        except Exception as e:
            pass
            
        return {
            "ratio": ratio,
            "images": images
        }
    except Exception as e:
        return {"ratio": "解析錯誤", "images": []}

@st.cache_data(ttl=1800, max_entries=2, show_spinner=False)
def get_major_institutional_data(date_str):
    """從證交所 API 抓取三大法人買賣金額統計 (套用正確 API 結構)"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={date_str}&response=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data.get("stat") != "OK":
            return None
        
        # 轉換為 DataFrame
        df = pd.DataFrame(data["data"], columns=data["fields"])
        
        # 清理數據：移除千分號並轉為數字
        cols_to_fix = ['買進金額', '賣出金額', '買賣差額']
        for col in cols_to_fix:
            df[col] = df[col].astype(str).str.replace(',', '').astype(float)
            
        return df
    except Exception as e:
            # 拒絕回傳 None，透過拋出異常來阻止 Streamlit 將失敗狀態寫入快取
            raise RuntimeError(f"連線異常不寫入快取: {e}")

def color_negative_positive(val):
    """定義表格文字顏色：正數紅、負數綠"""
    if isinstance(val, (int, float)):
        color = '#ff4b4b' if val > 0 else '#00e676' if val < 0 else 'white'
        return f'color: {color}'
    return ''

@st.cache_data(ttl=3600, max_entries=2, show_spinner=False)
def get_tw_stocker_data(direction):
    url = f"https://voidful.github.io/tw-institutional-stocker/data/top_three_inst_change_20_{direction}.json"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            data = r.json()
            if data:
                df = pd.DataFrame(data).head(20)
                if 'code' in df.columns:
                    df = df.rename(columns={
                        'code': '代號',
                        'name': '名稱',
                        'change': '持股變化(%)',
                        'three_inst_ratio': '三大法人持股(%)'
                    })
                    return df[['代號', '名稱', '持股變化(%)', '三大法人持股(%)']]
    except:
        pass
    return pd.DataFrame()

_GOODINFO_BROWSER_SEMAPHORE = threading.BoundedSemaphore(1)
_GOODINFO_URL = (
    "https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C"
    "&INDUSTRY_CAT=%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%28%E7%95%B6%E6%97%A5%29"
    "%40%40%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%40%40%E7%95%B6%E6%97%A5"
)
_GOODINFO_LEGACY_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/114.0.0.0 Safari/537.36'
)
def _goodinfo_normalize_text(value):
    """Normalize Goodinfo's nbsp/newline-heavy labels before schema checks."""
    return re.sub(r"\s+", "", str(value or "").replace("\xa0", "")).strip()


def _clean_goodinfo_table(dataframe):
    cleaned = dataframe.copy()
    if isinstance(cleaned.columns, pd.MultiIndex):
        new_columns = []
        for column in cleaned.columns:
            parts = []
            for item in column:
                text = str(item).replace(' ', '').strip()
                if text and not text.startswith('Unnamed') and text not in parts:
                    parts.append(text)
            new_columns.append('_'.join(parts))
        cleaned.columns = new_columns
    else:
        cleaned.columns = [str(column).replace(' ', '').strip() for column in cleaned.columns]
    header_text = '|'.join(map(str, cleaned.columns))
    has_code = any(keyword in header_text for keyword in ('代號', '股票代號'))
    has_name = '名稱' in header_text
    has_turnover = '週轉率' in header_text
    if len(cleaned) < 10 or not (has_code and has_name and has_turnover):
        return None
    code_column = next((column for column in cleaned.columns if '代號' in str(column)), None)
    turnover_column = next((column for column in cleaned.columns if '週轉率' in str(column)), None)
    valid_codes = cleaned[code_column].astype(str).str.extract(r'(\d{4,6})', expand=False).notna()
    turnover_values = pd.to_numeric(
        cleaned[turnover_column].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False),
        errors='coerce',
    )
    if int((valid_codes & turnover_values.notna()).sum()) < 10:
        return None
    return cleaned


def _parse_goodinfo_table_html(table_html_list):
    """Parse only DOM tables that already match Goodinfo's ranking schema."""
    candidates = []
    for table_html in table_html_list or []:
        try:
            tables = pd.read_html(io.StringIO(str(table_html)), flavor="lxml")
        except (ValueError, TypeError, ImportError):
            continue
        candidates.extend(
            cleaned for cleaned in (_clean_goodinfo_table(table) for table in tables)
            if cleaned is not None
        )
    return max(candidates, key=lambda table: table.shape[0] * table.shape[1]) if candidates else None


def _parse_goodinfo_original_page(markup):
    """Reproduce the original working whole-page / largest-table parser.

    This intentionally does not apply the newer DOM/schema gate.  Goodinfo's
    completed ranking has historically appeared with several different nested
    header layouts, while the original scraper reliably selected it as the
    largest table after the fixed 15-second wait.  The stricter parser remains
    available as a fallback after this compatibility path.
    """
    markup_text = str(markup or '').strip()
    if not markup_text:
        return None
    tables = []
    for read_kwargs in ({}, {'flavor': 'lxml'}):
        try:
            tables = pd.read_html(io.StringIO(markup_text), **read_kwargs)
            if tables:
                break
        except (ValueError, TypeError, ImportError, XMLSyntaxError):
            tables = []
    target = None
    for dataframe in tables:
        if dataframe.shape[0] <= 10 or dataframe.shape[1] <= 5:
            continue
        if target is None or (
            dataframe.shape[0] * dataframe.shape[1]
            > target.shape[0] * target.shape[1]
        ):
            target = dataframe.copy()
    if target is None:
        return None
    if isinstance(target.columns, pd.MultiIndex):
        normalized_columns = []
        for column in target.columns:
            parts = []
            for item in column:
                text = str(item).replace(' ', '').strip()
                if text and not text.startswith('Unnamed') and (
                    not parts or parts[-1] != text
                ):
                    parts.append(text)
            normalized_columns.append('_'.join(parts))
        target.columns = normalized_columns
    else:
        target.columns = [
            str(column).replace(' ', '').strip() for column in target.columns
        ]
    return target.dropna(how='all').reset_index(drop=True)


def _promote_uploaded_stock_header(dataframe, search_rows=30):
    """Find a stock-code/name header row in exported CSV-like tables."""
    if dataframe is None or dataframe.empty:
        return None
    scan_limit = min(max(int(search_rows), 1), len(dataframe))
    for row_index in range(scan_limit):
        labels = [
            _goodinfo_normalize_text(value)
            for value in dataframe.iloc[row_index].tolist()
        ]
        if not any('代號' in label for label in labels):
            continue
        if not any('名稱' in label for label in labels):
            continue
        unique_labels = []
        used_labels = set()
        for column_index, label in enumerate(labels):
            base_label = label or f'欄位{column_index + 1}'
            final_label = base_label
            suffix = 2
            while final_label in used_labels:
                final_label = f'{base_label}_{suffix}'
                suffix += 1
            used_labels.add(final_label)
            unique_labels.append(final_label)
        promoted = dataframe.iloc[row_index + 1:].copy()
        promoted.columns = unique_labels
        promoted = promoted.replace(r'^\s*$', np.nan, regex=True).dropna(how='all')
        return promoted.reset_index(drop=True)
    return None


def _read_uploaded_stock_csv(uploaded_file):
    """Read Goodinfo/manual CSV exports with preamble and encoding tolerance."""
    if uploaded_file is None:
        return pd.DataFrame()
    if hasattr(uploaded_file, 'getvalue'):
        raw_data = uploaded_file.getvalue()
    else:
        raw_data = uploaded_file.read()
    if isinstance(raw_data, str):
        raw_data = raw_data.encode('utf-8')
    if not raw_data:
        return pd.DataFrame()

    last_error = None
    for encoding in ('cp950', 'utf-8-sig', 'utf-8', 'big5'):
        try:
            raw_table = pd.read_csv(
                io.BytesIO(raw_data), dtype=str, encoding=encoding,
                header=None, sep=None, engine='python', keep_default_na=False,
            )
        except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as exc:
            last_error = exc
            continue
        promoted = _promote_uploaded_stock_header(raw_table)
        if promoted is not None:
            return promoted
    raise ValueError(
        '檔案內找不到「代號／名稱」標題，請確認下載的是 Goodinfo 排行 CSV。'
    ) from last_error


def _is_goodinfo_block_page(markup, status_code=None):
    """Recognize a Cloudflare/challenge response so the app can fail fast."""
    if status_code in {401, 403, 429, 430, 503}:
        return True
    normalized = str(markup or '').lower()
    markers = (
        'cf-chl-', 'challenge-platform', 'just a moment',
        'cloudflare ray id', 'attention required! | cloudflare',
        'too many requests', 'error 429', 'error 430',
    )
    return any(marker in normalized for marker in markers)


def _parse_goodinfo_legacy_page(markup):
    """Compatibility path for the original Goodinfo 15-second whole-page flow.

    Goodinfo has changed between ``th``/``td`` headers and nested tables.  The
    original scraper succeeded by selecting the largest completed data table;
    keep that behavior, but still require stock codes plus the turnover label so
    a menu or challenge table cannot be returned as a ranking.
    """
    if '週轉率' not in str(markup or ''):
        return None
    tables = []
    # Keep the exact parser selection used by the historical working flow as
    # the first attempt.  It lets pandas choose the installed HTML backend and
    # is more tolerant of Goodinfo's nested/legacy table markup than forcing a
    # single flavor.  The explicit lxml retry is retained for deployments where
    # pandas cannot auto-select a parser (and avoids the old html5lib ImportError
    # taking down the Streamlit page).
    for read_kwargs in ({}, {'flavor': 'lxml'}):
        try:
            tables = pd.read_html(io.StringIO(str(markup)), **read_kwargs)
            if tables:
                break
        except (ValueError, TypeError, ImportError):
            tables = []
    if not tables:
        return None
    candidates = []
    for dataframe in tables:
        if dataframe.shape[0] <= 10 or dataframe.shape[1] <= 5:
            continue
        cleaned = dataframe.copy()
        if isinstance(cleaned.columns, pd.MultiIndex):
            normalized_columns = []
            for column in cleaned.columns:
                parts = []
                for item in column:
                    text = _goodinfo_normalize_text(item)
                    if text and not text.startswith('Unnamed') and text not in parts:
                        parts.append(text)
                normalized_columns.append('_'.join(parts))
            cleaned.columns = normalized_columns
        else:
            cleaned.columns = [_goodinfo_normalize_text(column) for column in cleaned.columns]
        header_text = '|'.join(map(str, cleaned.columns))
        if '代號' not in header_text or '名稱' not in header_text:
            continue
        code_column = next((column for column in cleaned.columns if '代號' in str(column)), None)
        if code_column is None:
            continue
        valid_codes = cleaned[code_column].astype(str).str.extract(
            r'(?<!\d)(\d{4,6})(?!\d)', expand=False,
        ).notna()
        if int(valid_codes.sum()) >= 10:
            candidates.append(cleaned.dropna(how='all').reset_index(drop=True))
    return max(candidates, key=lambda table: table.shape[0] * table.shape[1]) if candidates else None


def _parse_goodinfo_turnover_table(page_html):
    """挑出 Goodinfo 週轉率排行表，並拒絕阻擋頁或尚未載入的空表。"""
    try:
        tables = pd.read_html(io.StringIO(str(page_html or '')), flavor='lxml')
    except (ValueError, TypeError, ImportError):
        return None

    candidates = []
    for table in tables:
        if table.shape[0] < 10 or table.shape[1] < 5:
            continue
        normalized = table.copy()
        if isinstance(normalized.columns, pd.MultiIndex):
            new_columns = []
            for column in normalized.columns:
                cleaned_parts = []
                for item in column:
                    item_text = re.sub(r"\s+", "", str(item)).strip()
                    if item_text and not item_text.startswith('Unnamed'):
                        if not cleaned_parts or cleaned_parts[-1] != item_text:
                            cleaned_parts.append(item_text)
                new_columns.append('_'.join(cleaned_parts))
            normalized.columns = new_columns
        else:
            normalized.columns = [
                re.sub(r"\s+", "", str(column)).strip()
                for column in normalized.columns
            ]

        column_text = '|'.join(map(str, normalized.columns))
        code_column = next(
            (column for column in normalized.columns if '代號' in str(column)),
            None,
        )
        name_column = next(
            (column for column in normalized.columns if '名稱' in str(column)),
            None,
        )

        # Goodinfo 的動態排行偶爾以 Big5 傳送，卻仍宣告 UTF-8。Chrome
        # 會把中文欄名解成 �N�� 等亂碼；此時不能再靠「代號／名稱」文字，
        # 改以每欄實際包含的 4～6 碼股票代號數量辨認原排行欄。
        if code_column is None or name_column is None:
            code_counts = []
            for position, column in enumerate(normalized.columns):
                code_count = int(
                    normalized.iloc[:, position].astype(str)
                    .str.extract(r'(?<!\d)(\d{4,6})(?!\d)', expand=False)
                    .notna().sum()
                )
                code_counts.append((code_count, position, column))
            valid_codes, code_position, code_column = max(
                code_counts,
                key=lambda item: item[0],
                default=(0, -1, None),
            )
            if valid_codes < 10 or code_position + 1 >= normalized.shape[1]:
                continue
            name_position = code_position + 1
            name_column = normalized.columns[name_position]
            renamed_columns = list(normalized.columns)
            renamed_columns[code_position] = '代號'
            renamed_columns[name_position] = '名稱'
            normalized.columns = renamed_columns
            code_column = '代號'
            name_column = '名稱'
            # 已被瀏覽器替換成 U+FFFD 的公司名稱無法無損還原，留空讓後續
            # 使用 stock_names.csv／線上名稱表依股票代號補回，避免顯示亂碼。
            garbled_names = normalized[name_column].astype(str).str.contains(
                '\ufffd', regex=False,
            )
            if int(garbled_names.sum()) >= max(1, len(normalized) // 2):
                normalized[name_column] = ''

        valid_codes = int(
            normalized[code_column].astype(str)
            .str.extract(r'(?<!\d)(\d{4,6})(?!\d)', expand=False).notna().sum()
        )
        if valid_codes < 10:
            continue
        column_text = '|'.join(map(str, normalized.columns))
        score = sum(
            keyword in column_text
            for keyword in ('代號', '名稱', '週轉', '成交')
        )
        candidates.append((
            score,
            valid_codes,
            normalized.shape[0] * normalized.shape[1],
            normalized,
        ))

    if not candidates:
        return None
    target_df = max(candidates, key=lambda item: (item[0], item[1], item[2]))[3]
    return target_df.dropna(how='all').reset_index(drop=True)


def fetch_goodinfo_data():
    """Use the original Selenium/User-Agent path to load Goodinfo's ranking.

    This deliberately performs one browser visit only.  The previous crawler
    retried/reloaded the page in the same click, which made rate-limit (429/430)
    responses more likely and extended the screen wait.  No visitor cookie,
    clearance token, or profile is persisted or copied into the application.
    """
    started_at = time.monotonic()
    fetch_goodinfo_data.last_status = {
        'state': 'loading', 'reason': '', 'elapsed': 0.0,
    }
    headed_mode = os.environ.get("GOODINFO_HEADED") == "1"
    chrome_options = Options()
    if not headed_mode:
        chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--lang=zh-TW')
    chrome_options.add_argument(
        f'--user-agent={_GOODINFO_LEGACY_USER_AGENT}'
    )
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.page_load_strategy = 'eager'
    if os.path.exists('/usr/bin/chromium'):
        chrome_options.binary_location = '/usr/bin/chromium'

    acquired = _GOODINFO_BROWSER_SEMAPHORE.acquire(timeout=3)
    if not acquired:
        logger.warning('Goodinfo browser is busy; skipped duplicate launch')
        fetch_goodinfo_data.last_status = {
            'state': 'failed', 'reason': 'browser_busy',
            'elapsed': round(time.monotonic() - started_at, 2),
            'headed_mode': headed_mode,
            'fetcher': 'selenium-legacy-ua',
        }
        return None
    driver = None
    try:
        try:
            service = (
                Service('/usr/bin/chromedriver')
                if os.path.exists('/usr/bin/chromedriver') else Service()
            )
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(7)
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-TW', 'zh', 'en-US', 'en']
                    });
                """,
            })
            try:
                driver.get(_GOODINFO_URL)
            except (TimeoutException, UnexpectedAlertPresentException):
                pass

            # Match the original short wait: wait for one completed page, do
            # not refresh or create a second browser request.
            deadline = time.monotonic() + 15.0
            last_markup = ''
            while time.monotonic() < deadline:
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    alert.accept()
                    logger.warning('Goodinfo alert: %s', alert_text)
                    alert_reason = (
                        'rate_limited'
                        if _is_goodinfo_block_page(alert_text)
                        else 'site_alert'
                    )
                    fetch_goodinfo_data.last_status = {
                        'state': 'failed', 'reason': alert_reason,
                        'elapsed': round(time.monotonic() - started_at, 2),
                        'headed_mode': headed_mode,
                        'fetcher': 'selenium-legacy-ua',
                    }
                    return None
                except NoAlertPresentException:
                    pass

                last_markup = driver.page_source
                if _is_goodinfo_block_page(last_markup):
                    fetch_goodinfo_data.last_status = {
                        'state': 'failed', 'reason': 'rate_limited',
                        'elapsed': round(time.monotonic() - started_at, 2),
                        'headed_mode': headed_mode,
                        'fetcher': 'selenium-legacy-ua',
                    }
                    return None
                ranking = _parse_goodinfo_original_page(last_markup)
                if ranking is not None:
                    fetch_goodinfo_data.last_status = {
                        'state': 'success', 'reason': '', 'rows': len(ranking),
                        'elapsed': round(time.monotonic() - started_at, 2),
                        'headed_mode': headed_mode,
                        'fetcher': 'selenium-legacy-ua',
                    }
                    return ranking
                time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))

            fetch_goodinfo_data.last_status = {
                'state': 'failed', 'reason': 'legacy_table_missing',
                'elapsed': round(time.monotonic() - started_at, 2),
                'headed_mode': headed_mode,
                'fetcher': 'selenium-legacy-ua',
            }
            logger.warning(
                'Goodinfo legacy browser did not return a ranking table (%s bytes)',
                len(last_markup),
            )
        except (WebDriverException, OSError, ValueError) as exc:
            logger.exception('Goodinfo legacy browser failed: %s', type(exc).__name__)
            fetch_goodinfo_data.last_status = {
                'state': 'failed', 'reason': 'browser_error',
                'error_type': type(exc).__name__,
                'elapsed': round(time.monotonic() - started_at, 2),
                'headed_mode': headed_mode,
                'fetcher': 'selenium-legacy-ua',
            }
    except Exception as exc:
        logger.exception('Goodinfo legacy fetch failed: %s', type(exc).__name__)
        fetch_goodinfo_data.last_status = {
            'state': 'failed', 'reason': 'browser_error',
            'error_type': type(exc).__name__,
            'elapsed': round(time.monotonic() - started_at, 2),
            'headed_mode': headed_mode,
            'fetcher': 'selenium-legacy-ua',
        }
    finally:
        if driver is not None:
            try:
                driver.quit()
            except WebDriverException:
                pass
        _GOODINFO_BROWSER_SEMAPHORE.release()
    return None

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="台股全盤戰略室", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    [data-testid="stSidebar"] button { white-space: nowrap !important; text-overflow: clip !important; padding-left: 5px !important; padding-right: 5px !important; }
    div.stButton > button { min-height: 45px; font-size: 20px; }
    .stButton { margin-top: 5px; }
    .calendar-header { font-size: clamp(1.65rem, 4vw, 2.5rem); font-weight: 900; text-align: center; color: #ff9800; margin-bottom: 10px; line-height: 1.35; font-family: 'Arial', sans-serif; }
    .calendar-desktop-grid { display:grid; grid-template-columns:minmax(42px,.42fr) repeat(7,minmax(105px,1fr)); gap:4px; align-items:stretch; overflow-x:auto; padding-bottom:4px; }
    .calendar-day-head, .calendar-week-head { text-align:center; font-size:.82rem; font-weight:800; padding:5px 2px; color:#dfe6e9; }
    .calendar-week-head { color:#ffb74d; }
    .cal-box { text-align:left; padding:7px; border-radius:6px; min-height:112px; border:1px solid #4b5563; font-size:0.9rem; line-height:1.3; overflow-wrap:anywhere; }
    .calendar-desktop-grid .cal-box > div:not(.cal-date) { font-size:0.78rem !important; line-height:1.3; }
    .cal-open { background-color: #000000 !important; color: #ffffff !important; }
    .cal-closed { background-color: #d32f2f !important; color: #ffffff !important; font-weight: bold; }
    .cal-week { background-color:#242a33; color:#ffb74d; font-weight:bold; display:flex; align-items:center; justify-content:center; font-size:.78rem; text-align:center; }
    .cal-empty { background:rgba(128,128,128,.04); border-color:rgba(128,128,128,.12); }
    .cal-date { font-size:1rem; font-weight:850; margin-bottom:4px; }
    .settle-m { color: #ffff00; font-weight: bold; font-size: 0.85em; margin-top: 2px; line-height: 1.2; } 
    .settle-w { color: #00e676; font-size: 0.8em; margin-top: 2px; } 
    .settle-f { color: #29b6f6; font-size: 0.8em; margin-top: 2px; } 
    .holiday-tag { font-size: 0.85em; margin-bottom: 2px; color: #ffeb3b; background-color: rgba(0,0,0,0.5); border-radius: 3px; padding: 1px;}
    .today-border { border: 3px solid #ffff00 !important; }
    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] { font-size:0.84rem; }
    div[data-testid="stDataFrame"] [role="gridcell"],
    div[data-testid="stDataFrame"] [role="columnheader"],
    div[data-testid="stDataEditor"] [role="gridcell"],
    div[data-testid="stDataEditor"] [role="columnheader"] {
        padding-left:3px !important;
        padding-right:3px !important;
    }
    div[data-testid="stDataFrame"] [role="columnheader"],
    div[data-testid="stDataEditor"] [role="columnheader"] { white-space:nowrap; }
    /* Keep decision tables compact without forcing long text to disappear. */
    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        max-width:100%;
        overflow-x:auto;
    }
    div[data-testid="stDataFrame"] [role="gridcell"],
    div[data-testid="stDataEditor"] [role="gridcell"] {
        line-height:1.25;
        vertical-align:middle;
    }
    [data-testid="stPlotlyChart"] { max-width:100%; overflow-x:auto; }
    .calendar-mobile-list { display:none; }
    @media (max-width: 700px) {
        /* Streamlit columns otherwise keep desktop-sized gutters on phones. */
        [data-testid="stAppViewContainer"] .block-container {
            padding-left:.55rem;
            padding-right:.55rem;
            padding-top:1rem;
            padding-bottom:2rem;
        }
        [data-testid="stAppViewContainer"] h1 { font-size:1.55rem; line-height:1.25; }
        [data-testid="stAppViewContainer"] h2 { font-size:1.3rem; line-height:1.3; }
        [data-testid="stAppViewContainer"] h3,
        [data-testid="stAppViewContainer"] h4 { font-size:1.08rem; line-height:1.35; }
        div[data-testid="stHorizontalBlock"] {
            gap:.4rem;
            align-items:stretch;
            flex-wrap:wrap;
        }
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            flex:1 1 calc(50% - .4rem) !important;
            min-width:9rem !important;
            padding-left:.12rem !important;
            padding-right:.12rem !important;
        }
        div.stButton > button {
            min-height:40px;
            padding:.42rem .48rem;
            font-size:.92rem;
            line-height:1.2;
            white-space:normal;
            word-break:break-word;
        }
        div[data-testid="stTextInput"] input,
        div[data-testid="stNumberInput"] input,
        div[data-testid="stSelectbox"] [role="combobox"] {
            font-size:.95rem;
        }
        div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
            font-size:.78rem;
            -webkit-overflow-scrolling:touch;
        }
        div[data-testid="stDataFrame"] [role="gridcell"],
        div[data-testid="stDataFrame"] [role="columnheader"],
        div[data-testid="stDataEditor"] [role="gridcell"],
        div[data-testid="stDataEditor"] [role="columnheader"] {
            padding-left:2px !important;
            padding-right:2px !important;
            line-height:1.18;
        }
        [data-testid="stPlotlyChart"] {
            margin-left:-.2rem;
            margin-right:-.2rem;
        }
        /* Index plan cards and option result cards remain readable in two columns. */
        [data-testid="stAppViewContainer"] .index-plan-main-grid { grid-template-columns:repeat(2,minmax(0,1fr)); gap:.4rem; }
        [data-testid="stAppViewContainer"] .index-plan-main-label { font-size:.74rem; }
        [data-testid="stAppViewContainer"] .index-plan-main-value { font-size:1.25rem; line-height:1.18; }
        [data-testid="stAppViewContainer"] .opt-card { padding:9px !important; margin-bottom:7px !important; }
        [data-testid="stAppViewContainer"] .opt-label { font-size:.72rem !important; }
        [data-testid="stAppViewContainer"] .opt-value { font-size:1rem !important; line-height:1.25; }
        [data-testid="stAppViewContainer"] .futures-expiry-reminder { gap:5px; padding:7px 8px; }
        [data-testid="stAppViewContainer"] .futures-expiry-contract { font-size:.85rem; }
        [data-testid="stAppViewContainer"] .futures-expiry-date { font-size:.92rem; }
        [data-testid="stAppViewContainer"] .futures-expiry-countdown { font-size:.8rem; }
        [data-testid="stAppViewContainer"] .futures-expiry-days { font-size:1.1rem; }
        .calendar-desktop-grid { display:none; }
        .calendar-mobile-list { display:flex; flex-direction:column; gap:7px; }
        .calendar-mobile-day { padding:9px 10px; border-radius:7px; border:1px solid #4b5563; text-align:left; font-size:.9rem; line-height:1.35; }
        .calendar-mobile-day > div:not(.calendar-mobile-date) { font-size:.84rem !important; }
        .calendar-mobile-date { display:flex; justify-content:space-between; gap:8px; align-items:center; font-size:1rem; font-weight:850; margin-bottom:5px; }
        .calendar-mobile-week { color:#ffb74d; font-size:.74rem; font-weight:750; white-space:nowrap; }
    }
    div[data-testid="column"] { text-align: center; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ 台股全盤戰略室 ⚡")

CONFIG_FILE = "config.json"

# 舊版總快取僅保留作為一次性相容來源，不再作為正常存檔位置。
DATA_CACHE_FILE = "data_cache.json"

# 各功能獨立本機快取。
STOCK_STRATEGY_CACHE_FILE = "stock_strategy_cache.json"
URL_CACHE_FILE = "url_cache.json"
SEARCH_CACHE_FILE = "search_cache.json"
STRATEGY_SIGNAL_LOG_FILE = "strategy_signal_log.json"
STRATEGY_SIGNAL_TOMBSTONES_FILE = "strategy_signal_tombstones.json"
FIBO_TAG_CACHE_FILE = "fibo_tags.json"
FUTURES_STATE_CACHE_FILE = "futures_strategy_cache.json"
COMPANY_EVENT_SNAPSHOT_FILE = "company_event_snapshot.json"

# Google Sheet 獨立 scope。
GOOGLE_SCOPE_STOCK = "stock_strategy"
GOOGLE_SCOPE_FIBO = "fibo_strategy"
GOOGLE_SCOPE_COMPANY = "company_events"
GOOGLE_SCOPE_SIGNALS = "strategy_signals"
GOOGLE_SCOPE_FUTURES = "futures_strategy"
DEFAULT_FIBO_TAGS = ["台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)", "晶彩科(3535)"]
ANALYSIS_MAX_WORKERS = 2
API_REQUEST_GAP_SECONDS = 0.1

_RUNTIME_FILE_LOCK = threading.RLock()


@st.cache_data(max_entries=1)
def load_local_stock_names():
    stock_name_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_names.csv")
    if os.path.exists(stock_name_path):
        try:
            df = pd.read_csv(stock_name_path, header=None, names=["code", "name"], dtype=str)
            codes = df['code'].astype(str).str.strip()
            names = df['name'].astype(str).str.strip()
            code_map = dict(zip(codes, names))
            # Prefer four-digit listed/OTC stocks when duplicate display names
            # also appear in warrants or other instruments.
            name_map = {}
            preferred_pairs = sorted(
                zip(names, codes),
                key=lambda pair: (not bool(re.fullmatch(r'\d{4}', pair[1])), len(pair[1]), pair[1]),
            )
            for name, code in preferred_pairs:
                name_map.setdefault(name, code)
            return code_map, name_map
        except Exception:
            pass
    return {}, {}


def normalize_fibo_quick_tag(value, code_map=None, name_map=None):
    """Normalize persisted/user-entered tags to the selector's 名稱(代號) value."""
    raw = str(value or '').strip().replace('（', '(').replace('）', ')')
    if not raw:
        return ''
    if code_map is None or name_map is None:
        code_map, name_map = load_local_stock_names()
    code_match = re.search(r'(?<!\d)(\d{4,6}[A-Za-z]?)(?:\.(?:TW|TWO))?(?!\d)', raw, re.I)
    if code_match:
        code = code_match.group(1).upper()
        name = code_map.get(code)
        if name:
            return f"{name}({code})"
    normalized_name = re.sub(r'\s+', '', raw.split('(', 1)[0])
    exact_code = name_map.get(normalized_name)
    if exact_code:
        return f"{code_map.get(exact_code, normalized_name)}({exact_code})"
    matched_stocks = [
        (name, code) for name, code in name_map.items()
        if normalized_name and normalized_name in re.sub(r'\s+', '', name)
    ]
    if matched_stocks:
        matched_stocks.sort(key=lambda item: (not item[1].isdigit(), len(item[1]), item[1]))
        name, code = matched_stocks[0]
        return f"{name}({code})"
    return raw


def load_config():
    with _RUNTIME_FILE_LOCK:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as file:
                    payload = json.load(file)
                return payload if isinstance(payload, dict) else {}
            except (OSError, ValueError, TypeError):
                return {}
        return {}

def save_config(font_size, limit_rows, sj_key="", sj_secret="", remember_sj=False):
    try:
        with _RUNTIME_FILE_LOCK:
            config = load_config()
            config.pop('auto_update', None)
            config.pop('delay_sec', None)
            config.update({
                "font_size": font_size,
                "limit_rows": limit_rows,
                "sj_key": sj_key if remember_sj else "",
                "sj_secret": sj_secret if remember_sj else "",
                "remember_sj": remember_sj,
            })
            _write_json_atomic(CONFIG_FILE, config)
        return True
    except (OSError, TypeError, ValueError):
        return False

def _json_safe(value):
    """將期貨快取轉為可寫入設定檔的基本型別。"""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.strftime('%Y/%m/%d %H:%M:%S')
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return str(value)


def load_futures_strategy_state():
    """讀取期貨戰略室上次成功取得的表格與使用者清單。"""
    cloud_state = st.session_state.get('_cached_futures_strategy_state', {})
    local_state = {}
    if os.path.exists(FUTURES_STATE_CACHE_FILE):
        try:
            with _RUNTIME_FILE_LOCK:
                with open(FUTURES_STATE_CACHE_FILE, 'r', encoding='utf-8') as file:
                    candidate = json.load(file)
            local_state = candidate if isinstance(candidate, dict) else {}
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            local_state = {}
    if not local_state:
        # 舊版把完整 1,800+ 列 universe 放在 config.json，會讓每次 rerun
        # 都解析約 1 MB。首次讀取時縮成 last-known-good 小快照並搬到獨立檔。
        config = load_config()
        legacy_state = config.get('futures_strategy_state', {})
        if isinstance(legacy_state, dict) and legacy_state:
            local_state = compact_futures_strategy_state(legacy_state)
            try:
                _write_json_atomic(FUTURES_STATE_CACHE_FILE, local_state, indent=2)
                config.pop('futures_strategy_state', None)
                _write_json_atomic(CONFIG_FILE, config)
            except (OSError, TypeError, ValueError):
                pass

    if not st.session_state.get('_futures_cloud_loaded', False):
        gsheet_api_url = get_app_secret('gsheet_api_url')
        if gsheet_api_url:
            remote_state, remote_error = _fetch_remote_scope(
                gsheet_api_url, GOOGLE_SCOPE_FUTURES, timeout=8,
            )
            if isinstance(remote_state, dict):
                cloud_state = remote_state
                st.session_state['_futures_cloud_error'] = ''
            else:
                st.session_state['_futures_cloud_error'] = remote_error or '讀取失敗'
        st.session_state['_futures_cloud_loaded'] = True

    merged_state = _merge_futures_strategy_state(cloud_state, local_state)
    # 結算日切月後，舊裝置／Google Sheet 可能仍保留已結算近月；
    # 讀取時先清掉，避免本機 cache 在下一次 rerun 將舊契約復活。
    merged_state, _ = prune_futures_settlement_state(merged_state)
    st.session_state['_cached_futures_strategy_state'] = merged_state
    if merged_state and merged_state != local_state:
        try:
            _write_json_atomic(FUTURES_STATE_CACHE_FILE, merged_state, indent=2)
        except (OSError, TypeError, ValueError):
            pass
    return merged_state


def _state_updated_at(value):
    if not isinstance(value, dict):
        return None
    try:
        timestamp = pd.Timestamp(value.get('updated_at'))
        return None if pd.isna(timestamp) else timestamp
    except (TypeError, ValueError, OverflowError):
        return None


def _newer_timestamped_state(first, second):
    """選擇較新的非空狀態；有雲端時間時不讓舊裝置覆寫新裝置。"""
    first = first if isinstance(first, dict) else {}
    second = second if isinstance(second, dict) else {}
    if not first:
        return dict(second)
    if not second:
        return dict(first)
    first_time = _state_updated_at(first)
    second_time = _state_updated_at(second)
    if first_time is None and second_time is None:
        return dict(first)
    if first_time is None:
        return dict(second)
    if second_time is None:
        return dict(first)
    return dict(first if first_time >= second_time else second)


def save_futures_strategy_state(
    universe=None, metadata=None, rank_cache=None, live_cache=None,
    manual=None, ignored=None, rank_time=None, live_time=None,
):
    """持久化期貨表格快照，重整後仍能還原最後成功資料。"""
    try:
        existing_state = load_futures_strategy_state()
        with _RUNTIME_FILE_LOCK:
            config = load_config()
            config.pop('futures_strategy_custom_prices', None)
            existing = _merge_futures_strategy_state(
                st.session_state.get('_cached_futures_strategy_state', {}),
                existing_state,
            )
            records = existing.get('universe', []) if isinstance(existing, dict) else []
            if isinstance(universe, pd.DataFrame) and not universe.empty:
                records = json.loads(universe.to_json(orient='records', force_ascii=False))
            next_manual = list(manual if manual is not None else (existing.get('manual', []) if isinstance(existing, dict) else []))
            next_ignored = list(ignored if ignored is not None else (existing.get('ignored', []) if isinstance(existing, dict) else []))
            selection_changed = (
                next_manual != list(existing.get('manual', []))
                or set(next_ignored) != set(existing.get('ignored', []))
            )
            now_iso = datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
            next_rank_cache = rank_cache if rank_cache is not None else existing.get('rank_cache', {})
            next_live_cache = live_cache if live_cache is not None else existing.get('live_cache', {})
            next_rank_time = rank_time if rank_cache is not None else existing.get('rank_time')
            next_live_time = live_time if live_cache is not None else existing.get('live_time')
            rank_changed = _json_safe(next_rank_cache) != _json_safe(existing.get('rank_cache', {}))
            live_changed = _json_safe(next_live_cache) != _json_safe(existing.get('live_cache', {}))
            saved_state = _json_safe({
                'universe': records,
                'metadata': metadata or (existing.get('metadata', {}) if isinstance(existing, dict) else {}),
                'rank_cache': next_rank_cache,
                'live_cache': next_live_cache,
                'manual': next_manual,
                'ignored': next_ignored,
                'rank_time': next_rank_time,
                'live_time': next_live_time,
                'rank_updated_at': now_iso if rank_changed else existing.get('rank_updated_at', existing.get('rank_time')),
                'live_updated_at': now_iso if live_changed else existing.get('live_updated_at', existing.get('live_time')),
                'selection_updated_at': (
                    now_iso if selection_changed else existing.get('selection_updated_at', existing.get('updated_at'))
                ),
                'updated_at': now_iso,
            })
            saved_state, _ = prune_futures_settlement_state(saved_state)
            saved_state = compact_futures_strategy_state(saved_state)
            _write_json_atomic(FUTURES_STATE_CACHE_FILE, saved_state, indent=2)
            if 'futures_strategy_state' in config:
                config.pop('futures_strategy_state', None)
                _write_json_atomic(CONFIG_FILE, config)
        sync_ok = True
        gsheet_api_url = get_app_secret('gsheet_api_url')
        if gsheet_api_url:
            with get_data_cache_sync_lock():
                remote_state, _ = _fetch_remote_scope(
                    gsheet_api_url, GOOGLE_SCOPE_FUTURES, timeout=8,
                )
                cloud_state = _merge_futures_strategy_state(remote_state, saved_state)
                sync_ok, _ = _save_remote_scope(
                    gsheet_api_url,
                    GOOGLE_SCOPE_FUTURES,
                    cloud_state,
                    updated_at=cloud_state.get('updated_at'),
                    timeout=8,
                    verify=True,
                )
                if sync_ok:
                    saved_state = cloud_state
                    _write_json_atomic(FUTURES_STATE_CACHE_FILE, saved_state, indent=2)
        st.session_state['_cached_futures_strategy_state'] = saved_state
        st.session_state['_futures_cloud_loaded'] = True
        st.session_state['_futures_cloud_save_status'] = 'ok' if sync_ok else 'local_only'
        return True
    except (OSError, TypeError, ValueError):
        return False


def merge_futures_official_snapshot(current, previous, current_meta=None, previous_meta=None):
    """合併官方期貨快照，避免暫時回傳空值／0 時覆蓋最後一筆好資料。

    期交所檔案偶爾會先回傳同一交易日的半成品。相同或較舊資料日只接受
    正常的新值，缺值與 0 則保留上一筆成功快照；真正較新的資料日則完整採用。
    回傳的 metadata 會標記是否曾採用 last-known-good，供畫面清楚顯示 as-of。
    """
    current_frame = current.copy() if isinstance(current, pd.DataFrame) else pd.DataFrame()
    previous_frame = previous.copy() if isinstance(previous, pd.DataFrame) else pd.DataFrame()
    current_info = dict(current_meta) if isinstance(current_meta, dict) else {}
    previous_info = dict(previous_meta) if isinstance(previous_meta, dict) else {}
    if current_frame.empty:
        if previous_frame.empty:
            return current_frame, current_info
        restored_info = dict(previous_info)
        restored_info.update({key: value for key, value in current_info.items() if key not in ('updated', 'margin_date')})
        restored_info['last_known_good'] = True
        return previous_frame.copy(), restored_info
    if previous_frame.empty or '契約鍵' not in current_frame.columns or '契約鍵' not in previous_frame.columns:
        return current_frame, current_info

    def date_key(value):
        digits = re.sub(r'[^0-9]', '', str(value or ''))
        return digits[:8] if len(digits) >= 8 else ''

    current_date = date_key(current_info.get('updated'))
    previous_date = date_key(previous_info.get('updated'))
    # 沒有明確日期時採保守策略；只有明確較新的官方資料才會覆蓋舊值。
    is_newer = bool(current_date and previous_date and current_date > previous_date)
    is_older_or_same = not is_newer
    margin_columns = {'所需保證金', '維持保證金'}
    market_columns = {
        '當日成交口數', '未平倉量', '開盤價', '當日高', '當日低', '收盤價',
        '漲跌幅', '當日漲停價', '當日跌停價',
    }
    previous_by_key = {
        str(row['契約鍵']): row
        for _, row in previous_frame.iterrows()
        if str(row.get('契約鍵', '')).strip()
    }
    reused = False
    margin_stale = False
    for index, row in current_frame.iterrows():
        old = previous_by_key.get(str(row.get('契約鍵', '')))
        if old is None:
            continue
        for column in market_columns:
            if column not in current_frame.columns or column not in old.index:
                continue
            current_value = row.get(column)
            previous_value = old.get(column)
            current_number = _safe_number(current_value)
            previous_number = _safe_number(previous_value)
            current_missing = current_value is None or pd.isna(current_value)
            previous_available = previous_value is not None and not pd.isna(previous_value)
            if is_older_or_same and previous_available and (
                current_missing or (
                    current_number is not None and current_number == 0
                    and previous_number is not None and previous_number > 0
                )
            ):
                current_frame.at[index, column] = previous_value
                reused = True
        if is_older_or_same and previous_date and current_date and current_date < previous_date:
            # API 回到舊交易日，整列行情不能倒退；契約名稱與資格欄仍採新回傳。
            for column in market_columns | {'資料日期'}:
                if column in current_frame.columns and column in old.index:
                    current_frame.at[index, column] = old.get(column)
                    reused = True

        # 保證金有獨立生效日；即使市場行情已換日，保證金端點暫時缺值時
        # 仍應保留上一份有效標準，不能顯示 0 或空白。
        current_margin_values = {
            column: _safe_number(row.get(column)) for column in margin_columns
            if column in current_frame.columns
        }
        previous_margin_values = {
            column: _safe_number(old.get(column)) for column in margin_columns
            if column in old.index
        }
        margin_incomplete = any(
            (current_margin_values.get(column) is None or current_margin_values.get(column) <= 0)
            and (previous_margin_values.get(column) or 0) > 0
            for column in margin_columns
        )
        if margin_incomplete:
            for column in margin_columns:
                if (previous_margin_values.get(column) or 0) > 0:
                    current_frame.at[index, column] = old.get(column)
            reused = True
            margin_stale = True

    merged_info = dict(current_info)
    if is_older_or_same and previous_date and (not current_date or current_date <= previous_date):
        if previous_info.get('updated'):
            merged_info['updated'] = previous_info['updated']
    current_margin_date = date_key(current_info.get('margin_date'))
    previous_margin_date = date_key(previous_info.get('margin_date'))
    if previous_info.get('margin_date') and (
        margin_stale
        or not current_margin_date
        or (previous_margin_date and current_margin_date < previous_margin_date)
    ):
        merged_info['margin_date'] = previous_info['margin_date']
    if reused:
        merged_info['last_known_good'] = True
        merged_info['last_known_good_as_of'] = previous_info.get('updated') or previous_info.get('margin_date')
    return current_frame, merged_info


def _parse_futures_state_time(value):
    text = str(value or '').strip()
    digits = re.sub(r'[^0-9]', '', text)
    if len(digits) >= 8 and not any(separator in text for separator in (':', 'T')):
        text = f'{digits[:4]}-{digits[4:6]}-{digits[6:8]}'
    try:
        timestamp = pd.Timestamp(text)
        return None if pd.isna(timestamp) else timestamp
    except (TypeError, ValueError, OverflowError):
        return None


def _prefer_futures_state_section(remote, local, time_key):
    remote_time = _parse_futures_state_time(remote.get(time_key))
    local_time = _parse_futures_state_time(local.get(time_key))
    if local_time is not None and (remote_time is None or local_time > remote_time):
        return local
    return remote if remote else local


def _merge_futures_strategy_state(remote, local):
    """分區合併期貨快照，避免單一裝置操作回退另一裝置的新行情。"""
    remote = remote if isinstance(remote, dict) else {}
    local = local if isinstance(local, dict) else {}
    if not remote:
        return compact_futures_strategy_state(dict(local))
    if not local:
        return compact_futures_strategy_state(dict(remote))

    selection_remote = dict(remote, _selection_marker=remote.get('selection_updated_at') or remote.get('updated_at'))
    selection_local = dict(local, _selection_marker=local.get('selection_updated_at') or local.get('updated_at'))
    selection_source = _prefer_futures_state_section(selection_remote, selection_local, '_selection_marker')
    rank_remote = dict(remote, _rank_marker=remote.get('rank_updated_at') or remote.get('rank_time') or remote.get('updated_at'))
    rank_local = dict(local, _rank_marker=local.get('rank_updated_at') or local.get('rank_time') or local.get('updated_at'))
    live_remote = dict(remote, _live_marker=remote.get('live_updated_at') or remote.get('live_time') or remote.get('updated_at'))
    live_local = dict(local, _live_marker=local.get('live_updated_at') or local.get('live_time') or local.get('updated_at'))
    rank_source = _prefer_futures_state_section(rank_remote, rank_local, '_rank_marker')
    live_source = _prefer_futures_state_section(live_remote, live_local, '_live_marker')

    remote_universe = pd.DataFrame(remote.get('universe', []))
    local_universe = pd.DataFrame(local.get('universe', []))
    merged_universe, merged_metadata = merge_futures_official_snapshot(
        local_universe, remote_universe,
        local.get('metadata', {}), remote.get('metadata', {}),
    )
    newest = _newer_timestamped_state(remote, local)
    merged = dict(newest)
    merged.update({
        'universe': _json_safe(merged_universe.to_dict(orient='records')),
        'metadata': _json_safe(merged_metadata),
        'rank_cache': _json_safe(rank_source.get('rank_cache', {})),
        'rank_time': rank_source.get('rank_time'),
        'rank_updated_at': rank_source.get('rank_updated_at') or rank_source.get('_rank_marker'),
        'live_cache': _json_safe(live_source.get('live_cache', {})),
        'live_time': live_source.get('live_time'),
        'live_updated_at': live_source.get('live_updated_at') or live_source.get('_live_marker'),
        'manual': list(selection_source.get('manual', [])),
        'ignored': list(selection_source.get('ignored', [])),
        'selection_updated_at': selection_source.get(
            'selection_updated_at', selection_source.get('updated_at'),
        ),
    })
    return compact_futures_strategy_state(_json_safe(merged))


def compact_futures_strategy_state(state, max_chars=14000):
    """保留常用／已更新契約，避免單一 Google Sheet 儲存格超過容量。"""
    state = state if isinstance(state, dict) else {}
    universe = pd.DataFrame(state.get('universe', []))
    rank_cache = dict(state.get('rank_cache', {}))
    live_cache = dict(state.get('live_cache', {}))
    manual = [str(key) for key in state.get('manual', [])][-12:]
    ignored = [str(key) for key in state.get('ignored', [])][-12:]
    forced_keys = list(dict.fromkeys(manual + ignored))

    ranked_keys = sorted(
        rank_cache,
        key=lambda key: _safe_number(rank_cache.get(key, {}).get('當日成交口數'), 0) or 0,
        reverse=True,
    )
    if not universe.empty and '契約鍵' in universe.columns:
        sorted_universe = universe.copy()
        if '當日成交口數' in sorted_universe.columns:
            sorted_universe['_persist_volume'] = pd.to_numeric(
                sorted_universe['當日成交口數'], errors='coerce',
            ).fillna(0)
            sorted_universe = sorted_universe.sort_values('_persist_volume', ascending=False)
        universe_keys = sorted_universe['契約鍵'].astype(str).tolist()
    else:
        universe_keys = []
    keep_keys = list(dict.fromkeys(forced_keys + ranked_keys[:16] + list(live_cache)[:16] + universe_keys[:16]))[:32]

    keep_columns = [
        '契約鍵', '期貨代碼', '契約月份', '名稱', '標的代號', '商品類型',
        'ETF期貨', '指數期貨', '小型期貨', '次月期貨', '月份順位', '交易時段',
        '當日成交口數', '未平倉量', '開盤價', '當日高', '當日低', '收盤價',
        '漲跌幅', '當日漲停價', '當日跌停價', '所需保證金', '維持保證金',
        '資料日期', '契約最後交易日', '乘數', '跳動點',
    ]
    if not universe.empty and '契約鍵' in universe.columns:
        compact_universe = universe[universe['契約鍵'].astype(str).isin(keep_keys)].copy()
        compact_universe = compact_universe[[column for column in keep_columns if column in compact_universe.columns]]
        universe_records = compact_universe.to_dict(orient='records')
    else:
        universe_records = []

    compact = {
        key: value for key, value in state.items()
        if key not in {'universe', 'rank_cache', 'live_cache'}
    }
    compact.update({
        'universe': _json_safe(universe_records),
        'rank_cache': _json_safe({key: rank_cache[key] for key in keep_keys if key in rank_cache}),
        'live_cache': _json_safe({key: live_cache[key] for key in keep_keys if key in live_cache}),
        'manual': manual,
        'ignored': ignored,
    })

    # 最壞情況逐筆裁掉非強制快取；保留版本標記與使用者清單。
    while len(json.dumps(_json_safe(compact), ensure_ascii=False)) > max_chars and compact['universe']:
        removable_index = next(
            (index for index in range(len(compact['universe']) - 1, -1, -1)
             if str(compact['universe'][index].get('契約鍵', '')) not in forced_keys),
            len(compact['universe']) - 1,
        )
        removed_key = str(compact['universe'][removable_index].get('契約鍵', ''))
        compact['universe'].pop(removable_index)
        if removed_key not in forced_keys:
            compact['rank_cache'].pop(removed_key, None)
            compact['live_cache'].pop(removed_key, None)
    for cache_name in ('rank_cache', 'live_cache'):
        while len(json.dumps(_json_safe(compact), ensure_ascii=False)) > max_chars and compact[cache_name]:
            removable_key = next(
                (key for key in reversed(list(compact[cache_name])) if key not in forced_keys),
                next(reversed(compact[cache_name])),
            )
            compact[cache_name].pop(removable_key, None)
    return _json_safe(compact)


def load_strategy_signal_log():
    """讀取策略訊號紀錄；格式錯誤時回傳空清單，不影響主程式。"""
    if not os.path.exists(STRATEGY_SIGNAL_LOG_FILE):
        return []
    try:
        with open(STRATEGY_SIGNAL_LOG_FILE, "r", encoding="utf-8") as file:
            records = json.load(file)
        if not isinstance(records, list):
            return []
        for record in records:
            if isinstance(record, dict) and record.get('策略') == '當沖預覽':
                record['策略'] = '當沖'
        deleted_keys = set(load_strategy_signal_tombstones())
        return [
            record for record in records
            if str(record.get('dedupe_key', '')).strip() not in deleted_keys
        ]
    except (OSError, ValueError, TypeError):
        return []

def _sync_strategy_signal_scope_from_cloud():
    """
    第一次使用策略驗證時，
    把 strategy_signals scope 與本機紀錄合併。
    """
    if st.session_state.get(
        '_strategy_signal_cloud_loaded',
        False
    ):
        return True

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    if not gsheet_api_url:
        st.session_state[
            '_strategy_signal_cloud_loaded'
        ] = True

        return True

    local_records = (
        load_strategy_signal_log()
    )

    local_deleted = (
        load_strategy_signal_tombstones()
    )

    remote_payload, remote_error = (
        _fetch_remote_scope(
            gsheet_api_url,
            GOOGLE_SCOPE_SIGNALS,
            timeout=8,
        )
    )

    if not isinstance(
        remote_payload,
        dict
    ):
        # Google 暫時讀不到時，
        # 保留本機資料，不阻止程式使用。
        return False

    remote_records = (
        remote_payload.get(
            'strategy_signal_log',
            remote_payload.get(
                'records',
                []
            )
        )
    )

    remote_deleted = (
        remote_payload.get(
            'strategy_signal_deleted_keys',
            remote_payload.get(
                'deleted_keys',
                []
            )
        )
    )

    merged_records, deleted_keys = (
        merge_strategy_signal_state(
            remote_records,
            local_records,
            remote_deleted,
            local_deleted,
        )
    )

    save_strategy_signal_tombstones(
        deleted_keys
    )

    try:
        with _RUNTIME_FILE_LOCK:
            _write_json_atomic(
                STRATEGY_SIGNAL_LOG_FILE,
                merged_records[-2000:],
                indent=2,
            )
    except (
        OSError,
        TypeError,
        ValueError,
    ):
        return False

    try:
        with _RUNTIME_FILE_LOCK:
            _write_json_atomic(
                STRATEGY_SIGNAL_TOMBSTONES_FILE,
                deleted_keys[-4000:],
                indent=2,
            )
    except (
        OSError,
        TypeError,
        ValueError,
    ):
        pass

    st.session_state[
        '_strategy_signal_cloud_loaded'
    ] = True

    return True


def _sync_strategy_signal_scope_to_cloud(
    records
):
    """把策略驗證資料獨立同步到 strategy_signals。"""
    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    if not gsheet_api_url:
        return True

    payload = {
        'version': 1,
        'strategy_signal_log': list(
            records
        )[-2000:],
        'strategy_signal_deleted_keys': (
            load_strategy_signal_tombstones()
        )[-4000:],
    }

    updated_at = (
        datetime.now(
            pytz.timezone(
                'Asia/Taipei'
            )
        ).isoformat()
    )

    with get_data_cache_sync_lock():
        sync_ok, _ = (
            _save_remote_scope(
                gsheet_api_url,
                GOOGLE_SCOPE_SIGNALS,
                payload,
                updated_at=updated_at,
                timeout=8,
                verify=True,
            )
        )

    return sync_ok

def save_strategy_signal_log(
    records
):
    """
    最多保留最近 2,000 筆策略訊號。

    本機 + Google Sheet strategy_signals。
    """
    normalized_records = list(
        records
    )[-2000:]

    try:
        with _RUNTIME_FILE_LOCK:
            _write_json_atomic(
                STRATEGY_SIGNAL_LOG_FILE,
                normalized_records,
                indent=2,
            )
    except (
        OSError,
        TypeError,
        ValueError,
    ):
        return False

    sync_ok = (
        _sync_strategy_signal_scope_to_cloud(
            normalized_records
        )
    )

    st.session_state[
        '_strategy_signal_sync_status'
    ] = (
        'ok'
        if sync_ok
        else 'local_only'
    )

    return sync_ok


def load_strategy_signal_tombstones():
    """讀取已刪除訊號鍵，避免舊雲端快取把紀錄復原。"""
    if not os.path.exists(STRATEGY_SIGNAL_TOMBSTONES_FILE):
        return []
    try:
        with open(STRATEGY_SIGNAL_TOMBSTONES_FILE, "r", encoding="utf-8") as file:
            values = json.load(file)
        if not isinstance(values, list):
            return []
        return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))[-4000:]
    except (OSError, ValueError, TypeError):
        return []

def save_strategy_signal_tombstones(values):
    """保存刪除標記；筆數上限高於訊號紀錄，確保舊副本無法復活。"""
    try:
        normalized = list(dict.fromkeys(
            str(value).strip() for value in values if str(value).strip()
        ))[-4000:]
        with _RUNTIME_FILE_LOCK:
            _write_json_atomic(
                STRATEGY_SIGNAL_TOMBSTONES_FILE, normalized, indent=2,
            )
        return True
    except (OSError, TypeError, ValueError):
        return False


def mark_strategy_signals_deleted(records):
    """將明確刪除的訊號留下墓碑，供本機與雲端合併時排除。"""
    deleted = load_strategy_signal_tombstones()
    deleted.extend(
        str(record.get('dedupe_key', '')).strip()
        for record in records if isinstance(record, dict)
    )
    return save_strategy_signal_tombstones(deleted)


def merge_strategy_signal_state(remote_records, local_records, remote_deleted=None, local_deleted=None):
    """合併多裝置訊號；刪除標記優先於任何舊紀錄。"""
    deleted = set(_merge_unique_values(remote_deleted or [], local_deleted or []))
    records = _merge_unique_records(remote_records, local_records, 'dedupe_key')
    records = [
        record for record in records
        if str(record.get('dedupe_key', '')).strip() not in deleted
    ][-2000:]
    return records, sorted(str(value) for value in deleted if str(value).strip())[-4000:]

def parse_trade_plan_numbers(plan_text):
    """從「進／停／目」摘要解析三個價位。"""
    text = str(plan_text or '')
    values = {}
    for label, key in [('進', 'entry'), ('停', 'stop'), ('目', 'target')]:
        match = re.search(rf'{label}\s*([0-9][0-9,]*(?:\.[0-9]+)?)', text)
        values[key] = float(match.group(1).replace(',', '')) if match else None
    return values

def classify_signal_state(rule, eligible, score=None, minimum_score=0):
    """把文字條件轉成簡短狀態，原始規則文字仍保留在明細。"""
    text = str(rule or '')
    if '資料不足' in text:
        return '⚪ 資料不足'
    if any(keyword in text for keyword in ('不交易', '排除', '處置', '禁止')):
        return '⛔ 暫停'
    if not eligible or (score is not None and float(score or 0) < float(minimum_score or 0)):
        return '🟡 等待'
    if any(keyword in text for keyword in ('回測確認', '站穩確認')):
        return '🔵 回測確認'
    if text.startswith('觸發：') or any(keyword in text for keyword in ('突破昨高後站穩', '跌破昨低後確認')):
        return '✅ 已觸發'
    return '🟡 接近觸發'

def parse_strategy_data_time(value):
    """將不同來源的日期時間轉成臺北時間的 naive Timestamp。"""
    if value in (None, '', '—'):
        return None
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            return None
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert('Asia/Taipei').tz_localize(None)
        return timestamp
    except (TypeError, ValueError, OverflowError):
        return None

def build_data_health(data_time=None, required_ready=True, live_expected=False):
    """建立不誤導的資料健康狀態；沒有即時時間時明確標示官方日行情。"""
    if not required_ready:
        return '⚪ 資料不足'
    timestamp = parse_strategy_data_time(data_time)
    if timestamp is None:
        return '🔵 官方日行情' if not live_expected else '⚪ 尚未即時更新'
    now_tw_naive = datetime.now(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
    age_seconds = (now_tw_naive - timestamp.to_pydatetime()).total_seconds()
    if age_seconds < -60:
        return '🟠 報價時間異常'
    age_seconds = max(0, age_seconds)
    if age_seconds <= 90:
        return '🟢 即時'
    if age_seconds <= 600:
        return f'🟡 {int(age_seconds // 60)}分前'
    return '🔴 報價過期'

def calculate_market_alignment(direction, market_bias):
    """比較策略方向與臺指期環境，只作附加標示，不改動原選股排序。"""
    normalized_direction = '偏多' if direction in ('多頭', '偏多') else '偏空'
    if market_bias not in ('偏多', '偏空'):
        return '⚪ 盤整／未確認'
    return '🟢 同向' if normalized_direction == market_bias else '🟡 逆勢'

def futures_expiry_date(contract_month):
    """以月契約第三個星期三估算到期日；休市時順延至次一交易日。"""
    match = re.fullmatch(r'(\d{4})(\d{2})', str(contract_month or ''))
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    month_calendar = calendar.monthcalendar(year, month)
    wednesdays = [week[calendar.WEDNESDAY] for week in month_calendar if week[calendar.WEDNESDAY]]
    if len(wednesdays) < 3:
        return None
    return adjust_to_next_market_day(date(year, month, wednesdays[2]))


FUTURES_STANDARD_SETTLEMENT_TYPES = {'股票', '指數', 'ETF'}
FUTURES_STANDARD_SETTLEMENT_ROOTS = {
    # 期交所指數／個股／ETF 期貨均以第三個星期三作月契約結算基準。
    # 未列入此集合的商品不以推算日期刪除，改等待官方實際到期日欄位。
    'TX', 'TXF', 'MTX', 'MXF', 'TMF', 'TE', 'EXF', 'TF', 'FXF',
}
FUTURES_SETTLEMENT_CUTOFF = dt_time(13, 30)


def _normalize_futures_taipei_datetime(value, default_time=FUTURES_SETTLEMENT_CUTOFF):
    """把期交所日期／時間欄位正規化為 Asia/Taipei aware datetime。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    date_only_value = isinstance(value, date) and not isinstance(value, datetime)
    if isinstance(value, str):
        time_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', value)
        date_only_value = not bool(time_match) or (
            time_match is not None
            and int(time_match.group(1)) == 0
            and int(time_match.group(2)) == 0
            and int(time_match.group(3) or 0) == 0
        )
    elif isinstance(value, datetime) and value.time() == dt_time.min:
        # API 將「日期」解碼成 datetime midnight 時，也視為沒有提供時間。
        date_only_value = True
    if isinstance(value, str):
        roc_match = re.search(r'(?<!\d)(\d{2,3})[./-](\d{1,2})[./-](\d{1,2})', value)
        if roc_match and int(roc_match.group(1)) < 1911:
            year, month, day = (int(part) for part in roc_match.groups())
            time_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', value)
            hour, minute, second = (
                (int(time_match.group(1)), int(time_match.group(2)), int(time_match.group(3) or 0))
                if time_match else (0, 0, 0)
            )
            try:
                value = datetime(year + 1911, month, day, hour, minute, second)
            except (TypeError, ValueError, OverflowError):
                return None
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, default_time)
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        timestamp = None
    if timestamp is None or pd.isna(timestamp):
        # 期交所部分舊端點會回傳民國年，例如 115/08/19。
        text = str(value or '').strip()
        match = re.search(r'(?<!\d)(\d{2,3})[./-](\d{1,2})[./-](\d{1,2})', text)
        if not match:
            return None
        year, month, day = (int(part) for part in match.groups())
        if year < 1911:
            year += 1911
        time_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', text)
        hour, minute, second = (
            (int(time_match.group(1)), int(time_match.group(2)), int(time_match.group(3) or 0))
            if time_match else (default_time.hour, default_time.minute, 0)
        )
        try:
            return pytz.timezone('Asia/Taipei').localize(
                datetime(year, month, day, hour, minute, second)
            )
        except (TypeError, ValueError, OverflowError):
            return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('Asia/Taipei')
    else:
        timestamp = timestamp.tz_convert('Asia/Taipei')
    if date_only_value and default_time != dt_time.min:
        timestamp = timestamp.normalize() + pd.Timedelta(
            hours=default_time.hour, minutes=default_time.minute,
        )
    return timestamp.to_pydatetime()


def _futures_actual_last_trading_date(rows):
    """從官方列找實際最後交易／交割日；沒有欄位時回傳 None。"""
    fields = (
        'LastTradingDate', 'LastTradingDay', 'LastTradeDate',
        'DeliveryDate', 'DeliveryDay', '最後交易日', '最後交易日(含)',
        '最後交易日／交割日', '交割日', '契約到期日', '結算日',
    )
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for field in fields:
            value = row.get(field)
            parsed = _normalize_futures_taipei_datetime(value)
            if parsed is not None:
                return parsed
    return None


def is_futures_contract_settled(
    root, contract_month, product_type='未知',
    actual_last_trading=None, now_dt=None, standard_settlement=None,
):
    """判斷契約是否已越過台北時間 13:30 結算切點。

    個股／指數／ETF 月契約才使用第三週三估算；商品若有官方最後交易日，
    一律優先使用官方日期。週選或其他非標準商品沒有實際日期時不誤刪。
    """
    tz_tw = pytz.timezone('Asia/Taipei')
    current = _normalize_futures_taipei_datetime(now_dt or datetime.now(tz_tw), dt_time.min)
    if current is None:
        current = datetime.now(tz_tw)

    actual = _normalize_futures_taipei_datetime(actual_last_trading)
    if actual is not None:
        return current >= actual

    match = re.fullmatch(r'(\d{4})(\d{2})', str(contract_month or '').strip())
    normalized_root = str(root or '').strip().upper()
    normalized_type = str(product_type or '').strip()
    if not match:
        return False
    use_standard = standard_settlement
    if use_standard is None:
        use_standard = (
            normalized_type in FUTURES_STANDARD_SETTLEMENT_TYPES
            or normalized_root in FUTURES_STANDARD_SETTLEMENT_ROOTS
        )
    if not use_standard:
        return False
    expiry = futures_expiry_date(contract_month)
    if expiry is None:
        return False
    cutoff = tz_tw.localize(datetime.combine(expiry, FUTURES_SETTLEMENT_CUTOFF))
    return current >= cutoff


def filter_active_futures_rows(rows, now_dt=None):
    """移除已結算契約，並依每個商品 root 重新計算月份順位。"""
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    if frame.empty or '契約月份' not in frame.columns:
        return frame, []
    now_value = now_dt or datetime.now(pytz.timezone('Asia/Taipei'))
    keep_rows = []
    removed_keys = []
    for index, row in frame.iterrows():
        root = str(row.get('期貨代碼', '') or '').strip().upper()
        month = str(row.get('契約月份', '') or '').strip()
        actual_last = row.get('契約最後交易日')
        settled = is_futures_contract_settled(
            root, month, row.get('商品類型', '未知'),
            actual_last_trading=actual_last, now_dt=now_value,
            standard_settlement=(
                str(row.get('商品類型', '') or '').strip() in FUTURES_STANDARD_SETTLEMENT_TYPES
                or bool(row.get('指數期貨', False))
                or bool(row.get('ETF期貨', False))
                or root in FUTURES_STANDARD_SETTLEMENT_ROOTS
            ),
        )
        if settled:
            key = str(row.get('契約鍵', f'{root}:{month}'))
            removed_keys.append(key)
        else:
            keep_rows.append(index)
    active = frame.loc[keep_rows].copy()
    if active.empty:
        return active.reset_index(drop=True), removed_keys
    active['_month_sort'] = pd.to_numeric(
        active['契約月份'].astype(str).str.extract(r'^(\d{6})')[0], errors='coerce'
    )
    active['_month_sort'] = active['_month_sort'].fillna(999999)
    active['月份順位'] = active.groupby('期貨代碼')['_month_sort'].rank(
        method='dense', ascending=True,
    ).astype(int) - 1
    active['次月期貨'] = active['月份順位'] > 0
    active.drop(columns=['_month_sort'], inplace=True)
    return active.reset_index(drop=True), removed_keys


def prune_futures_settlement_state(state, now_dt=None):
    """清除已結算契約及其 rank/live/manual/ignored 快取，避免下一輪復活。"""
    payload = dict(state) if isinstance(state, dict) else {}
    universe = pd.DataFrame(payload.get('universe', []))
    if not universe.empty:
        active, removed_from_universe = filter_active_futures_rows(universe, now_dt)
        payload['universe'] = active.to_dict(orient='records')
    else:
        removed_from_universe = []
    removed = set(str(key) for key in removed_from_universe)
    known_rows = {
        str(row.get('契約鍵', '')): row
        for row in universe.to_dict(orient='records')
        if str(row.get('契約鍵', '')).strip()
    }
    for cache_name in ('rank_cache', 'live_cache'):
        cache = dict(payload.get(cache_name, {}) or {})
        for key in list(cache):
            if key in removed:
                cache.pop(key, None)
                continue
            row = known_rows.get(str(key))
            if row is None:
                root, _, month = str(key).partition(':')
                if is_futures_contract_settled(root, month, now_dt=now_dt):
                    cache.pop(key, None)
        payload[cache_name] = cache
    for list_name in ('manual', 'ignored'):
        values = list(payload.get(list_name, []) or [])
        payload[list_name] = [str(key) for key in values if str(key) not in removed]
    return payload, sorted(removed)

def calculate_entry_confidence(
    base_score, state, current_price, plan_text, direction,
    data_health='', market_alignment='', base_detail=''
):
    """把條件一致度轉成進場信心；分數是觀察品質，不是歷史勝率。"""
    score = max(0, min(100, int(round(float(base_score or 0)))))
    reasons = [base_detail] if base_detail else []
    state_text = str(state or '')
    data_text = str(data_health or '')
    alignment_text = str(market_alignment or '')
    plan = parse_trade_plan_numbers(plan_text)
    price = _safe_number(current_price)
    direction_text = str(direction or '')
    is_long = direction_text in ('多頭', '偏多')
    maximum_score = 100

    if state_text.startswith(('⛔', '⚪ 資料不足')):
        maximum_score = min(maximum_score, 20)
        reasons.append('條件失效或資料不足')
    elif state_text == '✅ 已觸發':
        score = min(100, score + 5)
        reasons.append('進場條件已成立')
    elif state_text.startswith(('🟡', '⚪')):
        score = max(0, score - 8)
        reasons.append('尚待觸發確認')

    entry, stop, target = plan['entry'], plan['stop'], plan['target']
    if price is not None and None not in (entry, stop, target):
        invalidated = price <= stop if is_long else price >= stop
        target_reached = price >= target if is_long else price <= target
        trigger_distance = abs(entry - stop)
        progress = ((price - entry) if is_long else (entry - price)) / trigger_distance if trigger_distance > 0 else None
        if invalidated:
            maximum_score = min(maximum_score, 10)
            reasons.append('已越過失效點')
        elif target_reached:
            maximum_score = min(maximum_score, 25)
            reasons.append('已到第一目標，不宜追價')
        elif progress is not None and progress > 1:
            maximum_score = min(maximum_score, 45)
            reasons.append('已離進場點超過一個進場－失效距離')
        elif progress is not None and progress > 0.5:
            score = max(0, score - 15)
            reasons.append('已離進場點偏遠')
        elif progress is not None and progress >= 0:
            reasons.append('仍在觸發初段')

    if data_text.startswith(('🔴', '⚪')):
        maximum_score = min(maximum_score, 45)
        reasons.append('即時資料待確認')
    elif data_text.startswith('🟡'):
        maximum_score = min(maximum_score, 70)
        reasons.append('使用手動或較舊報價')
    if alignment_text.startswith('🟢'):
        score = min(100, score + 5)
        reasons.append('與市場方向一致')
    elif alignment_text.startswith('🟡'):
        score = max(0, score - 10)
        reasons.append('與市場方向相反')
    score = min(score, maximum_score)

    if score >= 80:
        label = '🟢 高'
    elif score >= 65:
        label = '🟡 中高'
    elif score >= 50:
        label = '🟠 中'
    else:
        label = '🔴 低'
    unique_reasons = list(dict.fromkeys(reason for reason in reasons if reason))
    return {'score': score, 'label': label, 'detail': '｜'.join(unique_reasons)}

def register_strategy_signals(records):
    """新增未重複的訊號；同商品同交易日、策略與進場價只留一筆。"""
    existing = load_strategy_signal_log()
    existing_keys = {str(record.get('dedupe_key', '')) for record in existing}
    deleted_keys = set(load_strategy_signal_tombstones())
    added = 0
    now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
    now_text = now_tw.strftime('%Y/%m/%d %H:%M:%S')
    for raw_record in records:
        record = dict(raw_record)
        if str(record.get('市場', '')) == '股票':
            entry = _safe_number(record.get('進場價'))
            stop = _safe_number(record.get('停損價'))
            target = _safe_number(record.get('目標價'))
            is_long = str(record.get('方向', '')) in ('多頭', '偏多')
            ordered = (
                stop < entry < target if is_long else target < entry < stop
            ) if None not in (entry, stop, target) else False
            risk_distance = abs(entry - stop) if ordered else 0
            reward_risk = abs(target - entry) / risk_distance if risk_distance > 0 else 0
            if not ordered or reward_risk < 1.3:
                continue
        trade_day = (
            get_futures_trading_date(now_tw).strftime('%Y%m%d')
            if str(record.get('市場', '')) == '期貨'
            else now_tw.strftime('%Y%m%d')
        )
        dedupe_key = '|'.join([
            trade_day, str(record.get('市場', '')), str(record.get('商品鍵', '')),
            str(record.get('策略', '')), str(record.get('方向', '')), str(record.get('進場價', '')),
        ])
        if dedupe_key in existing_keys or dedupe_key in deleted_keys:
            continue
        record.update({
            'dedupe_key': dedupe_key, '建立時間': now_text, '最後更新': now_text,
            '實際進場價': _safe_number(record.get('實際進場價')),
            '結果': '追蹤中', 'MFE(R)': 0.0, 'MAE(R)': 0.0, '結果(R)': None,
            '15分(R)': None, '30分(R)': None, '60分(R)': None, '收盤(R)': None,
        })
        existing.append(record)
        existing_keys.add(dedupe_key)
        added += 1
    return added, save_strategy_signal_log(existing)

def update_strategy_signal_outcomes(price_map):
    """Update tracked signals from timestamped snapshots without fabricating history."""
    records = load_strategy_signal_log()
    if not records:
        return 0
    updated_count = 0
    now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
    now_text = now_tw.strftime('%Y/%m/%d %H:%M:%S')
    for record in records:
        if str(record.get('結果')) != '追蹤中':
            continue
        key = str(record.get('商品鍵', ''))
        current_price = _safe_number(price_map.get(key))
        # 使用者有填實際成交點位時，以實際進場價計算所有後續 R 與結果；否則沿用計畫價。
        entry = _safe_number(record.get('實際進場價'))
        if entry is None:
            entry = _safe_number(record.get('進場價'))
        stop = _safe_number(record.get('停損價'))
        target = _safe_number(record.get('目標價'))
        if current_price is None or None in (entry, stop, target):
            continue
        risk_distance = abs(entry - stop)
        if risk_distance <= 0:
            continue
        is_long = str(record.get('方向')) in ('多頭', '偏多')
        valid_order = stop < entry < target if is_long else target < entry < stop
        if not valid_order:
            record['資料警示'] = '進場／停損／目標價格順序無效'
            continue
        snapshots = record.get('價格快照', [])
        if not isinstance(snapshots, list):
            snapshots = []
        snapshots.append({'時間': now_text, '價格': current_price})
        record['價格快照'] = snapshots[-500:]

        snapshot_values = [
            _safe_number(item.get('價格')) for item in record['價格快照']
            if isinstance(item, dict)
        ]
        snapshot_values = [value for value in snapshot_values if value is not None]
        favorable_values = [
            (value - entry) if is_long else (entry - value)
            for value in snapshot_values
        ]
        adverse_values = [
            (entry - value) if is_long else (value - entry)
            for value in snapshot_values
        ]
        current_r = ((current_price - entry) if is_long else (entry - current_price)) / risk_distance
        record['MFE(R)'] = round(max([0.0] + favorable_values) / risk_distance, 2)
        record['MAE(R)'] = round(max([0.0] + adverse_values) / risk_distance, 2)
        record['最新價'] = current_price
        record['最後更新'] = now_text
        created_at = parse_strategy_data_time(record.get('建立時間'))
        if created_at is not None:
            for minutes, column in ((15, '15分(R)'), (30, '30分(R)'), (60, '60分(R)')):
                target_time = created_at.to_pydatetime() + timedelta(minutes=minutes)
                age_from_target = abs(
                    (now_tw.replace(tzinfo=None) - target_time).total_seconds()
                )
                if age_from_target <= 5 * 60 and _safe_number(record.get(column)) is None:
                    record[column] = round(current_r, 2)
        if (
            str(record.get('市場')) == '股票'
            and created_at is not None
            and created_at.date() == now_tw.date()
            and now_tw.time() >= dt_time(13, 30)
            and _safe_number(record.get('收盤(R)')) is None
        ):
            record['收盤(R)'] = round(current_r, 2)
        stopped = current_price <= stop if is_long else current_price >= stop
        targeted = current_price >= target if is_long else current_price <= target
        if stopped:
            record['結果'], record['結果(R)'] = '停損', -1.0
            record['結案時間'] = now_text
        elif targeted:
            reward_r = abs(target - entry) / risk_distance
            record['結果'], record['結果(R)'] = '達標', round(reward_r, 2)
            record['結案時間'] = now_text
        updated_count += 1
    save_strategy_signal_log(records)
    return updated_count

def notify_signal_state_changes(scope, current_states, enabled):
    """只在手動刷新或條件重算後，提醒新進入觸發狀態的商品。"""
    state_key = f'_strategy_signal_states_{scope}'
    previous_states = st.session_state.get(state_key, {})
    if enabled and previous_states:
        changed = [
            key for key, state in current_states.items()
            if state in ('✅ 已觸發', '🔵 回測確認') and previous_states.get(key) != state
        ]
        if changed:
            shown = '、'.join(changed[:5])
            suffix = f' 等 {len(changed)} 檔' if len(changed) > 5 else ''
            st.toast(f'訊號狀態更新：{shown}{suffix}', icon='🔔')
    st.session_state[state_key] = dict(current_states)

def render_strategy_validation_room():
    """顯示已記錄訊號的追蹤結果；不主動抓取行情。"""
    st.caption("只有按下各戰略室的「記錄目前表格的已觸發訊號」才會新增；後續按即時更新時，沿用該次報價更新結果、MFE 與 MAE。")
    with st.expander("📖 策略驗證怎麼記錄與判讀", expanded=False):
        st.markdown("""
        1. **先記錄**：在股票或期貨戰略室按「記錄目前表格的已觸發訊號」。它會一次保存表內符合門檻的標的，不是只保存目前查看的那一檔；同一交易日、商品、策略、方向與進場價只會保存一筆。
        2. **再更新**：回到原戰略室按即時更新報價／分析時，該次取得的最新價會更新已記錄訊號的追蹤結果；策略驗證頁本身**不會額外抓行情**。
        3. **怎麼看表格**：建立時間、策略、方向、進場／停損／目標是建立訊號當下的計畫；可在「實際進場價」填入真實成交點位，後續 R、快照 MFE／MAE 與結果會優先以它計算，留白則沿用計畫進場價。15／30／60 分與收盤欄只在更新時間貼近該節點時記錄，不會用較晚價格回填。
        4. **R、MFE、MAE**：1R 是進場到失效點的距離，不是金額；例如多方進場 100、停損 95，1R = 5。MFE 是建立訊號後最有利曾走到多少 R，MAE 是最不利曾回撤多少 R，可用來檢查進場是否太晚、停損是否太近，不等於實際損益。
        5. **刪除與匯出**：可在下方明細勾選多筆「刪除」，再按刪除按鈕移除勾選紀錄；匯出按鈕會下載目前篩選後的 CSV。
        """)
    _sync_strategy_signal_scope_from_cloud()
    records = load_strategy_signal_log()
    with st.expander("🧹 策略驗證紀錄管理", expanded=False):
        confirm_clear = st.checkbox(
            "我確認要清除所選紀錄", key='confirm_clear_strategy_signals',
            help='清除後無法從 APP 內復原；若尚未提交檔案，可從 Git 或備份還原。'
        )
        clear_stock_col, clear_futures_col, clear_all_col = st.columns(3)
        with clear_stock_col:
            clear_stock_signals = st.button(
                "清除股票紀錄", width='stretch',
                disabled=not confirm_clear, key='clear_stock_strategy_signals'
            )
        with clear_futures_col:
            clear_futures_signals = st.button(
                "清除期貨紀錄", width='stretch',
                disabled=not confirm_clear, key='clear_futures_strategy_signals'
            )
        with clear_all_col:
            clear_all_signals = st.button(
                "全部清除", width='stretch', type='primary',
                disabled=not confirm_clear, key='clear_all_strategy_signals'
            )
        market_to_clear = '股票' if clear_stock_signals else ('期貨' if clear_futures_signals else None)
        if clear_stock_signals or clear_futures_signals or clear_all_signals:
            remaining = [] if clear_all_signals else [
                record for record in records if str(record.get('市場', '')) != market_to_clear
            ]
            removed = [] if len(remaining) == len(records) else [
                record for record in records if record not in remaining
            ]
            if mark_strategy_signals_deleted(removed) and save_strategy_signal_log(remaining):
                cleared_count = len(records) - len(remaining)
                st.toast(f"已清除 {cleared_count} 筆策略驗證紀錄", icon="🧹")
                st.rerun()
            else:
                st.error("紀錄清除失敗，請確認檔案是否可寫入。")
    if not records:
        st.info("目前尚無策略訊號紀錄。請先在股票或期貨戰略室啟用附加分析層，再記錄符合條件的訊號。")
        return

    data = pd.DataFrame(records)
    data['_紀錄ID'] = data.index
    available_markets = {
        str(value) for value in data.get('市場', pd.Series(dtype=str)).dropna().unique()
    }
    market_options = (
        ['全部']
        + [market for market in ('股票', '期貨') if market in available_markets]
        + sorted(available_markets - {'股票', '期貨'})
    )
    filter_col1, filter_col2, export_col = st.columns([2, 2, 2])
    with filter_col1:
        market_filter = st.selectbox('市場', market_options, key='signal_log_market_filter')

    # 策略選單必須跟著市場分層。過去先列出全部市場的策略，切到「期貨」後仍可能
    # 保留股票策略，產生空表；空表的 pandas dtype 又會與 TextColumn 設定衝突。
    market_scoped_data = data.copy()
    if market_filter != '全部':
        market_scoped_data = market_scoped_data[
            market_scoped_data['市場'].astype(str) == market_filter
        ]
    strategy_options = ['全部'] + sorted(
        str(value)
        for value in market_scoped_data.get('策略', pd.Series(dtype=str)).dropna().unique()
    )
    if st.session_state.get('signal_log_strategy_filter', '全部') not in strategy_options:
        st.session_state['signal_log_strategy_filter'] = '全部'
    with filter_col2:
        strategy_filter = st.selectbox('策略', strategy_options, key='signal_log_strategy_filter')
    filtered = market_scoped_data.copy()
    if strategy_filter != '全部':
        filtered = filtered[filtered['策略'].astype(str) == strategy_filter]

    with export_col:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        export_columns = [
            '建立時間', '市場', '代碼', '名稱', '策略', '方向', '訊號狀態', '評分', '信心判讀',
            '進場價', '實際進場價', '停損價', '目標價', '最新價', '15分(R)', '30分(R)', '60分(R)', '收盤(R)',
            '結果', 'MFE(R)', 'MAE(R)', '結果(R)', '資料狀態'
        ]
        csv_data = filtered.reindex(columns=export_columns).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            '⬇️ 匯出目前紀錄', csv_data,
            file_name=f"strategy-signals-{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv', width='stretch',
        )

    if filtered.empty:
        st.info('此市場目前沒有符合所選策略的紀錄。')
        return

    completed = filtered[filtered.get('結果', pd.Series(index=filtered.index, dtype=str)).isin(['達標', '停損'])]
    wins = int((completed.get('結果', pd.Series(dtype=str)) == '達標').sum()) if not completed.empty else 0
    hit_rate = wins / len(completed) * 100 if len(completed) else 0.0
    result_r = pd.to_numeric(completed.get('結果(R)', pd.Series(dtype=float)), errors='coerce')
    average_r = float(result_r.mean()) if not result_r.empty and result_r.notna().any() else 0.0
    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric('訊號數', len(filtered))
    metric2.metric('追蹤中', int((filtered.get('結果', pd.Series(dtype=str)) == '追蹤中').sum()))
    metric3.metric('已完成勝率', f'{_format_compact_number(hit_rate, 1)}%', help='只計算已有達標或停損結果的訊號。')
    metric4.metric('平均結果', f'{_format_compact_number(average_r, 2, signed=True)} R', help='R 為進場至停損的風險距離。')

    if not completed.empty:
        group_columns = [column for column in ['市場', '策略', '方向', '信心判讀'] if column in completed.columns]
        if group_columns:
            summary = completed.groupby(group_columns, dropna=False).agg(
                已完成=('結果', 'size'),
                達標數=('結果', lambda series: int((series == '達標').sum())),
                平均R=('結果(R)', 'mean'),
            ).reset_index()
            summary['勝率'] = summary['達標數'] / summary['已完成'] * 100
            summary['平均R'] = pd.to_numeric(summary['平均R'], errors='coerce').round(2)
            summary['勝率'] = summary['勝率'].round(1)
            st.markdown('#### 條件分組結果')

            def style_validation_summary(row):
                styles = [''] * len(row)
                for position, column in enumerate(row.index):
                    value = row.get(column)
                    if column == '方向':
                        styles[position] = (
                            'color:#ff4b4b;font-weight:bold;' if '多' in str(value)
                            else ('color:#00c853;font-weight:bold;' if '空' in str(value) else '')
                        )
                    elif column in ('平均R', '勝率'):
                        number = _safe_number(value)
                        if number is not None:
                            positive = number > (50 if column == '勝率' else 0)
                            negative = number < (50 if column == '勝率' else 0)
                            styles[position] = (
                                'color:#ff4b4b;font-weight:bold;' if positive
                                else ('color:#00c853;font-weight:bold;' if negative else 'color:#9e9e9e;')
                            )
                return styles

            st.dataframe(
                summary.style.apply(style_validation_summary, axis=1),
                column_config={
                    '已完成': st.column_config.NumberColumn(format='%d'),
                    '達標數': st.column_config.NumberColumn(format='%d'),
                    '平均R': st.column_config.NumberColumn(format='%.12g'),
                    '勝率': st.column_config.NumberColumn(format='%.12g'),
                },
                hide_index=True, width='stretch',
            )

    display_columns = [
        '建立時間', '市場', '代碼', '名稱', '策略', '方向', '訊號狀態', '評分', '信心判讀',
        '進場價', '實際進場價', '停損價', '目標價', '最新價', '15分(R)', '30分(R)', '60分(R)', '收盤(R)',
        '結果', 'MFE(R)', 'MAE(R)', '結果(R)', '資料狀態'
    ]
    for column in display_columns:
        if column not in filtered.columns:
            filtered[column] = None
    st.markdown('#### 訊號明細')
    validation_display = filtered[['_紀錄ID'] + display_columns].sort_values('建立時間', ascending=False).copy()
    validation_display.insert(0, '刪除', False)
    compact_number_columns = [
        '進場價', '停損價', '目標價', '最新價', '15分(R)', '30分(R)', '60分(R)', '收盤(R)',
        'MFE(R)', 'MAE(R)', '結果(R)'
    ]
    for column in compact_number_columns:
        validation_display[column] = validation_display[column].map(_compact_number_text)
    validation_display['評分'] = pd.to_numeric(validation_display['評分'], errors='coerce')
    # 使用文字欄承載可編輯數字，才能讓未填資料真正顯示空白（NumberColumn 會顯示 None）。
    validation_display['實際進場價'] = validation_display['實際進場價'].map(
        lambda value: '' if _safe_number(value) is None else _compact_number_text(value)
    )
    for column in (set(display_columns) - set(compact_number_columns) - {'評分', '實際進場價'}):
        validation_display[column] = validation_display[column].map(_blank_display_text)

    def style_validation_row(row):
        styles = [''] * len(row)
        direction = str(row.get('方向', ''))
        signal_state = str(row.get('訊號狀態', ''))
        confidence = str(row.get('信心判讀', ''))
        result_text = str(row.get('結果', ''))
        data_health = str(row.get('資料狀態', ''))
        for position, column in enumerate(row.index):
            if column == '方向':
                styles[position] = (
                    'color:#ff4b4b;font-weight:bold;' if '多' in direction
                    else ('color:#00c853;font-weight:bold;' if '空' in direction else '')
                )
            elif column == '名稱':
                styles[position] = 'font-weight:bold;'
            elif column == '市場':
                styles[position] = (
                    'color:#64b5f6;font-weight:bold;' if '股票' in str(row.get(column, ''))
                    else ('color:#ffb74d;font-weight:bold;' if '期貨' in str(row.get(column, '')) else '')
                )
            elif column == '訊號狀態':
                if signal_state.startswith('✅'):
                    styles[position] = 'color:#ffb300;font-weight:bold;'
                elif signal_state.startswith('🔵'):
                    styles[position] = 'color:#64b5f6;font-weight:bold;'
                elif signal_state.startswith(('⛔', '🔴')):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
                elif signal_state.startswith('🟡'):
                    styles[position] = 'color:#ffb300;font-weight:bold;'
            elif column == '信心判讀':
                if confidence.startswith('🟢'):
                    styles[position] = 'color:#00c853;font-weight:bold;'
                elif confidence.startswith(('🟡', '🟠')):
                    styles[position] = 'color:#ffb300;font-weight:bold;'
                elif confidence.startswith('🔴'):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
            elif column == '結果':
                if result_text == '達標':
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
                elif result_text == '停損':
                    styles[position] = 'color:#00c853;font-weight:bold;'
                elif result_text == '追蹤中':
                    styles[position] = 'color:#ffb300;font-weight:bold;'
            elif column == '評分':
                styles[position] = f'color:{_score_color(row.get(column))};font-weight:bold;'
            elif column in ('15分(R)', '30分(R)', '60分(R)', '收盤(R)', 'MFE(R)', 'MAE(R)', '結果(R)'):
                value = _safe_number(row.get(column))
                if value is not None:
                    styles[position] = (
                        'color:#ff4b4b;font-weight:bold;' if value > 0
                        else ('color:#00c853;font-weight:bold;' if value < 0 else '')
                    )
            elif column == '資料狀態':
                if data_health.startswith('🟢'):
                    styles[position] = 'color:#00e676;font-weight:bold;'
                elif data_health.startswith(('🔴', '⛔')):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
                elif data_health.startswith('🟡'):
                    styles[position] = 'color:#ffb300;'
                elif data_health.startswith('🔵'):
                    styles[position] = 'color:#64b5f6;'
                elif data_health.startswith('⚪'):
                    styles[position] = 'color:#9e9e9e;'
        return styles

    table_key = f"strategy_validation_editor_{abs(hash(tuple(validation_display['_紀錄ID'].tolist())))}"
    editor_frame = validation_display.drop(columns=['_紀錄ID']).copy()
    editor_frame['刪除'] = editor_frame['刪除'].fillna(False).astype(bool)
    editor_frame['評分'] = pd.to_numeric(editor_frame['評分'], errors='coerce').astype(float)
    for column in display_columns:
        if column != '評分':
            editor_frame[column] = editor_frame[column].fillna('').astype(str)

    # 完整資料先以唯讀彩色表呈現；下方編輯器保留實際進場價與多選刪除功能。
    color_frame = editor_frame.drop(columns=['刪除']).copy()
    color_frame['評分'] = color_frame['評分'].map(_compact_number_text)
    st.dataframe(
        color_frame.style.apply(style_validation_row, axis=1),
        hide_index=True, width='stretch', row_height=30,
    )
    st.caption('顏色提示：多方／上漲／達標為紅色，空方／下跌／停損為綠色；黃色代表追蹤中或等待確認。')
    editor_frame = editor_frame[['刪除', '市場', '代碼', '名稱', '實際進場價']].copy()
    st.markdown('##### ✏️ 編輯實際進場價／勾選刪除')
    edited_validation = st.data_editor(
        # 可編輯表格不傳 Styler，避免 Streamlit 1.5x／Python 3.14 對 Styler
        # 推斷出的 schema 與 TextColumn 設定不一致而直接拋出 APIException。
        editor_frame,
        column_config={
            '刪除': st.column_config.CheckboxColumn('刪除', width=50, help='勾選後可一次刪除多筆訊號紀錄。'),
            '評分': st.column_config.NumberColumn('進場信心', format='%d'),
            '進場價': st.column_config.TextColumn(),
            '實際進場價': st.column_config.TextColumn(
                '實際進場價 ✏️',
                help='填入實際成交點位；留白時仍以計畫進場價計算。變更會自動儲存。'
            ),
            '停損價': st.column_config.TextColumn(),
            '目標價': st.column_config.TextColumn(),
            '最新價': st.column_config.TextColumn(),
            '15分(R)': st.column_config.TextColumn(help='建立訊號滿 15 分鐘時的表現；1R 為進場到失效點距離。'),
            '30分(R)': st.column_config.TextColumn(help='建立訊號滿 30 分鐘時的表現。'),
            '60分(R)': st.column_config.TextColumn(help='建立訊號滿 60 分鐘時的表現。'),
            '收盤(R)': st.column_config.TextColumn(help='股票訊號在收盤後的表現快照。'),
            'MFE(R)': st.column_config.TextColumn('快照 MFE', help='已記錄更新快照中的最大有利變動；不是兩次更新之間完整行情路徑，單位為 R。'),
            'MAE(R)': st.column_config.TextColumn('快照 MAE', help='已記錄更新快照中的最大不利變動；不是兩次更新之間完整行情路徑，單位為 R。'),
            '結果(R)': st.column_config.TextColumn(help='達標或策略失效時的結果；以 R 表示。'),
        },
        disabled=['市場', '代碼', '名稱'],
        hide_index=True, width='stretch', key=table_key,
    )
    actual_entry_changed = False
    for position, edited_value in enumerate(edited_validation['實際進場價'].tolist()):
        record_id = int(validation_display.iloc[position]['_紀錄ID'])
        new_value = _safe_number(edited_value)
        raw_value = str(edited_value or '').strip()
        if raw_value and (new_value is None or new_value <= 0):
            st.error(f"實際進場價「{raw_value}」不是有效正數，該筆未儲存。")
            continue
        old_value = _safe_number(records[record_id].get('實際進場價'))
        if new_value != old_value:
            record = records[record_id]
            effective_entry = new_value if new_value is not None else _safe_number(record.get('進場價'))
            latest = _safe_number(record.get('最新價'))
            stop = _safe_number(record.get('停損價'))
            target = _safe_number(record.get('目標價'))
            is_long = str(record.get('方向')) in ('多頭', '偏多')
            valid_order = (
                None not in (effective_entry, stop, target)
                and (stop < effective_entry < target if is_long else target < effective_entry < stop)
            )
            if not valid_order:
                st.error(
                    f"{record.get('代碼', '')} 實際進場價不在停損與目標之間，該筆未儲存。"
                )
                continue
            record['實際進場價'] = new_value
            record['最後更新'] = datetime.now(
                pytz.timezone('Asia/Taipei')
            ).strftime('%Y/%m/%d %H:%M:%S')
            terminal_result = str(record.get('結果')) in ('停損', '達標')
            for derived_column in ('15分(R)', '30分(R)', '60分(R)', '收盤(R)'):
                record[derived_column] = None
            record['價格快照'] = []
            record['MFE(R)'] = 0.0
            record['MAE(R)'] = 0.0
            if None not in (effective_entry, latest, stop, target):
                risk_distance = abs(effective_entry - stop)
                favorable = (latest - effective_entry) if is_long else (effective_entry - latest)
                adverse = (effective_entry - latest) if is_long else (latest - effective_entry)
                record['MFE(R)'] = round(max(0, favorable / risk_distance), 2)
                record['MAE(R)'] = round(max(0, adverse / risk_distance), 2)
                if terminal_result:
                    if str(record.get('結果')) == '停損':
                        record['結果(R)'] = -1.0
                    else:
                        record['結果(R)'] = round(abs(target - effective_entry) / risk_distance, 2)
                else:
                    stopped = latest <= stop if is_long else latest >= stop
                    targeted = latest >= target if is_long else latest <= target
                    if stopped:
                        record['結果'], record['結果(R)'] = '停損', -1.0
                    elif targeted:
                        record['結果'] = '達標'
                        record['結果(R)'] = round(abs(target - effective_entry) / risk_distance, 2)
                    else:
                        record['結果'], record['結果(R)'] = '追蹤中', None
            actual_entry_changed = True
    if actual_entry_changed:
        if save_strategy_signal_log(records):
            st.toast('實際進場價已儲存；後續行情更新將以此計算盈虧與結果。', icon='✅')
        else:
            st.error('實際進場價儲存失敗，請確認檔案是否可寫入。')
    selected_mask = edited_validation['刪除'].fillna(False).astype(bool).to_numpy()
    selected_ids = set(validation_display.iloc[selected_mask]['_紀錄ID'].astype(int).tolist())
    delete_col, hint_col = st.columns([2, 4])
    with delete_col:
        delete_selected = st.button(
            f'🗑️ 刪除勾選訊號（{len(selected_ids)}）', type='primary', width='stretch',
            disabled=not selected_ids, key=f'{table_key}_delete_selected'
        )
    with hint_col:
        st.caption('刪除只影響勾選紀錄；其餘歷史訊號與目前篩選條件不受影響。')
    if delete_selected:
        removed = [record for index, record in enumerate(records) if index in selected_ids]
        remaining = [record for index, record in enumerate(records) if index not in selected_ids]
        if mark_strategy_signals_deleted(removed) and save_strategy_signal_log(remaining):
            st.toast(f'已刪除 {len(selected_ids)} 筆訊號紀錄', icon='🗑️')
            st.rerun()
        else:
            st.error('訊號刪除失敗，請確認紀錄檔是否可寫入。')

def load_fibo_tag_cache():
    """快速標籤使用獨立檔案，避免其他設定寫入或股票快取清除時被覆蓋。"""
    if not os.path.exists(FIBO_TAG_CACHE_FILE):
        return []
    try:
        with open(FIBO_TAG_CACHE_FILE, 'r', encoding='utf-8') as file:
            payload = json.load(file)
        tags = payload.get('tags', []) if isinstance(payload, dict) else payload
        return [str(tag) for tag in tags[:5]] if isinstance(tags, list) and len(tags) >= 5 else []
    except (OSError, ValueError, TypeError):
        return []


def _write_json_atomic(path, payload, *, indent=None):
    """完整寫入後再替換原檔，避免兩個 Streamlit 工作階段留下半份 JSON。"""
    temp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with _RUNTIME_FILE_LOCK:
        try:
            with open(temp_path, 'w', encoding='utf-8') as file:
                json.dump(payload, file, ensure_ascii=False, indent=indent)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass


@st.cache_resource(show_spinner=False)
def get_data_cache_sync_lock():
    """同一個 Streamlit 服務內，序列化不同裝置的遠端快取讀寫。"""
    return threading.RLock()


def _is_valid_data_cache_payload(value):
    if not isinstance(value, dict):
        return False
    expected_types = {
        'stock_data': list,
        'ignored_stocks': list,
        'all_candidates': list,
        'saved_notes': dict,
        'fibo_tags': list,
        'cached_notes': dict,
        'strategy_signal_log': list,
        'strategy_signal_deleted_keys': list,
        'stock_data_updated_at': str,
        'company_event_snapshot': dict,
        'futures_strategy_state': dict,
    }
    present = [key for key in expected_types if key in value]
    return bool(present) and all(
        isinstance(value[key], expected_types[key]) for key in present
    )


def _decode_data_cache_payload(payload):
    """相容 Apps Script 直接回傳 JSON、JSON 字串或包在 data 欄位中的格式。"""
    def decode(value, depth=0):
        if depth > 5:
            return {}
        if isinstance(value, bytes):
            return decode(value.decode('utf-8-sig', errors='replace'), depth + 1)
        if isinstance(value, str):
            text = value.strip().lstrip('\ufeff')
            if not text:
                return {}
            try:
                return decode(json.loads(text), depth + 1)
            except (ValueError, TypeError):
                return {}
        if isinstance(value, dict):
            candidate = {key: item for key, item in value.items() if key != 'data'}
            nested = value.get('data')
            if isinstance(nested, (dict, str, bytes)):
                decoded_nested = decode(nested, depth + 1)
                if decoded_nested:
                    candidate.update(decoded_nested)
            if 'fibo_tags' not in candidate and isinstance(candidate.get('tags'), list):
                candidate['fibo_tags'] = candidate['tags']
            return candidate if _is_valid_data_cache_payload(candidate) else {}
        return {}
    return decode(payload)


def _fetch_remote_data_cache(gsheet_api_url, timeout=5):
    """讀取 Google Sheet 快取；只回傳解析結果，不把網址或內容寫入畫面。"""
    if not gsheet_api_url:
        return None, '未設定 Google Sheet 同步'
    try:
        response = requests.get(gsheet_api_url, timeout=timeout)
        response.raise_for_status()
        payload = _decode_data_cache_payload(response.text)
        if not isinstance(payload, dict) or not payload:
            return None, 'Google Sheet 回傳格式無法辨識'
        return payload, None
    except (requests.RequestException, ValueError, TypeError) as exc:
        return None, f'{type(exc).__name__}'
def _fetch_remote_scope(
    gsheet_api_url,
    scope,
    timeout=8,
):
    """只讀取 Google Sheet 指定 scope。"""
    if not gsheet_api_url:
        return None, '未設定 Google Sheet 同步'

    scope = str(scope or '').strip()

    if not scope:
        return None, '未指定 Google Sheet scope'

    try:
        response = requests.get(
            gsheet_api_url,
            params={
                'scope': scope,
                '_ts': int(time.time() * 1000),
            },
            headers={
                'Cache-Control': 'no-cache, no-store, max-age=0',
                'Pragma': 'no-cache',
            },
            timeout=timeout,
        )

        response.raise_for_status()

        payload = response.json()

        if not isinstance(
            payload,
            dict
        ):
            return None, 'Google Sheet scope 回傳格式無法辨識'

        if payload.get(
            'success'
        ) is False:
            return None, str(
                payload.get(
                    'error'
                ) or 'Google Sheet 讀取失敗'
            )

        data = payload.get(
            'data'
        )

        if data is None:
            return {}, None

        if isinstance(
            data,
            str
        ):
            try:
                data = json.loads(
                    data
                )
            except (
                ValueError,
                TypeError,
            ):
                return None, 'Google Sheet scope JSON 無法解析'

        if not isinstance(
            data,
            dict
        ):
            return None, 'Google Sheet scope 資料不是物件'

        if payload.get(
            'updated_at'
        ):
            data[
                '_scope_updated_at'
            ] = str(
                payload.get(
                    'updated_at'
                )
            )

        return data, None

    except requests.RequestException as exc:
        return None, type(exc).__name__

    except (
        ValueError,
        TypeError,
    ):
        return None, 'Google Sheet scope 回傳格式錯誤'


def _save_remote_scope(gsheet_api_url, scope, payload, updated_at=None, timeout=8, verify=True):
    if not gsheet_api_url or not scope:
        return True, payload

    updated_at = updated_at or datetime.now(
        pytz.timezone('Asia/Taipei')
    ).isoformat()

    request_payload = {
        'action': 'save',
        'scope': scope,
        'data': json.dumps(_json_safe(payload), ensure_ascii=False),
        'updated_at': updated_at,
    }

    try:
        response = requests.post(
            gsheet_api_url,
            json=request_payload,
            timeout=timeout,
        )
        response.raise_for_status()
        result = response.json()

        if not isinstance(result, dict) or result.get('success') is not True:
            return False, None

        if verify:
            for attempt in range(2):
                saved_payload, read_error = _fetch_remote_scope(
                    gsheet_api_url,
                    scope,
                    timeout=timeout,
                )
                if _remote_scope_payload_matches(scope, payload, saved_payload):
                    return True, saved_payload
                if attempt == 0:
                    time.sleep(0.25)
            logger.warning(
                'Google Sheet scope verification failed: %s (%s)',
                scope,
                read_error or 'payload mismatch',
            )
            return False, saved_payload

        return True, payload

    except (requests.RequestException, ValueError, TypeError):
        return False, None


def _remote_scope_payload_matches(scope, expected, actual):
    """Verify that Apps Script persisted the requested independent scope."""
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return False
    if str(scope or '').strip() == GOOGLE_SCOPE_FIBO:
        expected_tags = [
            normalize_fibo_quick_tag(tag)
            for tag in _extract_fibo_tags(expected)
        ]
        actual_tags = [
            normalize_fibo_quick_tag(tag)
            for tag in _extract_fibo_tags(actual)
        ]
        return len(expected_tags) >= 5 and actual_tags == expected_tags
    expected_value = _json_safe(expected)
    actual_value = _json_safe(actual)
    if isinstance(actual_value, dict):
        actual_value.pop('_scope_updated_at', None)
    return actual_value == expected_value


def _read_json_cache_file(
    path
):
    """讀取指定本機 JSON 快取。"""
    if not os.path.exists(
        path
    ):
        return {}

    try:
        with _RUNTIME_FILE_LOCK:
            with open(
                path,
                "r",
                encoding="utf-8",
            ) as file:
                payload = json.load(
                    file
                )

        return (
            payload
            if isinstance(
                payload,
                dict
            )
            else {}
        )

    except (
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
    ):
        return {}

def _valid_fibo_tags(tags):
    if not isinstance(tags, list) or len(tags) < 5:
        return []
    return [str(tag) for tag in tags[:5]]


def _extract_fibo_tags(payload):
    """Read the newest valid tag set from current and legacy response envelopes."""
    queue = [payload]
    visited = set()
    candidates = []

    def add_candidate(tags, timestamp_value=None):
        valid_tags = _valid_fibo_tags(tags)
        if not valid_tags:
            return
        try:
            timestamp = pd.Timestamp(timestamp_value)
            if pd.isna(timestamp):
                timestamp = None
        except (TypeError, ValueError, OverflowError):
            timestamp = None
        candidates.append((valid_tags, timestamp, len(candidates)))

    for _ in range(12):
        if not queue:
            break
        value = queue.pop(0)
        marker = id(value)
        if marker in visited:
            continue
        visited.add(marker)
        if isinstance(value, bytes):
            value = value.decode('utf-8-sig', errors='replace')
        if isinstance(value, str):
            try:
                queue.append(json.loads(value.strip().lstrip('\ufeff')))
            except (ValueError, TypeError):
                pass
            continue
        if isinstance(value, dict):
            add_candidate(
                value.get('fibo_tags'),
                value.get('fibo_tags_updated_at') or value.get('updated_at'),
            )
            add_candidate(
                value.get('tags'),
                value.get('updated_at') or value.get('fibo_tags_updated_at'),
            )
            backup = value.get('fibo_tags_backup')
            if isinstance(backup, dict):
                add_candidate(backup.get('tags'), backup.get('updated_at'))
            queue.extend(
                value.get(key)
                for key in ('fibo_tags_backup', 'data', 'payload', 'result')
                if key in value
            )
        elif isinstance(value, list):
            tags = _valid_fibo_tags(value)
            if tags:
                add_candidate(tags)
            else:
                queue.extend(value[:10])
    if not candidates:
        return []
    timestamped = [candidate for candidate in candidates if candidate[1] is not None]
    if timestamped:
        return max(timestamped, key=lambda candidate: (candidate[1], -candidate[2]))[0]
    return candidates[0][0]


def _fibo_tag_timestamp(payload):
    if not isinstance(payload, dict):
        return None
    timestamp_values = [payload.get('fibo_tags_updated_at')]
    backup = payload.get('fibo_tags_backup')
    if isinstance(backup, dict):
        timestamp_values.append(backup.get('updated_at'))
    timestamps = []
    for timestamp_value in timestamp_values:
        try:
            timestamp = pd.Timestamp(timestamp_value)
            if not pd.isna(timestamp):
                timestamps.append(timestamp)
        except (TypeError, ValueError, OverflowError):
            continue
    return max(timestamps) if timestamps else None


def _newer_fibo_tag_payload(remote, local):
    """標籤獨立比對版本，避免較新的股票快照帶回較舊的標籤。"""
    remote_tags = _extract_fibo_tags(remote)
    local_tags = _extract_fibo_tags(local)
    if not local_tags:
        return remote if remote_tags else {}
    if not remote_tags:
        # 沒有明確儲存時間的本機值可能只是部署預設標籤，不能拿來清洗雲端。
        return local if _fibo_tag_timestamp(local) is not None else {}
    remote_time = _fibo_tag_timestamp(remote)
    local_time = _fibo_tag_timestamp(local)
    if local_time is not None and (remote_time is None or local_time > remote_time):
        return local
    return remote


def save_fibo_config(save_tags=True):
    config = load_config()

    if not save_tags:
        if 'ma_w' in st.session_state:
            config['ma_width'] = st.session_state.ma_w
        try:
            _write_json_atomic(
                CONFIG_FILE,
                config,
            )
        except (
            OSError,
            TypeError,
            ValueError,
        ):
            pass
        return True

    fibo_tags = [
        normalize_fibo_quick_tag(
            tag
        )
        for tag in [
            st.session_state.get(
                'custom_tag_1',
                "台積電(2330)"
            ),
            st.session_state.get(
                'custom_tag_2',
                "鴻海(2317)"
            ),
            st.session_state.get(
                'custom_tag_3',
                "聯發科(2454)"
            ),
            st.session_state.get(
                'custom_tag_4',
                "和椿(6215)"
            ),
            st.session_state.get(
                'custom_tag_5',
                "晶彩科(3535)"
            ),
        ]
    ]

    fibo_tags = [
        tag
        for tag in fibo_tags
        if tag
    ][:5]

    while len(fibo_tags) < 5:
        fibo_tags.append(
            DEFAULT_FIBO_TAGS[
                len(fibo_tags)
            ]
        )

    updated_at = (
        datetime.now(
            pytz.timezone(
                'Asia/Taipei'
            )
        ).isoformat()
    )

    config[
        'fibo_tags'
    ] = fibo_tags

    config[
        'fibo_tags_updated_at'
    ] = updated_at

    if 'ma_w' in st.session_state:
        config[
            'ma_width'
        ] = st.session_state.ma_w

    st.session_state[
        'fibo_tags'
    ] = list(fibo_tags)

    st.session_state[
        '_fibo_tags_updated_at'
    ] = updated_at

    try:
        _write_json_atomic(
            CONFIG_FILE,
            config,
        )

        _write_json_atomic(
            FIBO_TAG_CACHE_FILE,
            {
                'version': 3,
                'updated_at': updated_at,
                'tags': fibo_tags,
            },
            indent=2,
        )
    except (
        OSError,
        TypeError,
        ValueError,
    ):
        pass

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    sync_saved = True

    if gsheet_api_url:
        with get_data_cache_sync_lock():
            sync_saved, _ = (
                _save_remote_scope(
                    gsheet_api_url,
                    GOOGLE_SCOPE_FIBO,
                    {
                        'version': 3,
                        'fibo_tags': fibo_tags,
                        'fibo_tags_updated_at': updated_at,
                        'fibo_tags_backup': {
                            'tags': fibo_tags,
                            'updated_at': updated_at,
                        },
                    },
                    updated_at=updated_at,
                    timeout=8,
                    verify=True,
                )
            )

    st.session_state[
        '_fibo_cloud_save_status'
    ] = (
        'ok'
        if sync_saved
        else 'local_only'
    )

    st.session_state[
        '_fibo_tags_source'
    ] = (
        'google_sheet'
        if sync_saved and gsheet_api_url
        else 'local_tag_cache'
    )

    st.session_state[
        '_fibo_cloud_loaded'
    ] = True

    return sync_saved


def _merge_unique_records(remote_records, local_records, key_name):
    merged = {}
    unkeyed = []
    for record in list(remote_records or []) + list(local_records or []):
        if not isinstance(record, dict):
            continue
        key = str(record.get(key_name, '')).strip()
        if key:
            merged[key] = record
        else:
            unkeyed.append(record)
    return list(merged.values()) + unkeyed


def _merge_unique_values(remote_values, local_values):
    merged = []
    seen = set()
    for value in list(remote_values or []) + list(local_values or []):
        marker = json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True)
        if marker not in seen:
            merged.append(value)
            seen.add(marker)
    return merged


def _newer_company_event_snapshot(remote_snapshot, local_snapshot):
    """Keep the newest non-empty company snapshot when computer and phone both save."""
    remote_snapshot = normalize_company_event_snapshot(remote_snapshot)
    local_snapshot = normalize_company_event_snapshot(local_snapshot)
    remote_has_events = bool(remote_snapshot.get('events'))
    local_has_events = bool(local_snapshot.get('events'))
    if not local_has_events:
        return remote_snapshot
    if not remote_has_events:
        return local_snapshot
    try:
        remote_time = pd.Timestamp(remote_snapshot.get('updated_at'))
        local_time = pd.Timestamp(local_snapshot.get('updated_at'))
        if pd.notna(remote_time) and pd.notna(local_time):
            return local_snapshot if local_time >= remote_time else remote_snapshot
    except (TypeError, ValueError):
        pass
    return local_snapshot


def _cache_payload_timestamp(payload):
    """Return the stock snapshot timestamp when available, otherwise cache time."""
    if not isinstance(payload, dict):
        return None
    value = payload.get('stock_data_updated_at') or payload.get('updated_at')
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            return None
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert('Asia/Taipei').tz_localize(None)
        return timestamp
    except (TypeError, ValueError, OverflowError):
        return None


def _prefer_newer_cache_payload(remote_payload, local_payload):
    """Do not let a delayed Google Sheet response roll a newer local snapshot back."""
    remote_payload = remote_payload if isinstance(remote_payload, dict) else {}
    local_payload = local_payload if isinstance(local_payload, dict) else {}
    remote_time = _cache_payload_timestamp(remote_payload)
    local_time = _cache_payload_timestamp(local_payload)
    if local_payload and (
        not remote_payload or local_time is not None and (
            remote_time is None or local_time > remote_time
        )
    ):
        return local_payload, 'local_newer'
    return remote_payload, 'google_sheet'


def _stock_rows_signature(payload):
    """Canonical comparison for the persisted stock snapshot, including strategy fields."""
    if not isinstance(payload, dict) or not isinstance(payload.get('stock_data'), list):
        return None
    return json.dumps(
        _json_safe(payload['stock_data']), ensure_ascii=False,
        sort_keys=True, separators=(',', ':'),
    )


def _stock_snapshot_matches(saved_payload, expected_payload):
    return _stock_rows_signature(saved_payload) == _stock_rows_signature(expected_payload)


def mark_stock_data_updated():
    """Mark only real stock-data refreshes as a newer snapshot than the cloud copy."""
    timestamp = datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
    st.session_state['_stock_data_updated_at'] = timestamp
    return timestamp


def _merge_data_cache_payload(
    remote, local, *, explicit_fibo_tag_save=False,
    clear_stock_data=False, replace_ignored=False, replace_stock_data=False,
):
    remote = remote if isinstance(remote, dict) else {}
    local = local if isinstance(local, dict) else {}
    remote_stock_time = _cache_payload_timestamp(remote)
    local_stock_time = _cache_payload_timestamp(local)
    use_local_stock_snapshot = (
        not remote
        or local_stock_time is not None and (
            remote_stock_time is None or local_stock_time >= remote_stock_time
        )
    )
    if explicit_fibo_tag_save and remote:
        merged = dict(remote)
        use_local_stock_snapshot = False
        saved_tags = list(local.get('fibo_tags', []))
        tags_updated_at = str(local.get('fibo_tags_updated_at', '')).strip()
        merged['fibo_tags'] = saved_tags
        merged['fibo_tags_updated_at'] = tags_updated_at
        merged['fibo_tags_backup'] = {
            'tags': saved_tags,
            'updated_at': tags_updated_at,
        }
    elif clear_stock_data:
        merged = dict(remote)
        use_local_stock_snapshot = True
        for key, empty_value in (
            ('stock_data', []), ('ignored_stocks', []), ('all_candidates', []),
            ('saved_notes', {}), ('cached_notes', {}),
        ):
            merged[key] = empty_value
        merged['fibo_tags'] = (
            _valid_fibo_tags(remote.get('fibo_tags', []))
        )
        if 'fibo_tags_updated_at' in remote:
            merged['fibo_tags_updated_at'] = remote.get('fibo_tags_updated_at')
        if 'fibo_tags_backup' in remote:
            merged['fibo_tags_backup'] = remote.get('fibo_tags_backup')
        merged['futures_strategy_state'] = _merge_futures_strategy_state(
            remote.get('futures_strategy_state'), local.get('futures_strategy_state'),
        )
        merged['strategy_signal_log'] = remote.get(
            'strategy_signal_log', local.get('strategy_signal_log', []),
        )
        merged['company_event_snapshot'] = _newer_company_event_snapshot(
            remote.get('company_event_snapshot'), local.get('company_event_snapshot'),
        )
    else:
        remote_ignored = set(remote.get('ignored_stocks', []))
        local_ignored = set(local.get('ignored_stocks', []))
        ignored = local_ignored if replace_ignored else remote_ignored | local_ignored
        if replace_stock_data:
            # The visible stock table is a current snapshot. Prefer the newer
            # snapshot, so an old browser session cannot overwrite new analysis.
            stock_rows = list((local if use_local_stock_snapshot else remote).get('stock_data', []))
        else:
            stock_rows = _merge_unique_records(
                remote.get('stock_data', []), local.get('stock_data', []), '代號',
            )
        stock_rows = [
            row for row in stock_rows
            if str(row.get('代號', '')).strip() not in ignored
        ]
        saved_notes = dict(remote.get('saved_notes', {}))
        saved_notes.update(local.get('saved_notes', {}))
        cached_notes = dict(remote.get('cached_notes', {}))
        cached_notes.update(local.get('cached_notes', {}))
        fibo_source = _newer_fibo_tag_payload(remote, local)
        fibo_tags = _extract_fibo_tags(fibo_source)
        merged = {
            'stock_data': stock_rows,
            'ignored_stocks': sorted(ignored),
            'all_candidates': _merge_unique_values(
                remote.get('all_candidates', []), local.get('all_candidates', []),
            ),
            'saved_notes': saved_notes,
            'cached_notes': cached_notes,
            # Only an explicit "save quick tags" action may change cloud tags.
            # A transient read failure must never promote local defaults and wipe
            # the last good phone/computer configuration.
            'fibo_tags': fibo_tags,
            'strategy_signal_log': _merge_unique_records(
                remote.get('strategy_signal_log', []),
                local.get('strategy_signal_log', []),
                'dedupe_key',
            )[-2000:],
            'company_event_snapshot': _newer_company_event_snapshot(
                remote.get('company_event_snapshot'), local.get('company_event_snapshot'),
            ),
            'futures_strategy_state': _merge_futures_strategy_state(
                remote.get('futures_strategy_state'), local.get('futures_strategy_state'),
            ),
        }
        if fibo_tags:
            tag_time = str((fibo_source or {}).get('fibo_tags_updated_at', '')).strip()
            tag_backup = (fibo_source or {}).get('fibo_tags_backup')
            if not tag_time and isinstance(tag_backup, dict):
                tag_time = str(tag_backup.get('updated_at', '')).strip()
            merged['fibo_tags_updated_at'] = tag_time
            merged['fibo_tags_backup'] = {
                'tags': fibo_tags,
                'updated_at': tag_time,
            }
    signal_records, deleted_signal_keys = merge_strategy_signal_state(
        remote.get('strategy_signal_log', []),
        local.get('strategy_signal_log', []),
        remote.get('strategy_signal_deleted_keys', []),
        local.get('strategy_signal_deleted_keys', []),
    )
    merged['strategy_signal_log'] = signal_records
    merged['strategy_signal_deleted_keys'] = deleted_signal_keys
    newest_stock_payload = local if use_local_stock_snapshot else remote
    merged['stock_data_updated_at'] = str(
        newest_stock_payload.get('stock_data_updated_at')
        or newest_stock_payload.get('updated_at')
        or datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
    )
    merged['version'] = 3
    merged['updated_at'] = datetime.now(pytz.timezone('Asia/Taipei')).isoformat()
    return _json_safe(merged)


def _compact_cloud_data_cache_payload(payload, max_chars=45000):
    """控制單一 Apps Script JSON 大小，不刪除股票或公司事件核心資料。"""
    compact = dict(payload) if isinstance(payload, dict) else {}
    compact['futures_strategy_state'] = compact_futures_strategy_state(
        compact.get('futures_strategy_state', {}), max_chars=12000,
    )

    def payload_size():
        return len(json.dumps(_json_safe(compact), ensure_ascii=False, separators=(',', ':')))

    signals = list(compact.get('strategy_signal_log', []))
    while payload_size() > max_chars and len(signals) > 100:
        signals = signals[-max(100, len(signals) // 2):]
        compact['strategy_signal_log'] = signals
    if payload_size() > max_chars:
        # cached_notes 可由 stock_data 的戰略備註重建，不影響分析數值。
        compact['cached_notes'] = {}
    candidates = list(compact.get('all_candidates', []))
    if payload_size() > max_chars and len(candidates) > 300:
        compact['all_candidates'] = candidates[-300:]
    if payload_size() > max_chars:
        compact['futures_strategy_state'] = compact_futures_strategy_state(
            compact.get('futures_strategy_state', {}), max_chars=6000,
        )
    compact['_cloud_payload_chars'] = payload_size()
    return _json_safe(compact)


def save_data_cache(
    df,
    ignored_set,
    candidates=None,
    saved_notes=None,
    fibo_tags=None,
    *,
    clear_stock_data=False,
    replace_ignored=False,
    replace_stock_data=True,
    verify_stock_data=False,
):
    """
    股票戰略室專用儲存。

    保留原有函式參數，
    但現在只寫 stock_strategy。

    fibo_tags 不再寫進股票 scope。
    company_event_snapshot 不再寫進股票 scope。
    strategy_signal_log 不再寫進股票 scope。
    """
    candidates = list(
        candidates or []
    )

    saved_notes = dict(
        saved_notes or {}
    )

    try:
        df_save = (
            df.fillna(
                ""
            ).copy()
            if isinstance(
                df,
                pd.DataFrame
            )
            else pd.DataFrame()
        )

        ignored_list = list(
            ignored_set or []
        )

        empty_col = pd.Series(
            "",
            index=df_save.index,
            dtype=str,
        )

        codes = (
            df_save
            .get(
                '代號',
                empty_col
            )
            .astype(str)
            .str.strip()
        )

        notes = (
            df_save
            .get(
                '戰略備註',
                empty_col
            )
            .astype(str)
            .str.strip()
        )

        auto_notes = (
            df_save
            .get(
                '_auto_note',
                empty_col
            )
            .astype(str)
            .str.strip()
        )

        cached_notes = {
            code: {
                'note': note,
                'auto': auto,
            }
            for code, note, auto
            in zip(
                codes,
                notes,
                auto_notes,
            )
            if code
        }

        df_save.drop(
            columns=[
                '_auto_note'
            ],
            errors='ignore',
            inplace=True,
        )

        stock_data_updated_at = str(
            st.session_state.get(
                '_stock_data_updated_at',
                ''
            )
        ).strip()

        if not stock_data_updated_at:
            stock_data_updated_at = (
                datetime.now(
                    pytz.timezone(
                        'Asia/Taipei'
                    )
                ).isoformat()
            )

            st.session_state[
                '_stock_data_updated_at'
            ] = stock_data_updated_at

        local_payload = _json_safe({
            'version': 4,
            'stock_data': (
                df_save.to_dict(
                    orient='records'
                )
            ),
            'ignored_stocks': (
                ignored_list
            ),
            'all_candidates': (
                candidates
            ),
            'saved_notes': (
                saved_notes
            ),
            'cached_notes': (
                cached_notes
            ),
            'stock_data_updated_at': (
                stock_data_updated_at
            ),
        })

        # 股票獨立本機快取。
        _write_json_atomic(
            STOCK_STRATEGY_CACHE_FILE,
            local_payload,
            indent=2,
        )

        gsheet_api_url = (
            get_app_secret(
                'gsheet_api_url'
            )
        )

        sync_ok = True

        if gsheet_api_url:
            with get_data_cache_sync_lock():
                sync_ok, _ = (
                    _save_remote_scope(
                        gsheet_api_url,
                        GOOGLE_SCOPE_STOCK,
                        local_payload,
                        updated_at=(
                            stock_data_updated_at
                        ),
                        timeout=8,
                        verify=True,
                    )
                )

        st.session_state[
            '_data_cache_sync_status'
        ] = (
            'ok'
            if sync_ok
            else 'local_only'
        )

        if sync_ok:
            st.session_state[
                '_data_cache_remote_error'
            ] = ''
        else:
            st.session_state[
                '_data_cache_remote_error'
            ] = (
                '股票戰略室資料已保留於本機，'
                '但 Google Sheet 回讀未確認成功。'
            )

        return sync_ok

    except (
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        st.session_state[
            '_data_cache_sync_status'
        ] = type(exc).__name__

        return False

def load_data_cache():
    """
    只讀取股票戰略室 scope。

    回傳：
        df,
        ignored,
        candidates,
        saved_notes,
        cached_notes
    """
    local_payload = _read_json_cache_file(
        STOCK_STRATEGY_CACHE_FILE
    )

    # 舊版本機總快取相容。
    if not local_payload:
        legacy_payload = (
            _read_json_cache_file(
                DATA_CACHE_FILE
            )
        )

        if _is_valid_data_cache_payload(
            legacy_payload
        ):
            local_payload = {
                'version': 4,
                'stock_data': (
                    legacy_payload.get(
                        'stock_data',
                        []
                    )
                ),
                'ignored_stocks': (
                    legacy_payload.get(
                        'ignored_stocks',
                        []
                    )
                ),
                'all_candidates': (
                    legacy_payload.get(
                        'all_candidates',
                        []
                    )
                ),
                'saved_notes': (
                    legacy_payload.get(
                        'saved_notes',
                        {}
                    )
                ),
                'cached_notes': (
                    legacy_payload.get(
                        'cached_notes',
                        {}
                    )
                ),
                'stock_data_updated_at': (
                    legacy_payload.get(
                        'stock_data_updated_at'
                    )
                    or legacy_payload.get(
                        'updated_at',
                        ''
                    )
                ),
            }

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    remote_payload = None
    remote_error = None

    if gsheet_api_url:
        remote_payload, remote_error = (
            _fetch_remote_scope(
                gsheet_api_url,
                GOOGLE_SCOPE_STOCK,
                timeout=8,
            )
        )

    data = {}

    if (
        isinstance(
            remote_payload,
            dict
        )
        and remote_payload
    ):
        local_timestamp = (
            _cache_payload_timestamp(
                local_payload
            )
        )

        remote_timestamp = (
            _cache_payload_timestamp(
                remote_payload
            )
        )

        if (
            local_payload
            and local_timestamp is not None
            and remote_timestamp is not None
            and local_timestamp
            > remote_timestamp
        ):
            data = local_payload

            st.session_state[
                '_data_cache_remote_error'
            ] = (
                'Google Sheet 尚未完成前次寫入，'
                '已保留較新的股票本機資料。'
            )

            source = 'local_newer'

        else:
            data = remote_payload

            st.session_state[
                '_data_cache_remote_error'
            ] = ''

            source = 'google_sheet'

            try:
                _write_json_atomic(
                    STOCK_STRATEGY_CACHE_FILE,
                    _json_safe(
                        remote_payload
                    ),
                    indent=2,
                )
            except (
                OSError,
                TypeError,
                ValueError,
            ):
                pass

    elif local_payload:
        data = local_payload

        source = 'local'

        st.session_state[
            '_data_cache_remote_error'
        ] = (
            remote_error or ''
        )

    else:
        source = 'default'

        st.session_state[
            '_data_cache_remote_error'
        ] = (
            remote_error or ''
        )

    df = pd.DataFrame(
        data.get(
            'stock_data',
            []
        )
    )

    ignored = set(
        data.get(
            'ignored_stocks',
            []
        )
    )

    candidates = data.get(
        'all_candidates',
        []
    )

    saved_notes = data.get(
        'saved_notes',
        {}
    )

    cached_notes = data.get(
        'cached_notes',
        {}
    )

    st.session_state[
        '_stock_data_updated_at'
    ] = str(
        data.get(
            'stock_data_updated_at',
            ''
        )
    ).strip()

    st.session_state[
        '_data_cache_source'
    ] = source

    return (
        df,
        ignored,
        candidates,
        saved_notes,
        cached_notes,
    )

def load_fibo_tags_from_cloud(
    force=False
):
    """
    一般啟動以裝置本機標籤為優先；手動匯入時只接受 fibo_strategy。

    ``force=True`` 絕不退回本機或預設值，避免雲端失敗卻誤報匯入成功。
    """
    if (
        not force
        and st.session_state.get(
            '_fibo_cloud_loaded',
            False
        )
    ):
        return list(
            st.session_state.get(
                'fibo_tags',
                DEFAULT_FIBO_TAGS
            )
        )

    gsheet_api_url = (
        get_app_secret(
            'gsheet_api_url'
        )
    )

    tags = []
    source = 'default'
    cloud_error = ''

    if force:
        if not gsheet_api_url:
            cloud_error = '未設定 Google Sheet 同步網址'
        else:
            payload, error = _fetch_remote_scope(
                gsheet_api_url,
                GOOGLE_SCOPE_FIBO,
                timeout=10,
            )
            if isinstance(payload, dict):
                tags = _extract_fibo_tags(payload)
                if len(tags) >= 5:
                    tags = [normalize_fibo_quick_tag(tag) for tag in tags]
                    source = 'google_sheet'
                    timestamp = _fibo_tag_timestamp(payload)
                    updated_at = timestamp.isoformat() if timestamp is not None else ''
                    if updated_at:
                        st.session_state['_fibo_tags_updated_at'] = updated_at
                else:
                    cloud_error = 'fibo_strategy 內沒有五組有效標籤'
            else:
                cloud_error = error or 'Google Sheet 未回傳 fibo_strategy'

        st.session_state['_fibo_cloud_load_error'] = cloud_error
        st.session_state['_fibo_cloud_loaded'] = True
        if len(tags) < 5:
            return []
    else:
        # 手機與電腦各自保留本機操作；只有明確按「從 Google Sheet
        # 重新載入」時，才讓另一裝置保存的標籤覆蓋本機 widget。
        tags = _valid_fibo_tags(
            load_fibo_tag_cache()
        )

        if len(tags) >= 5:
            tags = [
                normalize_fibo_quick_tag(
                    tag
                )
                for tag in tags
            ]

            source = (
                'local_tag_cache'
            )

        if len(tags) < 5:
            config = load_config()

            config_tags = _valid_fibo_tags(
                config.get(
                    'fibo_tags',
                    []
                )
            )

            if len(config_tags) >= 5:
                tags = [
                    normalize_fibo_quick_tag(tag)
                    for tag in config_tags
                ]
                source = 'config'

        if len(tags) < 5 and gsheet_api_url:
            payload, error = _fetch_remote_scope(
                gsheet_api_url,
                GOOGLE_SCOPE_FIBO,
                timeout=8,
            )
            if isinstance(payload, dict):
                tags = _extract_fibo_tags(payload)
                if len(tags) >= 5:
                    tags = [normalize_fibo_quick_tag(tag) for tag in tags]
                    source = 'google_sheet'
                    timestamp = _fibo_tag_timestamp(payload)
                    if timestamp is not None:
                        st.session_state['_fibo_tags_updated_at'] = timestamp.isoformat()
            if len(tags) < 5:
                cloud_error = error or 'fibo_strategy 內沒有五組有效標籤'

    if len(tags) < 5:
        tags = list(
            DEFAULT_FIBO_TAGS
        )

        source = 'default'

    st.session_state[
        'fibo_tags'
    ] = list(tags)

    st.session_state[
        '_fibo_tags_source'
    ] = source

    st.session_state[
        '_fibo_cloud_loaded'
    ] = True

    st.session_state['_fibo_cloud_load_error'] = cloud_error

    return list(tags)


def reload_fibo_tags_from_cloud():
    """強制只從 fibo_strategy scope 還原快速標籤。"""
    st.session_state.pop(
        '_fibo_cloud_loaded',
        None
    )

    tags = load_fibo_tags_from_cloud(
        force=True
    )

    if not tags or len(tags) < 5:
        return (
            False,
            st.session_state.get(
                '_fibo_cloud_load_error',
                '雲端尚無五組快速標籤',
            )
        )

    if st.session_state.get('_fibo_tags_source') != 'google_sheet':
        return False, 'Google Sheet 未回傳可用的 fibo_strategy 標籤'

    st.session_state[
        'fibo_tags'
    ] = list(tags)

    for index, tag in enumerate(
        tags,
        start=1
    ):
        st.session_state[
            f'custom_tag_{index}'
        ] = tag

    st.session_state[
        '_fibo_cloud_save_status'
    ] = 'loaded'

    try:
        config = load_config()

        config[
            'fibo_tags'
        ] = tags

        updated_at = str(
            st.session_state.get(
                '_fibo_tags_updated_at',
                ''
            )
        ).strip()

        if updated_at:
            config[
                'fibo_tags_updated_at'
            ] = updated_at

        _write_json_atomic(
            CONFIG_FILE,
            config,
        )

        _write_json_atomic(
            FIBO_TAG_CACHE_FILE,
            {
                'version': 3,
                'updated_at': (
                    updated_at
                    or datetime.now(
                        pytz.timezone(
                            'Asia/Taipei'
                        )
                    ).isoformat()
                ),
                'tags': tags,
            },
            indent=2,
        )

    except (
        OSError,
        TypeError,
        ValueError,
    ):
        pass

    return (
        True,
        '已從 Google Sheet 還原快速標籤'
    )

def load_url_history():
    if os.path.exists(URL_CACHE_FILE):
        try:
            with open(URL_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                if "url" in data and isinstance(data["url"], str) and data["url"]: return [data["url"]]
                return data.get("urls", [])
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return []
    return []

def save_url_history(urls):
    try:
        unique_urls = []
        seen = set()
        for u in urls:
            u_clean = u.strip()
            if u_clean and u_clean not in seen:
                unique_urls.append(u_clean)
                seen.add(u_clean)
        _write_json_atomic(URL_CACHE_FILE, {"urls": unique_urls})
        return True
    except (OSError, TypeError, ValueError):
        return False

def load_search_cache():
    if os.path.exists(SEARCH_CACHE_FILE):
        try:
            with open(SEARCH_CACHE_FILE, "r", encoding='utf-8') as f: data = json.load(f)
            return data.get("selected", [])
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return []
    return []

def save_search_cache(selected_items):
    try:
        _write_json_atomic(SEARCH_CACHE_FILE, {"selected": selected_items})
    except (OSError, TypeError, ValueError):
        pass

if 'stock_data' not in st.session_state:
    (
        cached_df,
        cached_ignored,
        cached_candidates,
        cached_saved_notes,
        cached_note_dict,
    ) = load_data_cache()

    st.session_state.stock_data = cached_df

    # 持久化表格只保存股票符號、排序、備註；
    # 新工作階段仍會依既有流程重新取得最新行情。
    st.session_state._stock_cache_refresh_pending = (
        not cached_df.empty
    )

    st.session_state.ignored_stocks = (
        cached_ignored
    )

    st.session_state.all_candidates = (
        cached_candidates
    )

    st.session_state.saved_notes = (
        cached_saved_notes
    )

    st.session_state.cached_notes = (
        cached_note_dict
    )

if 'stock_strategy_editor_revision' not in st.session_state:
    st.session_state.stock_strategy_editor_revision = 0

if 'ignored_stocks' not in st.session_state: st.session_state.ignored_stocks = set()
if 'all_candidates' not in st.session_state: st.session_state.all_candidates = []
if 'calc_base_price' not in st.session_state: st.session_state.calc_base_price = 100.0
if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = 100.0
if 'url_history' not in st.session_state: st.session_state.url_history = load_url_history()
if 'cloud_url_input' not in st.session_state: st.session_state.cloud_url_input = st.session_state.url_history[0] if st.session_state.url_history else ""
if 'search_multiselect' not in st.session_state: st.session_state.search_multiselect = load_search_cache()
if 'saved_notes' not in st.session_state: st.session_state.saved_notes = {}
if 'futures_list' not in st.session_state: st.session_state.futures_list = {}
if 'ignored_data_cache' not in st.session_state: st.session_state.ignored_data_cache = {} # 新增這行：忽略資料快取
if 'prefetch_cache' not in st.session_state: st.session_state.prefetch_cache = {} # 🚀 新增：預載快取
if 'cached_notes' not in st.session_state: st.session_state.cached_notes = {}    

saved_config = load_config()

# =========================================================
# Fibo 標籤獨立初始化
# =========================================================

fibo_tags_source = (
    load_fibo_tags_from_cloud()
)

fibo_tags_source = [
    normalize_fibo_quick_tag(
        tag
    )
    for tag in fibo_tags_source
]

st.session_state[
    'fibo_tags'
] = list(
    fibo_tags_source
)

if 'custom_tag_1' not in st.session_state:
    st.session_state[
        'custom_tag_1'
    ] = (
        fibo_tags_source[0]
        if len(fibo_tags_source) > 0
        else "台積電(2330)"
    )

if 'custom_tag_2' not in st.session_state:
    st.session_state[
        'custom_tag_2'
    ] = (
        fibo_tags_source[1]
        if len(fibo_tags_source) > 1
        else "鴻海(2317)"
    )

if 'custom_tag_3' not in st.session_state:
    st.session_state[
        'custom_tag_3'
    ] = (
        fibo_tags_source[2]
        if len(fibo_tags_source) > 2
        else "聯發科(2454)"
    )

if 'custom_tag_4' not in st.session_state:
    st.session_state[
        'custom_tag_4'
    ] = (
        fibo_tags_source[3]
        if len(fibo_tags_source) > 3
        else "和椿(6215)"
    )

if 'custom_tag_5' not in st.session_state:
    st.session_state[
        'custom_tag_5'
    ] = (
        fibo_tags_source[4]
        if len(fibo_tags_source) > 4
        else "晶彩科(3535)"
    )

if 'fibo_search_input' not in st.session_state: st.session_state.fibo_search_input = ""
if 'fibo_trigger_search' not in st.session_state: st.session_state.fibo_trigger_search = False

if 'custom_tag_1' not in st.session_state: st.session_state.custom_tag_1 = fibo_tags_source[0] if len(fibo_tags_source)>0 else "台積電(2330)"
if 'custom_tag_2' not in st.session_state: st.session_state.custom_tag_2 = fibo_tags_source[1] if len(fibo_tags_source)>1 else "鴻海(2317)"
if 'custom_tag_3' not in st.session_state: st.session_state.custom_tag_3 = fibo_tags_source[2] if len(fibo_tags_source)>2 else "聯發科(2454)"
if 'custom_tag_4' not in st.session_state: st.session_state.custom_tag_4 = fibo_tags_source[3] if len(fibo_tags_source)>3 else "和椿(6215)"
if 'custom_tag_5' not in st.session_state: st.session_state.custom_tag_5 = fibo_tags_source[4] if len(fibo_tags_source)>4 else "晶彩科(3535)"

if 'ma_w' not in st.session_state: st.session_state.ma_w = saved_config.get('ma_width', 1.5)

# 控制圖表預設時間週期
if 'fibo_interval' not in st.session_state: st.session_state.fibo_interval = "1d" 
if 'fibo_font_size' not in st.session_state: st.session_state.fibo_font_size = 15

tz_tw = pytz.timezone('Asia/Taipei')
now_tw = datetime.now(tz_tw)
if 'cal_year' not in st.session_state: st.session_state.cal_year = now_tw.year
if 'cal_month' not in st.session_state: st.session_state.cal_month = now_tw.month

@st.cache_resource
def init_shioaji_connection(api_key, secret_key):
    api = sj.Shioaji(simulation=False)
    api.login(api_key, secret_key)
    return api

if 'font_size' not in st.session_state: st.session_state.font_size = saved_config.get('font_size', 15)
if 'limit_rows' not in st.session_state: st.session_state.limit_rows = saved_config.get('limit_rows', 5)

# 永豐API帳密：
secret_sj_key = get_app_secret('sj_key', '')
secret_sj_secret = get_app_secret('sj_secret', '')
if 'sj_key' not in st.session_state: 
    st.session_state.sj_key = secret_sj_key or saved_config.get('sj_key', '')
if 'sj_secret' not in st.session_state: 
    st.session_state.sj_secret = secret_sj_secret or saved_config.get('sj_secret', '')
if 'remember_sj' not in st.session_state: 
    st.session_state.remember_sj = True if secret_sj_key else saved_config.get('remember_sj', False)

if sj and st.session_state.remember_sj and st.session_state.sj_key and not st.session_state.get('manual_logout', False):
    try:
        # 取得快取的連線物件
        api_obj = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
        # 測試連線是否存活 (防呆：解決 AuthError: Not authenticated)
        try:
            api_obj.usage()
            st.session_state.sj_api = api_obj
            st.session_state.sj_logged_in = True
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            # 若快取的連線已失效，則清除快取並強制重新登入
            clear_market_stream(api_obj)
            init_shioaji_connection.clear()
            st.session_state.sj_api = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
            st.session_state.sj_logged_in = True
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        st.session_state.sj_logged_in = False
        st.session_state.sj_connection_error = type(exc).__name__

@st.cache_data(ttl=300, max_entries=512)
def get_stock_name_online(code):
    code = str(code).strip()
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    return code

@st.cache_data(ttl=86400, max_entries=512)
def search_code_online(query):
    query = query.strip()
    if query.isdigit(): return query
    _, name_map = load_local_stock_names()
    if query in name_map: return name_map[query]
    return None

with st.sidebar:
    st.header("🔑 永豐證券 API 登入")
    if sj is None:
        st.error(
            "⚠️ Shioaji 暫時無法載入，已停用永豐即時行情；其他分頁仍可使用。"
            + (f"（{sj_import_error}）" if sj_import_error else "")
        )
    else:
        if st.session_state.get('sj_logged_in', False):
            st.success("✅ 永豐 API 已登入")
            
            try:
                usage = st.session_state.sj_api.usage()
                rem_mb = usage.remaining_bytes / (1024 * 1024)
                st.caption(f"📊 API 今日剩餘流量: {_format_compact_number(rem_mb, 2)} MB")
            except Exception:
                st.caption("📊 API 今日剩餘流量: 暫時無法獲取 (連線讀取中)")

            stream_status = get_market_stream_status(st.session_state.sj_api)
            st.caption(
                f"📡 串流已訂閱 {stream_status['subscriptions']} 項｜"
                f"已接收 {stream_status['stream_quotes']} 項即時行情"
            )
            if stream_status['errors']:
                st.caption("⚠️ 部分商品暫無串流，已自動使用暖機報價備援。")

            col_logout, col_relogin = st.columns(2)
            with col_logout:
                if st.button("登出", key="btn_logout_sj", width='stretch'):
                    st.session_state.sj_logged_in = False
                    st.session_state.manual_logout = True # 標記為手動登出，阻擋重整時自動登入
                    
                    try:
                        clear_market_stream(st.session_state.sj_api)
                        st.session_state.sj_api.logout()
                        init_shioaji_connection.clear() 
                    except: pass
                    
                    # 保持儲存現有的 Key 與記住狀態
                    save_config(
                        st.session_state.font_size, 
                        st.session_state.limit_rows, 
                        st.session_state.sj_key, 
                        st.session_state.sj_secret, 
                        st.session_state.remember_sj
                    )
                    st.rerun()
            with col_relogin:
                relogin_clicked = st.button("快速重新登入", key="btn_relogin_sj", width='stretch')
                
            msg_placeholder = st.empty()
            
            if relogin_clicked:
                try:
                    clear_market_stream(st.session_state.sj_api)
                    st.session_state.sj_api.logout()
                    init_shioaji_connection.clear() # 必須清除快取，否則只會拿到舊的連線物件
                    time.sleep(0.5)
                    
                    st.session_state.sj_api = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
                    st.session_state.sj_logged_in = True
                    msg_placeholder.success("✅ 重新登入成功！")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    msg_placeholder.error(f"❌ 重新登入失敗: {e}")
                    st.session_state.sj_logged_in = False
                    st.rerun()
        else:
            sj_api_key = st.text_input("API Key", type="password", value=st.session_state.sj_key, key="input_sj_key")
            sj_secret = st.text_input("Secret Key", type="password", value=st.session_state.sj_secret, key="input_sj_secret")
            remember_sj = st.checkbox("記住 API 資訊", value=st.session_state.remember_sj, key="input_remember_sj")
            
            if st.button("登入 Shioaji"):
                try:
                    st.session_state.manual_logout = False # 解除手動登出標記
                    st.session_state.sj_api = init_shioaji_connection(sj_api_key, sj_secret)
                    st.session_state.sj_logged_in = True
                    
                    st.session_state.sj_key = sj_api_key
                    st.session_state.sj_secret = sj_secret
                    st.session_state.remember_sj = remember_sj
                    save_config(
                        st.session_state.font_size, 
                        st.session_state.limit_rows, 
                        sj_api_key, 
                        sj_secret, 
                        remember_sj
                    )
                    st.success("✅ 永豐 API 登入成功！")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 登入失敗: {e}")
    st.markdown("---")

def render_stock_external_resources():
    """Render stock data-source links alongside upload/quick-search controls."""
    st.markdown("#### 外部資源")

    def perform_goodinfo_fetch():
        with st.spinner("正在以舊版瀏覽器模式載入排行，最長約 15 秒..."):
            result = fetch_goodinfo_data()
            status = dict(getattr(fetch_goodinfo_data, 'last_status', {}) or {})
            if result is not None and not result.empty:
                st.session_state['goodinfo_df'] = result.astype(str)
                st.session_state['goodinfo_fetch_failed'] = False
                st.success(
                    f"抓取成功，共 {len(result)} 筆（{status.get('elapsed', 0):g} 秒）；"
                    "已載入暫存，回到股票分析按執行即可。"
                )
            else:
                st.session_state['goodinfo_fetch_failed'] = True
                reason_text = {
                    'browser_busy': '另一個頁面正在抓取，請稍後再按一次。',
                    'rate_limited': 'Goodinfo 暫時限制請求（429／430）；請稍等後再試，或改用下方手動 CSV 備援。',
                    'site_alert': 'Goodinfo 回傳網站載入警示，請稍等後再試，或改用下方手動 CSV 備援。',
                    'legacy_table_missing': '頁面已開啟，但 15 秒內沒有收到完整排行表格。',
                    'browser_error': '瀏覽器啟動、網站驗證或頁面解析失敗。',
                }.get(status.get('reason'), '抓取失敗或查無資料。')
                st.error(reason_text)
                st.info(
                    "🛟 備援方式：開啟右側 Goodinfo 排行，在網站完成驗證並匯出 CSV；"
                    "回到上方「本機」上傳該檔案，再按「執行分析」。"
                )
                if 'goodinfo_df' in st.session_state:
                    st.warning("本次沒有覆蓋先前成功暫存，可先沿用並留意資料時間。")

    resource_col1, resource_col2, resource_col3 = st.columns(3)
    with resource_col1:
        if st.button(
            "📥 抓取 Goodinfo 週轉率排行",
            help="使用原本 Selenium＋一般瀏覽器 User-Agent 模式；每次僅請求一次，最長等待約 15 秒。",
            width='stretch', key='fetch_goodinfo_in_stock_room'
        ):
            perform_goodinfo_fetch()
        if st.session_state.get('goodinfo_fetch_failed', False):
            if st.button("🔄 重新抓取", width='stretch', key='retry_goodinfo_btn'):
                perform_goodinfo_fetch()
        if 'goodinfo_df' in st.session_state:
            st.download_button(
                "💾 下載 Report.csv",
                data=st.session_state['goodinfo_df'].to_csv(index=False).encode('utf-8-sig'),
                file_name="Report.csv", mime="text/csv", width='stretch'
            )
    with resource_col2:
        st.link_button(
            "🌐 開啟 Goodinfo／下載 CSV",
            _GOODINFO_URL,
            help="由你的瀏覽器完成 Goodinfo 驗證；載入排行後使用網站的匯出功能下載 CSV。",
            width='stretch'
        )
        st.caption("下載後回到「本機」上傳 CSV；不需要再等伺服器爬取。")
    with resource_col3:
        st.link_button(
            "🚨 上市處置公告", "https://www.twse.com.tw/zh/announcement/punish.html",
            width='stretch'
        )
        st.link_button(
            "🚨 上櫃處置公告", "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html",
            width='stretch'
        )


def render_stock_strategy_controls():
    """將原當沖戰略室的側欄設定與資料管理移至股票戰略室。"""
    with st.expander("⚙️ 股票戰略室設定與資料管理", expanded=False):
        settings_col, data_col = st.columns(2)

        with settings_col:
            st.markdown("#### 顯示設定")
            hide_non_stock = st.checkbox(
                "隱藏非個股（ETF／權證／債券）", value=True,
                key='stock_hide_non_stock'
            )
            st.checkbox(
                "查詢權證（含圖表快速標籤）", value=False,
                key="allow_warrant_search"
            )
            show_3d_hilo = st.checkbox(
                "近 3 日高低點（戰略備註）", value=False,
                key='stock_show_3d_hilo',
                help="在戰略備註加入前天、昨天、今天的高低點。"
            )
            current_limit_rows = st.number_input(
                "顯示筆數（檔案／雲端）", min_value=1,
                value=st.session_state.limit_rows, key='limit_rows_input'
            )
            st.session_state.limit_rows = current_limit_rows
            if st.button("💾 儲存股票設定", width='stretch', key='save_stock_room_settings'):
                if save_config(
                    st.session_state.get('font_size', 15), current_limit_rows,
                    st.session_state.sj_key, st.session_state.sj_secret,
                    st.session_state.remember_sj
                ):
                    st.toast("股票設定已儲存", icon="✅")

        with data_col:
            st.markdown("#### 資料管理")
            if st.session_state.ignored_stocks:
                ignored_list = sorted(st.session_state.ignored_stocks)
                option_map = {f"{code} {get_stock_name_online(code)}": code for code in ignored_list}
                selected = st.multiselect(
                    "忽略名單（取消勾選以復原）", list(option_map),
                    default=list(option_map),
                    key=f"stock_ignored_manager_{abs(hash(tuple(ignored_list)))}"
                )
                selected_codes = {option_map[label] for label in selected}
                if selected_codes != st.session_state.ignored_stocks:
                    restored = st.session_state.ignored_stocks - selected_codes
                    st.session_state.ignored_stocks = selected_codes
                    if restored:
                        st.session_state.pending_unignore = restored
                    save_data_cache(
                        st.session_state.stock_data, st.session_state.ignored_stocks,
                        st.session_state.all_candidates, st.session_state.saved_notes,
                        replace_ignored=True,
                    )
                    st.rerun()
            else:
                st.caption("目前無忽略股票")

            restore_col, clear_col = st.columns(2)
            with restore_col:
                if st.button("♻️ 全部復原", width='stretch', key='restore_all_stocks'):
                    st.session_state.pending_unignore = set(st.session_state.ignored_stocks)
                    st.session_state.ignored_stocks.clear()
                    save_data_cache(
                        st.session_state.stock_data, st.session_state.ignored_stocks,
                        st.session_state.all_candidates, st.session_state.saved_notes,
                        replace_ignored=True,
                    )
                    st.rerun()
            with clear_col:
                if st.button("🗑️ 全部清空", type="primary", width='stretch', key='clear_all_stock_data'):
                    st.session_state.stock_data = pd.DataFrame()
                    st.session_state.ignored_stocks = set()
                    st.session_state.all_candidates = []
                    st.session_state.search_multiselect = []
                    st.session_state.saved_notes = {}
                    st.session_state.cached_notes = {}
                    st.session_state.pop('stock_independent_raw_results', None)
                    save_search_cache([])
                    save_data_cache(
                        pd.DataFrame(), set(), [], {},
                        clear_stock_data=True, replace_ignored=True,
                    )
                    st.rerun()
            st.info("在股票表格左側勾選「刪除」，會立即隱藏並自動遞補下一檔。")

    return hide_non_stock, show_3d_hilo

def render_stock_strategy_explanation():
    """股票戰略室的靜態說明，與操作設定分開並預設折疊。"""
    with st.expander("📖 股票戰略室說明", expanded=False):
        st.markdown("""
- **① 載入**：抓 Goodinfo 排行、上傳檔案，或直接輸入股票；再按「執行分析」。
- **② 先看四欄**：`訊號狀態`、`進場信心`、`支撐壓力`、`進出場預判`。信心分代表條件是否一致，**不是勝率**。
- **③ 等條件成立**：`✅ 已觸發` 或 `🔵 回測確認` 才進一步評估；`進／停／目` 是觀察進場、失效離場、第一目標，不會自動下單。
- **🔴 多／🟢 空**：當沖看分 K、VWAP、開盤區間與量能；隔日／波段看日 K、5／20 日線與前高前低。方向依據只列最重要的兩項。
- **買賣價差**：顯示最佳買價與最佳賣價的距離（跳數）；跳數越少通常越容易成交。`—` 代表目前沒有即時報價。
- **ATR**：最近常見的波動幅度。乖離太大時不追價；**VWAP**：盤中平均成本，站上偏多、跌破偏空。
- **⚠️ 注意／處置**：`注意 1` 是累計 1 次；`注意 2+` 是至少 2 次，預設排除；`🚫 處置中` 直接排除；`⚪ 未查核` 代表官方資料尚未讀完。
- 原本的週轉率排序與戰略備註不變；附加分析只提供判讀與風險提醒。
        """)

def render_futures_strategy_explanation():
    """期貨戰略室的靜態說明，預設折疊避免占用表格空間。"""
    with st.expander("📖 期貨戰略室說明", expanded=False):
        st.markdown("""
- **① 選商品**：主表預設按當日成交口數排序；「只顯示小型股票期貨」只保留小型個股期，不含小台、微台與 ETF 期貨。
- **② 更新**：盤後資料按「更新成交量／保證金」；盤中或夜盤先登入 Shioaji，再按「即時更新報價與分析」。
- **③ 看訊號**：`🔴 偏多` 等突破壓力，`🟢 偏空` 等跌破支撐；`進／停／目` 是觀察進場、失效離場、第一目標。
- **成交與風險**：成交量、未平倉量高，且買賣價差較小，通常較容易進出。進場信心是條件一致度，**不是勝率**。
- **交易時段**：`日盤+夜盤` 表示商品有夜盤；策略只採查詢當下的同一時段資料，避免把兩段行情混成一根 K 棒。
- 股票戰略室中的股期會依股票順序附加在排行後方；隱藏與月份篩選仍會套用。
        """)

@st.cache_data(ttl=300, max_entries=1)
def fetch_futures_list():
    errors = []
    try:
        url = "https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining"
        r = requests.get(url, headers={'accept': 'application/json'}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            stock_contracts = {}
            for item in data:
                code = str(item.get("UnderlyingSecurityCode", "")).strip()
                contract = str(item.get("Contract", "")).strip()
                if code and contract:
                    if code not in stock_contracts:
                        stock_contracts[code] = set()
                    stock_contracts[code].add(contract)
            
            futures_dict = {}
            for code, contracts in stock_contracts.items():
                futures_dict[code] = "✅(有小型)" if len(contracts) > 1 else "✅"
            if futures_dict:
                return futures_dict
    except (requests.RequestException, ValueError, TypeError) as exc:
        errors.append(type(exc).__name__)

    try:
        url = "https://www.taifex.com.tw/cht/2/stockLists"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        response.raise_for_status()
        dfs = pd.read_html(io.StringIO(response.text))
        futures_dict = {}
        if dfs:
            for df in dfs:
                df_cols = [str(c).replace('\n', '').replace(' ', '') for c in df.columns]
                df.columns = df_cols
                
                code_col = next((c for c in df.columns if '證券代號' in c or 'StockCode' in c), None)
                if code_col:
                    small_col = next((c for c in df.columns if '小' in c or 'Small' in c), None)
                    for _, row in df.iterrows():
                        code = str(row[code_col]).strip()
                        if pd.isna(code) or not code: continue
                        is_small = False
                        if small_col and pd.notna(row[small_col]):
                            val = str(row[small_col]).strip()
                            if val and val not in ['-', 'nan', 'NaN', '']:
                                is_small = True
                        
                        if is_small:
                            futures_dict[code] = "✅(有小型)"
                        elif code not in futures_dict:
                            futures_dict[code] = "✅"
            return futures_dict
    except (requests.RequestException, ValueError, TypeError) as exc:
        errors.append(type(exc).__name__)
    logger.warning("期貨名單暫時無法取得：%s", '/'.join(errors) or 'empty')
    return {}

def _decode_taifex_legacy_text(value):
    """部分期交所 ETF 端點仍回傳 Big5 字串，必要時轉回繁體中文。"""
    text = str(value or '').strip()
    if not text:
        return ''
    try:
        decoded = text.encode('latin1').decode('big5')
        return decoded if decoded else text
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

def _safe_number(value, default=None):
    try:
        text = (
            str(value).replace(',', '').replace('%', '')
            .replace('↑', '').replace('↓', '').replace('→', '').strip()
        )
        if text in ('', '-', 'NULL', 'nan', 'None'):
            return default
        number = float(text)
        return default if not math.isfinite(number) else number
    except (TypeError, ValueError):
        return default


def futures_limit_state(price, limit_up, limit_down):
    """Return a limit state only when the traded price actually equals the limit."""
    price_value = _safe_number(price)
    up_value = _safe_number(limit_up)
    down_value = _safe_number(limit_down)
    if price_value is None:
        return ''
    if up_value is not None and math.isclose(
        price_value, up_value, rel_tol=1e-10, abs_tol=1e-9,
    ):
        return 'up'
    if down_value is not None and math.isclose(
        price_value, down_value, rel_tol=1e-10, abs_tol=1e-9,
    ):
        return 'down'
    return ''


def stock_limit_state(price, limit_up, limit_down):
    """Return a stock limit state only when the displayed trade price is at its limit."""
    price_value = _safe_number(price)
    up_value = _safe_number(limit_up)
    down_value = _safe_number(limit_down)
    if price_value is None:
        return ''
    try:
        # A fraction of the legal tick absorbs only float conversion noise.
        tolerance = max(float(get_tick_size(price_value)) * 0.001, 1e-6)
    except (TypeError, ValueError):
        tolerance = 1e-6
    if up_value is not None and math.isclose(price_value, up_value, rel_tol=0, abs_tol=tolerance):
        return 'up'
    if down_value is not None and math.isclose(price_value, down_value, rel_tol=0, abs_tol=tolerance):
        return 'down'
    return ''


TAIFEX_NIGHT_SESSION_ROOTS = {
    # 期交所盤後交易商品；用商品資格補足每日行情僅回傳日盤列的情況。
    'TX', 'MTX', 'TMF', 'TE', 'ZEF', 'SOF',
    'CD', 'QF', 'CC', 'NY', 'SR', 'RZ',
    'RHF', 'RTF', 'XJF', 'XEF', 'UDF', 'SPF', 'UNF', 'SXF', 'F1F', 'XBF', 'XAF',
    'GDF', 'TGF', 'BRF',
}


def is_futures_night_session_product(root):
    """Match both TAIFEX display roots (QF) and OpenAPI contract roots (QFF)."""
    normalized_root = str(root or '').strip().upper()
    root_aliases = {normalized_root}
    if len(normalized_root) > 2 and normalized_root.endswith('F'):
        root_aliases.add(normalized_root[:-1])
    return bool(root_aliases & TAIFEX_NIGHT_SESSION_ROOTS)


def is_small_futures_product(name, root='', product_type=''):
    """辨識小型／微型期貨，涵蓋期交所名稱的常見縮寫。

    個股期貨的官方名稱不一定使用「小型」，例如可能寫成「小台積電期貨」；
    只對股票、ETF 或已知指數根做縮寫判斷，避免把一般商品（例如小麥）誤判。
    """
    normalized_name = re.sub(r'\s+', '', str(name or '').strip().lower())
    normalized_root = str(root or '').strip().upper()
    normalized_type = str(product_type or '').strip()
    if normalized_root in {'MTX', 'TMF'}:
        return True
    if any(token in normalized_name for token in ('小型', '微型', '小台', '小電子', '小金融', '小櫃')):
        return True
    if normalized_type in {'股票', 'ETF', '指數'} and normalized_name.startswith(('小', '微')):
        return True
    return False


def get_futures_session_label(session_names, fallback='未知', root=''):
    """Normalize TAIFEX day/night session labels for the table cell."""
    normalized_names = {str(name).strip() for name in session_names if str(name).strip()}
    has_day_session = any('一般' in name or '日盤' in name for name in normalized_names)
    has_night_session = is_futures_night_session_product(root) or any(
        ('盤後' in name or '夜盤' in name) and not ('一般' in name or '日盤' in name)
        for name in normalized_names
    )
    if has_day_session and has_night_session:
        return '日盤+夜盤'
    if has_day_session:
        return '日盤'
    return str(fallback or '未知')

def snapshot_change_rate(snapshot, price=None):
    """優先採券商快照漲跌幅，舊版物件缺欄位時再由漲跌價反推。"""
    direct_rate = _safe_number(getattr(snapshot, 'change_rate', None))
    if direct_rate is not None:
        return direct_rate
    current_price = _safe_number(price) or _safe_number(getattr(snapshot, 'close', None))
    change = _safe_number(getattr(snapshot, 'change_price', getattr(snapshot, 'change', None)))
    reference = current_price - change if current_price is not None and change is not None else None
    return (change / reference * 100) if reference is not None and reference > 0 else None


def price_change_amount(price, change_rate):
    """由成交價與官方漲跌幅反推相對昨收的價差。"""
    current_price = _safe_number(price)
    rate = _safe_number(change_rate)
    denominator = 1 + rate / 100 if rate is not None else None
    if current_price is None or denominator is None or denominator <= 0:
        return None
    return current_price - current_price / denominator


FUTURES_FIXED_TICK_SIZES = {
    'TX': 1.0, 'MTX': 1.0, 'TMF': 1.0,
    'TE': 0.05, 'TF': 0.2, 'GTF': 0.05,
}


def get_futures_tick_size(root, price, product_type='未知'):
    """Return an orderable TAIFEX tick; unsupported products return None."""
    normalized_root = str(root or '').strip().upper()
    if normalized_root in FUTURES_FIXED_TICK_SIZES:
        return FUTURES_FIXED_TICK_SIZES[normalized_root]
    numeric_price = _safe_number(price)
    if numeric_price is None or numeric_price <= 0:
        return None
    if product_type == 'ETF':
        return 0.01 if numeric_price < 50 else 0.05
    if product_type == '股票':
        if numeric_price < 10:
            return 0.01
        if numeric_price < 50:
            return 0.05
        if numeric_price < 100:
            return 0.1
        if numeric_price < 500:
            return 0.5
        if numeric_price < 2500:
            return 1.0
        return 5.0
    return None


def round_futures_price(value, tick, rounding=ROUND_HALF_UP):
    if tick is None or tick <= 0:
        return None
    decimal_tick = Decimal(str(tick))
    decimal_value = Decimal(str(value))
    rounded = (decimal_value / decimal_tick).to_integral_value(rounding=rounding) * decimal_tick
    return float(rounded)


def clamp_futures_intraday_levels(entry, stop, target, limit_up, limit_down, tick):
    """Keep futures intraday plan prices inside the legal daily price band."""
    upper = _safe_number(limit_up)
    lower = _safe_number(limit_down)
    if tick is None or tick <= 0 or (upper is None and lower is None):
        return entry, stop, target
    if upper is not None and lower is not None and lower > upper:
        return entry, stop, target

    def clamp(value):
        numeric = _safe_number(value)
        if numeric is None:
            return value
        if lower is not None:
            numeric = max(numeric, lower)
        if upper is not None:
            numeric = min(numeric, upper)
        return round_futures_price(numeric, tick, ROUND_HALF_UP)

    return clamp(entry), clamp(stop), clamp(target)


def futures_rollover_cache_key(reference_dt=None):
    """讓 Streamlit cache 在 13:30 結算切點自動換一個 key。"""
    tz_tw = pytz.timezone('Asia/Taipei')
    now_tw = _normalize_futures_taipei_datetime(
        reference_dt or datetime.now(tz_tw), dt_time.min,
    ) or datetime.now(tz_tw)
    phase = 'post_settlement' if now_tw.time() >= FUTURES_SETTLEMENT_CUTOFF else 'pre_settlement'
    return f'{now_tw:%Y%m%d}_{phase}'


@st.cache_data(ttl=300, max_entries=4, show_spinner=False)
def fetch_futures_strategy_universe(rollover_key=None):
    """整併期交所成交量、近月契約名稱與保證金，供期貨戰略室排序。"""
    index_futures_roots = {
        'TX', 'MTX', 'TMF', 'T5F', 'TE', 'ZEF', 'TF', 'ZFF', 'TBF', 'GTF',
        'TQF', 'E4F', 'BTF', 'SOF', 'SHF', 'JTF', 'UDF', 'SPF', 'UNF', 'PUF', 'UKF'
    }
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (compatible; StockApp/1.0; +https://openapi.taifex.com.tw/)',
        'Referer': 'https://openapi.taifex.com.tw/',
        'Cache-Control': 'no-cache',
    }

    def fetch_endpoint(endpoint):
        url = f'https://openapi.taifex.com.tw/v1/{endpoint}'
        last_error = None
        for attempt in range(2):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params={'_': int(time.time())} if attempt else None,
                    timeout=(5, 12),
                )
                response.raise_for_status()
                response.encoding = 'utf-8-sig'
                payload = response.json()
                if not isinstance(payload, list) or not payload:
                    raise ValueError("期交所回傳空白或非預期格式")
                return payload
            except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < 1:
                    time.sleep(0.6 * (attempt + 1))
        raise RuntimeError(f'{endpoint} 連線失敗（已重試 2 次）：{last_error}')

    product_meta = {}
    errors = []
    sync_dates = []

    try:
        for item in fetch_endpoint('SingleStockFuturesMargining'):
            root = str(item.get('Contract', '')).strip().upper()
            if not root:
                continue
            name = str(item.get('ContractName', root)).strip()
            underlying_code = str(item.get('UnderlyingSecurityCode', '')).strip()
            is_small = is_small_futures_product(name, root, '股票')
            product_meta[root] = {
                '名稱': name, '標的代號': underlying_code, 'ETF期貨': False, '指數期貨': False,
                '商品類型': '股票',
                '小型期貨': is_small, '乘數': 100 if is_small else 2000,
                '原始保證金率': _safe_number(item.get('InitialMarginRate')),
                '維持保證金率': _safe_number(item.get('MaintenanceMarginRate')),
                '原始保證金固定': None, '維持保證金固定': None,
            }
            if item.get('Date'):
                sync_dates.append(str(item['Date']))
    except Exception as exc:
        errors.append(f'個股期貨保證金：{exc}')

    try:
        for item in fetch_endpoint('SingleStockFuturesETFMargining'):
            root = str(item.get('Contract', '')).strip().upper()
            if not root:
                continue
            name = _decode_taifex_legacy_text(item.get('ContractName', root))
            underlying_code = str(item.get('UnderlyingSecurityCode', '')).strip()
            is_small = is_small_futures_product(name, root, 'ETF')
            product_meta[root] = {
                '名稱': name or f'{underlying_code} ETF期貨', '標的代號': underlying_code,
                'ETF期貨': True, '指數期貨': False, '小型期貨': is_small,
                '商品類型': 'ETF',
                '乘數': 100 if is_small else 1000,
                '原始保證金率': 0, '維持保證金率': 0,
                '原始保證金固定': _safe_number(item.get('InitialMargin')),
                '維持保證金固定': _safe_number(item.get('MaintenanceMargin')),
            }
            if item.get('Date'):
                sync_dates.append(str(item['Date']))
    except Exception as exc:
        errors.append(f'ETF期貨保證金：{exc}')

    index_root_map = {
        '臺股期貨': ('TX', False), '台股期貨': ('TX', False),
        '小型臺指': ('MTX', True), '小型台指': ('MTX', True),
        '微型臺指': ('TMF', True), '微型台指': ('TMF', True),
        '電子期貨': ('TE', False), '金融期貨': ('TF', False),
    }
    try:
        for item in fetch_endpoint('IndexFuturesAndOptionsMargining'):
            raw_name = str(item.get('Contract', '')).strip()
            if '客製化' in raw_name:
                continue
            match = next((value for key, value in index_root_map.items() if key in raw_name), None)
            if not match:
                continue
            root, is_small = match
            product_meta[root] = {
                '名稱': raw_name, '標的代號': root, 'ETF期貨': False, '指數期貨': True,
                '商品類型': '指數',
                '小型期貨': is_small, '乘數': 1,
                '原始保證金率': 0, '維持保證金率': 0,
                '原始保證金固定': _safe_number(item.get('InitialMargin')),
                '維持保證金固定': _safe_number(item.get('MaintenanceMargin')),
            }
            if item.get('Date'):
                sync_dates.append(str(item['Date']))
    except Exception as exc:
        errors.append(f'指數期貨保證金：{exc}')

    try:
        market_rows = fetch_endpoint('DailyMarketReportFut')
    except Exception as exc:
        return pd.DataFrame(), {'updated': None, 'margin_date': max(sync_dates) if sync_dates else None, 'errors': errors + [f'每日行情：{exc}']}

    grouped = {}
    for item in market_rows:
        root = str(item.get('Contract', '')).strip().upper()
        month = str(item.get('ContractMonth(Week)', '')).strip()
        if not root or not re.fullmatch(r'\d{6}', month):
            continue
        if root not in product_meta:
            product_meta[root] = {
                '名稱': f'{root} 期貨', '標的代號': root,
                'ETF期貨': False, '指數期貨': root in index_futures_roots,
                '商品類型': '未知',
                '小型期貨': is_small_futures_product(
                    f'{root} 期貨', root, '指數'
                ),
                '乘數': None, '原始保證金率': None, '維持保證金率': None,
                '原始保證金固定': None, '維持保證金固定': None,
            }
        key = (root, month)
        grouped.setdefault(key, []).append(item)

    # 結算日 13:30 後，官方檔案仍可能保留已結算近月列；先移除再計算
    # 月份順位，否則 09 會被標成次月並被「隱藏次月」誤刪。
    rollover_now = datetime.now(pytz.timezone('Asia/Taipei'))
    active_grouped = {}
    settled_contracts = []
    for key, rows in grouped.items():
        root, month = key
        meta = product_meta.get(root, {})
        actual_last = _futures_actual_last_trading_date(rows)
        if is_futures_contract_settled(
            root, month, meta.get('商品類型', '未知'),
            actual_last_trading=actual_last, now_dt=rollover_now,
            standard_settlement=(
                meta.get('商品類型') in FUTURES_STANDARD_SETTLEMENT_TYPES
                or bool(meta.get('指數期貨'))
                or bool(meta.get('ETF期貨'))
                or root in FUTURES_STANDARD_SETTLEMENT_ROOTS
            ),
        ):
            settled_contracts.append(f'{root}:{month}')
            continue
        active_grouped[key] = rows
    grouped = active_grouped

    month_map = {}
    for root, month in grouped:
        month_map.setdefault(root, []).append(month)
    month_rank = {
        (root, month): rank
        for root, months in month_map.items()
        for rank, month in enumerate(sorted(set(months)))
    }

    result_rows = []
    for (root, month), rows in grouped.items():
        meta = product_meta[root]
        general_rows = [
            row for row in rows
            if (
                '一般' in str(row.get('TradingSession', '')).strip()
                or '日盤' in str(row.get('TradingSession', '')).strip()
            )
        ]
        session_rows = general_rows or [rows[0]]
        quote_row = session_rows[0]
        session_names = {
            str(row.get('TradingSession', '')).strip()
            for row in rows if str(row.get('TradingSession', '')).strip()
        }
        session_label = get_futures_session_label(
            session_names, quote_row.get('TradingSession', '未知'), root
        )
        valid_highs = [_safe_number(row.get('High')) for row in session_rows]
        valid_lows = [_safe_number(row.get('Low')) for row in session_rows]
        high = max((value for value in valid_highs if value is not None), default=None)
        low = min((value for value in valid_lows if value is not None), default=None)
        close = _safe_number(quote_row.get('Last')) or _safe_number(quote_row.get('SettlementPrice'))
        open_price = _safe_number(quote_row.get('Open'))
        change = _safe_number(quote_row.get('Change'), 0) or 0
        change_pct = _safe_number(quote_row.get('%'), 0) or 0
        volume = int(sum(_safe_number(row.get('Volume'), 0) or 0 for row in session_rows))
        open_interest = int(_safe_number(quote_row.get('OpenInterest'), 0) or 0)
        reference = close - change if close is not None else None
        futures_tick = get_futures_tick_size(root, reference, meta.get('商品類型', '未知'))
        if reference and reference > 0 and futures_tick is not None:
            limit_up = round_futures_price(reference * 1.10, futures_tick, ROUND_FLOOR)
            limit_down = round_futures_price(reference * 0.90, futures_tick, ROUND_CEILING)
        else:
            limit_up = limit_down = None
        if meta['原始保證金固定'] is not None:
            initial_margin = meta['原始保證金固定']
            maintenance_margin = meta['維持保證金固定']
        elif (
            close is not None
            and meta.get('乘數') is not None
            and (_safe_number(meta.get('原始保證金率')) or 0) > 0
            and (_safe_number(meta.get('維持保證金率')) or 0) > 0
        ):
            initial_margin = round(close * meta['乘數'] * meta['原始保證金率'] / 100)
            maintenance_margin = round(close * meta['乘數'] * meta['維持保證金率'] / 100)
        else:
            initial_margin = maintenance_margin = None

        result_rows.append({
            '契約鍵': f'{root}:{month}', '期貨代碼': root, '契約月份': month,
            '名稱': meta['名稱'], '標的代號': meta['標的代號'],
            '當日成交口數': volume, '未平倉量': open_interest,
            '開盤價': open_price, '當日高': high, '當日低': low,
            '收盤價': close, '漲跌幅': change_pct,
            '當日漲停價': limit_up, '當日跌停價': limit_down,
            '所需保證金': initial_margin, '維持保證金': maintenance_margin,
            '原始保證金率': meta['原始保證金率'], '維持保證金率': meta['維持保證金率'],
            '乘數': meta['乘數'], 'ETF期貨': meta['ETF期貨'], '商品類型': meta.get('商品類型', '未知'),
            '指數期貨': meta.get('指數期貨', root in index_futures_roots), '小型期貨': meta['小型期貨'],
            '次月期貨': month_rank[(root, month)] > 0,
            '月份順位': month_rank[(root, month)],
            '交易時段': session_label,
            '資料日期': str(quote_row.get('Date', '')),
            '契約最後交易日': (
                _futures_actual_last_trading_date(rows).strftime('%Y/%m/%d %H:%M:%S')
                if _futures_actual_last_trading_date(rows) is not None else None
            ),
        })

    result = pd.DataFrame(result_rows)
    if not result.empty:
        result = result.sort_values(['當日成交口數', '月份順位'], ascending=[False, True]).reset_index(drop=True)
    return result, {
        'updated': max((str(row.get('Date', '')) for row in market_rows), default=None),
        'margin_date': max(sync_dates) if sync_dates else None,
        'errors': errors,
        'settled_contracts_removed': settled_contracts,
        'rollover_key': rollover_key or futures_rollover_cache_key(rollover_now),
    }

SHIOAJI_FUTURES_ROOT_ALIASES = {
    # TAIFEX OpenAPI product codes differ from Shioaji roots for these indices.
    'TX': 'TXF', 'MTX': 'MXF', 'TE': 'EXF', 'TF': 'FXF',
}
FUTURES_MONTH_CODE = {
    1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F',
    7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L',
}


def shioaji_futures_root_candidates(root):
    normalized = str(root or '').strip().upper()
    candidates = [SHIOAJI_FUTURES_ROOT_ALIASES.get(normalized), normalized]
    return [value for index, value in enumerate(candidates) if value and value not in candidates[:index]]


def expected_shioaji_futures_code(root, contract_month):
    match = re.search(r'(\d{4})(\d{2})', str(contract_month or ''))
    if not match:
        return ''
    year, month = int(match.group(1)), int(match.group(2))
    month_code = FUTURES_MONTH_CODE.get(month)
    roots = shioaji_futures_root_candidates(root)
    return f'{roots[0]}{month_code}{year % 10}' if roots and month_code else ''


def is_small_stock_futures_record(record):
    """True only for mini single-stock futures (not index/ETF mini futures)."""
    product_type = str(record.get('商品類型', '') or '').strip()
    return (
        bool(record.get('小型期貨', False))
        and product_type == '股票'
        and not bool(record.get('指數期貨', False))
        and not bool(record.get('ETF期貨', False))
    )


def _contract_delivery_month(contract):
    value = getattr(contract, 'delivery_month', None) or getattr(contract, 'delivery_date', '')
    match = re.search(r'\d{6}', str(value).replace('-', '').replace('/', ''))
    return match.group() if match else ''


def _is_actual_futures_contract(contract):
    code = str(getattr(contract, 'code', '') or '').upper()
    return bool(code) and not code.endswith(('R1', 'R2')) and '/' not in code


def _list_shioaji_futures_contracts(api):
    """Load the Shioaji 1.7 lazy futures catalog, with legacy compatibility."""
    if api is None:
        return []
    contracts = []
    try:
        security_type = sj.SecurityType.Futures if sj is not None else 'FUT'
        contracts.extend(list(api.contracts.list(security_type) or []))
    except (AttributeError, TypeError, ValueError):
        pass
    if not contracts:
        try:
            for category in api.Contracts.Futures:
                try:
                    contracts.extend(list(category))
                except TypeError:
                    continue
        except (AttributeError, TypeError):
            pass
    unique = {}
    for contract in contracts:
        code = str(getattr(contract, 'code', '') or '').upper()
        if code:
            unique.setdefault(code, contract)
    return list(unique.values())


def resolve_shioaji_futures_contract(api, root, contract_month):
    """依商品代碼與年月尋找 Shioaji 實際契約，支援 1.7 延遲載入。"""
    if api is None:
        return None
    target_month_match = re.search(r'\d{6}', str(contract_month or ''))
    target_month = target_month_match.group() if target_month_match else ''
    if not target_month:
        return None
    roots = shioaji_futures_root_candidates(root)
    # 結算切點後不再向 Shioaji 查詢已結算的標準近月，避免舊契約快照
    # 失敗後又被寫回即時排行／分析快取。
    settlement_checker = globals().get('is_futures_contract_settled')
    standard_roots = globals().get('FUTURES_STANDARD_SETTLEMENT_ROOTS', set())
    if callable(settlement_checker) and settlement_checker(
        root, target_month, now_dt=datetime.now(pytz.timezone('Asia/Taipei')),
        standard_settlement=(str(root or '').strip().upper() in standard_roots),
    ):
        return None
    candidates = []

    # Shioaji 1.7 no longer downloads the whole futures catalog at login.
    # Querying the root is both faster and reliable on a fresh mobile session.
    for query_root in roots:
        try:
            try:
                found = api.contracts.futures(query_root, delivery_month=target_month)
            except TypeError:
                found = api.contracts.futures(query_root)
            candidates.extend(list(found or []))
        except (AttributeError, KeyError, TypeError, ValueError):
            continue

    # Older SDK/session objects still expose category attributes.
    if not candidates:
        for query_root in roots:
            try:
                candidates.extend(list(getattr(api.Contracts.Futures, query_root)))
            except (AttributeError, KeyError, TypeError):
                continue
    if not candidates:
        candidates = _list_shioaji_futures_contracts(api)

    matched = [
        contract for contract in candidates
        if _is_actual_futures_contract(contract)
        and _contract_delivery_month(contract) == target_month
        and (
            str(getattr(contract, 'root', '') or '').upper() in roots
            or any(str(getattr(contract, 'code', '') or '').upper().startswith(item) for item in roots)
        )
    ]
    if not matched:
        return None
    return min(
        matched,
        key=lambda item: str(
            getattr(item, 'delivery_date', None) or getattr(item, 'last_trading_date', None)
            or getattr(item, 'delivery_month', '999999')
        ),
    )

def calculate_futures_strategy_levels(row, strategy_mode='當沖', direction_choice='自動', kbars=None):
    """計算期貨支撐壓力與條件式進出場點位；無即時 K 棒時採官方日行情備援。"""
    root = str(row.get('期貨代碼', ''))
    close = _safe_number(row.get('收盤價'))
    open_price = _safe_number(row.get('開盤價'))
    high = _safe_number(row.get('當日高'))
    low = _safe_number(row.get('當日低'))
    atr = None
    vwap = None

    if isinstance(kbars, pd.DataFrame) and not kbars.empty:
        data = kbars.copy().sort_index().dropna(subset=['High', 'Low', 'Close'])
        if not data.empty:
            close = float(data['Close'].iloc[-1])
            previous_close = data['Close'].shift(1)
            true_range = pd.concat([
                data['High'] - data['Low'],
                (data['High'] - previous_close).abs(),
                (data['Low'] - previous_close).abs(),
            ], axis=1).max(axis=1)
            if strategy_mode == '當沖':
                session_kind = 'night' if (
                    datetime.now(pytz.timezone('Asia/Taipei')).time() >= dt_time(15, 0)
                    or datetime.now(pytz.timezone('Asia/Taipei')).time() < dt_time(5, 0)
                ) else 'day'
                session_labels = pd.Series(
                    [
                        (
                            get_futures_trading_date(
                                ts.to_pydatetime().replace(
                                    tzinfo=pytz.timezone('Asia/Taipei')
                                ) if ts.tzinfo is None else ts.to_pydatetime()
                            ).date(),
                            'night' if ts.time() >= dt_time(15, 0) or ts.time() < dt_time(5, 0) else 'day',
                        )
                        for ts in data.index
                    ],
                    index=data.index,
                )
                matching_sessions = session_labels[session_labels.map(lambda value: value[1] == session_kind)]
                selected_session = matching_sessions.iloc[-1] if not matching_sessions.empty else session_labels.iloc[-1]
                recent = data.loc[
                    session_labels.map(lambda value: value == selected_session)
                ].tail(36)
                reference_bars = recent.iloc[:-1] if len(recent) >= 4 else recent
                support = float(reference_bars['Low'].min())
                resistance = float(reference_bars['High'].max())
                atr = float(true_range.tail(min(14, len(true_range))).mean())
                if 'Volume' in recent.columns and float(recent['Volume'].sum()) > 0:
                    typical = (recent['High'] + recent['Low'] + recent['Close']) / 3
                    vwap = float((typical * recent['Volume']).sum() / recent['Volume'].sum())
            else:
                trade_date = pd.Series(
                    [
                        get_futures_trading_date(
                            ts.to_pydatetime().replace(
                                tzinfo=pytz.timezone('Asia/Taipei')
                            ) if ts.tzinfo is None else ts.to_pydatetime()
                        ).date()
                        for ts in data.index
                    ],
                    index=data.index,
                )
                daily = data.assign(_trade_date=trade_date.values).groupby('_trade_date').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last',
                    **({'Volume': 'sum'} if 'Volume' in data.columns else {})
                }).dropna()
                recent_daily = daily.tail(min(20, len(daily)))
                if not recent_daily.empty:
                    reference_daily = recent_daily.iloc[:-1] if len(recent_daily) >= 4 else recent_daily
                    support = float(reference_daily['Low'].min())
                    resistance = float(reference_daily['High'].max())
                    daily_prev = daily['Close'].shift(1)
                    daily_tr = pd.concat([
                        daily['High'] - daily['Low'],
                        (daily['High'] - daily_prev).abs(),
                        (daily['Low'] - daily_prev).abs(),
                    ], axis=1).max(axis=1)
                    atr = float(daily_tr.tail(min(14, len(daily_tr))).mean())
                else:
                    support, resistance = low, high
        else:
            support, resistance = low, high
    else:
        support, resistance = low, high

    if close is None or support is None or resistance is None:
        return {'支撐壓力': '資料不足', '進出場點位': '資料不足', '方向': '—'}

    direction = direction_choice
    if direction == '自動':
        comparison = vwap if strategy_mode == '當沖' and vwap is not None else (open_price if open_price is not None else close)
        direction = '偏多' if close >= comparison else '偏空'
    tick = get_futures_tick_size(root, close, str(row.get('商品類型', '未知')))
    if tick is None:
        return {
            '支撐壓力': f'支 {fmt_price(support)}｜壓 {fmt_price(resistance)}',
            '進出場點位': '商品跳動點未設定', '方向': direction,
        }
    round_future = lambda value: round_futures_price(value, tick)
    observed_range = max(float(resistance) - float(support), tick * 2)
    risk_distance = max((atr or observed_range) * (0.55 if strategy_mode == '當沖' else 1.0), tick * 2)
    limit_up = _safe_number(row.get('當日漲停價')) if strategy_mode == '當沖' else None
    limit_down = _safe_number(row.get('當日跌停價')) if strategy_mode == '當沖' else None
    if direction == '偏多':
        entry = round_future(float(resistance) + tick)
        stop = round_future(entry - risk_distance)
        target = round_future(entry + (entry - stop) * 1.5)
        trigger = '突破壓力後回測不破'
    else:
        entry = round_future(float(support) - tick)
        stop = round_future(entry + risk_distance)
        target = round_future(entry - (stop - entry) * 1.5)
        trigger = '跌破支撐、反彈未能站回'
    if strategy_mode == '當沖':
        entry, stop, target = clamp_futures_intraday_levels(
            entry, stop, target, limit_up, limit_down, tick,
        )
    return {
        '支撐壓力': f'支 {fmt_price(support)}｜壓 {fmt_price(resistance)}',
        '進出場點位': f'進 {fmt_price(entry)}｜停 {fmt_price(stop)}｜目 {fmt_price(target)}',
        '方向': direction, '觸發條件': trigger,
        'VWAP': vwap, 'ATR': atr,
    }

def enrich_futures_strategy_rows(rows, strategy_mode, market_bias='盤整'):
    """加入可關閉的進場信心、流動性與到期資訊，不改動原成交量排序。"""
    if rows.empty:
        return rows
    enriched = rows.copy()
    trading_date = get_futures_trading_date(datetime.now(pytz.timezone('Asia/Taipei'))).date()
    for index, row in enriched.iterrows():
        volume = int(_safe_number(row.get('當日成交口數'), 0) or 0)
        open_interest = int(_safe_number(row.get('未平倉量'), 0) or 0)
        volume_oi_ratio = volume / open_interest if open_interest > 0 else None
        bid = _safe_number(row.get('買價'))
        ask = _safe_number(row.get('賣價'))
        price = _safe_number(row.get('收盤價'))
        tick = get_futures_tick_size(
            row.get('期貨代碼'), price, str(row.get('商品類型', '未知')),
        )
        spread_ticks = (
            (ask - bid) / tick
            if tick is not None and bid is not None and ask is not None and ask >= bid and tick > 0
            else None
        )
        quote_time = row.get('報價時間')
        if quote_time:
            data_health = build_data_health(quote_time, required_ready=price is not None, live_expected=True)
        else:
            data_health = build_data_health(None, required_ready=price is not None, live_expected=False)

        if data_health.startswith('🔴'):
            liquidity = '⛔ 報價過期'
        elif spread_ticks is not None and spread_ticks > 2:
            liquidity = f'🟡 價差 {spread_ticks:.0f} 跳'
        elif spread_ticks is not None and spread_ticks <= 1 and volume >= 1000 and open_interest >= 100:
            liquidity = '🟢 良好'
        elif volume >= 100 and open_interest > 0:
            liquidity = '🟡 普通'
        else:
            liquidity = '🔴 偏低'

        expiry = futures_expiry_date(row.get('契約月份'))
        days_to_expiry = (expiry - trading_date).days if expiry else None
        if days_to_expiry is None:
            rollover = '—'
        elif days_to_expiry < 0:
            rollover = '⛔ 已到期'
        elif days_to_expiry <= 3:
            rollover = f'🔴 {days_to_expiry}日'
        elif days_to_expiry <= 7:
            rollover = f'🟡 {days_to_expiry}日'
        else:
            rollover = f'{days_to_expiry}日'

        plan_text = row.get('進出場點位')
        plan = parse_trade_plan_numbers(plan_text)
        direction = str(row.get('方向', ''))
        state = '⚪ 資料不足'
        if price is not None and plan['entry'] is not None and plan['stop'] is not None and plan['target'] is not None:
            is_long = direction == '偏多'
            if data_health.startswith('🔴') or rollover.startswith('⛔'):
                state = '⛔ 暫停'
            elif (is_long and price <= plan['stop']) or (not is_long and price >= plan['stop']):
                state = '⛔ 條件失效'
            elif (is_long and price >= plan['target']) or (not is_long and price <= plan['target']):
                state = '⛔ 已過目標'
            elif (is_long and price >= plan['entry']) or (not is_long and price <= plan['entry']):
                state = '✅ 已觸發'
            else:
                risk_distance = abs(plan['entry'] - plan['stop'])
                entry_distance = abs(plan['entry'] - price)
                state = '🟡 接近觸發' if risk_distance > 0 and entry_distance <= risk_distance * 0.5 else '⚪ 等待'
            if not quote_time and data_health.startswith('⚪'):
                state = '⚪ 待即時報價'

        confidence_base = 0
        confidence_base += 20 if volume >= 1000 else (12 if volume >= 100 else 4)
        confidence_base += 10 if open_interest >= 100 else (5 if open_interest > 0 else 0)
        confidence_base += 15 if spread_ticks is not None and spread_ticks <= 1 else (8 if spread_ticks is None or spread_ticks <= 2 else 0)
        confidence_base += 15 if data_health.startswith('🟢') else (8 if not data_health.startswith(('🔴', '⚪')) else 0)
        alignment = calculate_market_alignment(direction, market_bias)
        confidence_base += 15 if alignment.startswith('🟢') else (8 if alignment.startswith('⚪') else 0)
        confidence_base += 25 if state == '✅ 已觸發' else (15 if state.startswith('🟡') else (8 if state == '⚪ 等待' else 0))
        confidence = calculate_entry_confidence(
            confidence_base, state, price, plan_text, direction, data_health, alignment,
            '量能／未平倉、價差、報價、方向與觸發位置綜合判讀'
        )
        enriched.at[index, '量倉比'] = round(volume_oi_ratio, 2) if volume_oi_ratio is not None else None
        enriched.at[index, '買賣價差'] = f'{spread_ticks:.0f}跳' if spread_ticks is not None else '—'
        enriched.at[index, '可交易性'] = liquidity
        enriched.at[index, '資料狀態'] = data_health
        enriched.at[index, '到期提醒'] = rollover
        enriched.at[index, '訊號狀態'] = state
        enriched.at[index, '市場一致'] = alignment
        enriched.at[index, '信心分'] = confidence['score']
        enriched.at[index, '信心判讀'] = confidence['label']
        enriched.at[index, '_信心明細'] = confidence['detail']
        enriched.at[index, '_附加可記錄'] = state == '✅ 已觸發' and not liquidity.startswith(('⛔', '🔴'))
    return enriched

def fetch_futures_contract_kbars(api, contract, lookback_days=20):
    """取得指定期貨契約 K 棒；保留夜盤資料供當沖與波段分析。"""
    if api is None or contract is None:
        return pd.DataFrame()
    try:
        now = datetime.now(pytz.timezone('Asia/Taipei'))
        raw = api.kbars(
            contract=contract,
            start=(now - timedelta(days=lookback_days)).strftime('%Y-%m-%d'),
            end=now.strftime('%Y-%m-%d')
        )
        if not raw or not hasattr(raw, 'ts') or len(raw.ts) == 0:
            return pd.DataFrame()
        data = pd.DataFrame({**raw})
        data['ts'] = pd.to_datetime(data['ts'])
        if data['ts'].dt.tz is not None:
            data['ts'] = data['ts'].dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
        data = data.set_index('ts').sort_index()
        stream_quote = get_stream_quotes(api, [contract], snapshot_fallback=False)
        return merge_stream_quote_into_intraday(
            data, stream_quote[0] if stream_quote else None, '1m'
        )
    except Exception:
        return pd.DataFrame()

def update_futures_live_rows(rows, api, strategy_mode, direction_choice, include_analysis=True):
    """批次更新顯示中的實際契約快照，並選擇性重算支撐壓力。"""
    if rows.empty or api is None:
        return rows, 0
    updated, _ = filter_active_futures_rows(rows)
    if updated.empty:
        updated.attrs['resolved_contract_count'] = 0
        return updated, 0
    resolved = []
    for index, row in updated.iterrows():
        contract = resolve_shioaji_futures_contract(api, row['期貨代碼'], row['契約月份'])
        if contract is not None:
            resolved.append((index, contract))
    updated.attrs['resolved_contract_count'] = len(resolved)
    if not resolved:
        return updated, 0

    snapshots = {}
    try:
        for start in range(0, len(resolved), 30):
            batch = resolved[start:start + 30]
            batch_snapshots = get_stream_quotes(api, [contract for _, contract in batch])
            for (index, _), snapshot in zip(batch, batch_snapshots):
                snapshots[index] = snapshot
    except Exception:
        snapshots = {}

    update_count = 0
    for index, contract in resolved:
        snapshot = snapshots.get(index)
        if snapshot is not None:
            price = _safe_number(getattr(snapshot, 'close', None)) or _safe_number(getattr(snapshot, 'open', None))
            if price is not None and price > 0:
                updated.at[index, '收盤價'] = price
                change = _safe_number(getattr(snapshot, 'change_price', getattr(snapshot, 'change', None)), 0) or 0
                reference = price - change
                live_change_rate = snapshot_change_rate(snapshot, price)
                updated.at[index, '漲跌幅'] = live_change_rate if live_change_rate is not None else 0
                updated.at[index, '當日高'] = _safe_number(getattr(snapshot, 'high', None), updated.at[index, '當日高'])
                updated.at[index, '當日低'] = _safe_number(getattr(snapshot, 'low', None), updated.at[index, '當日低'])
                updated.at[index, '當日成交口數'] = int(_safe_number(getattr(snapshot, 'total_volume', None), updated.at[index, '當日成交口數']) or 0)
                updated.at[index, '買價'] = _safe_number(getattr(snapshot, 'buy_price', None))
                updated.at[index, '賣價'] = _safe_number(getattr(snapshot, 'sell_price', None))
                updated.at[index, '報價時間'] = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S')
                if reference > 0:
                    tick = get_futures_tick_size(
                        updated.at[index, '期貨代碼'], reference,
                        str(updated.at[index, '商品類型']) if '商品類型' in updated.columns else '未知',
                    )
                    updated.at[index, '當日漲停價'] = (
                        round_futures_price(reference * 1.10, tick, ROUND_FLOOR)
                        if tick is not None else None
                    )
                    updated.at[index, '當日跌停價'] = (
                        round_futures_price(reference * 0.90, tick, ROUND_CEILING)
                        if tick is not None else None
                    )
                initial_rate = _safe_number(updated.at[index, '原始保證金率'], 0) or 0
                maintenance_rate = _safe_number(updated.at[index, '維持保證金率'], 0) or 0
                multiplier = _safe_number(updated.at[index, '乘數'], 0) or 0
                if initial_rate > 0 and multiplier > 0:
                    updated.at[index, '所需保證金'] = round(price * multiplier * initial_rate / 100)
                    updated.at[index, '維持保證金'] = round(price * multiplier * maintenance_rate / 100)
                update_count += 1
        kbars = fetch_futures_contract_kbars(api, contract, 20 if strategy_mode == '當沖' else 60) if include_analysis else None
        analysis = calculate_futures_strategy_levels(updated.loc[index], strategy_mode, direction_choice, kbars)
        for column, value in analysis.items():
            updated.at[index, column] = value
        updated.at[index, '實際契約'] = str(getattr(contract, 'code', updated.at[index, '期貨代碼']))
    return updated, update_count

def update_futures_universe_live(rows, api):
    """以共享串流更新完整期貨清單，供盤中／夜盤提前重排成交量。"""
    if rows.empty or api is None:
        return rows, 0
    updated, _ = filter_active_futures_rows(rows)
    if updated.empty:
        return updated, 0
    roots = sorted(set(updated['期貨代碼'].astype(str).str.upper()), key=len, reverse=True)
    contract_map = {}
    available_contracts = _list_shioaji_futures_contracts(api)
    contracts_by_code = {
        str(getattr(contract, 'code', '') or '').upper(): contract
        for contract in available_contracts if getattr(contract, 'code', None)
    }
    # api.contracts.list() intentionally returns compact Contract objects without
    # delivery_month.  Their actual code still deterministically identifies the
    # month, so map official rows before inspecting richer FuturesInfo objects.
    for _, row in updated.iterrows():
        root = str(row['期貨代碼']).upper()
        month = str(row['契約月份'])
        expected_code = expected_shioaji_futures_code(root, month)
        if expected_code in contracts_by_code:
            contract_map[(root, month)] = contracts_by_code[expected_code]

    for contract in available_contracts:
        code = str(getattr(contract, 'code', '')).upper()
        if not _is_actual_futures_contract(contract):
            continue
        month = _contract_delivery_month(contract)
        contract_root = str(getattr(contract, 'root', '') or '').upper()
        root = next(
            (
                candidate for candidate in roots
                if contract_root in shioaji_futures_root_candidates(candidate)
                or any(code.startswith(alias) for alias in shioaji_futures_root_candidates(candidate))
            ),
            None,
        )
        if root and month:
            contract_map.setdefault((root, month), contract)

    resolved = [
        (index, contract_map.get((str(row['期貨代碼']).upper(), str(row['契約月份']))))
        for index, row in updated.iterrows()
    ]
    resolved = [(index, contract) for index, contract in resolved if contract is not None]
    if not resolved:
        return updated, 0

    snapshots = {}
    for start in range(0, len(resolved), 200):
        batch = resolved[start:start + 200]
        try:
            batch_snapshots = get_stream_quotes(api, [contract for _, contract in batch])
        except Exception:
            continue
        for (index, _), snapshot in zip(batch, batch_snapshots):
            snapshots[index] = snapshot

    update_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S')
    update_count = 0
    for index, snapshot in snapshots.items():
        price = _safe_number(getattr(snapshot, 'close', None)) or _safe_number(getattr(snapshot, 'open', None))
        total_volume = _safe_number(getattr(snapshot, 'total_volume', None))
        if price is None and total_volume is None:
            continue
        if price is not None and price > 0:
            updated.at[index, '收盤價'] = price
            live_change_rate = snapshot_change_rate(snapshot, price)
            if live_change_rate is not None:
                updated.at[index, '漲跌幅'] = live_change_rate
        if total_volume is not None:
            updated.at[index, '當日成交口數'] = int(total_volume)
        updated.at[index, '當日高'] = _safe_number(getattr(snapshot, 'high', None), updated.at[index, '當日高'])
        updated.at[index, '當日低'] = _safe_number(getattr(snapshot, 'low', None), updated.at[index, '當日低'])
        updated.at[index, '買價'] = _safe_number(getattr(snapshot, 'buy_price', None))
        updated.at[index, '賣價'] = _safe_number(getattr(snapshot, 'sell_price', None))
        updated.at[index, '報價時間'] = update_time
        update_count += 1
    if update_count:
        updated = updated.sort_values(
            ['當日成交口數', '月份順位'], ascending=[False, True]
        ).reset_index(drop=True)
    return updated, update_count


OPENING_SIGNAL_WEIGHTS = {
    'TX_NIGHT': 30,
    'DJI': 5, 'NASDAQ': 7, 'SP500': 6, 'SOX': 12,
    'NQ_FUT': 12, 'YM_FUT': 8,
    'NIKKEI': 10, 'KOSPI': 10,
}
OPENING_SIGNAL_CACHE_VERSION = 6

NASDAQ_MARKET_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'
    ),
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://www.nasdaq.com',
    'Referer': 'https://www.nasdaq.com/',
}


def _parse_nasdaq_number(value):
    """將 Nasdaq 回傳的逗號、美元符號價格轉成浮點數。"""
    if value is None:
        return None
    cleaned = re.sub(r'[^0-9.\-]', '', str(value))
    return _safe_number(cleaned)


def _nth_weekday(year, month, weekday, occurrence):
    """回傳指定月份第 occurrence 個 weekday。"""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday(year, month, weekday):
    """回傳指定月份最後一個 weekday。"""
    last_day = calendar.monthrange(year, month)[1]
    candidate = date(year, month, last_day)
    return candidate - timedelta(days=(candidate.weekday() - weekday) % 7)


def _easter_sunday(year):
    """Meeus/Jones/Butcher 演算法，用來推算美股休市的耶穌受難日。"""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day_value = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day_value)


def _observed_us_holiday(day_value):
    """美國固定日期假日遇週末時的交易所補休日。"""
    if day_value.weekday() == 5:
        return day_value - timedelta(days=1)
    if day_value.weekday() == 6:
        return day_value + timedelta(days=1)
    return day_value


def _us_market_holidays(year):
    """建立 NYSE／Nasdaq 共同主要休市日，供盤前資料日期校驗。"""
    holidays = {
        _observed_us_holiday(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),       # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),       # Presidents' Day
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),         # Memorial Day
        _observed_us_holiday(date(year, 6, 19)),
        _observed_us_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),       # Labor Day
        _nth_weekday(year, 11, 3, 4),      # Thanksgiving
        _observed_us_holiday(date(year, 12, 25)),
    }
    # 12/31 可能是下一年元旦的補休日。
    next_new_year = _observed_us_holiday(date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays.add(next_new_year)
    return holidays


def _expected_us_close_date(taiwan_trading_day):
    """臺股開盤前應已完成的最近一個美股交易日。"""
    candidate = taiwan_trading_day - timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in _us_market_holidays(candidate.year):
        candidate -= timedelta(days=1)
    return candidate


def fetch_yahoo_us_index_close_signals(trading_day, expected_date, requested_keys=None):
    """取得真正美股指數日線；資料日期不正確時一律不採用。"""
    errors = []
    instruments = {
        'DJI': ('道瓊工業指數', '^DJI'),
        'NASDAQ': ('Nasdaq 綜合指數', '^IXIC'),
        'SP500': ('標普 500 指數', '^GSPC'),
        'SOX': ('費城半導體指數', '^SOX'),
    }
    if requested_keys is not None:
        instruments = {key: value for key, value in instruments.items() if key in requested_keys}
    period_start = int(pytz.UTC.localize(
        datetime.combine(expected_date - timedelta(days=12), dt_time.min)
    ).timestamp())
    period_end = int(pytz.UTC.localize(
        datetime.combine(trading_day + timedelta(days=1), dt_time.min)
    ).timestamp())

    def fetch_one(item):
        key, (label, symbol) = item
        try:
            response = requests.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}',
                params={
                    'period1': period_start, 'period2': period_end,
                    'interval': '1d', 'includePrePost': 'false',
                },
                headers={'User-Agent': NASDAQ_MARKET_HEADERS['User-Agent']}, timeout=8,
            )
            response.raise_for_status()
            result = (response.json().get('chart', {}).get('result') or [None])[0] or {}
            timestamps = result.get('timestamp') or []
            closes = (result.get('indicators', {}).get('quote') or [{}])[0].get('close') or []
            parsed = []
            for timestamp, close in zip(timestamps, closes):
                try:
                    trade_date = datetime.fromtimestamp(
                        float(timestamp), tz=pytz.utc,
                    ).date()
                except (TypeError, ValueError, OverflowError, OSError):
                    continue
                close = _safe_number(close)
                if close is not None and close > 0:
                    parsed.append((trade_date, close))
            parsed.sort(key=lambda pair: pair[0], reverse=True)
            if len(parsed) < 2:
                raise ValueError('收盤資料不足')
            latest_date, latest = parsed[0]
            _, previous = parsed[1]
            if latest_date != expected_date:
                raise ValueError(
                    f'資料停在 {latest_date:%m/%d}，應為 {expected_date:%m/%d}，已排除評分'
                )
            return {
                'key': key, 'label': label, 'group': '美股收盤',
                'price': latest, 'pct': (latest / previous - 1) * 100,
                'time': f'{latest_date:%m/%d} 收盤',
                'weight': OPENING_SIGNAL_WEIGHTS[key], 'source': f'Yahoo Finance｜{label}',
            }, None
        except Exception as exc:
            return None, f'{label}：{exc}'

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(fetch_one, instruments.items()))
    rows = []
    for row, error in results:
        if row:
            rows.append(row)
        elif error:
            errors.append(error)
    return rows, errors


def fetch_nasdaq_us_close_signals(trading_day, requested_keys=None):
    """從 Nasdaq 取得最近美股收盤，並拒絕早於最近應有交易日的資料。"""
    expected_date = _expected_us_close_date(trading_day)
    errors = []

    # Nasdaq 的公開端點可直接提供 COMP／SOX，僅在 Yahoo 真正指數資料缺漏時補齊。
    instruments = {
        'NASDAQ': ('Nasdaq 綜合指數', 'COMP', 'index', 'Nasdaq｜Nasdaq 綜合指數'),
        'SOX': ('費城半導體指數', 'SOX', 'index', 'Nasdaq｜費城半導體指數'),
    }
    if requested_keys is not None:
        instruments = {key: value for key, value in instruments.items() if key in requested_keys}
    from_date = (expected_date - timedelta(days=12)).isoformat()
    to_date = trading_day.isoformat()

    def fetch_one(item):
        key, (label, symbol, asset_class, source) = item
        try:
            response = requests.get(
                f'https://api.nasdaq.com/api/quote/{symbol}/historical',
                params={
                    'assetclass': asset_class, 'fromdate': from_date,
                    'todate': to_date, 'limit': 12,
                },
                headers=NASDAQ_MARKET_HEADERS, timeout=8,
            )
            response.raise_for_status()
            payload = response.json()
            records = payload.get('data', {}).get('tradesTable', {}).get('rows') or []
            parsed = []
            for record in records:
                try:
                    trade_date = datetime.strptime(str(record.get('date')), '%m/%d/%Y').date()
                except (TypeError, ValueError):
                    continue
                close = _parse_nasdaq_number(record.get('close'))
                if close is not None and close > 0:
                    parsed.append((trade_date, close))
            parsed.sort(key=lambda pair: pair[0], reverse=True)
            if len(parsed) < 2:
                return None, f'{label}：Nasdaq 收盤資料不足'
            latest_date, latest = parsed[0]
            _, previous = parsed[1]
            if latest_date != expected_date:
                return None, (
                    f'{label}：資料停在 {latest_date:%m/%d}，'
                    f'最近應有交易日為 {expected_date:%m/%d}，已排除評分'
                )
            return {
                'key': key, 'label': label, 'group': '美股收盤', 'price': latest,
                'pct': (latest / previous - 1) * 100,
                'time': f'{latest_date:%m/%d} 收盤',
                'weight': OPENING_SIGNAL_WEIGHTS[key], 'source': source,
            }, None
        except Exception as exc:
            return None, f'{label}：{exc}'

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(fetch_one, instruments.items()))
    rows = []
    for row, error in results:
        if row:
            rows.append(row)
        if error:
            errors.append(error)
    return rows, errors, expected_date


def _extract_yfinance_close(downloaded, ticker):
    """從 yfinance 單／多商品結果安全取出 Close，統一轉成臺北時間。"""
    if downloaded is None or downloaded.empty:
        return pd.Series(dtype=float)
    try:
        if isinstance(downloaded.columns, pd.MultiIndex):
            level0 = downloaded.columns.get_level_values(0)
            if ticker in level0:
                series = downloaded[ticker]['Close']
            elif 'Close' in level0:
                series = downloaded['Close'][ticker]
            else:
                return pd.Series(dtype=float)
        else:
            series = downloaded['Close']
        series = pd.to_numeric(series, errors='coerce').dropna()
        if series.empty:
            return series
        index = pd.to_datetime(series.index)
        if getattr(index, 'tz', None) is None:
            index = index.tz_localize('UTC')
        series.index = index.tz_convert('Asia/Taipei')
        return series[~series.index.duplicated(keep='last')].sort_index()
    except Exception:
        return pd.Series(dtype=float)


def _opening_window_change(series, start_at, cutoff_at):
    """以指定開盤時間前最後成交為基準，計算至 08:30 截止的漲跌幅。"""
    if series.empty:
        return None
    before = series[series.index < start_at]
    current = series[(series.index >= start_at) & (series.index <= cutoff_at)]
    if before.empty or current.empty:
        return None
    reference = _safe_number(before.iloc[-1])
    latest = _safe_number(current.iloc[-1])
    if reference is None or latest is None or reference <= 0:
        return None
    return {
        'price': latest,
        'pct': (latest / reference - 1) * 100,
        'time': current.index[-1].strftime('%m/%d %H:%M'),
    }


def _opening_night_change(series, cutoff_at):
    """Yahoo 備援：找最近一段 15:00–05:00 台指期夜盤並對比夜盤前收盤。"""
    if series.empty:
        return None
    eligible = series[
        (series.index <= cutoff_at)
        & ((series.index.time >= dt_time(15, 0)) | (series.index.time <= dt_time(5, 0)))
    ]
    if eligible.empty:
        return None
    last_time = eligible.index[-1]
    start_date = (last_time - timedelta(days=1)).date() if last_time.time() <= dt_time(5, 0) else last_time.date()
    start_at = pytz.timezone('Asia/Taipei').localize(datetime.combine(start_date, dt_time(15, 0)))
    night = series[(series.index >= start_at) & (series.index <= cutoff_at)]
    before = series[series.index < start_at]
    if night.empty or before.empty:
        return None
    reference = _safe_number(before.iloc[-1])
    latest = _safe_number(night.iloc[-1])
    if reference is None or latest is None or reference <= 0:
        return None
    return {
        'price': latest,
        'pct': (latest / reference - 1) * 100,
        'time': night.index[-1].strftime('%m/%d %H:%M'),
    }


@st.cache_data(ttl=300, max_entries=6, show_spinner=False)
def fetch_opening_overseas_signals(trading_date_text, cutoff_text):
    """批次取得美股收盤、美期與日韓盤；固定以 08:30 前資料計算。"""
    tz_tw = pytz.timezone('Asia/Taipei')
    trading_day = datetime.strptime(trading_date_text, '%Y-%m-%d').date()
    cutoff_at = tz_tw.localize(datetime.strptime(cutoff_text, '%Y-%m-%d %H:%M'))
    rows = []
    errors = []
    intraday_symbols = {
        'NQ_FUT': ('小那期', 'NQ=F', '06:00 美期', dt_time(6, 0)),
        'YM_FUT': ('小道期', 'YM=F', '06:00 美期', dt_time(6, 0)),
        'NIKKEI': ('日經', '^N225', '08:00 日韓', dt_time(8, 0)),
        'KOSPI': ('韓股綜合', '^KS11', '08:00 日韓', dt_time(8, 0)),
    }

    expected_us_close = _expected_us_close_date(trading_day)
    yahoo_rows, yahoo_errors = fetch_yahoo_us_index_close_signals(
        trading_day, expected_us_close
    )
    us_close_by_key = {row['key']: row for row in yahoo_rows}
    missing_us_keys = {'DJI', 'NASDAQ', 'SP500', 'SOX'} - set(us_close_by_key)
    nasdaq_errors = []
    if missing_us_keys:
        nasdaq_rows, nasdaq_errors, _ = fetch_nasdaq_us_close_signals(
            trading_day, missing_us_keys
        )
        for row in nasdaq_rows:
            us_close_by_key.setdefault(row['key'], row)
    rows.extend(us_close_by_key.values())
    missing_us_keys = {'DJI', 'NASDAQ', 'SP500', 'SOX'} - set(us_close_by_key)
    if missing_us_keys:
        errors.extend(yahoo_errors)
        errors.extend(nasdaq_errors)

    try:
        downloaded = yf.download(
            [item[1] for item in intraday_symbols.values()],
            period='5d', interval='5m', group_by='ticker', auto_adjust=False,
            prepost=True, progress=False, threads=False,
        )
        for key, (label, ticker, group, start_time) in intraday_symbols.items():
            start_at = tz_tw.localize(datetime.combine(trading_day, start_time))
            measured = _opening_window_change(_extract_yfinance_close(downloaded, ticker), start_at, cutoff_at)
            if measured:
                rows.append({
                    'key': key, 'label': label, 'group': group, **measured,
                    'weight': OPENING_SIGNAL_WEIGHTS[key], 'source': 'Yahoo Finance 5分線',
                })
    except Exception as exc:
        errors.append(f'盤前即時市場：{exc}')
    if not rows and not errors:
        errors.append('Yahoo Finance 指數／Nasdaq／盤前行情來源暫未回傳可用資料')
    return rows, errors


def fetch_shioaji_tx_night_signal(api, now_tw):
    """取得歸屬今日交易日的臺指期夜盤，避免 08:45 後混入日盤價格。"""
    if api is None:
        return None
    try:
        contracts = [
            contract for contract in api.Contracts.Futures.TXF
            if str(getattr(contract, 'code', ''))[-2:] not in ('R1', 'R2')
            and '/' not in str(getattr(contract, 'code', ''))
        ]
        contract = min(contracts, key=lambda item: getattr(item, 'delivery_date', '999999'))
    except Exception:
        try:
            contract = api.Contracts.Futures.TXF.TXFR1
        except Exception:
            return None

    kbars = fetch_futures_contract_kbars(api, contract, 4)
    if kbars.empty or 'Close' not in kbars.columns:
        return None
    target_date = get_futures_trading_date(now_tw).date()
    session_dates = []
    for stamp in kbars.index:
        stamp_dt = stamp.to_pydatetime() if hasattr(stamp, 'to_pydatetime') else stamp
        aware = pytz.timezone('Asia/Taipei').localize(stamp_dt) if stamp_dt.tzinfo is None else stamp_dt
        session_dates.append(get_futures_trading_date(aware).date())
    session_dates = pd.Series(session_dates, index=kbars.index)
    time_mask = (kbars.index.time >= dt_time(15, 0)) | (kbars.index.time <= dt_time(5, 0))
    night = kbars[(session_dates == target_date).to_numpy() & time_mask]
    if night.empty:
        return None
    before = kbars[kbars.index < night.index[0]]
    day_mask = (before.index.time >= dt_time(8, 45)) & (before.index.time <= dt_time(13, 45))
    prior_day = before[day_mask]
    if prior_day.empty:
        return None
    reference = _safe_number(prior_day['Close'].iloc[-1])
    latest = _safe_number(night['Close'].iloc[-1])
    if reference is None or latest is None or reference <= 0:
        return None
    return {
        'key': 'TX_NIGHT', 'label': '台指期夜盤', 'group': '台指夜盤',
        'price': latest, 'pct': (latest / reference - 1) * 100,
        'time': night.index[-1].strftime('%m/%d %H:%M'),
        'weight': OPENING_SIGNAL_WEIGHTS['TX_NIGHT'], 'source': 'Shioaji 臺指期分K',
    }


def calculate_opening_direction(rows):
    """依跨市場加權變動與多空廣度產生盤前觀察方向，不改動個股策略。"""
    valid = [row for row in rows if _safe_number(row.get('pct')) is not None]
    available_weight = sum(row['weight'] for row in valid)
    if not valid or available_weight < 45:
        return {'direction': '資料不足', 'score': None, 'coverage': available_weight, 'weighted_pct': None}
    weighted_pct = sum(row['pct'] * row['weight'] for row in valid) / available_weight
    positive_weight = sum(row['weight'] for row in valid if row['pct'] > 0.10)
    negative_weight = sum(row['weight'] for row in valid if row['pct'] < -0.10)
    breadth = (positive_weight - negative_weight) / available_weight
    score = max(0, min(100, round(50 + weighted_pct * 10 + breadth * 8)))
    direction = '偏多' if score >= 54 else ('偏空' if score <= 46 else '中性')
    return {
        'direction': direction, 'score': score, 'coverage': available_weight,
        'weighted_pct': weighted_pct,
    }


@st.fragment(run_every=60)
def render_opening_direction_prompt():
    """在股期戰略室頂端顯示 08:30 試搓用的跨市場方向提示。"""
    tz_tw = pytz.timezone('Asia/Taipei')
    now_tw = datetime.now(tz_tw)
    active_window = dt_time(8, 20) <= now_tw.time() <= dt_time(9, 0)
    auto_refresh_window = dt_time(8, 0) <= now_tw.time() <= dt_time(8, 40)
    cutoff_time = min(now_tw.time(), dt_time(8, 30))
    cutoff_minute = (cutoff_time.minute // 5) * 5
    cutoff_at = datetime.combine(now_tw.date(), dt_time(cutoff_time.hour, cutoff_minute))

    title_col, refresh_col = st.columns([9, 1], vertical_alignment='center')
    with title_col:
        st.markdown('<span style="font-size:18px;font-weight:800;">🧭 台股試搓方向</span>', unsafe_allow_html=True)
    with refresh_col:
        refresh = st.button('🔄 更新', key='refresh_opening_direction', width='stretch')
    if refresh:
        fetch_opening_overseas_signals.clear()
        st.session_state.pop('opening_overseas_signal', None)
        st.session_state.pop('opening_tx_night_signal', None)

    trading_date_text = now_tw.strftime('%Y-%m-%d')
    overseas_cache = st.session_state.get('opening_overseas_signal')
    cache_outdated = (
        not overseas_cache
        or overseas_cache.get('date') != trading_date_text
        or overseas_cache.get('version') != OPENING_SIGNAL_CACHE_VERSION
    )
    if refresh or auto_refresh_window or cache_outdated:
        rows, errors = fetch_opening_overseas_signals(
            trading_date_text, cutoff_at.strftime('%Y-%m-%d %H:%M'),
        )
        st.session_state.opening_overseas_signal = {
            'version': OPENING_SIGNAL_CACHE_VERSION,
            'date': trading_date_text, 'rows': rows, 'errors': errors,
        }
    else:
        rows = list(overseas_cache.get('rows', []))
        errors = list(overseas_cache.get('errors', []))
    if st.session_state.get('sj_logged_in', False) and st.session_state.get('sj_api') is not None:
        cache = st.session_state.get('opening_tx_night_signal')
        cache_stale = not cache or cache.get('date') != now_tw.strftime('%Y-%m-%d')
        if refresh or cache_stale:
            tx_signal = fetch_shioaji_tx_night_signal(st.session_state.sj_api, now_tw)
            st.session_state.opening_tx_night_signal = {
                'date': now_tw.strftime('%Y-%m-%d'), 'row': tx_signal,
            }
        tx_signal = st.session_state.get('opening_tx_night_signal', {}).get('row')
        if tx_signal:
            rows = [row for row in rows if row['key'] != 'TX_NIGHT'] + [tx_signal]

    market_closed = is_market_closed_func(now_tw.date())
    result = calculate_opening_direction(rows)
    if market_closed:
        result = {
            'direction': '休市', 'score': None, 'coverage': result['coverage'],
            'weighted_pct': result['weighted_pct'],
        }
    direction = result['direction']
    palette = {
        '偏多': ('#ff4b4b', '優先等待多方條件：試搓守穩、突破壓力且量能確認後再評估。'),
        '偏空': ('#00c853', '優先等待空方條件：試搓轉弱、跌破支撐且反彈不過後再評估。'),
        '中性': ('#ffb300', '多空訊號分歧：先等開盤區間、VWAP 與量能確認，不預設方向。'),
        '資料不足': ('#9e9e9e', '可用市場權重不足，暫不提供方向；可更新資料或登入 Shioaji 補齊台指期夜盤。'),
        '休市': ('#9e9e9e', '今日臺股休市，不產生試搓操作方向；行情僅保留作為下一交易日背景。'),
    }
    color, advice = palette[direction]
    groups = ['台指夜盤', '美股收盤', '06:00 美期', '08:00 日韓']
    group_badges = []
    for group in groups:
        group_rows = [row for row in rows if row['group'] == group]
        if group_rows:
            group_weight = sum(row['weight'] for row in group_rows)
            group_pct = sum(row['pct'] * row['weight'] for row in group_rows) / group_weight
            detail_text = '｜'.join(
                f"{row['label']} {_signed_percent_arrow(row['pct'])}（{row['time']}）"
                for row in group_rows
            )
            group_badges.append(
                "<span style='white-space:nowrap;padding:3px 8px;border-radius:999px;"
                "background:rgba(128,128,128,.12);' "
                f"title='{html.escape(detail_text, quote=True)}'>"
                f"{html.escape(group)} {_percent_badge_html(group_pct, 13)}</span>"
            )
        else:
            group_badges.append(
                "<span style='white-space:nowrap;padding:3px 8px;border-radius:999px;"
                "background:rgba(128,128,128,.12);color:#9e9e9e;'>"
                f"{html.escape(group)} —</span>"
            )

    score_text = (
        f"<span style='color:{_score_color(result['score'])};font-weight:800;'>{result['score']}</span>"
        if result['score'] is not None else '—'
    )
    st.markdown(
        f"<div style='border-left:5px solid {color};padding:7px 10px;"
        "background:rgba(128,128,128,.08);border-radius:6px;line-height:1.65;'>"
        f"<span style='font-size:18px;font-weight:800;color:{color};'>{direction}</span>　"
        f"分數 {score_text} 分　"
        f"{' '.join(group_badges)}<br>"
        f"<span style='font-size:14px;color:#c8c8c8;'>{html.escape(advice)}"
        f"　｜資料覆蓋 {result['coverage']}%</span></div>",
        unsafe_allow_html=True,
    )

    phase = '08:30 試搓提示期間' if active_window else (
        '08:30 前逐步納入已開盤市場' if now_tw.time() < dt_time(8, 30)
        else '已固定美期／日韓資料至 08:30'
    )
    with st.expander('查看參考市場、資料時間與判讀方式', expanded=False):
        if rows:
            details = pd.DataFrame(rows).rename(columns={
                'label': '市場', 'group': '群組', 'price': '價格', 'pct': '漲跌幅',
                'time': '資料時間', 'weight': '權重', 'source': '來源',
            })
            details['漲跌幅'] = details['漲跌幅'].apply(_signed_percent)

            def style_opening_detail(row):
                styles = [''] * len(row)
                value = _to_number(
                    str(row.get('漲跌幅', '')).replace('%', '').replace('+', '')
                )
                if '漲跌幅' in row.index and value is not None:
                    position = row.index.get_loc('漲跌幅')
                    styles[position] = (
                        'color:#ff4b4b;font-weight:bold;' if value > 0
                        else ('color:#00c853;font-weight:bold;' if value < 0 else 'color:#9e9e9e;')
                    )
                return styles

            st.dataframe(
                details[['群組', '市場', '價格', '漲跌幅', '資料時間', '權重', '來源']]
                .style.apply(style_opening_detail, axis=1),
                column_config={
                    '價格': st.column_config.NumberColumn(format='%.12g', width=78),
                    '漲跌幅': st.column_config.TextColumn(width=90),
                    '權重': st.column_config.NumberColumn(format='%d', width=60),
                },
                hide_index=True, width='stretch',
            )
        st.caption(
            f'{phase}。美股採最近完整收盤；美期採 06:00 後、日韓採 08:00 後，盤後查看仍以 08:30 為截止。'
            '美股只採真正指數的日線收盤：Yahoo Finance 指數為主、Nasdaq 指數資料為備援；只有資料日等於最近應有美股交易日才會納入評分。'
            '分數代表跨市場方向一致度，不是勝率；個股仍需依原戰略備註、支撐壓力與進場條件確認。'
        )
        if errors:
            st.warning('部分行情來源暫時無法取得：' + '；'.join(errors))


@st.cache_data(ttl=900, max_entries=1, show_spinner=False)
def fetch_market_risk_lists():
    """取得上市、上櫃注意／處置名單；僅在使用者手動更新時呼叫。"""
    attention_counts = {}
    disposition_codes = set()
    disposition_tomorrow_codes = set()
    errors = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
    }
    def fetch_json(name, url, expected_type):
        """官方站偶發回空頁或 5xx；每個 API 獨立執行重試。"""
        session = requests.Session()
        last_error = None

        try:
            for attempt in range(2):
                try:
                    response = session.get(
                        url,
                        headers=headers,
                        params={'_': int(time.time() * 1000)} if attempt else None,
                        timeout=(3, 8),
                    )
                    response.raise_for_status()

                    payload = response.json()

                    if not isinstance(payload, expected_type):
                        raise ValueError(
                            f"回傳格式應為 {expected_type.__name__}"
                        )

                    if (
                        isinstance(payload, dict)
                        and not isinstance(payload.get('data', []), list)
                    ):
                        raise ValueError("缺少名單資料列")

                    return payload

                except (
                    requests.RequestException,
                    ValueError,
                    json.JSONDecodeError,
                ) as exc:
                    last_error = exc

                    if attempt < 1:
                        time.sleep(0.5)

            raise RuntimeError(
                f"{name} 已重試 2 次仍失敗：{last_error}"
            )
        finally:
            session.close()

    def get_disposition_status(period_raw, target_date=None):
        """判斷處置開始日是今天還是明天，支援民國年與西元年格式。"""
        period_raw = str(period_raw or '').strip()
        target_date = target_date or datetime.now(
            pytz.timezone('Asia/Taipei')
        ).date()

        date_matches = re.findall(
            r'(\d{3,4})[年/-](\d{1,2})[月/-](\d{1,2})日?',
            period_raw
        )

        if len(date_matches) < 2:
            compact_dates = re.findall(
                r'(\d{7,8})',
                period_raw
            )

            if len(compact_dates) >= 2:
                date_matches = []

                for value in compact_dates[:2]:
                    if len(value) == 7:
                        date_matches.append(
                            (
                                value[:3],
                                value[3:5],
                                value[5:7],
                            )
                        )
                    elif len(value) == 8:
                        date_matches.append(
                            (
                                value[:4],
                                value[4:6],
                                value[6:8],
                            )
                        )

        if len(date_matches) < 2:
            return None

        start_year, start_month, start_day = map(int, date_matches[0])
        end_year, end_month, end_day = map(int, date_matches[1])

        if start_year < 1000:
            start_year += 1911
        if end_year < 1000:
            end_year += 1911

        try:
            start_date = date(start_year, start_month, start_day)
            end_date = date(end_year, end_month, end_day)
        except ValueError:
            return None

        if start_date <= target_date <= end_date:
            return 'today'

        if start_date == target_date + timedelta(days=1):
            return 'tomorrow'

        return None

    def parse_twse_rows(
        payload,
        target,
        is_attention=False,
        minimum_attention=1,
    ):
        fields = payload.get('fields', [])

        for values in payload.get('data', []):
            record = dict(zip(fields, values))

            raw_code = str(
                record.get(
                    '證券代號',
                    record.get('有價證券代號', ''),
                )
            ).strip()

            # 只處理 4 碼普通股票。
            # 5～6 碼的權證、公司債等有價證券不納入股票戰略室。
            code_match = re.search(
                r'\d{4,6}',
                raw_code,
            )

            if not code_match:
                continue

            code = code_match.group(0)

            if len(code) != 4:
                continue

            if is_attention:
                raw_count = str(
                    record.get(
                        '累計次數',
                        record.get('累計', '1'),
                    )
                )

                count_match = re.search(
                    r'\d+',
                    raw_count,
                )

                parsed_count = (
                    int(count_match.group())
                    if count_match
                    else minimum_attention
                )

                target[code] = max(
                    target.get(code, 0),
                    minimum_attention,
                    parsed_count,
                )

            else:
                # TWSE /announcement/punish：
                # row[6] 為「處置起迄時間」，例如 115/07/03～115/07/16。
                period_raw = str(
                    values[6]
                    if len(values) > 6
                    else record.get(
                        '處置起迄時間',
                        '',
                    )
                ).strip()

                disposition_state = get_disposition_status(
                    period_raw
                )

                if disposition_state == 'today':
                    target.add(code)
                elif disposition_state == 'tomorrow':
                    disposition_tomorrow_codes.add(code)

    # 6 個官方名單 API 同時抓取，避免逐一等待造成更新按鈕卡頓。
    from concurrent.futures import ThreadPoolExecutor, as_completed

    fetch_jobs = [
        (
            '上市注意',
            'https://openapi.twse.com.tw/v1/announcement/notice',
            list,
        ),
        (
            '上市處置',
            'https://openapi.twse.com.tw/v1/announcement/punish',
            list,
        ),
        (
            '上市注意累計異常',
            'https://openapi.twse.com.tw/v1/announcement/notetrans',
            list,
        ),
        (
            '上櫃注意',
            'https://www.tpex.org.tw/openapi/v1/tpex_trading_warning_information',
            list,
        ),
        (
            '上櫃注意累計異常',
            'https://www.tpex.org.tw/openapi/v1/tpex_trading_warning_note',
            list,
        ),
        (
            '上櫃處置',
            'https://www.tpex.org.tw/openapi/v1/tpex_disposal_information',
            list,
        ),
    ]

    fetched = {}

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {
            executor.submit(fetch_json, name, url, expected_type): name
            for name, url, expected_type in fetch_jobs
        }

        for future in as_completed(future_map):
            name = future_map[future]
            try:
                fetched[name] = future.result()
            except Exception as exc:
                errors.append(f'{name}: {exc}')

    # TWSE OpenAPI 回傳 list；集中市場三個來源分開解析。
    twse_notice = fetched.get('上市注意')
    if twse_notice is not None:
        try:
            for record in twse_notice:
                code = str(record.get('Code', '')).strip()
                code_match = re.search(r'\d{4,6}', code)
                if not code_match:
                    continue

                code = code_match.group(0)

                if len(code) == 4:
                    attention_counts[code] = max(
                        attention_counts.get(code, 0),
                        1,
                    )
        except Exception as exc:
            errors.append(f'上市注意解析失敗: {exc}')

    twse_disposition = fetched.get('上市處置')
    if twse_disposition is not None:
        try:
            for record in twse_disposition:
                code = str(record.get('Code', '')).strip()
                code_match = re.search(r'\d{4,6}', code)
                if not code_match:
                    continue

                code = code_match.group(0)

                if len(code) != 4:
                    continue

                period_raw = str(
                    record.get('DispositionPeriod', '')
                ).strip()

                disposition_state = get_disposition_status(period_raw)

                if disposition_state == 'today':
                    disposition_codes.add(code)
                elif disposition_state == 'tomorrow':
                    disposition_tomorrow_codes.add(code)

        except Exception as exc:
            errors.append(f'上市處置解析失敗: {exc}')

    twse_accumulated = fetched.get('上市注意累計異常')
    if twse_accumulated is not None:
        try:
            chinese_digits = {
                '零': 0,
                '一': 1,
                '二': 2,
                '三': 3,
                '四': 4,
                '五': 5,
                '六': 6,
                '七': 7,
                '八': 8,
                '九': 9,
                '十': 10,
            }

            def parse_attention_count(text):
                text = str(text or '')
                matches = re.findall(
                    r'(?:連續|已有)([一二三四五六七八九十\d]+)次',
                    text,
                )

                counts = []

                for value in matches:
                    if value.isdigit():
                        counts.append(int(value))
                    elif value == '十':
                        counts.append(10)
                    elif len(value) == 2 and value[0] == '十':
                        counts.append(10 + chinese_digits.get(value[1], 0))
                    elif len(value) == 2 and value[1] == '十':
                        counts.append(chinese_digits.get(value[0], 0) * 10)
                    elif len(value) == 3 and value[1] == '十':
                        counts.append(
                            chinese_digits.get(value[0], 0) * 10
                            + chinese_digits.get(value[2], 0)
                        )
                    elif len(value) == 1:
                        counts.append(chinese_digits.get(value, 0))

                return max(counts, default=2)

            for record in twse_accumulated:
                code = str(record.get('Code', '')).strip()
                code_match = re.search(r'\d{4,6}', code)
                if not code_match:
                    continue

                code = code_match.group(0)

                if len(code) != 4:
                    continue

                criteria_text = record.get(
                    'RecentlyMetAttentionSecuritiesCriteria',
                    '',
                )

                attention_count = parse_attention_count(
                    criteria_text
                )

                attention_counts[code] = max(
                    attention_counts.get(code, 0),
                    attention_count,
                    2,
                )

        except Exception as exc:
            errors.append(f'上市注意累計異常解析失敗: {exc}')

    # TPEx 三個 API 維持原本解析方式。
    for name, is_accumulated_note in [
        ('上櫃注意', False),
        ('上櫃注意累計異常', True),
        ('上櫃處置', False),
    ]:
        payload = fetched.get(name)

        if payload is None:
            continue

        try:
            for record in payload:
                raw_code = str(
                    record.get(
                        'SecuritiesCompanyCode',
                        '',
                    )
                ).strip()

                code_match = re.search(
                    r'\d{4,6}',
                    raw_code,
                )

                if not code_match:
                    continue

                code = code_match.group(0)

                if len(code) != 4:
                    continue

                if name != '上櫃處置':
                    count = (
                        2
                        if is_accumulated_note
                        else 1
                    )

                    attention_counts[code] = max(
                        attention_counts.get(code, 0),
                        count,
                    )
                else:
                    period_raw = str(
                        record.get(
                            'DispositionPeriod',
                            '',
                        )
                    ).strip()

                    disposition_state = get_disposition_status(
                        period_raw
                    )

                    if disposition_state == 'today':
                        disposition_codes.add(code)
                    elif disposition_state == 'tomorrow':
                        disposition_tomorrow_codes.add(code)

        except Exception as exc:
            errors.append(
                f'{name}: {exc}'
            )

    return (
        attention_counts,
        sorted(disposition_codes),
        sorted(disposition_tomorrow_codes),
        errors,
    )


def merge_market_risk_refresh(
    previous_state, attention, disposition, disposition_tomorrow, errors,
    attempted_at=None,
):
    """Keep the last complete official lists when a manual refresh is partial."""
    attempted_at = attempted_at or datetime.now(
        pytz.timezone('Asia/Taipei')
    ).strftime('%Y/%m/%d %H:%M:%S')
    previous = dict(previous_state or {})
    error_list = list(errors or [])
    if error_list and previous.get('updated'):
        previous['errors'] = error_list
        previous['last_attempt'] = attempted_at
        previous['using_last_success'] = True
        return previous
    return {
        'attention': dict(attention or {}),
        'disposition': list(disposition or []),
        'disposition_tomorrow': list(disposition_tomorrow or []),
        'updated': attempted_at,
        'last_attempt': attempted_at,
        'errors': error_list,
        'using_last_success': False,
    }

def _as_float(value, default=None):
    try:
        number = float(value)
        return default if math.isnan(number) else number
    except (TypeError, ValueError):
        return default


def determine_stock_direction(row, is_daytrade_mode=False, direction_choice='系統自動'):
    """逐檔以分 K／日 K、VWAP 與支撐壓力判斷策略方向。"""
    manual_map = {'多': '多頭', '多頭': '多頭', '空': '空頭', '空頭': '空頭'}
    if direction_choice in manual_map:
        direction = manual_map[direction_choice]
        return {
            'direction': direction,
            'label': '🔴 多（手動）' if direction == '多頭' else '🟢 空（手動）',
            'basis': '手動方向｜仍檢查風險。',
            'long_score': None,
            'short_score': None,
            'source': '手動',
        }

    long_score = 0
    short_score = 0
    long_reasons = []
    short_reasons = []

    def add_signal(condition, long_points, short_points, long_text, short_text):
        nonlocal long_score, short_score
        if condition is True:
            long_score += long_points
            long_reasons.append(long_text)
        elif condition is False:
            short_score += short_points
            short_reasons.append(short_text)

    daily_close = _as_float(row.get('收盤價'))
    ma5 = _as_float(row.get('_ma5'))
    ma20 = _as_float(row.get('_risk_ma20'))
    ma20_slope = _as_float(row.get('_risk_ma20_slope'))
    previous_high = _as_float(row.get('_risk_prev_high'))
    previous_low = _as_float(row.get('_risk_prev_low'))
    close_position = _as_float(row.get('_risk_close_position'))
    change_pct = _as_float(row.get('漲跌幅'))

    if daily_close is not None and ma5 is not None:
        add_signal(daily_close >= ma5, 2, 2, '日 K 站上 5 日線', '日 K 跌破 5 日線')
    if daily_close is not None and ma20 is not None:
        add_signal(daily_close >= ma20, 2, 2, '日 K 站上 20 日線', '日 K 跌破 20 日線')
    if ma20_slope is not None and ma20_slope != 0:
        add_signal(ma20_slope > 0, 1, 1, '20 日線向上', '20 日線向下')
    if daily_close is not None and previous_high is not None and daily_close > previous_high:
        long_score += 2
        long_reasons.append('突破日 K 前高壓力')
    if daily_close is not None and previous_low is not None and daily_close < previous_low:
        short_score += 2
        short_reasons.append('跌破日 K 前低支撐')
    if close_position is not None:
        if close_position >= 60:
            long_score += 1
            long_reasons.append('日 K 收近高位')
        elif close_position <= 40:
            short_score += 1
            short_reasons.append('日 K 收近低位')
    if change_pct is not None and change_pct != 0:
        add_signal(change_pct > 0, 1, 1, '價格動能偏多', '價格動能偏空')

    source = '日K'
    intraday_close = _as_float(row.get('_daytrade_close'))
    vwap = _as_float(row.get('_daytrade_vwap'))
    opening_high = _as_float(row.get('_daytrade_or_high'))
    opening_low = _as_float(row.get('_daytrade_or_low'))
    intraday_open = _as_float(row.get('_daytrade_open'))
    volume_ratio = _as_float(row.get('_daytrade_volume_ratio'))
    has_intraday = is_daytrade_mode and None not in (intraday_close, vwap, opening_high, opening_low)
    if has_intraday:
        daily_long_reason_count = len(long_reasons)
        daily_short_reason_count = len(short_reasons)
        source = '分K＋日K'
        add_signal(intraday_close >= vwap, 4, 4, '分 K 站上 VWAP', '分 K 跌破 VWAP')
        if intraday_close > opening_high:
            long_score += 4
            long_reasons.append('突破開盤區間壓力')
        elif intraday_close < opening_low:
            short_score += 4
            short_reasons.append('跌破開盤區間支撐')
        elif opening_high > opening_low:
            range_position = (intraday_close - opening_low) / (opening_high - opening_low)
            if range_position >= 0.65:
                long_score += 2
                long_reasons.append('位於開盤區間上緣')
            elif range_position <= 0.35:
                short_score += 2
                short_reasons.append('位於開盤區間下緣')
        if intraday_open is not None:
            add_signal(intraday_close >= intraday_open, 1, 1, '分 K 高於開盤價', '分 K 低於開盤價')
        if volume_ratio is not None and volume_ratio >= 1:
            if intraday_close >= vwap:
                long_score += 1
                long_reasons.append('量能放大支持多方')
            else:
                short_score += 1
                short_reasons.append('量能放大支持空方')
        # 當沖說明先顯示分 K／VWAP／開盤區間，再補充日 K 方向。
        long_reasons = long_reasons[daily_long_reason_count:] + long_reasons[:daily_long_reason_count]
        short_reasons = short_reasons[daily_short_reason_count:] + short_reasons[:daily_short_reason_count]
    elif is_daytrade_mode:
        source = '日K備援'

    if long_score == short_score:
        note = str(row.get('戰略備註', ''))
        if '空' in note and '多' not in note:
            short_score += 1
            short_reasons.append('原戰略備註偏空')
        elif '多' in note and '空' not in note:
            long_score += 1
            long_reasons.append('原戰略備註偏多')
        elif change_pct is not None and change_pct < 0:
            short_score += 1
            short_reasons.append('同分時依價格動能偏空')
        else:
            long_score += 1
            long_reasons.append('同分時依價格動能偏多')

    is_long = long_score > short_score
    chosen_reasons = long_reasons if is_long else short_reasons
    direction = '多頭' if is_long else '空頭'
    label = '🔴 建議多' if is_long else '🟢 建議空'
    # Keep the table decision-oriented: detailed scoring remains in the
    # confidence details, while this cell shows only the two key drivers.
    evidence = '、'.join(chosen_reasons[:2]) if chosen_reasons else '指標不足'
    basis = f'{source}｜{evidence}'
    return {
        'direction': direction,
        'label': label,
        'basis': basis,
        'long_score': long_score,
        'short_score': short_score,
        'source': source,
    }


def calculate_risk_filter_result(row, direction, max_extension_atr, attention_counts=None, disposition_codes=None, market_lists_updated=False, block_attention=True, disposition_tomorrow_codes=None):
    """建立風險篩選預覽資料；不會改寫原本的選股資料或下單行為。"""
    attention_counts = attention_counts or {}
    disposition_codes = set(disposition_codes or [])
    disposition_tomorrow_codes = set(disposition_tomorrow_codes or [])
    code = str(row.get('代號', '')).strip()
    close = _as_float(row.get('收盤價'))
    ma5 = _as_float(row.get('_ma5'))
    ma20 = _as_float(row.get('_risk_ma20'))
    ma20_slope = _as_float(row.get('_risk_ma20_slope'))
    atr14 = _as_float(row.get('_risk_atr14'))
    close_position = _as_float(row.get('_risk_close_position'))
    previous_high = _as_float(row.get('_risk_prev_high'))
    previous_low = _as_float(row.get('_risk_prev_low'))
    is_long = direction == '多頭'

    if close is None or ma5 is None or ma20 is None or atr14 is None or atr14 <= 0:
        return {
            'score': 0, 'risk': '⚪ 資料不足', 'extension': None,
            'rule': '資料不足，維持原判斷', 'eligible': False,
            'detail': '缺少計算 20 日趨勢或 ATR 所需資料。'
        }

    extension = (close - ma20) / atr14 if is_long else (ma20 - close) / atr14
    trend_score = 0
    if (is_long and close > ma5) or (not is_long and close < ma5):
        trend_score += 10
    if (is_long and close > ma20) or (not is_long and close < ma20):
        trend_score += 10
    if ma20_slope is not None and ((is_long and ma20_slope > 0) or (not is_long and ma20_slope < 0)):
        trend_score += 10

    if extension <= 1:
        extension_score = 20
    elif extension <= 1.5:
        extension_score = 15
    elif extension <= 2:
        extension_score = 8
    else:
        extension_score = 0

    candle_score = 5
    if close_position is not None:
        if (is_long and close_position >= 65) or (not is_long and close_position <= 35):
            candle_score = 15
        elif (is_long and close_position >= 50) or (not is_long and close_position <= 50):
            candle_score = 10

    attention_count = attention_counts.get(code, 0)
    is_disposed = code in disposition_codes
    is_tomorrow_disposition = code in disposition_tomorrow_codes

    if is_disposed:
        risk_label, risk_score = '🚫 處置中', 0
    elif is_tomorrow_disposition:
        risk_label, risk_score = '🔶 明天處置', 0
    elif attention_count >= 2:
        risk_label, risk_score = f'🔴 注意 {attention_count}', 0
    elif attention_count == 1:
        risk_label, risk_score = '🟡 注意 1', 10
    elif market_lists_updated:
        risk_label, risk_score = '🟢 官方名單未列示', 20
    else:
        risk_label, risk_score = '⚪ 未查核', 12

    has_breakout = previous_high is not None and close > previous_high if is_long else previous_low is not None and close < previous_low
    confirmation_score = 15 if has_breakout else 5
    score = trend_score + extension_score + candle_score + risk_score + confirmation_score
    too_extended = extension > max_extension_atr
    eligible = not is_disposed and (not block_attention or attention_count < 2) and not too_extended
    if is_long:
        rule = '突破昨高後站穩' if has_breakout else '回踩 5／10 日線止穩'
    else:
        rule = '跌破昨低後確認' if has_breakout else '反彈不過 5／10 日線'
    if is_disposed:
        rule = '排除：處置中'
    elif attention_count >= 2 and block_attention:
        rule = '排除：注意累計偏高'
    elif attention_count >= 2:
        rule = '高風險：僅等待確認'
    elif too_extended:
        rule = f'排除：乖離超過 {_format_compact_number(max_extension_atr, 1)} ATR'

    return {
        'score': score, 'risk': risk_label, 'extension': extension,
        'rule': rule, 'eligible': eligible,
        'detail': f'趨勢 {trend_score}/30｜乖離 {extension_score}/20｜K棒 {candle_score}/15｜風險 {risk_score}/20｜確認 {confirmation_score}/15'
    }

RISK_METRIC_COLUMNS = [
    '_risk_atr14', '_risk_ma20', '_risk_ma20_slope',
    '_risk_close_position', '_risk_prev_high', '_risk_prev_low',
    '_plan_prev_high', '_plan_prev_low',
]

# 當沖資料只在使用者手動更新時回填，避免影響原本選股載入速度。
DAYTRADE_METRIC_COLUMNS = [
    '_daytrade_vwap', '_daytrade_or_high', '_daytrade_or_low',
    '_daytrade_volume_ratio', '_daytrade_close', '_daytrade_data_time'
]
DAYTRADE_REQUIRED_COLUMNS = [
    '_daytrade_vwap', '_daytrade_or_high', '_daytrade_or_low', '_daytrade_close'
]


def _is_opening_micro_window(now_tw=None):
    """09:00–09:15 使用快照與 1 分 K，之後再採完整 5 分 K。"""
    current = now_tw or datetime.now(pytz.timezone('Asia/Taipei'))
    return dt_time(9, 0) <= current.time() < dt_time(9, 15)


def calculate_daytrade_metrics(intraday_df, live_snapshot=None, now_tw=None, interval_label='5 分 K'):
    """計算當沖指標；開盤前 15 分鐘可直接使用即時快照與 1 分 K。"""
    required_columns = {'High', 'Low', 'Close', 'Volume'}
    current = now_tw or datetime.now(pytz.timezone('Asia/Taipei'))
    snapshot_opening = _is_opening_micro_window(current) and live_snapshot is not None
    if (
        not snapshot_opening
        and (intraday_df is None or intraday_df.empty or not required_columns.issubset(intraday_df.columns))
    ):
        return None

    data = intraday_df.copy().sort_index() if isinstance(intraday_df, pd.DataFrame) else pd.DataFrame()
    if not data.empty and required_columns.issubset(data.columns):
        data = data[(data.index.time >= dt_time(9, 0)) & (data.index.time <= dt_time(13, 30))]
        data = data.dropna(subset=['High', 'Low', 'Close', 'Volume'])
    else:
        data = pd.DataFrame()

    if snapshot_opening:
        close = _safe_number(getattr(live_snapshot, 'close', None))
        open_price = _safe_number(getattr(live_snapshot, 'open', None))
        opening_high = _safe_number(getattr(live_snapshot, 'high', None))
        opening_low = _safe_number(getattr(live_snapshot, 'low', None))
        vwap = _safe_number(getattr(live_snapshot, 'average_price', None))
        current_volume = _safe_number(getattr(live_snapshot, 'total_volume', None))
        if None in (close, open_price, opening_high, opening_low) or current_volume is None or current_volume <= 0:
            return None
        if vwap is None or vwap <= 0:
            vwap = (opening_high + opening_low + close) / 3
        latest_day = pd.Timestamp(current.date())
        cutoff_time = current.time()
        data_time = current.strftime('%Y/%m/%d %H:%M:%S')
        phase = '開盤形成中'
        source = 'Shioaji 快照＋1 分 K 同時段量能'
    else:
        if data.empty:
            return None
        latest_day = data.index.normalize().max()
        today = data[data.index.normalize() == latest_day].copy()
        if today.empty or float(today['Volume'].sum()) <= 0:
            return None
        typical_price = (today['High'] + today['Low'] + today['Close']) / 3
        cumulative_volume = today['Volume'].cumsum()
        vwap = float((typical_price * today['Volume']).cumsum().iloc[-1] / cumulative_volume.iloc[-1])
        opening_range = today[(today.index.time >= dt_time(9, 0)) & (today.index.time < dt_time(9, 15))]
        if opening_range.empty:
            return None
        close = float(today['Close'].iloc[-1])
        open_price = float(today['Open'].iloc[0]) if 'Open' in today.columns else float(today['Close'].iloc[0])
        opening_high = float(opening_range['High'].max())
        opening_low = float(opening_range['Low'].min())
        current_volume = float(today['Volume'].sum())
        cutoff_time = today.index[-1].time()
        data_time = today.index[-1].strftime('%Y/%m/%d %H:%M')
        forming_today = (
            latest_day.date() == current.date()
            and _is_opening_micro_window(current)
        )
        phase = '開盤形成中' if forming_today else '開盤區間完成'
        source = interval_label

    # 以最近三個交易日相同的盤中截止時間比較累積量，避免直接拿全天量誤判。
    daily_volume_to_cutoff = []
    if not data.empty:
        for trading_day, frame in data.groupby(data.index.normalize()):
            if trading_day == latest_day:
                continue
            comparable = frame[frame.index.time <= cutoff_time]
            if not comparable.empty:
                daily_volume_to_cutoff.append(float(comparable['Volume'].sum()))
    average_prior_volume = np.mean(daily_volume_to_cutoff[-2:]) if daily_volume_to_cutoff else np.nan
    volume_ratio = float(current_volume / average_prior_volume) if average_prior_volume and average_prior_volume > 0 else None

    return {
        '_daytrade_vwap': round(vwap, 2),
        '_daytrade_or_high': round(opening_high, 2),
        '_daytrade_or_low': round(opening_low, 2),
        '_daytrade_volume_ratio': round(volume_ratio, 2) if volume_ratio is not None else None,
        '_daytrade_close': round(close, 2),
        '_daytrade_open': round(open_price, 2),
        '_daytrade_phase': phase,
        '_daytrade_source': source,
        '_daytrade_data_time': data_time,
    }

def calculate_daytrade_filter_result(row, direction, attention_counts=None, disposition_codes=None, market_lists_updated=False, block_attention=True):
    """建立當沖用的盤中預覽；分數代表條件一致性，並非交易指令。"""
    attention_counts = attention_counts or {}
    disposition_codes = set(disposition_codes or [])
    code = str(row.get('代號', '')).strip()
    is_long = direction == '多頭'
    close = _as_float(row.get('_daytrade_close'))
    vwap = _as_float(row.get('_daytrade_vwap'))
    opening_high = _as_float(row.get('_daytrade_or_high'))
    opening_low = _as_float(row.get('_daytrade_or_low'))
    volume_ratio = _as_float(row.get('_daytrade_volume_ratio'))
    open_price = _as_float(row.get('_daytrade_open'))
    opening_phase = str(row.get('_daytrade_phase', '開盤區間完成'))
    is_opening_micro = opening_phase == '開盤形成中'
    ma5 = _as_float(row.get('_ma5'))
    ma20 = _as_float(row.get('_risk_ma20'))
    ma20_slope = _as_float(row.get('_risk_ma20_slope'))

    if None in (close, vwap, opening_high, opening_low):
        return {
            'score': 0, 'rule': '資料不足：先更新盤中資料', 'eligible': False,
            'vwap_status': '—', 'opening_range': '—',
            'detail': '尚未取得即時串流／分 K、VWAP 或開盤區間資料。', 'data_time': None
        }

    daily_trend_score = 0
    daily_close = _as_float(row.get('收盤價'))
    if daily_close is not None and ma5 is not None and ((is_long and daily_close > ma5) or (not is_long and daily_close < ma5)):
        daily_trend_score += 10
    if daily_close is not None and ma20 is not None and ((is_long and daily_close > ma20) or (not is_long and daily_close < ma20)):
        daily_trend_score += 10
    if ma20_slope is not None and ((is_long and ma20_slope > 0) or (not is_long and ma20_slope < 0)):
        daily_trend_score += 5

    vwap_aligned = close > vwap if is_long else close < vwap
    vwap_score = 25 if vwap_aligned else 0
    if is_opening_micro:
        range_width = max(opening_high - opening_low, get_tick_size(close))
        close_position = (close - opening_low) / range_width
        open_aligned = open_price is not None and (close > open_price if is_long else close < open_price)
        near_live_edge = close_position >= 0.70 if is_long else close_position <= 0.30
        range_broken = bool(open_aligned and near_live_edge)
        range_score = 12 if range_broken else (6 if open_aligned else 0)
    else:
        range_broken = close > opening_high if is_long else close < opening_low
        range_score = 15 if range_broken else 0
    if volume_ratio is None:
        volume_score = 8
    elif volume_ratio >= 1.5:
        volume_score = 20
    elif volume_ratio >= 1.0:
        volume_score = 12
    elif is_opening_micro and volume_ratio >= 0.8:
        volume_score = 8
    else:
        volume_score = 0

    attention_count = attention_counts.get(code, 0)
    is_disposed = code in disposition_codes
    if is_disposed or attention_count >= 2:
        risk_score = 0
    elif attention_count == 1:
        risk_score = 7
    elif market_lists_updated:
        risk_score = 15
    else:
        risk_score = 8

    score = daily_trend_score + vwap_score + volume_score + range_score + risk_score
    risk_blocked = is_disposed or (block_attention and attention_count >= 2)
    minimum_volume_ratio = 0.8 if is_opening_micro else 1.0
    eligible = (
        not risk_blocked and vwap_aligned and range_broken
        and (volume_ratio is None or volume_ratio >= minimum_volume_ratio)
    )
    direction_text = '站上' if is_long else '跌破'
    range_text = ('目前開盤高檔' if is_long else '目前開盤低檔') if is_opening_micro else ('開盤區間高' if is_long else '開盤區間低')
    if risk_blocked:
        rule = '不交易：處置／注意風險'
    elif not vwap_aligned:
        rule = f'觀察：價格需{direction_text} VWAP'
    elif not range_broken:
        rule = (
            f'觀察：需維持 VWAP 並接近{range_text}'
            if is_opening_micro else f'觀察：需{direction_text}{range_text}'
        )
    elif volume_ratio is not None and volume_ratio < minimum_volume_ratio:
        rule = '觀察：量能未達近期同時段平均'
    else:
        rule = (
            f'觸發：開盤動能{direction_text} VWAP＋接近{range_text}'
            if is_opening_micro else f'觸發：{direction_text} VWAP＋{direction_text}{range_text}'
        )

    data_source = str(row.get('_daytrade_source', '—'))
    return {
        'score': score, 'rule': rule, 'eligible': eligible,
        'vwap_status': (
            '偏多：站上 VWAP' if close > vwap
            else ('偏空：跌破 VWAP' if close < vwap else '中性：貼近 VWAP')
        ),
        'opening_range': f"{'形成中 ' if is_opening_micro else ''}{fmt_price(opening_low)}－{fmt_price(opening_high)}",
        'detail': (
            f'日 K 趨勢 {daily_trend_score}/25｜VWAP {vwap_score}/25｜量能 {volume_score}/20｜'
            f"{'開盤動能' if is_opening_micro else '開盤區間'} {range_score}/{'12' if is_opening_micro else '15'}｜"
            f'風險 {risk_score}/15｜來源 {data_source}'
        ),
        'data_time': row.get('_daytrade_data_time')
    }

def build_trade_plan(row, direction, is_daytrade_mode, filter_result):
    """依已通過的篩選條件建立觀察用進場、停損與目標價，不執行下單。"""
    minimum_reward_risk = 1.3
    maximum_chase_r = 0.25
    if not filter_result.get('eligible'):
        return {
            'summary': '—',
            'detail': f"未通過條件，不預判點位（{filter_result.get('rule', '資料不足')}）",
            'valid': False, 'reward_risk': None,
        }

    is_long = direction == '多頭'

    def _round(value):
        return round_to_tick(max(0.01, value))

    def _format_plan(entry, stop, target, trigger_text, current_price=None):
        if is_long and not (stop < entry < target):
            return {
                'summary': '⛔ 不追價｜可用獲利空間不足',
                'detail': '停損、進場與目標的順序不合理，本次不建立訊號。',
                'valid': False, 'reward_risk': None,
                'blocking_reason': '價位空間不足',
            }
        if not is_long and not (target < entry < stop):
            return {
                'summary': '⛔ 不追價｜可用獲利空間不足',
                'detail': '停損、進場與目標的順序不合理，本次不建立訊號。',
                'valid': False, 'reward_risk': None,
                'blocking_reason': '價位空間不足',
            }
        risk_distance = abs(entry - stop)
        reward_distance = abs(target - entry)
        reward_risk = reward_distance / risk_distance if risk_distance > 0 else 0
        if reward_risk < minimum_reward_risk:
            acceptable_entry = (
                (target + minimum_reward_risk * stop) / (1 + minimum_reward_risk)
            )
            rounded_acceptable_entry = _round(acceptable_entry)
            if is_long and rounded_acceptable_entry > acceptable_entry:
                rounded_acceptable_entry = move_tick(rounded_acceptable_entry, -1)
            elif not is_long and rounded_acceptable_entry < acceptable_entry:
                rounded_acceptable_entry = move_tick(rounded_acceptable_entry, 1)
            acceptable_label = '≤' if is_long else '≥'
            return {
                'summary': (
                    f"⛔ 不追價｜可接受進場 {acceptable_label}{fmt_price(rounded_acceptable_entry)}｜"
                    f"停 {fmt_price(stop)}｜目 {fmt_price(target)}｜RR {reward_risk:.2f}"
                ),
                'detail': (
                    f'漲跌停或壓力限制後，剩餘風報比僅 {reward_risk:.2f}，低於 {minimum_reward_risk:.1f}；'
                    '等待價格回測到可接受區，不建立策略驗證訊號。'
                ),
                'valid': False, 'reward_risk': round(reward_risk, 2),
                'blocking_reason': '剩餘風報比不足',
            }
        price = _as_float(current_price)
        chase_r = (
            ((price - entry) if is_long else (entry - price)) / risk_distance
            if price is not None and risk_distance > 0 else None
        )
        if chase_r is not None and chase_r > maximum_chase_r:
            return {
                'summary': (
                    f"⛔ 不追價｜等待回測 {fmt_price(entry)} 附近｜"
                    f"停 {fmt_price(stop)}｜目 {fmt_price(target)}"
                ),
                'detail': (
                    f'目前價格已越過原進場點 {chase_r:.2f}R，超過 {maximum_chase_r:.2f}R 追價上限；'
                    '等待回測守穩，不建立策略驗證訊號。'
                ),
                'valid': False, 'reward_risk': round(reward_risk, 2),
                'blocking_reason': '已偏離進場點',
            }
        summary = (
            f"進 {fmt_price(entry)}｜停 {fmt_price(stop)}｜目 {fmt_price(target)}｜"
            f"RR {reward_risk:.2f}"
        )
        return {
            'summary': summary,
            'detail': (
                f"{trigger_text}；預判進場 {fmt_price(entry)}、策略失效離場 {fmt_price(stop)}、"
                f"第一目標 {fmt_price(target)}（實際風報比 {reward_risk:.2f}）"
            ),
            'valid': True, 'reward_risk': round(reward_risk, 2),
        }

    if is_daytrade_mode:
        vwap = _as_float(row.get('_daytrade_vwap'))
        opening_high = _as_float(row.get('_daytrade_or_high'))
        opening_low = _as_float(row.get('_daytrade_or_low'))
        opening_forming = str(row.get('_daytrade_phase', '')) == '開盤形成中'
        if None in (vwap, opening_high, opening_low):
            return {
                'summary': '—', 'detail': '缺少 VWAP 或開盤區間，不預判點位。',
                'valid': False, 'reward_risk': None,
            }

        current_price = _as_float(row.get('_daytrade_close'))

        if is_long:
            limit_up = _as_float(row.get('_交易日漲停價', row.get('當日漲停價')))
            limit_down = _as_float(row.get('_交易日跌停價', row.get('當日跌停價')))
            if limit_up is not None and opening_high >= limit_up:
                return {
                    'summary': '⛔ 不追價｜已到漲停，無可用突破價',
                    'detail': '開盤區間高點已達當日漲停，無法再建立合法的向上突破進場價。',
                    'valid': False, 'reward_risk': None,
                    'blocking_reason': '已到漲停',
                }
            entry = _round(move_tick(opening_high, 1))
            stop = _round(max(vwap, opening_low))
            if stop >= entry:
                stop = _round(move_tick(entry, -2))
            target = _round(entry + (entry - stop) * 1.5)
            if limit_up is not None and limit_up > 0:
                # Entry/target must not cross the current day's upper limit;
                # leave one legal tick for a trigger above the range high.
                entry = min(entry, _round(move_tick(limit_up, -1)))
                target = min(target, _round(limit_up))
            if limit_down is not None and limit_down > 0:
                stop = max(stop, _round(limit_down))
            trigger = (
                '突破目前開盤高點且維持 VWAP 上方（區間形成中）'
                if opening_forming else '開盤區間高點突破後，維持 VWAP 上方'
            )
            return _format_plan(entry, stop, target, trigger, current_price)

        limit_down = _as_float(row.get('_交易日跌停價', row.get('當日跌停價')))
        limit_up = _as_float(row.get('_交易日漲停價', row.get('當日漲停價')))
        if limit_down is not None and opening_low <= limit_down:
            return {
                'summary': '⛔ 不追價｜已到跌停，無可用跌破價',
                'detail': '開盤區間低點已達當日跌停，無法再建立合法的向下跌破進場價。',
                'valid': False, 'reward_risk': None,
                'blocking_reason': '已到跌停',
            }
        entry = _round(move_tick(opening_low, -1))
        stop = _round(min(vwap, opening_high))
        if stop <= entry:
            stop = _round(move_tick(entry, 2))
        target = _round(entry - (stop - entry) * 1.5)
        if limit_down is not None and limit_down > 0:
            entry = max(entry, _round(move_tick(limit_down, 1)))
            target = max(target, _round(limit_down))
        if limit_up is not None and limit_up > 0:
            stop = min(stop, _round(limit_up))
        trigger = (
            '跌破目前開盤低點且維持 VWAP 下方（區間形成中）'
            if opening_forming else '開盤區間低點跌破後，維持 VWAP 下方'
        )
        return _format_plan(entry, stop, target, trigger, current_price)

    atr14 = _as_float(row.get('_risk_atr14'))
    previous_high = _as_float(row.get('_plan_prev_high', row.get('_risk_prev_high')))
    previous_low = _as_float(row.get('_plan_prev_low', row.get('_risk_prev_low')))
    if atr14 is None or atr14 <= 0 or previous_high is None or previous_low is None:
        return {
            'summary': '—', 'detail': '缺少 ATR 或昨高／昨低，不預判次日開盤點位。',
            'valid': False, 'reward_risk': None,
        }

    if is_long:
        entry = _round(move_tick(previous_high, 1))
        stop = _round(entry - atr14)
        target = _round(entry + (entry - stop) * 1.5)
        return _format_plan(entry, stop, target, '次日開盤站穩昨高後再觀察進場')

    entry = _round(move_tick(previous_low, -1))
    stop = _round(entry + atr14)
    target = _round(entry - (stop - entry) * 1.5)
    return _format_plan(entry, stop, target, '次日開盤跌破昨低後再觀察進場')

def build_stock_support_resistance(row, is_daytrade_mode=False):
    """沿用已取得的原策略價位，整理出目前價格最近的支撐與壓力。"""
    if is_daytrade_mode:
        opening_low = _safe_number(row.get('_daytrade_or_low'))
        opening_high = _safe_number(row.get('_daytrade_or_high'))
        vwap = _safe_number(row.get('_daytrade_vwap'))
        if opening_low is not None and opening_high is not None:
            suffix = f'｜VWAP {fmt_price(vwap)}' if vwap is not None else ''
            return f'支 {fmt_price(opening_low)}｜壓 {fmt_price(opening_high)}{suffix}'

    current_price = _safe_number(row.get('收盤價'))
    values = []
    for point in row.get('_points', []) if isinstance(row.get('_points', []), list) else []:
        value = _safe_number(point.get('val')) if isinstance(point, dict) else None
        if value is not None and value > 0:
            values.append(value)
    previous_low = _safe_number(row.get('_risk_prev_low'))
    previous_high = _safe_number(row.get('_risk_prev_high'))
    values.extend(value for value in (previous_low, previous_high) if value is not None)
    if current_price is None or not values:
        return '資料不足'
    supports = [value for value in values if value <= current_price]
    resistances = [value for value in values if value >= current_price]
    support = max(supports) if supports else min(values)
    resistance = min(resistances) if resistances else max(values)
    return f'支 {fmt_price(support)}｜壓 {fmt_price(resistance)}'

def get_tick_size(price):
    try: price = float(price)
    except: return 0.01
    if pd.isna(price) or price <= 0: return 0.01
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def apply_tick_rules(price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded = (Decimal(str(p)) / Decimal(str(tick))).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
        return float(rounded)
    except:
        try: return float(price)
        except: return 0.0

def calculate_stop_loss_price(base_price, stop_loss_percent, is_long=True):
    """依交易方向及台股跳動單位，計算可下單的停損價格。"""
    try:
        base = float(base_price)
        percent = float(stop_loss_percent)
        if math.isnan(base) or math.isnan(percent) or base <= 0 or percent < 0:
            return 0.0
        raw_price = base * (1 - percent / 100) if is_long else base * (1 + percent / 100)
        return apply_tick_rules(raw_price)
    except (TypeError, ValueError):
        return 0.0

def calculate_limits(price):
    try:
        base = Decimal(str(price))
        if not base.is_finite() or base <= 0:
            return 0, 0
        raw_up = base * Decimal('1.10')
        tick_up = Decimal(str(get_tick_size(raw_up)))
        limit_up = (raw_up / tick_up).to_integral_value(rounding=ROUND_FLOOR) * tick_up
        raw_down = base * Decimal('0.90')
        tick_down = Decimal(str(get_tick_size(raw_down)))
        limit_down = (raw_down / tick_down).to_integral_value(rounding=ROUND_CEILING) * tick_down
        return float(limit_up), float(limit_down)
    except (ArithmeticError, TypeError, ValueError):
        return 0, 0


def stock_limit_context(previous_close, current_close, now_tw=None):
    """Return today's limits and the limits shown for the next session.

    Taiwan stocks switch from the current trading day to the next session's
    reference at 14:30.  Keep both pairs: the displayed columns can show the
    next-session limits after the switch, while limit coloring and day-trade
    plans must still compare against the *actual current-day* limits.
    """
    current = now_tw or datetime.now(pytz.timezone('Asia/Taipei'))
    if not hasattr(current, 'time'):
        current = datetime.now(pytz.timezone('Asia/Taipei'))
    previous_value = _safe_number(previous_close)
    current_value = _safe_number(current_close)
    today_up, today_down = calculate_limits(previous_value) if previous_value else (0, 0)
    after_close = current.time() >= dt_time(14, 30)
    display_reference = current_value if after_close and current_value else previous_value
    display_up, display_down = calculate_limits(display_reference) if display_reference else (0, 0)
    return {
        'today_up': today_up,
        'today_down': today_down,
        'display_up': display_up,
        'display_down': display_down,
        'display_is_next_session': bool(after_close),
    }

def move_tick(price, steps):
    try:
        curr = float(price)
        if steps > 0:
            for _ in range(steps):
                tick = get_tick_size(curr)
                curr = round(curr + tick, 2)
        elif steps < 0:
            for _ in range(abs(steps)):
                tick = get_tick_size(curr - 0.0001)
                curr = round(curr - tick, 2)
        return curr
    except: return price

def apply_sr_rules(price, base_price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        d_val = Decimal(str(p))
        d_tick = Decimal(str(tick))
        if p < base_price: return float(math.ceil(d_val / d_tick) * d_tick)
        elif p > base_price: return float(math.floor(d_val / d_tick) * d_tick)
        else: return apply_tick_rules(p)
    except: return price

def fmt_price(v):
    try:
        if pd.isna(v) or v == "": return ""
        return f"{float(v):.2f}".rstrip('0').rstrip('.')
    except: return str(v)


def calculate_note_width(series, font_size):
    def get_width(s):
        w = 0
        for c in str(s): w += 2.0 if ord(c) > 127 else 1.0
        return w
    if series.empty: return 50
    max_w = series.apply(get_width).max()
    if pd.isna(max_w): max_w = 0
    pixel_width = int(max_w * (font_size * 0.44))
    return max(50, pixel_width)

def recalculate_row(row, points_map):
    # 股票戰略室改以成交價作為唯一計算基準，不再使用舊版自訂價。
    custom_price = row.get('收盤價')
    code = row.get('代號')
    status = ""
    if pd.isna(custom_price) or str(custom_price).strip() == "": return status
    try:
        price = float(custom_price)
        # After 14:30 the visible columns may already show next-session
        # limits; status coloring must still use the current trading day's
        # limits so a normal price is never mislabeled as 漲停／跌停.
        limit_up = row.get('_交易日漲停價', row.get('當日漲停價'))
        limit_down = row.get('_交易日跌停價', row.get('當日跌停價'))
        l_up = float(limit_up) if limit_up and str(limit_up).replace('.','').isdigit() else None
        l_down = float(limit_down) if limit_down and str(limit_down).replace('.','').isdigit() else None
        
        strat_values = []
        points = points_map.get(code, [])
        if isinstance(points, list):
            for p in points: strat_values.append(p['val'])
            
        note_text = str(row.get('戰略備註', ''))
        found_prices = re.findall(r'\d+\.?\d*', note_text)
        for fp in found_prices:
            try: strat_values.append(float(fp))
            except: pass
            
        if l_up is not None and abs(price - l_up) < 0.01: status = "漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: status = "跌停"
        elif strat_values:
            max_val = max(strat_values)
            min_val = min(strat_values)
            if price > max_val: status = "強"
            elif price < min_val: status = "弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "命中"
        return status
    except: return status

def generate_note_from_points(points, manual_note, show_3d):
    # 修正：加入安全判斷，防止重整或合併時產生 NaN 導致的 TypeError
    if not isinstance(points, list):
        points = []
        
    display_candidates = []
    target_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        t = p.get('tag', '')
        if t in target_tags and not show_3d: continue
        if p['val'] <= 0: continue
        display_candidates.append(p)
        
    display_candidates.sort(key=lambda x: x['val'])
    note_parts = []
    seen_vals = set() 
    
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        if val in seen_vals: continue
        seen_vals.add(val)
        g_list = list(group)
        tags = [x['tag'] for x in g_list if x['tag']]
        
        final_tag = ""
        if "漲停高" in tags: final_tag = "漲停高"
        elif "跌停低" in tags: final_tag = "跌停低" 
        elif "漲停" in tags: final_tag = "漲停"
        elif "跌停" in tags: final_tag = "跌停"
        elif "多" in tags: final_tag = "多"
        elif "空" in tags: final_tag = "空"
        elif "平" in tags: final_tag = "平"
        elif "高" in tags: final_tag = "高"
        elif "低" in tags: final_tag = "低"
        elif "今高" in tags: final_tag = "今高"
        elif "今低" in tags: final_tag = "今低"
        elif "昨高" in tags: final_tag = "昨高"
        elif "昨低" in tags: final_tag = "昨低"
        elif "前高" in tags: final_tag = "前高"
        elif "前低" in tags: final_tag = "前低"
        
        v_str = fmt_price(val)
        suffix_tags = ["多", "空", "平"]
        prefix_tags = ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]
        numeric_only_tags = ["前高", "前低", "昨高", "昨低", "今高", "今低"]
        
        if final_tag in suffix_tags: 
            if final_tag == "多":
                item = f"🔴{v_str}{final_tag}"
            elif final_tag == "空":
                item = f"🟢{v_str}{final_tag}"
            else:
                item = f"{v_str}{final_tag}"
        elif final_tag in prefix_tags: item = f"{final_tag}{v_str}"
        elif final_tag in numeric_only_tags: item = v_str 
        elif final_tag: item = f"{v_str}{final_tag}" 
        else: item = v_str
        note_parts.append(item)
        
    auto_note = "-".join(note_parts)
    if manual_note:
        if manual_note.startswith("[M]"): return manual_note[3:], auto_note
        if auto_note and manual_note.strip().startswith(auto_note.strip()): return manual_note, auto_note
        return f"{auto_note}{manual_note}", auto_note
    return auto_note, auto_note

def fetch_stock_data_raw(
    code, name_hint="", extra_data=None, futures_set=None,
    saved_notes_dict=None, name_map_dict=None, sj_logged_in=False,
    sj_api=None, include_live_quote=True,
):
    code = str(code).strip()
    hist = pd.DataFrame()
    source_used = "none"
    live_quote_price = live_quote_rate = live_quote_bid = live_quote_ask = None
    live_quote_time = None
    
    # 優先使用永豐 API 擷取昨日/歷史日 K 線資料
    if sj_logged_in and sj_api is not None:
        sj_df = fetch_shioaji_data(sj_api, code, interval='1d', lookback_days=40)
        if not sj_df.empty:
            hist = sj_df
            source_used = "shioaji"
        if include_live_quote and re.fullmatch(r'\d{4,6}', code):
            try:
                stock_contract = sj_api.Contracts.Stocks[code]
                stock_snapshots = get_stream_quotes(sj_api, [stock_contract])
                if stock_snapshots:
                    stock_snapshot = stock_snapshots[0]
                    live_quote_price = _safe_number(getattr(stock_snapshot, 'close', None))
                    live_quote_rate = snapshot_change_rate(stock_snapshot, live_quote_price)
                    live_quote_bid = _safe_number(getattr(stock_snapshot, 'buy_price', None))
                    live_quote_ask = _safe_number(getattr(stock_snapshot, 'sell_price', None))
                    live_quote_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S')
            except Exception:
                pass

    # 若永豐未登入或沒抓到，退回使用 twstock 擷取
    if hist.empty:
        try:
            stock = twstock.Stock(code)
            tw_data = stock.fetch_31()
            if tw_data and len(tw_data) > 0:
                df_tw = pd.DataFrame(tw_data)
                df_tw['Date'] = pd.to_datetime(df_tw['date'])
                df_tw = df_tw.set_index('Date')
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}
                df_tw = df_tw.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in cols: df_tw[c] = pd.to_numeric(df_tw[c], errors='coerce')
                if not df_tw.empty:
                    hist = df_tw[cols]
                    source_used = "twstock"
        except: pass

    if hist.empty and si is not None:
        try:
            try: df_yf = si.get_data(f"{code}.TW", start_date=(datetime.now() - timedelta(days=40)))
            except:
                try: df_yf = si.get_data(f"{code}.TWO", start_date=(datetime.now() - timedelta(days=40)))
                except: df_yf = pd.DataFrame()
            
            if not df_yf.empty:
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
                df_yf = df_yf.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(c in df_yf.columns for c in cols):
                    hist = df_yf[cols]
                    source_used = "yahoo_fin"
        except: pass

    if hist.empty:
        try:
            ticker_obj = yf.Ticker(f"{code}.TW")
            hist_yf = ticker_obj.history(period="3mo", auto_adjust=False)
            if hist_yf.empty:
                ticker_obj = yf.Ticker(f"{code}.TWO")
                hist_yf = ticker_obj.history(period="3mo", auto_adjust=False)
            if not hist_yf.empty:
                hist = hist_yf
                source_used = "yfinance"
        except Exception: 
            pass

    # 僅當未使用永豐 API，且需獲取即時資訊時，才透過 twstock.realtime 補足今日最新
    if include_live_quote and source_used != "shioaji" and live_quote_price is None:
        try:
            rt_data = twstock.realtime.get(code)
            if rt_data['success'] and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                rt_price = float(rt_data['realtime']['latest_trade_price'])
                rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                live_quote_price = rt_price
                yesterday_close = _safe_number(rt_data['realtime'].get('yesterday_close'))
                if yesterday_close is not None and yesterday_close > 0:
                    live_quote_rate = (rt_price - yesterday_close) / yesterday_close * 100
                
                rt_time_str = rt_data['info']['time']
                rt_dt = datetime.strptime(rt_time_str, "%Y-%m-%d %H:%M:%S")
                live_quote_time = rt_time_str
                today_date = pd.Timestamp(datetime.now(tz_tw).date())

                if hist.empty:
                    hist = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                else:
                    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
                    last_hist_date = hist.index[-1]
                    if last_hist_date < today_date:
                        if datetime.now(tz_tw).weekday() < 5:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                            hist = pd.concat([hist, new_row])
                            hist.sort_index(inplace=True)
                    elif last_hist_date == today_date:
                        hist.at[last_hist_date, 'Close'] = rt_price
                        hist.at[last_hist_date, 'High'] = max(hist.at[last_hist_date, 'High'], rt_high)
                        hist.at[last_hist_date, 'Low'] = min(hist.at[last_hist_date, 'Low'], rt_low)
                        hist.at[last_hist_date, 'Volume'] = rt_vol
                        if hist.at[last_hist_date, 'Open'] == 0: hist.at[last_hist_date, 'Open'] = rt_open
        except: pass 

    if hist.empty: return None
    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

    # Preserve the current session's reference before the pre-14:30 strategy
    # view removes an in-progress candle.  The previous implementation then
    # used the day-before-yesterday close for today's limit, which was visible
    # as a wrong limit immediately after a 14:30 refresh.
    tz_tw_calc = pytz.timezone('Asia/Taipei')
    now_tw_calc = datetime.now(tz_tw_calc)
    switch_time = dt_time(14, 30)
    current_session_prev_close = None
    current_session_close = None
    if not hist.empty:
        latest_session_date = pd.Timestamp(hist.index[-1]).date()
        current_session_close = _safe_number(hist.iloc[-1].get('Close'))
        if latest_session_date == now_tw_calc.date() and len(hist) >= 2:
            current_session_prev_close = _safe_number(hist.iloc[-2].get('Close'))
        else:
            # No current-day candle (common before the realtime source is
            # available): the latest completed close is today's reference.
            current_session_prev_close = current_session_close

    if now_tw_calc.time() < switch_time:
        if not hist.empty and hist.index[-1].date() == now_tw_calc.date():
            if len(hist) > 1:
                hist = hist.iloc[:-1]

    if hist.empty: return None

    # 修正夜盤基準：若是期貨，透過快照直接擷取官方基準價 (日盤 13:45 收盤價)
    if include_live_quote and sj_logged_in and sj_api is not None and code in ["TWF=F", "TMF=F"]:
        try:
            contract = None
            if code == "TWF=F":
                contract = min([c for c in sj_api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code], key=lambda c: getattr(c, 'delivery_date', '999999'))
            else:
                contract = sj_api.Contracts.Futures.TMF.TMFR1
            if contract:
                snap = get_stream_quotes(sj_api, [contract])
                if snap and len(snap) > 0:
                    s = snap[0]
                    rt_p = s.close if s.close > 0 else s.open
                    live_quote_price = _safe_number(rt_p)
                    live_quote_rate = snapshot_change_rate(s, live_quote_price)
                    live_quote_bid = _safe_number(getattr(s, 'buy_price', None))
                    live_quote_ask = _safe_number(getattr(s, 'sell_price', None))
                    live_quote_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S')
                    change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                    if change_val is not None and rt_p > 0:
                        official_ref = rt_p - float(change_val)
                        if len(hist) >= 2:
                            hist.iloc[-2, hist.columns.get_loc('Close')] = official_ref
        except:
            pass

    live_base_price = hist.iloc[-1]['Close']
    if len(hist) >= 2: live_prev_price = hist.iloc[-2]['Close']
    else: live_prev_price = live_base_price
    if live_prev_price > 0: live_pct_change = ((live_base_price - live_prev_price) / live_prev_price) * 100
    else: live_pct_change = 0.0

    hist_strat = hist.copy()

    # 風險篩選預覽所需指標：只沿用已取得的日 K，不增加任何資料請求。
    risk_atr14 = risk_ma20 = risk_ma20_slope = risk_close_position = None
    risk_prev_high = risk_prev_low = None
    plan_prev_high = plan_prev_low = None
    if len(hist_strat) >= 2:
        prev_close_series = hist_strat['Close'].shift(1)
        true_range = pd.concat([
            hist_strat['High'] - hist_strat['Low'],
            (hist_strat['High'] - prev_close_series).abs(),
            (hist_strat['Low'] - prev_close_series).abs()
        ], axis=1).max(axis=1)
        risk_atr14 = float(true_range.tail(min(14, len(true_range))).mean())
        risk_prev_high = float(hist_strat['High'].iloc[-2])
        risk_prev_low = float(hist_strat['Low'].iloc[-2])
        plan_prev_high = float(hist_strat['High'].iloc[-1])
        plan_prev_low = float(hist_strat['Low'].iloc[-1])
        latest_range = float(hist_strat['High'].iloc[-1] - hist_strat['Low'].iloc[-1])
        if latest_range > 0:
            risk_close_position = float((hist_strat['Close'].iloc[-1] - hist_strat['Low'].iloc[-1]) / latest_range * 100)

    if len(hist_strat) >= 20:
        ma20_series = hist_strat['Close'].rolling(20).mean()
        risk_ma20 = float(ma20_series.iloc[-1])
        if len(ma20_series.dropna()) >= 6:
            risk_ma20_slope = float(ma20_series.iloc[-1] - ma20_series.iloc[-6])

    strategy_base_price = hist_strat.iloc[-1]['Close']
    if len(hist_strat) >= 2: prev_of_base = hist_strat.iloc[-2]['Close']
    else: prev_of_base = strategy_base_price 

    # Before 14:30 the columns refer to today's limits; after 14:30 they refer
    # to the next session.  Keep today's pair separately for color and daytrade
    # target clamping so those calculations never use tomorrow's limits.
    base_price_for_limit = prev_of_base
    if current_session_prev_close is None:
        current_session_prev_close = _safe_number(base_price_for_limit)
    if current_session_close is None:
        current_session_close = _safe_number(strategy_base_price)
    limit_context = stock_limit_context(
        current_session_prev_close, current_session_close, now_tw_calc,
    )
    limit_up_show = limit_context['display_up']
    limit_down_show = limit_context['display_down']
    limit_up_today = limit_context['today_up']
    limit_down_today = limit_context['today_down']
    limit_up_T = limit_up_today
    limit_down_T = limit_down_today

    target_price = apply_sr_rules(strategy_base_price * 1.03, strategy_base_price)
    stop_price = apply_sr_rules(strategy_base_price * 0.97, strategy_base_price)
    
    points = []
    recent_records = hist_strat.tail(3).to_dict('records')
    recent_records.reverse()
    days_map = {0: "今", 1: "昨", 2: "前"}
    
    for idx, row in enumerate(recent_records):
        if idx in days_map:
            prefix = days_map[idx]
            h_val = apply_tick_rules(row['High'])
            l_val = apply_tick_rules(row['Low'])
            if h_val > 0 and limit_down_show <= h_val <= limit_up_show: points.append({"val": h_val, "tag": f"{prefix}高"})
            if l_val > 0 and limit_down_show <= l_val <= limit_up_show: points.append({"val": l_val, "tag": f"{prefix}低"})

    if len(hist_strat) >= 5:
        last_5_closes = hist_strat['Close'].tail(5).values
        avg_val = sum(Decimal(str(x)) for x in last_5_closes) / Decimal("5")
        ma5_raw = float(avg_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1]
        p_open = apply_tick_rules(last_candle['Open'])
        if limit_down_show <= p_open <= limit_up_show: points.append({"val": p_open, "tag": ""})

        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        if limit_down_show <= p_low <= limit_up_show: 
             tag_low = "跌停" if limit_down_T and abs(p_low - limit_down_T) < 0.01 else ""
             points.append({"val": p_low, "tag": tag_low})

    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2]
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    show_plus_3 = False
    show_minus_3 = False
    
    if not hist_strat.empty:
        high_90_raw = hist_strat['High'].max()
        low_vals = hist_strat['Low'][hist_strat['Low'] > 0]
        low_90_raw = low_vals.min() if not low_vals.empty else hist_strat['Low'].min()
            
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})
        
        if len(hist_strat) >= 2:
             today_high = hist_strat.iloc[-1]['High']
             if limit_up_T and abs(today_high - limit_up_T) < 0.01:
                 tag_label = "漲停高" if (abs(limit_up_T - high_90_raw) < 0.05) else "漲停"
                 if limit_down_show <= limit_up_T <= limit_up_show: points.append({"val": limit_up_T, "tag": tag_label})

        if len(hist_strat) >= 2:
            high_T = hist_strat.iloc[-1]['High']
            low_T = hist_strat.iloc[-1]['Low']
            close_T = hist_strat.iloc[-1]['Close']
            if (limit_up_T and high_T >= limit_up_T - 0.01) and (limit_up_T and close_T >= limit_up_T * 0.97): show_plus_3 = True
            if (limit_down_T and low_T <= limit_down_T + 0.01) and (limit_down_T and close_T <= limit_down_T * 1.03): show_minus_3 = True

    if show_plus_3: points.append({"val": target_price, "tag": ""})
    if show_minus_3: points.append({"val": stop_price, "tag": ""})
        
    full_calc_points = []
    threed_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        v = float(f"{p['val']:.2f}")
        if p.get('force', False) or p.get('tag') in threed_tags or (limit_down_show <= v <= limit_up_show): full_calc_points.append(p) 
    
    manual_note = saved_notes_dict.get(code, "") if saved_notes_dict else ""
    strategy_note, auto_note = generate_note_from_points(full_calc_points, manual_note, show_3d=False)
    
    if name_hint: final_name = name_hint
    elif name_map_dict and code in name_map_dict: final_name = name_map_dict[code]
    else: final_name = code

    final_name_display = final_name
    # 兼容字典與舊版集合的判定
    has_futures = futures_set.get(code, "") if isinstance(futures_set, dict) else ("✅" if futures_set and code in futures_set else "")
    
    display_price = live_quote_price if live_quote_price is not None and live_quote_price > 0 else live_base_price
    display_change_rate = live_quote_rate if live_quote_rate is not None else live_pct_change
    return {
        "代號": code, "名稱": final_name_display, "收盤價": round(display_price, 2), "漲跌幅": display_change_rate, "期貨": has_futures,
        "成交價價差": price_change_amount(display_price, display_change_rate),
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show,
        "_交易日漲停價": limit_up_today, "_交易日跌停價": limit_down_today,
        "_漲跌停基準": "隔日開盤" if limit_context['display_is_next_session'] else "當日",
        "戰略備註": strategy_note, "_points": full_calc_points, "狀態": "", "_auto_note": auto_note, "_ma5": ma5 if 'ma5' in locals() else None,
        "_risk_atr14": risk_atr14, "_risk_ma20": risk_ma20, "_risk_ma20_slope": risk_ma20_slope,
        "_risk_close_position": risk_close_position, "_risk_prev_high": risk_prev_high, "_risk_prev_low": risk_prev_low,
        "_plan_prev_high": plan_prev_high, "_plan_prev_low": plan_prev_low,
        "_quote_bid": live_quote_bid, "_quote_ask": live_quote_ask, "_quote_time": live_quote_time,
        "_data_as_of": live_quote_time or pd.Timestamp(hist.index[-1]).strftime('%Y/%m/%d'),
        "_data_stale": False,
    }


def _stale_stock_identity_row(row):
    """Keep a failed symbol visible without presenting yesterday's strategy as current."""
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    identity_columns = (
        '代號', '名稱', '期貨', '_source', '_order', '_source_rank', '_data_as_of',
    )
    stale_row = {
        column: row_dict.get(column)
        for column in identity_columns
        if column in row_dict
    }
    stale_row['_data_stale'] = True
    return stale_row


def _merge_refreshed_stock_rows(stock_data, fetched_results):
    """Replace cached rows with fresh results while retaining ordering metadata."""
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        return pd.DataFrame(), 0
    refreshed_rows = []
    updated_count = 0
    result_map = {
        str(code).strip(): result
        for code, result in (fetched_results or [])
        if str(code).strip()
    }
    metadata_columns = ('_source', '_order', '_source_rank')
    for _, cached_row in stock_data.iterrows():
        code = str(cached_row.get('代號', '')).strip()
        fresh = result_map.get(code)
        if isinstance(fresh, dict) and fresh:
            merged = dict(fresh)
            for column in metadata_columns:
                if column in cached_row.index:
                    merged[column] = cached_row.get(column)
            if not str(merged.get('期貨', '')).strip() and str(cached_row.get('期貨', '')).strip():
                merged['期貨'] = cached_row.get('期貨')
            merged['_data_stale'] = False
            refreshed_rows.append(merged)
            updated_count += 1
        else:
            refreshed_rows.append(_stale_stock_identity_row(cached_row))
    refreshed = pd.DataFrame(refreshed_rows)
    if '_source_rank' in refreshed.columns and '_order' in refreshed.columns:
        refreshed = refreshed.sort_values(['_source_rank', '_order'], kind='stable')
    return refreshed.reset_index(drop=True), updated_count


def refresh_persisted_stock_rows(
    stock_data, futures_set, saved_notes_dict, name_map_dict,
    sj_logged_in=False, sj_api=None,
):
    """Refresh restored table rows; persisted data supplies identity/order, not market values."""
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty or '代號' not in stock_data.columns:
        return stock_data, 0
    futures_copy = dict(futures_set) if isinstance(futures_set, dict) else futures_set
    notes_copy = dict(saved_notes_dict or {})
    name_copy = dict(name_map_dict or {})
    tasks = [
        (str(row.get('代號', '')).strip(), str(row.get('名稱', '')).strip())
        for _, row in stock_data.iterrows()
        if str(row.get('代號', '')).strip()
    ]

    def fetch_latest(task):
        code, name = task
        for attempt in range(2):
            try:
                time.sleep(API_REQUEST_GAP_SECONDS if attempt == 0 else 0.25)
                result = fetch_stock_data_raw(
                    code, name, None, futures_copy, notes_copy, name_copy,
                    sj_logged_in, sj_api,
                )
                if isinstance(result, dict) and result:
                    return code, result
            except Exception:
                pass
        return code, None

    with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
        results = list(executor.map(fetch_latest, tasks))
    return _merge_refreshed_stock_rows(stock_data, results)


def refresh_risk_metrics_for_codes(stock_data, futures_set, saved_notes_dict, name_map_dict, sj_logged_in=False, sj_api=None):
    """手動重抓日 K，僅回填風險篩選所需欄位，保留原本的表格與備註資料。"""
    if stock_data.empty or '代號' not in stock_data.columns:
        return stock_data, 0

    futures_copy = dict(futures_set) if isinstance(futures_set, dict) else futures_set
    notes_copy = dict(saved_notes_dict or {})
    name_copy = dict(name_map_dict or {})
    tasks = [
        (str(row['代號']), str(row.get('名稱', '')))
        for _, row in stock_data.iterrows()
    ]

    def fetch_metrics(task):
        code, name = task
        try:
            time.sleep(API_REQUEST_GAP_SECONDS)
            result = fetch_stock_data_raw(
                code, name, None, futures_copy, notes_copy, name_copy,
                sj_logged_in, sj_api, include_live_quote=False,
            )
            if result:
                return code, {column: result.get(column) for column in RISK_METRIC_COLUMNS}
        except Exception:
            pass
        return code, None

    with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
        results = list(executor.map(fetch_metrics, tasks))

    refreshed = stock_data.copy()
    updated_count = 0
    for code, metrics in results:
        if not metrics or metrics.get('_risk_atr14') is None or metrics.get('_risk_ma20') is None:
            continue
        row_mask = refreshed['代號'].astype(str) == code
        for column, value in metrics.items():
            refreshed.loc[row_mask, column] = value
        updated_count += int(row_mask.sum())
    return refreshed, updated_count

def fetch_stock_snapshot_map(api, codes):
    """以單一批次取得股票快照，避免逐檔重複請求。"""
    if api is None:
        return {}
    contracts = []
    for code in [str(value) for value in codes]:
        try:
            contract = api.Contracts.Stocks[code]
        except (KeyError, TypeError, AttributeError):
            try:
                contract = api.contracts.get(code)
            except (AttributeError, KeyError, TypeError):
                contract = None
        if contract is not None:
            contracts.append(contract)
    if not contracts:
        return {}
    try:
        snapshots = get_stream_quotes(api, contracts)
        return {
            str(getattr(snapshot, 'code', '')): snapshot for snapshot in snapshots
            if getattr(snapshot, 'code', None)
        }
    except Exception:
        return {}


def merge_realtime_stock_snapshots(stock_data, snapshot_map, points_map=None, quote_time=None):
    """Apply one batched Shioaji snapshot response to the stock table."""
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        return stock_data, 0
    refreshed = stock_data.copy()
    quote_time = quote_time or datetime.now(
        pytz.timezone('Asia/Taipei')
    ).strftime('%Y/%m/%d %H:%M:%S')
    updated_count = 0
    for row_index, row in refreshed.iterrows():
        code = str(row.get('代號', '')).strip()
        snapshot = (snapshot_map or {}).get(code)
        if snapshot is None:
            continue
        price = _safe_number(getattr(snapshot, 'close', None))
        if price is None or price <= 0:
            continue
        change_rate = snapshot_change_rate(snapshot, price)
        refreshed.at[row_index, '收盤價'] = price
        if change_rate is not None:
            refreshed.at[row_index, '漲跌幅'] = change_rate
        refreshed.at[row_index, '成交價價差'] = price_change_amount(price, change_rate)
        refreshed.at[row_index, '_quote_bid'] = _safe_number(
            getattr(snapshot, 'buy_price', None)
        )
        refreshed.at[row_index, '_quote_ask'] = _safe_number(
            getattr(snapshot, 'sell_price', None)
        )
        refreshed.at[row_index, '_quote_time'] = quote_time
        if points_map is not None:
            refreshed.at[row_index, '狀態'] = recalculate_row(
                refreshed.loc[row_index], points_map,
            )
        updated_count += 1
    return refreshed, updated_count


def refresh_daytrade_metrics_for_codes(stock_data, sj_logged_in=False, sj_api=None):
    """手動抓取盤中資料；09:00–09:15 採快照＋1 分 K，其後採 5 分 K。"""
    if stock_data.empty or '代號' not in stock_data.columns or not sj_logged_in or sj_api is None:
        return stock_data, 0

    codes = stock_data['代號'].astype(str).tolist()
    now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
    opening_micro = _is_opening_micro_window(now_tw)
    interval = '1m' if opening_micro else '5m'
    interval_label = '1 分 K' if opening_micro else '5 分 K'
    snapshot_map = fetch_stock_snapshot_map(sj_api, codes)

    def fetch_metrics(code):
        try:
            time.sleep(API_REQUEST_GAP_SECONDS)
            intraday_df = fetch_shioaji_data(sj_api, code, interval=interval, lookback_days=3)
            snapshot = snapshot_map.get(code)
            metrics = calculate_daytrade_metrics(
                intraday_df, live_snapshot=snapshot, now_tw=now_tw,
                interval_label=interval_label,
            )
            if metrics and snapshot is not None:
                price = _safe_number(getattr(snapshot, 'close', None))
                change_rate = snapshot_change_rate(snapshot, price) if price is not None else None
                metrics.update({
                    '_quote_bid': _safe_number(getattr(snapshot, 'buy_price', None)),
                    '_quote_ask': _safe_number(getattr(snapshot, 'sell_price', None)),
                    '_quote_time': now_tw.strftime('%Y/%m/%d %H:%M:%S'),
                })
                if price is not None and price > 0:
                    metrics['收盤價'] = price
                    if change_rate is not None:
                        metrics['漲跌幅'] = change_rate
                        metrics['成交價價差'] = price_change_amount(price, change_rate)
            return code, metrics
        except Exception:
            return code, None

    with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
        results = list(executor.map(fetch_metrics, codes))

    refreshed = stock_data.copy()
    updated_count = 0
    for code, metrics in results:
        if not metrics:
            continue
        row_mask = refreshed['代號'].astype(str) == code
        for column, value in metrics.items():
            refreshed.loc[row_mask, column] = value
        updated_count += int(row_mask.sum())
    return refreshed, updated_count

# ==========================================
# 處理待加回的忽略股票 (防止 NameError & 提速)
# ==========================================
if 'pending_unignore' in st.session_state and st.session_state.pending_unignore:
    unignored_codes = st.session_state.pending_unignore
    st.session_state.pending_unignore = set()
    
    if 'futures_list' not in st.session_state or not st.session_state.futures_list:
        st.session_state.futures_list = fetch_futures_list()
    futures_copy = dict(st.session_state.futures_list)
    notes_copy = dict(st.session_state.get('saved_notes', {}))
    code_map_copy, _ = load_local_stock_names()
    sj_logged = st.session_state.get('sj_logged_in', False)
    sj_api_obj = st.session_state.get('sj_api', None)
    
    with st.spinner("正在將股票加回分析區..."):
        cached_rows = []
        fetch_tasks = []
        for c_code in unignored_codes:
            cand_info = next((c for c in st.session_state.all_candidates if c[0] == c_code), None)
            c_name = cand_info[1] if cand_info else get_stock_name_online(c_code)
            c_source = cand_info[2] if cand_info else 'search'
            c_order = cand_info[3] if cand_info else 999
            c_rank = 1 if c_source == 'upload' else 2
            
            # 速度優化：如果有暫存的分析資料，直接取用，省去等待
            if c_code in st.session_state.get('ignored_data_cache', {}):
                cached_row = st.session_state.ignored_data_cache.pop(c_code)
                cached_row.update({'_source': c_source, '_order': c_order, '_source_rank': c_rank})
                cached_rows.append(cached_row)
            else:
                fetch_tasks.append((c_code, c_name, c_source, c_order, c_rank))

        if cached_rows:
            st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(cached_rows)], ignore_index=True)
            
        if fetch_tasks:
            def _fetch_worker(task):
                time.sleep(API_REQUEST_GAP_SECONDS)  # 保留既有請求間隔，避免 API 封鎖
                t_code, t_name, t_src, t_ord, t_rnk = task
                res = fetch_stock_data_raw(t_code, t_name, None, futures_copy, notes_copy, code_map_copy, sj_logged, sj_api_obj)
                if res: res.update({'_source': t_src, '_order': t_ord, '_source_rank': t_rnk})
                return res
                
            with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                results = list(executor.map(_fetch_worker, fetch_tasks))
                valid_results = [r for r in results if r]
                if valid_results:
                    st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(valid_results)], ignore_index=True)

        if not st.session_state.stock_data.empty and '_source_rank' in st.session_state.stock_data.columns:
            st.session_state.stock_data = st.session_state.stock_data.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
            
    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
    st.session_state.stock_strategy_editor_revision += 1
    st.rerun()


def render_futures_strategy_room():
    """期貨成交量排行、即時分析、忽略遞補與獨立計算介面。"""
    persisted_futures_state = load_futures_strategy_state()
    if 'futures_strategy_ignored' not in st.session_state:
        st.session_state.futures_strategy_ignored = set(persisted_futures_state.get('ignored', []))
    if 'futures_strategy_manual' not in st.session_state:
        st.session_state.futures_strategy_manual = list(persisted_futures_state.get('manual', []))
    if 'futures_strategy_live_cache' not in st.session_state:
        st.session_state.futures_strategy_live_cache = dict(persisted_futures_state.get('live_cache', {}))
    if 'futures_strategy_live_time' not in st.session_state:
        st.session_state.futures_strategy_live_time = persisted_futures_state.get('live_time')
    if 'futures_strategy_rank_cache' not in st.session_state:
        st.session_state.futures_strategy_rank_cache = dict(persisted_futures_state.get('rank_cache', {}))
    if 'futures_strategy_rank_time' not in st.session_state:
        st.session_state.futures_strategy_rank_time = persisted_futures_state.get('rank_time')
    if 'futures_strategy_editor_revision' not in st.session_state:
        st.session_state.futures_strategy_editor_revision = 0
    if 'futures_enhanced_layer_enabled' not in st.session_state:
        st.session_state.futures_enhanced_layer_enabled = True
    if int(st.session_state.get('futures_minimum_volume', 1) or 1) < 1:
        st.session_state.futures_minimum_volume = 1

    settings_col, info_col = st.columns([3, 2])
    with settings_col:
        with st.expander("⚙️ 期貨篩選、策略與顯示設定", expanded=False):
            enhanced_col1, enhanced_col2, enhanced_col3 = st.columns([3, 2, 2])
            with enhanced_col1:
                enhanced_layer = st.checkbox(
                    "🧭 啟用附加分析層（可關閉回原表）",
                    key='futures_enhanced_layer_enabled',
                    help='加入支撐壓力、訊號狀態、進場信心、資料品質與流動性；不改變成交量排序。'
                )
            with enhanced_col2:
                compact_futures_table = st.checkbox(
                    "精簡主表", value=True, key='futures_compact_table', disabled=not enhanced_layer,
                    help='保留主要決策欄位，其餘資料放在個別明細；關閉可查看完整表格。'
                )
            with enhanced_col3:
                futures_notify = st.checkbox(
                    "🔔 訊號變化提醒", value=True, key='futures_strategy_notify', disabled=not enhanced_layer
                )

            control1, control2, control3 = st.columns(3)
            with control1:
                strategy_mode = st.radio("策略週期", ["當沖", "波段"], horizontal=True, key="futures_strategy_mode")
                direction_choice = st.radio(
                    "分析方向", ["自動", "偏多", "偏空"], horizontal=True,
                    key="futures_direction_choice",
                    help="自動：依成交價相對開盤價（當沖則優先參考 VWAP）判定；偏多：固定以突破壓力的做多計畫計算；偏空：固定以跌破支撐的做空計畫計算。"
                )
            with control2:
                limit_rows = st.number_input("顯示筆數", min_value=1, max_value=50, value=5, step=1, key="futures_strategy_limit")
                minimum_volume = st.number_input("最低成交口數", min_value=1, value=1, step=1, key="futures_minimum_volume")
            with control3:
                hide_index = st.checkbox("隱藏指數期貨", value=True, key="futures_hide_index")
                hide_etf = st.checkbox("隱藏 ETF 期貨", value=False, key="futures_hide_etf")
                only_small = st.checkbox(
                    "只顯示小型股票期貨", value=False, key="futures_only_small",
                    help="只保留小型個股期貨，依成交口數由高到低排序；一般股票期貨、指數期貨與 ETF 期貨都會隱藏。"
                )
                hide_small = st.checkbox("隱藏小型期貨", value=False, key="futures_hide_small")
                hide_next = st.checkbox("隱藏次月期貨", value=True, key="futures_hide_next")
    with info_col:
        render_futures_strategy_explanation()

    action_col1, action_col2, action_col3 = st.columns(3)
    with action_col1:
        refresh_official = st.button("🔄 更新成交量／保證金", width='stretch', key="refresh_futures_official")
    with action_col2:
        refresh_rank = st.button(
            "📊 即時更新成交量排行", width='stretch', key="refresh_futures_rank",
            help="登入 Shioaji 後批次取得盤中／夜盤累計成交量並重新排序；不會背景輪詢。"
        )
    with action_col3:
        refresh_live = st.button(
            "⏱️ 即時更新報價與分析", width='stretch', type="primary", key="refresh_futures_live",
            help='登入 Shioaji 後更新目前表格的成交價、夜盤資料與支撐壓力。'
        )

    if refresh_official:
        fetch_futures_strategy_universe.clear()
        st.session_state.futures_strategy_live_cache = {}
        st.session_state.futures_strategy_live_time = None
        st.session_state.futures_strategy_rank_cache = {}
        st.session_state.futures_strategy_rank_time = None
        st.session_state.futures_strategy_editor_revision += 1

    with st.spinner("正在讀取期交所成交量與保證金..."):
        universe, universe_meta = fetch_futures_strategy_universe(
            futures_rollover_cache_key()
        )
    saved_universe_records = persisted_futures_state.get('universe', [])
    saved_universe = pd.DataFrame(saved_universe_records) if isinstance(saved_universe_records, list) else pd.DataFrame()
    saved_meta = persisted_futures_state.get('metadata', {})
    if not universe.empty and not saved_universe.empty:
        universe, universe_meta = merge_futures_official_snapshot(
            universe,
            saved_universe,
            universe_meta,
            saved_meta if isinstance(saved_meta, dict) else {},
        )
    if universe.empty:
        if saved_universe.empty:
            st.error("目前無法取得期交所期貨排行資料，請稍後重試。")
            if universe_meta.get('errors'):
                st.caption("｜".join(universe_meta['errors']))
            st.markdown("資料來源：[臺灣期貨交易所 OpenAPI](https://openapi.taifex.com.tw/)")
            return
        universe = saved_universe
        universe_meta = {
            **(saved_meta if isinstance(saved_meta, dict) else {}),
            'errors': universe_meta.get('errors', []), 'restored': True,
        }
        st.info("期交所資料暫時無法取得，已還原上次成功保存的期貨表格。")

    # 官方交易日切換時，舊盤中排行不可覆蓋新日盤的成交量；
    # rank cache 沒有跨日合併的意義，改由新資料重新建立。
    saved_updated = saved_meta.get('updated') if isinstance(saved_meta, dict) else None
    if (
        universe_meta.get('updated') and saved_updated
        and str(universe_meta.get('updated')) != str(saved_updated)
    ):
        st.session_state.futures_strategy_rank_cache = {}
        st.session_state.futures_strategy_rank_time = None

    # 舊快照可能仍保存「日盤」；依目前商品代碼資格重新正規化，避免 QFF 等
    # OpenAPI 三碼代碼未被舊版 QF 商品代碼表辨識。
    if '交易時段' in universe.columns and '期貨代碼' in universe.columns:
        universe['交易時段'] = universe.apply(
            lambda row: get_futures_session_label(
                [row.get('交易時段', '')], row.get('交易時段', '未知'), row.get('期貨代碼', '')
            ),
            axis=1,
        )

    # 契約到期或從官方清單移除後，同步清掉快速新增與行情快取，避免舊列黏在表尾。
    valid_contract_keys = set(universe.get('契約鍵', pd.Series(dtype=str)).astype(str))
    expired_manual_keys = {
        str(key) for key in st.session_state.futures_strategy_manual
        if str(key) not in valid_contract_keys
    }
    stale_cache_keys = set()
    for cache_name in ('futures_strategy_live_cache', 'futures_strategy_rank_cache'):
        cache_value = st.session_state.get(cache_name, {})
        stale_cache_keys.update(
            str(key) for key in list(cache_value)
            if str(key) not in valid_contract_keys
        )
    settlement_removed_keys = expired_manual_keys | stale_cache_keys
    if settlement_removed_keys:
        st.session_state.futures_strategy_manual = [
            key for key in st.session_state.futures_strategy_manual
            if str(key) not in settlement_removed_keys
        ]
        for cache_name in ('futures_strategy_live_cache', 'futures_strategy_rank_cache'):
            cache_value = st.session_state.get(cache_name, {})
            for key in settlement_removed_keys:
                cache_value.pop(key, None)

    def persist_futures_room_state(snapshot=None):
        return save_futures_strategy_state(
            universe=snapshot if isinstance(snapshot, pd.DataFrame) else universe,
            metadata=universe_meta,
            rank_cache=st.session_state.futures_strategy_rank_cache,
            live_cache=st.session_state.futures_strategy_live_cache,
            manual=st.session_state.futures_strategy_manual,
            ignored=st.session_state.futures_strategy_ignored,
            rank_time=st.session_state.futures_strategy_rank_time,
            live_time=st.session_state.futures_strategy_live_time,
        )

    rank_cache = st.session_state.futures_strategy_rank_cache
    if rank_cache:
        for index, row in universe.iterrows():
            cached_rank = rank_cache.get(str(row['契約鍵']), {})
            for column, value in cached_rank.items():
                if column in universe.columns or column in ('買價', '賣價', '報價時間'):
                    universe.at[index, column] = value
        universe = universe.sort_values(
            ['當日成交口數', '月份順位'], ascending=[False, True]
        ).reset_index(drop=True)

    if refresh_rank:
        if not st.session_state.get('sj_logged_in', False) or st.session_state.get('sj_api') is None:
            st.warning("請先登入永豐 Shioaji，才能在期交所盤後資料更新前取得即時成交量排行。")
        else:
            with st.spinner("正在批次取得盤中／夜盤期貨成交量排行..."):
                live_universe, live_count = update_futures_universe_live(
                    universe, st.session_state.sj_api
                )
            if live_count:
                rank_columns = [
                    '當日成交口數', '收盤價', '漲跌幅', '當日高', '當日低',
                    '買價', '賣價', '報價時間'
                ]
                st.session_state.futures_strategy_rank_cache = {
                    str(row['契約鍵']): {
                        column: row.get(column) for column in rank_columns if column in live_universe.columns
                    }
                    for _, row in live_universe.iterrows()
                }
                st.session_state.futures_strategy_rank_time = datetime.now(
                    pytz.timezone('Asia/Taipei')
                ).strftime('%Y/%m/%d %H:%M:%S')
                universe = live_universe
                st.session_state.futures_strategy_editor_revision += 1
                persist_futures_room_state(live_universe)
                st.toast(f"已用即時串流更新 {live_count} 個契約並重新排序", icon="📊")
            else:
                st.warning("未取得可用的期貨快照；請確認 Shioaji 契約資料與連線狀態。")

    saved_metadata = persisted_futures_state.get('metadata', {})
    if (
        refresh_official or settlement_removed_keys or not saved_universe_records
        or universe_meta.get('updated') != (saved_metadata.get('updated') if isinstance(saved_metadata, dict) else None)
    ):
        persist_futures_room_state(universe)

    official_date = str(universe_meta.get('updated') or '')
    margin_date = str(universe_meta.get('margin_date') or '')
    live_time = st.session_state.futures_strategy_live_time
    status_text = f"行情資料：{official_date or '—'}｜保證金：{margin_date or '—'}"
    if universe_meta.get('last_known_good'):
        status_text += "｜部分欄位沿用上次成功資料"
    if st.session_state.futures_strategy_rank_time:
        status_text += f"｜即時排行：{st.session_state.futures_strategy_rank_time}"
    if live_time:
        status_text += f"｜即時更新：{live_time}"
    st.caption(status_text)
    if universe_meta.get('last_known_good'):
        st.caption(
            f"部分行情暫時未回傳，保留最後成功快照（as-of：{universe_meta.get('last_known_good_as_of') or '—'}）；"
            "下一次官方資料完整更新後會自動替換。"
        )
    st.caption(
        "資料說明：『行情資料』是期交所該交易日收盤後的成交量與結算行情；"
        "『保證金』是該生效日的官方標準。夜盤或次一交易日官方檔尚未更新時，日期會維持上一個已完成日盤；"
        "盤中／夜盤請另按『即時更新成交量排行』或『即時更新報價與分析』。"
    )
    if universe_meta.get('errors'):
        st.warning("部分官方資料未完整取得：" + "｜".join(universe_meta['errors']))

    front_index_rows = universe[
        universe['期貨代碼'].isin(['TX', 'MTX', 'TMF']) & (universe['月份順位'] == 0)
    ].copy()
    if not front_index_rows.empty:
        front_index_rows['_環境優先'] = front_index_rows['期貨代碼'].map({'TX': 0, 'MTX': 1, 'TMF': 2})
        market_row = front_index_rows.sort_values(['_環境優先', '當日成交口數'], ascending=[True, False]).iloc[0]
        market_change = _safe_number(market_row.get('漲跌幅'), 0) or 0
        market_bias = '偏多' if market_change >= 0.3 else ('偏空' if market_change <= -0.3 else '盤整')
        st.session_state.strategy_market_environment = {
            'bias': market_bias,
            'source': f"{market_row['期貨代碼']} {market_row['契約月份']}",
            'change': market_change,
            'updated': official_date,
        }
    else:
        market_bias = str(st.session_state.get('strategy_market_environment', {}).get('bias', '盤整'))

    filtered = universe.copy()
    if only_small:
        filtered = filtered[
            filtered.apply(is_small_stock_futures_record, axis=1)
        ].sort_values('當日成交口數', ascending=False)
    else:
        if hide_index:
            filtered = filtered[~filtered['指數期貨']]
        if hide_etf:
            filtered = filtered[~filtered['ETF期貨']]
        if hide_small:
            filtered = filtered[~filtered['小型期貨']]
    if hide_next:
        filtered = filtered[~filtered['次月期貨']]
    filtered = filtered[filtered['當日成交口數'] >= int(minimum_volume)]
    filtered = filtered[~filtered['契約鍵'].isin(st.session_state.futures_strategy_ignored)]
    base_rows = filtered.head(int(limit_rows)).copy()

    # 將股票戰略室內有股期的標的依股票原順序附加：A 股期、A 小型股期、B 股期、B 小型股期。
    linked_pool = universe.copy()
    if only_small:
        linked_pool = linked_pool[
            linked_pool.apply(is_small_stock_futures_record, axis=1)
        ].sort_values('當日成交口數', ascending=False)
    else:
        if hide_index:
            linked_pool = linked_pool[~linked_pool['指數期貨']]
        if hide_etf:
            linked_pool = linked_pool[~linked_pool['ETF期貨']]
        if hide_small:
            linked_pool = linked_pool[~linked_pool['小型期貨']]
    linked_pool = linked_pool[
        (linked_pool['月份順位'] == 0)
        & ~linked_pool['契約鍵'].isin(st.session_state.futures_strategy_ignored)
    ]
    linked_keys = []
    stock_rows = st.session_state.get('stock_data', pd.DataFrame())
    if isinstance(stock_rows, pd.DataFrame) and not stock_rows.empty and '代號' in stock_rows.columns:
        ignored_stock_codes = {
            str(code) for code in st.session_state.get('ignored_stocks', set())
        }
        for stock_code in stock_rows['代號'].astype(str):
            if stock_code in ignored_stock_codes:
                continue
            matches = linked_pool[linked_pool['標的代號'].astype(str) == stock_code].copy()
            if matches.empty:
                continue
            matches = matches.sort_values(
                ['小型期貨', '當日成交口數', '期貨代碼'], ascending=[True, False, True]
            )
            for key in matches['契約鍵'].astype(str):
                if key not in linked_keys:
                    linked_keys.append(key)
    base_keys = set(base_rows['契約鍵'].astype(str))
    linked_keys = [key for key in linked_keys if key not in base_keys]
    linked_rows = linked_pool[linked_pool['契約鍵'].astype(str).isin(linked_keys)].copy()
    if not linked_rows.empty:
        linked_order = {key: order for order, key in enumerate(linked_keys)}
        linked_rows['_linked_order'] = linked_rows['契約鍵'].astype(str).map(linked_order)
        linked_rows = linked_rows.sort_values('_linked_order').drop(columns=['_linked_order'])

    manual_keys = [
        key for key in st.session_state.futures_strategy_manual
        if key not in st.session_state.futures_strategy_ignored
        and key not in base_keys
        and key not in set(linked_keys)
    ]
    manual_rows = universe[universe['契約鍵'].isin(manual_keys)].copy()
    if not manual_rows.empty:
        order_map = {key: order for order, key in enumerate(manual_keys)}
        manual_rows['_manual_order'] = manual_rows['契約鍵'].map(order_map)
        manual_rows = manual_rows.sort_values('_manual_order').drop(columns=['_manual_order'])
    display_rows = pd.concat([base_rows, manual_rows, linked_rows], ignore_index=True)
    cache = st.session_state.futures_strategy_live_cache

    for index, row in display_rows.iterrows():
        contract_key = str(row['契約鍵'])
        cached = cache.get(contract_key)
        if cached:
            for column, value in cached.items():
                if column == '交易時段':
                    continue
                if column in display_rows.columns or column in ('支撐壓力', '進出場點位', '方向', '觸發條件', '實際契約'):
                    display_rows.at[index, column] = value
        if not cached or cached.get('_策略週期') != strategy_mode or cached.get('_分析方向') != direction_choice:
            analysis = calculate_futures_strategy_levels(display_rows.loc[index], strategy_mode, direction_choice)
            for column, value in analysis.items():
                display_rows.at[index, column] = value

    def refresh_futures_live_data():
        if not st.session_state.get('sj_logged_in', False) or st.session_state.get('sj_api') is None:
            st.warning("請先登入永豐 Shioaji，才能更新近月／次月實際契約的即時報價與夜盤 K 棒。")
        elif display_rows.empty:
            st.info("目前沒有可更新的期貨。")
        else:
            with st.spinner("正在更新期貨快照、夜盤資料與支撐壓力..."):
                updated_rows, updated_count = update_futures_live_rows(
                    display_rows, st.session_state.sj_api, strategy_mode, direction_choice, include_analysis=True
                )
            for _, updated_row in updated_rows.iterrows():
                cached_row = updated_row.to_dict()
                cached_row['_策略週期'] = strategy_mode
                cached_row['_分析方向'] = direction_choice
                st.session_state.futures_strategy_live_cache[str(updated_row['契約鍵'])] = cached_row
            st.session_state.futures_strategy_live_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S')
            if updated_count:
                update_strategy_signal_outcomes({
                    str(row['契約鍵']): _safe_number(row.get('收盤價'))
                    for _, row in updated_rows.iterrows()
                })
                persist_futures_room_state()
                save_data_cache(
                    st.session_state.stock_data, st.session_state.ignored_stocks,
                    st.session_state.all_candidates, st.session_state.saved_notes
                )
                st.session_state.futures_strategy_editor_revision += 1
                st.toast(f"已更新 {updated_count} 檔期貨報價與分析", icon="✅")
                st.rerun()
            else:
                resolved_count = int(updated_rows.attrs.get('resolved_contract_count', 0) or 0)
                if resolved_count:
                    st.warning("已找到實際契約，但目前沒有可用報價；請稍候數秒再更新，或使用快速重新登入。")
                else:
                    st.warning("找不到目前月份的實際契約；已重新讀取 Shioaji 契約檔，請確認合約月份仍在交易。")

    if enhanced_layer:
        display_rows = enrich_futures_strategy_rows(display_rows, strategy_mode, market_bias)
        if compact_futures_table:
            futures_display_columns = [
                '忽略', '期貨代碼', '契約月份', '名稱', '方向', '收盤價', '漲跌幅',
                '訊號狀態', '信心分', '信心判讀', '支撐壓力', '進出場點位', '市場一致',
                '資料狀態', '可交易性', '交易時段', '當日成交口數', '未平倉量', '量倉比',
                '到期提醒', '所需保證金'
            ]
        else:
            futures_display_columns = [
                '忽略', '期貨代碼', '契約月份', '名稱', '方向', '支撐壓力', '進出場點位', '觸發條件',
                '當日漲停價', '當日跌停價', '收盤價', '漲跌幅', '訊號狀態', '信心分', '信心判讀',
                '可交易性', '資料狀態', '市場一致', '買賣價差', '量倉比', '到期提醒',
                '交易時段', '當日成交口數', '未平倉量', '所需保證金', '維持保證金'
            ]
    else:
        futures_display_columns = [
            '忽略', '期貨代碼', '契約月份', '名稱', '方向', '支撐壓力', '進出場點位', '觸發條件',
            '當日漲停價', '當日跌停價', '收盤價', '漲跌幅',
            '交易時段', '當日成交口數', '未平倉量', '所需保證金', '維持保證金'
        ]

    def style_futures_row(row):
        styles = [''] * len(row)
        change = _safe_number(row.get('漲跌幅'), 0) or 0
        direction = str(row.get('方向', ''))
        signal_state = str(row.get('訊號狀態', ''))
        liquidity = str(row.get('可交易性', ''))
        data_health = str(row.get('資料狀態', ''))
        limit_state = futures_limit_state(
            row.get('收盤價'), row.get('當日漲停價'), row.get('當日跌停價'),
        )
        if limit_state == 'up':
            name_style = 'background-color: #ff4b4b; color: #ffffff; font-weight: bold;'
        elif limit_state == 'down':
            name_style = 'background-color: #00e676; color: #ffffff; font-weight: bold;'
        else:
            name_style = 'color:#ff4b4b;font-weight:bold;' if direction == '偏多' else ('color:#00c853;font-weight:bold;' if direction == '偏空' else '')
        for position, column in enumerate(row.index):
            if column == '名稱':
                styles[position] = name_style
            elif column in ('收盤價', '漲跌幅'):
                styles[position] = 'color:#ff4b4b;font-weight:bold;' if change > 0 else ('color:#00c853;font-weight:bold;' if change < 0 else '')
            elif column == '方向':
                styles[position] = 'color:#ff4b4b;font-weight:bold;' if direction == '偏多' else ('color:#00c853;font-weight:bold;' if direction == '偏空' else '')
            elif column == '支撐壓力':
                styles[position] = 'color:#4fc3f7;font-weight:600;'
            elif column in ('進出場點位', '觸發條件'):
                value = str(row.get(column, ''))
                if any(keyword in value for keyword in ('資料不足', '不建立', '等待')):
                    styles[position] = 'color:#ffd166;'
                elif direction == '偏多':
                    styles[position] = 'color:#ff6b6b;'
                elif direction == '偏空':
                    styles[position] = 'color:#35d07f;'
            elif column == '當日漲停價':
                styles[position] = 'color:#ff4b4b;font-weight:bold;'
            elif column == '當日跌停價':
                styles[position] = 'color:#00e676;font-weight:bold;'
            elif column in ('當日成交口數', '未平倉量'):
                styles[position] = 'color:#ff9800;font-weight:bold;'
            elif column in ('所需保證金', '維持保證金'):
                styles[position] = 'color:#90caf9;'
            elif column == '買賣價差':
                spread_text = str(row.get('買賣價差', ''))
                try:
                    spread_ticks = float(re.search(r'[\d.]+', spread_text).group())
                except (AttributeError, TypeError, ValueError):
                    spread_ticks = None
                styles[position] = (
                    'color:#4fc3f7;' if spread_ticks is not None and spread_ticks <= 2
                    else ('color:#ffd166;' if spread_ticks is not None else 'color:#94a3b8;')
                )
            elif column == '交易時段':
                styles[position] = 'color:#c4b5fd;'
            elif column == '訊號狀態':
                if signal_state.startswith('✅'):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
                elif signal_state.startswith('⛔'):
                    styles[position] = 'color:#ff9800;font-weight:bold;'
                elif signal_state.startswith('🟡'):
                    styles[position] = 'color:#ffeb3b;'
            elif column in ('可交易性', '資料狀態'):
                value = liquidity if column == '可交易性' else data_health
                if value.startswith('🟢'):
                    styles[position] = 'color:#00e676;'
                elif value.startswith(('🔴', '⛔')):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
                elif value.startswith('🟡'):
                    styles[position] = 'color:#ffeb3b;'
            elif column == '信心判讀':
                confidence = str(row.get('信心判讀', ''))
                if confidence.startswith('🟢'):
                    styles[position] = 'color:#00e676;font-weight:bold;'
                elif confidence.startswith(('🟡', '🟠')):
                    styles[position] = 'color:#ffb300;font-weight:bold;'
                elif confidence.startswith('🔴'):
                    styles[position] = 'color:#ff4b4b;font-weight:bold;'
        return styles

    def futures_column_config(frame=None, include_ignore=True):
        """依實際儲存格內容收合欄寬，長訊息保留完整可讀空間。"""
        def content_width(column, minimum, maximum=520, full_content=False):
            values = frame.get(column) if isinstance(frame, pd.DataFrame) else None
            return _content_column_width(values, minimum, maximum, full_content)

        config = {
            '忽略': st.column_config.CheckboxColumn('隱藏', width=40),
            '期貨代碼': st.column_config.TextColumn(width=content_width('期貨代碼', 44, 70), disabled=True),
            '契約月份': st.column_config.TextColumn(width=content_width('契約月份', 54, 82), disabled=True),
            '名稱': st.column_config.TextColumn(width=content_width('名稱', 48, 130), disabled=True),
            '交易時段': st.column_config.TextColumn(
                width=content_width('交易時段', 54, 100), disabled=True,
                help='「日盤+夜盤」代表此商品同時有日盤與夜盤；表內行情與策略計算仍採同一交易時段資料，避免混合不同時段的開高低收與成交量。',
            ),
            '當日成交口數': st.column_config.NumberColumn(format='%d', width=content_width('當日成交口數', 56, 100), disabled=True),
            '未平倉量': st.column_config.NumberColumn(format='%d', width=content_width('未平倉量', 56, 100), disabled=True),
            '方向': st.column_config.TextColumn(width=content_width('方向', 48, 86), disabled=True),
            '支撐壓力': st.column_config.TextColumn(width=content_width('支撐壓力', 80), disabled=True),
            '進出場點位': st.column_config.TextColumn(width=content_width('進出場點位', 96, 720, True), disabled=True, help='進＝條件成立後觀察價；停＝失效點；目＝第一目標。'),
            '觸發條件': st.column_config.TextColumn(width=content_width('觸發條件', 72), disabled=True, help='條件成立後才評估進場；未成立時不以預判價直接下單。'),
            '當日漲停價': st.column_config.NumberColumn(format='%.12g', width=content_width('當日漲停價', 54, 96), disabled=True, help='依期交所漲跌資料反推參考價後估算；實際限制以期交所與券商下單畫面為準。'),
            '當日跌停價': st.column_config.NumberColumn(format='%.12g', width=content_width('當日跌停價', 54, 96), disabled=True, help='依期交所漲跌資料反推參考價後估算；實際限制以期交所與券商下單畫面為準。'),
            '收盤價': st.column_config.TextColumn('成交價', width=content_width('收盤價', 54, 96), disabled=True),
            '漲跌幅': st.column_config.TextColumn(width=content_width('漲跌幅', 54, 96), disabled=True),
            '所需保證金': st.column_config.NumberColumn(format='%,.0f', width=content_width('所需保證金', 62, 110), disabled=True),
            '維持保證金': st.column_config.NumberColumn(format='%,.0f', width=content_width('維持保證金', 62, 110), disabled=True),
            '訊號狀態': st.column_config.TextColumn(width=content_width('訊號狀態', 64, 140), disabled=True, help='等待、接近、觸發或失效；不會自動下單。'),
            '信心分': st.column_config.ProgressColumn('進場信心', min_value=0, max_value=100, format='%d', width=82, help='綜合觸發位置、成交量、未平倉、價差、報價與市場方向；代表條件一致度，不是勝率。'),
            '信心判讀': st.column_config.TextColumn(width=content_width('信心判讀', 54, 96), disabled=True, help='高／中高／中／低；若追離進場點、條件失效或資料過期會自動降級。'),
            '可交易性': st.column_config.TextColumn(width=content_width('可交易性', 64, 150), disabled=True, help='綜合成交量、未平倉量、買賣價差與報價新鮮度。'),
            '資料狀態': st.column_config.TextColumn(width=content_width('資料狀態', 56, 150), disabled=True, help='顯示即時、官方日行情、尚未更新或報價過期。'),
            '市場一致': st.column_config.TextColumn(width=content_width('市場一致', 56, 130), disabled=True, help='策略方向是否與近月臺指期環境一致；不改變原排序。'),
            '買賣價差': st.column_config.TextColumn(
                width=content_width('買賣價差', 56, 96), disabled=True,
                help='最佳賣價－最佳買價換算成跳動單位；跳數越少通常代表進出成本較低、報價較連續。無即時買賣價時顯示「—」。'
            ),
            '量倉比': st.column_config.NumberColumn(format='%.12g', width=content_width('量倉比', 52, 90), disabled=True, help='當日成交口數 ÷ 未平倉量。'),
            '到期提醒': st.column_config.TextColumn(
                width=content_width('到期提醒', 56, 110), disabled=True,
                help='依目前契約距到期日的天數提示。接近到期時注意流動性、轉倉與近月／次月價格差；不是強制平倉通知。'
            ),
        }
        if not include_ignore:
            config.pop('忽略')
        return config

    edited = pd.DataFrame()
    futures_editor_key = None
    display_key_map = {}
    if display_rows.empty:
        st.info("目前沒有符合篩選條件的期貨；可取消隱藏條件或降低最低成交口數。")
    else:
        display_rows['忽略'] = False
        for column in futures_display_columns:
            if column not in display_rows.columns:
                display_rows[column] = None

        table_signature = abs(hash(tuple(display_rows['契約鍵'].astype(str))))
        futures_editor_key = (
            f"futures_strategy_editor_{st.session_state.futures_strategy_editor_revision}_{table_signature}"
        )
        editor_display = display_rows[futures_display_columns].copy()
        editor_display['收盤價'] = editor_display['收盤價'].apply(fmt_price)
        editor_display['漲跌幅'] = editor_display['漲跌幅'].apply(_signed_percent)
        edited = st.data_editor(
            editor_display.style.apply(style_futures_row, axis=1),
            column_config=futures_column_config(editor_display),
            hide_index=True, width='stretch', row_height=30,
            key=futures_editor_key
        )
        display_key_map = {
            f"{row['期貨代碼']}:{row['契約月份']}": str(row['契約鍵'])
            for _, row in display_rows.iterrows()
        }
        hidden_rows = edited[edited['忽略'] == True]
        if not hidden_rows.empty:
            for _, hidden in hidden_rows.iterrows():
                key = display_key_map.get(f"{hidden['期貨代碼']}:{hidden['契約月份']}")
                if key:
                    st.session_state.futures_strategy_ignored.add(key)
            st.session_state.futures_strategy_editor_revision += 1
            persist_futures_room_state()
            st.rerun()

    if enhanced_layer and not display_rows.empty:
        signal_states = {
            f"{row['期貨代碼']} {row['契約月份']}": str(row.get('訊號狀態', ''))
            for _, row in display_rows.iterrows()
        }
        notify_signal_state_changes('futures', signal_states, futures_notify)
        detail_map = {
            f"{row['期貨代碼']} {row['契約月份']}｜{row['名稱']}": index
            for index, row in display_rows.iterrows()
        }
        detail_col, record_col = st.columns([5, 2])
        with detail_col:
            selected_detail = st.selectbox(
                '查看期貨策略信心明細', list(detail_map), key='futures_strategy_detail'
            )
            detail_row = display_rows.loc[detail_map[selected_detail]]
            st.caption(
                f"原分析：{detail_row.get('支撐壓力', '—')}｜{detail_row.get('觸發條件', '—')}｜"
                f"進場信心 {detail_row.get('信心分', '—')} 分（{detail_row.get('信心判讀', '—')}）｜"
                f"價差 {detail_row.get('買賣價差', '—')}｜量倉比 {detail_row.get('量倉比', '—')}｜"
                f"到期 {detail_row.get('到期提醒', '—')}。{detail_row.get('_信心明細', '')}。"
            )
        with record_col:
            st.markdown("<div style='margin-top:28px'></div>", unsafe_allow_html=True)
            record_futures_signals = st.button(
                '📝 記錄目前表格的已觸發期貨訊號', width='stretch', key='record_futures_strategy_signals',
                help='一次記錄目前期貨表格中所有已觸發、且流動性未亮紅燈的契約；不是只記錄正在查看的單一契約。'
            )
        if record_futures_signals:
            records = []
            for _, row in display_rows[display_rows['_附加可記錄'] == True].iterrows():
                plan = parse_trade_plan_numbers(row.get('進出場點位'))
                records.append({
                    '市場': '期貨', '商品鍵': str(row['契約鍵']), '代碼': str(row['期貨代碼']),
                    '名稱': str(row['名稱']), '策略': strategy_mode, '方向': str(row.get('方向', '')),
                    '訊號狀態': str(row.get('訊號狀態', '')), '評分': _safe_number(row.get('信心分')),
                    '信心判讀': str(row.get('信心判讀', '')),
                    '進場價': plan['entry'], '停損價': plan['stop'], '目標價': plan['target'],
                    '最新價': _safe_number(row.get('收盤價')),
                    '風險': str(row.get('可交易性', '')), '資料狀態': str(row.get('資料狀態', '')),
                })
            added, saved = register_strategy_signals(records)
            if saved:
                save_data_cache(
                    st.session_state.stock_data, st.session_state.ignored_stocks,
                    st.session_state.all_candidates, st.session_state.saved_notes
                )
                st.toast(f'已新增 {added} 筆期貨訊號；重複訊號不另建。', icon='📝')
            else:
                st.error('訊號紀錄儲存失敗，請確認檔案是否可寫入。')

    if refresh_live:
        refresh_futures_live_data()

    st.markdown("#### 🔍 快速新增期貨")
    option_map = {
        f"{row['期貨代碼']} {row['契約月份']}｜{row['名稱']}｜成交 {int(row['當日成交口數']):,} 口": str(row['契約鍵'])
        for _, row in universe.iterrows()
    }
    key_to_option = {key: label for label, key in option_map.items()}
    default_manual_labels = [
        key_to_option[key] for key in st.session_state.futures_strategy_manual
        if key in key_to_option
    ]
    selected_to_add = st.multiselect(
        "輸入中文名稱或期貨代碼（取消選取即從快速新增清單移除）",
        list(option_map), default=default_manual_labels, key='futures_quick_add',
        placeholder='例如：CDF、台積電期貨'
    )
    selected_manual_keys = [option_map[label] for label in selected_to_add if label in option_map]
    if selected_manual_keys != st.session_state.futures_strategy_manual:
        removed_manual_keys = set(st.session_state.futures_strategy_manual) - set(selected_manual_keys)
        st.session_state.futures_strategy_manual = selected_manual_keys
        for key in selected_manual_keys:
            st.session_state.futures_strategy_ignored.discard(key)
        for cache_name in ('futures_strategy_live_cache', 'futures_strategy_rank_cache'):
            cache_value = st.session_state.get(cache_name, {})
            for key in removed_manual_keys:
                cache_value.pop(key, None)
        st.session_state.futures_strategy_editor_revision += 1
        persist_futures_room_state()
        st.rerun()

    if st.session_state.futures_strategy_ignored:
        with st.expander("🚫 管理已隱藏期貨", expanded=False):
            ignored_options = {
                f"{row['期貨代碼']} {row['契約月份']}｜{row['名稱']}": str(row['契約鍵'])
                for _, row in universe[universe['契約鍵'].isin(st.session_state.futures_strategy_ignored)].iterrows()
            }
            selected_ignored = st.multiselect(
                "取消勾選即可恢復原成交量排序位置", list(ignored_options),
                default=list(ignored_options),
                key=f"manage_futures_ignored_{abs(hash(tuple(sorted(st.session_state.futures_strategy_ignored))))}"
            )
            current_ignored = {ignored_options[label] for label in selected_ignored}
            if current_ignored != st.session_state.futures_strategy_ignored:
                st.session_state.futures_strategy_ignored = current_ignored
                st.session_state.futures_strategy_editor_revision += 1
                persist_futures_room_state()
                st.rerun()

    st.markdown("---")
    st.markdown("### ⚡ 獨立計算")
    independent_labels = st.multiselect(
        "快速查詢（中文名稱／期貨代碼）", list(option_map),
        key='futures_independent_search', placeholder='選擇一檔或多檔期貨'
    )
    if st.button("🚀 執行期貨獨立分析", key='run_futures_independent') and independent_labels:
        keys = [option_map[label] for label in independent_labels]
        independent_rows = universe[universe['契約鍵'].isin(keys)].copy()
        if st.session_state.get('sj_logged_in', False) and st.session_state.get('sj_api') is not None:
            with st.spinner("正在取得實際契約報價與 K 棒..."):
                independent_rows, _ = update_futures_live_rows(
                    independent_rows, st.session_state.sj_api, strategy_mode, direction_choice, include_analysis=True
                )
        else:
            for index, row in independent_rows.iterrows():
                analysis = calculate_futures_strategy_levels(row, strategy_mode, direction_choice)
                for column, value in analysis.items():
                    independent_rows.at[index, column] = value
            st.info("目前未登入 Shioaji，獨立計算先使用期交所日行情；登入後可加入即時與夜盤 K 棒。")
        if enhanced_layer:
            independent_rows = enrich_futures_strategy_rows(independent_rows, strategy_mode, market_bias)
        independent_columns = [column for column in futures_display_columns if column != '忽略']
        for column in independent_columns:
            if column not in independent_rows.columns:
                independent_rows[column] = None
        independent_display = independent_rows[independent_columns].copy()
        independent_display['收盤價'] = independent_display['收盤價'].apply(fmt_price)
        independent_display['漲跌幅'] = independent_display['漲跌幅'].apply(_signed_percent)
        st.dataframe(
            independent_display.style.apply(style_futures_row, axis=1),
            column_config=futures_column_config(independent_display, include_ignore=False),
            hide_index=True, width='stretch', row_height=30
        )

# ==========================================
# 主介面 (Tabs)
# ==========================================
tab1, tab_fibo, tab2, tab_db, tab_company, tab3 = st.tabs([
    "⚡ 股期戰略室 ⚡",
    "📈 指數操盤室",
    "💰 交易損益室 💰",
    "📚 戰略資料庫",
    "🏢 公司營收與財報",
    "📅 股市行事曆",
], key="main_workspace_active_tab", on_change="rerun")

with tab1:
    if tab1.open:
        render_opening_direction_prompt()
    stock_strategy_tab, futures_strategy_tab, validation_strategy_tab = st.tabs([
        "📈 股票戰略室", "🧭 期貨戰略室", "📊 策略驗證"
    ], key="strategy_room_active_tab", on_change="rerun")
    with stock_strategy_tab:
        stock_strategy_container = st.container()
    with futures_strategy_tab:
        if tab1.open and futures_strategy_tab.open:
            render_futures_strategy_room()
    with validation_strategy_tab:
        if tab1.open and validation_strategy_tab.open:
            render_strategy_validation_room()

with stock_strategy_container:
    if st.session_state.pop('_stock_cache_refresh_pending', False):
        code_name_map, _ = load_local_stock_names()
        cached_row_count = len(st.session_state.stock_data)
        with st.spinner(f"正在更新保留表格中的 {cached_row_count} 檔股票資料..."):
            refreshed_stock_data, refreshed_stock_count = refresh_persisted_stock_rows(
                st.session_state.stock_data,
                st.session_state.get('futures_list', {}),
                st.session_state.get('saved_notes', {}),
                code_name_map,
                st.session_state.get('sj_logged_in', False),
                st.session_state.get('sj_api'),
            )
        st.session_state.stock_data = refreshed_stock_data
        mark_stock_data_updated()
        st.session_state.stock_strategy_editor_revision += 1
        save_data_cache(
            st.session_state.stock_data, st.session_state.ignored_stocks,
            st.session_state.all_candidates, st.session_state.saved_notes,
            replace_stock_data=True,
        )
        if refreshed_stock_count:
            st.session_state['_stock_auto_refresh_notice'] = (
                f"已自動更新 {refreshed_stock_count}/{cached_row_count} 檔；"
                "未更新成功的股票會保留並標示資料過期。"
            )
        else:
            st.session_state['_stock_auto_refresh_notice'] = (
                "本次未取得最新行情，已保留原表並標示資料過期；可稍後按執行分析重試。"
            )
    stock_settings_col, stock_help_col = st.columns([3, 2])
    with stock_settings_col:
        hide_non_stock, show_3d_hilo = render_stock_strategy_controls()
    with stock_help_col:
        render_stock_strategy_explanation()
    stock_auto_refresh_notice = st.session_state.pop('_stock_auto_refresh_notice', '')
    if stock_auto_refresh_notice:
        if st.session_state.stock_data.get('_data_stale', pd.Series(dtype=bool)).fillna(False).any():
            st.warning(stock_auto_refresh_notice)
        else:
            st.caption(stock_auto_refresh_notice)
    col_search = st.expander(
        "📥 選股資料來源與快速查詢",
        expanded=st.session_state.stock_data.empty,
    )
    with col_search:
        code_map, name_map = load_local_stock_names()
        stock_options = []
        for code, name in sorted(code_map.items()):
            if not st.session_state.get('allow_warrant_search', False) and is_warrant(code):
                continue
            stock_options.append(f"{code} {name}")
        
        src_tab1, src_tab2 = st.tabs(["📂 本機", "☁️ 雲端"])
        with src_tab1:
            uploaded_file = st.file_uploader(
                "上傳 Goodinfo 下載檔或其他選股檔案",
                type=['xlsx', 'csv', 'html', 'xls'],
                help="支援 Goodinfo 匯出的 CSV；會自動辨識 CP950／UTF-8 與前置說明列。",
                key='stock_source_upload',
            )
            selected_sheet = 0
            if uploaded_file:
                st.caption(f"✅ 已選擇 {uploaded_file.name}；下方按「執行分析」即可。")
                try:
                    if not uploaded_file.name.lower().endswith('.csv'):
                        xl_file = pd.ExcelFile(uploaded_file)
                        sheet_options = xl_file.sheet_names
                        default_idx = sheet_options.index("週轉率") if "週轉率" in sheet_options else 0
                        selected_sheet = st.selectbox("選擇工作表", sheet_options, index=default_idx)
                except: pass

        with src_tab2:
            def on_history_change(): st.session_state.cloud_url_input = st.session_state.history_selected
            history_opts = st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"]
            c_sel, c_del = st.columns([8, 1], gap="small")
            with c_sel:
                selected = st.selectbox("📜 歷史紀錄 (選取自動填入)", options=history_opts, key="history_selected", index=None, placeholder="請選擇...", on_change=on_history_change, label_visibility="collapsed")
            with c_del:
                if st.button("🗑️", help="刪除選取的歷史紀錄"):
                    if st.session_state.history_selected and st.session_state.history_selected in st.session_state.url_history:
                        st.session_state.url_history.remove(st.session_state.history_selected)
                        save_url_history(st.session_state.url_history)
                        st.toast("已刪除。", icon="🗑️")
                        st.rerun()
            st.text_input("輸入連結 (CSV/Excel/Google Sheet)", key="cloud_url_input", placeholder="https://...")
        
        def update_search_cache():
            save_search_cache(
                st.session_state.get("search_multiselect", [])
            )
        search_selection = st.multiselect("🔍 快速查詢 (中文/代號)", options=stock_options, key="search_multiselect", on_change=update_search_cache, placeholder="輸入 2330 或 台積電...")
        st.markdown("---")
        render_stock_external_resources()

    c_run, c_space = st.columns([1.5, 5])
    analysis_source_ready = bool(
        uploaded_file or st.session_state.cloud_url_input.strip()
        or search_selection or 'goodinfo_df' in st.session_state
    )
    with c_run:
        btn_run = st.button(
            "🚀 執行分析", width='stretch', disabled=not analysis_source_ready,
            help="請先上傳檔案、輸入雲端連結、抓取 Goodinfo，或選擇快速查詢標的。"
        )

    if btn_run:
        save_search_cache(st.session_state.search_multiselect)
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []
        df_up = pd.DataFrame()
        current_url = st.session_state.cloud_url_input.strip()
        if current_url:
            if current_url not in st.session_state.url_history:
                st.session_state.url_history.insert(0, current_url) 
                save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0)
                fname = uploaded_file.name.lower()
                if fname.endswith('.csv'):
                    df_up = _read_uploaded_stock_csv(uploaded_file)
                elif fname.endswith('.html') or fname.endswith('.htm') or fname.endswith('.xls'):
                    try: dfs = pd.read_html(uploaded_file, encoding='cp950')
                    except:
                        uploaded_file.seek(0)
                        dfs = pd.read_html(uploaded_file, encoding='utf-8')
                    for df in dfs:
                        if df.apply(lambda r: r.astype(str).str.contains('代號').any(), axis=1).any():
                             df_up = df
                             for i, row in df.iterrows():
                                 if "代號" in row.values:
                                     df_up.columns = row
                                     df_up = df_up.iloc[i+1:]
                                     break
                             break
                    if df_up.empty and dfs: df_up = dfs[0]
                elif fname.endswith('.xlsx'):
                    df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)

            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url and "/spreadsheets/" in url and "/edit" in url:
                    url = url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(url, dtype=str)
                except:
                    try: df_up = pd.read_excel(url, dtype=str)
                    except: st.error("❌ 無法讀取雲端檔案。")
            
            # 🟢 新增：若無上傳與雲端輸入，且檢測到有 Goodinfo 暫存資料時直接讀取
            elif 'goodinfo_df' in st.session_state:
                df_up = st.session_state['goodinfo_df'].copy()
                st.toast("已直接載入 Goodinfo 週轉率排行暫存資料進行分析！", icon="🔄")
                
        except Exception as e: st.error(f"讀取失敗: {e}")
        
        # 先處理上傳/資料帶入的股票 (優先權高)
        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            if c_col:
                limit_rows = st.session_state.limit_rows
                count = 0
                for _, row in df_up.iterrows():
                    c_raw = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    code_match = re.search(r'(?<!\d)\d{4,6}(?!\d)', c_raw)
                    if not code_match:
                        continue
                    c_raw = code_match.group(0)
                    if c_raw in st.session_state.ignored_stocks: continue
                    if hide_non_stock:
                        is_etf = c_raw.startswith('00')
                        is_warrant_flag = (len(c_raw) > 4) and c_raw.isdigit()
                        if is_etf or is_warrant_flag: continue
                    n = str(row[n_col]) if n_col else ""
                    if n.lower() == 'nan': n = ""
                    targets.append((c_raw, n, 'upload', count))
                    count += 1

        # 再處理快速查詢的股票，確保其排序在後面
        if search_selection:
            for i, item in enumerate(search_selection):
                parts = item.split(' ', 1)
                targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', i))

        st.session_state.all_candidates = targets
        seen = set()
        status_text = st.empty()
        bar = st.progress(0)
        
        upload_limit = st.session_state.limit_rows
        upload_current = 0
        existing_data = {}
        previous_stock_data = st.session_state.stock_data.copy()
        
        futures_copy = dict(st.session_state.futures_list)
        notes_copy = dict(st.session_state.saved_notes)
        code_map_copy, _ = load_local_stock_names()

        def process_stock_task(t_code, t_name, t_source, t_extra, f_set, n_dict, c_map, sj_logged, sj_api_obj):
            # 將原本 0.5~1.5 秒的長時間延遲，改為 0.1 秒的微小緩衝以防 API 封鎖
            time.sleep(API_REQUEST_GAP_SECONDS)
            try: return (t_code, t_source, t_extra, fetch_stock_data_raw(t_code, t_name, t_extra, f_set, n_dict, c_map, sj_logged, sj_api_obj))
            except Exception: return (t_code, t_source, t_extra, None)

        tasks_to_run = []
        for i, (code, name, source, extra) in enumerate(targets):
            if source == 'upload' and upload_current >= upload_limit: continue
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            tasks_to_run.append((code, name, source, extra))
            if source == 'upload': upload_current += 1
            seen.add((code, source))

        sj_logged_in_flag = st.session_state.get('sj_logged_in', False)
        sj_api_obj = st.session_state.get('sj_api', None)

        with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
            future_to_task = {executor.submit(process_stock_task, t[0], t[1], t[2], t[3], futures_copy, notes_copy, code_map_copy, sj_logged_in_flag, sj_api_obj): t for t in tasks_to_run}
            completed_count = 0
            total_tasks = len(tasks_to_run) if len(tasks_to_run) > 0 else 1
            
            for future in as_completed(future_to_task):
                t_code, t_source, t_extra, data = future.result()
                completed_count += 1
                bar.progress(min(completed_count / total_tasks, 1.0))
                status_text.text(f"正在分析 ({completed_count}/{total_tasks}): {t_code} ...")
                if data:
                    data['_source'] = t_source
                    data['_order'] = t_extra
                    data['_source_rank'] = 1 if t_source == 'upload' else 2
                    
                    # 檢查是否已存在資料，若存在則依據來源優先權判斷是否覆蓋
                    if t_code in existing_data:
                        if existing_data[t_code]['_source'] == 'upload' and t_source == 'search':
                            pass  # 已有在顯示筆數內的檔案資料，忽略查詢資料的覆蓋，保留檔案排序
                        elif existing_data[t_code]['_source'] == 'search' and t_source == 'upload':
                            existing_data[t_code] = data  # 檔案資料優先權高，覆蓋掉原先寫入的查詢資料
                        else:
                            existing_data[t_code] = data
                    else:
                        existing_data[t_code] = data
                # 每個單一股票任務結束後即時主動回收
                del data
        
        bar.empty()
        status_text.empty()

        failed_codes = {
            str(task[0]) for task in tasks_to_run
            if str(task[0]) not in existing_data
        }
        if failed_codes and not previous_stock_data.empty and '代號' in previous_stock_data.columns:
            previous_failed = previous_stock_data[
                previous_stock_data['代號'].astype(str).isin(failed_codes)
            ]
            for _, previous_row in previous_failed.iterrows():
                previous_code = str(previous_row.get('代號', ''))
                if previous_code:
                    existing_data[previous_code] = _stale_stock_identity_row(previous_row)
            if not previous_failed.empty:
                st.warning(
                    f"{len(previous_failed)} 檔本次更新失敗；已保留代號，舊行情與舊策略不再顯示。"
                )
        
        if existing_data:
            df_temp = pd.DataFrame(list(existing_data.values()))
            # 確保寫入 stock_data 時，嚴格按照來源優先權與順序排序
            if '_source_rank' in df_temp.columns:
                df_temp = df_temp.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
            st.session_state.stock_data = df_temp
            mark_stock_data_updated()
            st.session_state.stock_strategy_editor_revision += 1
            cloud_sync_ok = save_data_cache(
                st.session_state.stock_data, st.session_state.ignored_stocks,
                st.session_state.all_candidates, st.session_state.saved_notes,
                replace_stock_data=True,
                verify_stock_data=True,
            )
            if not cloud_sync_ok:
                st.warning(
                    "分析結果已保留在目前工作階段與本機快取，但 Google Sheet 尚未回讀確認；"
                    "請稍後再執行一次分析，確認同步狀態。"
                )
        else:
            st.session_state.stock_data = previous_stock_data
            if tasks_to_run:
                st.warning("本次未取得任何有效股票資料，已保留原本表格；請確認連線或稍後再試。")
            else:
                st.warning("沒有可分析的標的，已保留原本表格；請檢查忽略名單與篩選設定。")
            
        # 強制清除大型臨時變數並回收記憶體
        del tasks_to_run
        del existing_data
        gc.collect()

    if not st.session_state.stock_data.empty:
        # 舊版股票自訂價不再參與顯示或策略計算；成交價統一採行情資料。
        legacy_stock_columns = [
            column for column in ('自訂價(可修)', '自訂價價差')
            if column in st.session_state.stock_data.columns
        ]
        if legacy_stock_columns:
            st.session_state.stock_data = st.session_state.stock_data.drop(columns=legacy_stock_columns)

        # 新增：當限制筆數減少時，自動隱藏多餘的檔案上傳資料
        df_check = st.session_state.stock_data
        if '_source' in df_check.columns:
            upload_mask = df_check['_source'] == 'upload'
            if upload_mask.sum() > st.session_state.limit_rows:
                keep_upload = df_check[upload_mask].head(st.session_state.limit_rows)
                keep_other = df_check[~upload_mask]
                st.session_state.stock_data = pd.concat([keep_upload, keep_other]).sort_values(by=['_source_rank', '_order'] if '_source_rank' in df_check.columns else None)
                st.session_state.stock_strategy_editor_revision += 1
                save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)

        df_all = st.session_state.stock_data.copy()
        if '_source' not in df_all.columns: df_all['_source'] = 'upload'
        df_all = df_all.rename(columns={"漲停價": "當日漲停價", "跌停價": "當日跌停價", "獲利目標": "+3%", "防守停損": "-3%"})
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        
        if hide_non_stock:
             mask_etf = df_all['代號'].str.startswith('00')
             mask_warrant = (df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()
             df_all = df_all[~(mask_etf | mask_warrant)]
        
        if '_source_rank' in df_all.columns: df_all = df_all.sort_values(by=['_source_rank', '_order'])
        df_display = df_all.reset_index(drop=True)
        
        for i, row in df_display.iterrows():
            code = row['代號']
            points = row.get('_points', [])
            manual = st.session_state.saved_notes.get(code, "")
        
            if not points:
                cached = st.session_state.get('cached_notes', {}).get(code, {})
                if cached and cached.get('note'):
                    new_full_note = cached['note']
                    new_auto_note = cached.get('auto', '')
                else:
                    new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
            else:
                new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
        
            df_display.at[i, "戰略備註"] = new_full_note
            df_display.at[i, "_auto_note"] = new_auto_note
            clean_name = row['名稱'].replace('🔴 ', '').replace('🟢 ', '').replace('⚪ ', '')
            df_display.at[i, "名稱"] = clean_name

        note_width_px = calculate_note_width(df_display['戰略備註'], 15)
        df_display["移除"] = False
        points_map = df_display.set_index('代號')['_points'].to_dict() if '_points' in df_display.columns else {}
        auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict() if '_auto_note' in df_display.columns else {}

        # 成交價價差是相對昨收的點數；5 日線價差則是成交價相對 MA5 的距離。
        if "成交價價差" not in df_display.columns: df_display["成交價價差"] = None
        if "5日線價差" not in df_display.columns: df_display["5日線價差"] = None
        
        for i, row in df_display.iterrows():
            price_difference = price_change_amount(row.get('收盤價'), row.get('漲跌幅'))
            df_display.at[i, '成交價價差'] = (
                round(price_difference, 2) if price_difference is not None else None
            )
            ma5_val = row.get('_ma5')
            if pd.isna(ma5_val):
                for p in row.get('_points', []):
                    if p.get('tag') in ['多', '空', '平']:
                        ma5_val = p.get('val')
                        break
            
            if pd.notna(ma5_val):
                close_p = row.get('收盤價')
                if pd.notna(close_p) and str(close_p).strip() != "":
                    try: df_display.at[i, '5日線價差'] = round(float(close_p) - float(ma5_val), 2)
                    except: pass

        # 附加層只讀取原選股結果；關閉後維持既有表格、排序與戰略備註。
        risk_preview_enabled = st.checkbox(
            "🛡️ 啟用附加分析層（可隨時關閉回到原表）",
            value=True,
            key="risk_filter_preview_enabled",
            help="加入支撐壓力、進出場點位、訊號、信心、資料品質與成效紀錄；不改動週轉率排序或原始戰略備註。"
        )
        risk_details = {}
        risk_show_only_eligible = False
        stock_compact_table = False
        stock_notify = False

        if risk_preview_enabled:
            if st.session_state.get('risk_filter_strategy_mode') == '當沖預覽':
                st.session_state['risk_filter_strategy_mode'] = '當沖'
            if 'risk_filter_market_data' not in st.session_state:
                st.session_state.risk_filter_market_data = {
                    'attention': {}, 'disposition': [], 'updated': None, 'errors': []
                }

            reopen_strategy_settings = st.session_state.pop(
                '_reopen_stock_strategy_settings', False,
            )
            with st.expander(
                "🧭 選股條件與進場信心設定",
                expanded=reopen_strategy_settings,
            ):
                strategy_mode = st.radio(
                    "策略模式", ["當沖", "隔日／波段"], horizontal=True,
                    key="risk_filter_strategy_mode",
                    help="隔日／波段維持原有規則；09:00–09:15 採即時串流＋1 分 K，之後採 5 分 K、VWAP 與完整開盤區間，不會自動下單。"
                )
                is_daytrade_mode = strategy_mode == "當沖"
                if st.session_state.get('risk_filter_direction') in ('多頭', '空頭'):
                    st.session_state['risk_filter_direction'] = '多' if st.session_state['risk_filter_direction'] == '多頭' else '空'
                risk_col1, risk_col2, risk_col3 = st.columns(3)
                with risk_col1:
                    risk_direction = st.radio(
                        "判斷方向", ["系統自動", "多", "空"], horizontal=True,
                        key="risk_filter_direction",
                        help="系統自動會逐檔判斷：當沖以分 K、VWAP、開盤區間及量能為主；隔日／波段以日 K 均線、前高前低與支撐壓力為主。選多或空可強制整表使用指定方向。"
                    )
                    risk_min_score = st.slider("最低進場信心", min_value=60, max_value=90, value=75, key="risk_filter_min_score")
                with risk_col2:
                    risk_max_extension = st.slider("最大乖離（ATR）", min_value=1.0, max_value=3.0, value=2.0, step=0.1, key="risk_filter_max_extension")
                    risk_block_attention = st.checkbox("封鎖注意累計 ≥ 2", value=True, key="risk_filter_block_attention")
                    stock_compact_table = st.checkbox(
                        "精簡主表", value=True, key='stock_strategy_compact_table',
                        help='戰略備註維持原樣；其餘細節移到個股明細。'
                    )
                with risk_col3:
                    risk_show_only_eligible = st.checkbox("只顯示可操作候選", value=False, key="risk_filter_show_eligible")
                    stock_notify = st.checkbox("🔔 訊號變化提醒", value=True, key='stock_strategy_notify')
                    if st.button("📊 重抓日 K 並計算策略指標", key="refresh_risk_filter_metrics"):
                        code_name_map, _ = load_local_stock_names()
                        with st.spinner("正在重抓日 K 並回填策略指標..."):
                            refreshed_data, updated_count = refresh_risk_metrics_for_codes(
                                st.session_state.stock_data,
                                st.session_state.get('futures_list', {}),
                                st.session_state.get('saved_notes', {}),
                                code_name_map,
                                st.session_state.get('sj_logged_in', False),
                                st.session_state.get('sj_api', None)
                            )
                        if updated_count:
                            st.session_state.stock_data = refreshed_data
                            mark_stock_data_updated()
                            cloud_sync_ok = save_data_cache(
                                st.session_state.stock_data,
                                st.session_state.ignored_stocks,
                                st.session_state.all_candidates,
                                st.session_state.saved_notes,
                                verify_stock_data=True,
                            )
                            if not cloud_sync_ok:
                                st.session_state['_stock_cache_sync_notice'] = (
                                    "日 K 已更新，但 Google Sheet 尚未回讀確認；目前先保留本機最新資料。"
                                )
                            st.session_state['_reopen_stock_strategy_settings'] = True
                            st.toast(f"已回填 {updated_count} 檔的 20 日趨勢與 ATR 指標。", icon="✅")
                            st.rerun()
                        else:
                            st.warning("沒有可回填的資料；請確認標的至少有 20 個交易日的日 K。")
                    if is_daytrade_mode:
                        if st.button("📈 更新盤中資料與當沖條件", key="refresh_daytrade_filter_metrics"):
                            if not st.session_state.get('sj_logged_in', False) or st.session_state.get('sj_api') is None:
                                st.warning("當沖需要先登入永豐 Shioaji，才能取得即時串流與分 K 資料。")
                            else:
                                with st.spinner("正在讀取即時串流與分 K、計算 VWAP 與開盤條件..."):
                                    refreshed_data, updated_count = refresh_daytrade_metrics_for_codes(
                                        st.session_state.stock_data,
                                        st.session_state.get('sj_logged_in', False),
                                        st.session_state.get('sj_api', None)
                                    )
                                if updated_count:
                                    st.session_state.stock_data = refreshed_data
                                    mark_stock_data_updated()
                                    cloud_sync_ok = save_data_cache(
                                        st.session_state.stock_data,
                                        st.session_state.ignored_stocks,
                                        st.session_state.all_candidates,
                                        st.session_state.saved_notes,
                                        verify_stock_data=True,
                                    )
                                    if not cloud_sync_ok:
                                        st.session_state['_stock_cache_sync_notice'] = (
                                            "盤中資料已更新，但 Google Sheet 尚未回讀確認；目前先保留本機最新資料。"
                                        )
                                    st.session_state['_reopen_stock_strategy_settings'] = True
                                    st.toast(f"已更新 {updated_count} 檔的即時成交、VWAP 與開盤條件", icon="📈")
                                    st.rerun()
                                else:
                                    st.warning("沒有取得足夠的盤中 5 分 K；請確認 Shioaji 連線與交易時段資料。")
                    if st.button("🔄 更新上市／上櫃注意與處置名單", key="refresh_risk_filter_market_data"):
                        fetch_market_risk_lists.clear()
                        with st.spinner("正在更新上市／上櫃注意與處置名單..."):
                            attention, disposition, disposition_tomorrow, errors = fetch_market_risk_lists()
                        st.session_state.risk_filter_market_data = merge_market_risk_refresh(
                            st.session_state.get('risk_filter_market_data', {}),
                            attention, disposition, disposition_tomorrow, errors,
                        )
                        st.session_state['_reopen_stock_strategy_settings'] = True
                        st.rerun()

                cache_sync_notice = st.session_state.pop('_stock_cache_sync_notice', '')
                if cache_sync_notice:
                    st.warning(cache_sync_notice)
                market_risk_data = st.session_state.risk_filter_market_data
                if market_risk_data.get('updated') and not market_risk_data.get('errors'):
                    st.caption(f"上市／上櫃注意與處置名單更新：{market_risk_data['updated']}。")
                elif market_risk_data.get('errors'):
                    if market_risk_data.get('using_last_success'):
                        st.warning(
                            f"本次名單未完整取得，沿用 {market_risk_data.get('updated')} 的最後成功資料；"
                            "不會以空名單覆蓋。"
                        )
                    else:
                        st.warning("注意／處置名單暫時無法完整更新；本次不會將未查核資料誤標為安全。")
                else:
                    st.info("尚未更新上市／上櫃注意與處置名單；資料未查核時不會被誤判為安全。")

                risk_ready_mask = df_display.reindex(columns=RISK_METRIC_COLUMNS).apply(
                    lambda row: all(_safe_number(row.get(column)) is not None for column in RISK_METRIC_COLUMNS),
                    axis=1,
                )
                risk_ready_count = int(risk_ready_mask.sum())
                st.caption(f"日 K 策略指標：{risk_ready_count} / {len(df_display)} 檔可計算；資料不足時，先按「重抓日 K 並計算策略指標」。")
                if is_daytrade_mode:
                    daytrade_ready_mask = df_display.reindex(columns=DAYTRADE_METRIC_COLUMNS).apply(
                        lambda row: (
                            all(_safe_number(row.get(column)) is not None for column in DAYTRADE_REQUIRED_COLUMNS)
                            and parse_strategy_data_time(row.get('_daytrade_data_time')) is not None
                        ),
                        axis=1,
                    )
                    daytrade_ready_count = int(daytrade_ready_mask.sum())
                    st.caption(f"當沖盤中指標：{daytrade_ready_count} / {len(df_display)} 檔可計算；09:00–09:15 使用快照＋1 分 K，09:15 後使用 5 分 K，僅在手動按更新時抓取。")

            market_risk_data = st.session_state.risk_filter_market_data
            attention_counts = market_risk_data.get('attention', {})
            disposition_codes = market_risk_data.get('disposition', [])
            disposition_tomorrow_codes = market_risk_data.get('disposition_tomorrow', [])
            market_lists_updated = bool(market_risk_data.get('updated')) and not market_risk_data.get('errors')
            market_environment = st.session_state.get('strategy_market_environment', {})
            market_bias = str(market_environment.get('bias', '盤整'))
            market_source = str(market_environment.get('source', '臺指期資料不足'))
            market_change = _safe_number(market_environment.get('change'))
            market_change_text = f" {_signed_percent_arrow(market_change)}" if market_change is not None else ''
            st.caption(f"市場環境：{market_bias}｜依據 {market_source}{market_change_text}；只提供順逆勢標示，不改動原選股順位。")

            for i, row in df_display.iterrows():
                direction_info = determine_stock_direction(row, is_daytrade_mode, risk_direction)
                row_direction = direction_info['direction']
                result = calculate_daytrade_filter_result(
                    row, row_direction, attention_counts, disposition_codes,
                    market_lists_updated, risk_block_attention
                ) if is_daytrade_mode else calculate_risk_filter_result(
                    row, row_direction, risk_max_extension, attention_counts, disposition_codes,
                    market_lists_updated, risk_block_attention,
                    disposition_tomorrow_codes=disposition_tomorrow_codes
                )
                code = str(row.get('代號', ''))
                if is_daytrade_mode:
                    daily_risk = calculate_risk_filter_result(
                        row, row_direction, risk_max_extension, attention_counts, disposition_codes,
                        market_lists_updated, risk_block_attention,
                        disposition_tomorrow_codes=disposition_tomorrow_codes
                    )
                    # 日 ATR 乖離與官方風險是當沖的盤前門檻；盤中訊號成立也不放行過度延伸標的。
                    if not daily_risk['eligible']:
                        result['eligible'] = False
                        if result['rule'].startswith('觸發：'):
                            result['rule'] = f"不交易：盤前門檻未通過（{daily_risk['rule']}）"
                    df_display.at[i, '風險'] = daily_risk['risk']
                    df_display.at[i, 'VWAP 狀態'] = result['vwap_status']
                    df_display.at[i, '開盤區間'] = result['opening_range']
                    volume_ratio = _as_float(row.get('_daytrade_volume_ratio'))
                    df_display.at[i, '量能'] = f"{_format_compact_number(volume_ratio, 2)}x" if volume_ratio is not None else "資料不足"
                    df_display.at[i, '當沖評分'] = result['score']
                    df_display.at[i, '盤中觸發'] = result['rule']
                else:
                    df_display.at[i, '風險'] = result['risk']
                    df_display.at[i, '評分'] = result['score']
                    df_display.at[i, '乖離'] = f"{_format_compact_number(result['extension'], 1, signed=True)} ATR" if result['extension'] is not None else "—"
                    df_display.at[i, '隔日規則'] = result['rule']
                trade_plan = build_trade_plan(row, row_direction, is_daytrade_mode, result)
                if result.get('eligible') and not trade_plan.get('valid', False):
                    result['eligible'] = False
                    result['rule'] = f"觀察：{trade_plan.get('blocking_reason', '進場品質未達門檻')}"
                    if is_daytrade_mode:
                        df_display.at[i, '盤中觸發'] = result['rule']
                    else:
                        df_display.at[i, '隔日規則'] = result['rule']
                result['trade_plan'] = trade_plan
                result['direction_info'] = direction_info
                risk_details[code] = result
                df_display.at[i, '建議方向'] = direction_info['label']
                df_display.at[i, '方向依據'] = direction_info['basis']
                df_display.at[i, '_系統方向'] = row_direction
                df_display.at[i, '進出場預判'] = trade_plan['summary']
                signal_state = classify_signal_state(result['rule'], result['eligible'], result['score'], risk_min_score)
                quote_time = row.get('_quote_time') or (row.get('_daytrade_data_time') if is_daytrade_mode else None)
                required_ready = bool(result.get('data_time')) if is_daytrade_mode else result.get('extension') is not None
                if bool(row.get('_data_stale', False)):
                    data_health = '🔴 資料過期'
                elif row.get('_quote_time'):
                    data_health = build_data_health(row.get('_quote_time'), required_ready, live_expected=True)
                else:
                    data_health = build_data_health(quote_time, required_ready, live_expected=is_daytrade_mode)
                bid = _safe_number(row.get('_quote_bid'))
                ask = _safe_number(row.get('_quote_ask'))
                reference_price = _safe_number(row.get('收盤價'))
                tick = get_tick_size(reference_price) if reference_price is not None else 0.01
                spread_ticks = (ask - bid) / tick if bid is not None and ask is not None and ask >= bid and tick > 0 else None
                market_alignment = calculate_market_alignment(row_direction, market_bias)
                current_price = (
                    _safe_number(row.get('_daytrade_close')) if is_daytrade_mode else None
                ) or reference_price
                confidence = calculate_entry_confidence(
                    result['score'], signal_state, current_price, trade_plan['summary'], row_direction,
                    data_health, market_alignment, result.get('detail', '')
                )
                result['confidence'] = confidence
                eligible_with_score = result['eligible'] and confidence['score'] >= risk_min_score
                df_display.at[i, '訊號狀態'] = signal_state
                df_display.at[i, '信心分'] = confidence['score']
                df_display.at[i, '信心判讀'] = confidence['label']
                df_display.at[i, '支撐壓力'] = build_stock_support_resistance(row, is_daytrade_mode)
                df_display.at[i, '市場一致'] = market_alignment
                df_display.at[i, '資料狀態'] = data_health
                df_display.at[i, '買賣價差'] = f'{spread_ticks:.0f}跳' if spread_ticks is not None else '—'
                df_display.at[i, '_risk_eligible'] = eligible_with_score
                df_display.at[i, '_附加可記錄'] = (
                    bool(trade_plan.get('valid')) and eligible_with_score
                    and signal_state in ('✅ 已觸發', '🔵 回測確認')
                )

            notify_signal_state_changes(
                'stocks',
                {str(row['代號']): str(row.get('訊號狀態', '')) for _, row in df_display.iterrows()},
                stock_notify,
            )

            if risk_show_only_eligible:
                df_display = df_display[df_display['_risk_eligible']].reset_index(drop=True)
                if df_display.empty:
                    st.warning("目前沒有符合門檻的候選；可降低最低評分、放寬最大乖離，或切換回原表。")

            if stock_compact_table:
                input_cols = [
                    "移除", "代號", "名稱", "戰略備註", "收盤價", "漲跌幅",
                    "建議方向", "方向依據", "支撐壓力", "進出場預判", "5日線價差", "信心分", "信心判讀",
                    "訊號狀態", "市場一致", "風險", "資料狀態"
                ]
            elif is_daytrade_mode:
                input_cols = ["移除", "代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "建議方向", "方向依據", "狀態", "成交價價差", "5日線價差", "風險", "VWAP 狀態", "開盤區間", "量能", "信心分", "信心判讀", "支撐壓力", "盤中觸發", "進出場預判", "訊號狀態", "市場一致", "當日漲停價", "當日跌停價", "期貨", "資料狀態", "買賣價差"]
            else:
                input_cols = ["移除", "代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "建議方向", "方向依據", "狀態", "成交價價差", "5日線價差", "風險", "信心分", "信心判讀", "乖離", "支撐壓力", "隔日規則", "進出場預判", "訊號狀態", "市場一致", "當日漲停價", "當日跌停價", "期貨", "資料狀態", "買賣價差"]
        else:
            input_cols = ["移除", "代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "狀態", "成交價價差", "5日線價差", "當日漲停價", "當日跌停價", "期貨"]
        for col in input_cols:
            if col not in df_display.columns: df_display[col] = None

        cols_to_fmt = ["當日漲停價", "當日跌停價", "成交價價差", "5日線價差"]
        for c in cols_to_fmt:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        if "收盤價" in df_display.columns: df_display["收盤價"] = df_display["收盤價"].astype(object)
        if "漲跌幅" in df_display.columns: df_display["漲跌幅"] = df_display["漲跌幅"].astype(object)

        if "收盤價" in df_display.columns and "漲跌幅" in df_display.columns:
            for i in range(len(df_display)):
                try:
                    p = float(df_display.at[i, "收盤價"])
                    chg = float(df_display.at[i, "漲跌幅"])
                    df_display.at[i, "收盤價"] = fmt_price(p)
                    df_display.at[i, "漲跌幅"] = _signed_percent(chg)
                except:
                    df_display.at[i, "收盤價"] = fmt_price(df_display.at[i, "收盤價"])
                    try: df_display.at[i, "漲跌幅"] = _signed_percent(float(df_display.at[i, '漲跌幅']))
                    except: pass

        df_display = df_display.reset_index(drop=True)
        for col in input_cols:
            if col not in ["移除", "信心分"]:
                df_display[col] = df_display[col].map(_blank_display_text)

        # 定義上色邏輯
        def style_tab1_df(row, source_frame=None):
            styles = [''] * len(row)
            note = str(row.get('戰略備註', ''))
            # 精簡主表不顯示「狀態」，名稱底色必須以目前成交價與漲跌停價
            # 重新比對；不能沿用可能是上一輪報價留下的「漲停／跌停」文字。
            st_val = str(row.get('狀態', ''))
            frame = source_frame if isinstance(source_frame, pd.DataFrame) else df_display
            if not st_val and row.name in frame.index:
                st_val = str(frame.at[row.name, '狀態'])
            source_row = frame.loc[row.name] if row.name in frame.index else row
            limit_state = stock_limit_state(
                row.get('收盤價'),
                source_row.get('_交易日漲停價', row.get('當日漲停價')),
                source_row.get('_交易日跌停價', row.get('當日跌停價')),
            )
            risk_val = str(row.get('風險', ''))
            vwap_val = str(row.get('VWAP 狀態', ''))
            signal_state = str(row.get('訊號狀態', ''))
            data_health = str(row.get('資料狀態', ''))
            market_alignment = str(row.get('市場一致', ''))
            
            if limit_state == 'up':
                name_c = 'background-color: #ff4b4b; color: #ffffff; font-weight: bold;'
            elif limit_state == 'down':
                name_c = 'background-color: #00e676; color: #ffffff; font-weight: bold;'
            elif st_val == "命中":
                name_c = 'background-color: #ffeb3b; color: #000000; font-weight: bold;'
            else:
                name_c = (
                    'color: #ff4b4b; font-weight: bold;' if "多" in note
                    else ('color: #00e676; font-weight: bold;' if "空" in note else 'font-weight: bold;')
                )
            
            price_c = ''
            try:
                c_val = float(
                    str(row.get('漲跌幅', '0')).replace('%', '').replace('+', '')
                    .replace('↑', '').replace('↓', '').replace('→', '').strip()
                )
                price_c = 'color: #ff4b4b;' if c_val > 0 else ('color: #00e676;' if c_val < 0 else '')
            except: pass
            
            status_c = 'color: #ff4b4b;' if st_val in ["漲停", "強"] else ('color: #00e676;' if st_val in ["跌停", "弱"] else ('color: #ffeb3b;' if st_val == "命中" else ''))
            
            for idx, col in enumerate(row.index):
                if col == "名稱": styles[idx] = name_c
                elif col in ["收盤價", "漲跌幅"]: styles[idx] = price_c
                elif col == "狀態": styles[idx] = status_c
                elif col == "建議方向":
                    direction_text = str(row.get('建議方向', ''))
                    if '多' in direction_text:
                        styles[idx] = 'color:#ff4b4b;font-weight:bold;'
                    elif '空' in direction_text:
                        styles[idx] = 'color:#00e676;font-weight:bold;'
                elif col == "方向依據":
                    direction_text = str(row.get('建議方向', ''))
                    styles[idx] = (
                        'color:#ff6b6b;' if '多' in direction_text
                        else ('color:#35d07f;' if '空' in direction_text else 'color:#cbd5e1;')
                    )
                elif col == "支撐壓力":
                    styles[idx] = 'color:#4fc3f7;font-weight:600;'
                elif col in ("盤中觸發", "隔日規則", "進出場預判"):
                    value = str(row.get(col, ''))
                    if any(keyword in value for keyword in ('資料不足', '不建立', '觀察：', '等待')):
                        styles[idx] = 'color:#ffd166;'
                    elif '多' in str(row.get('建議方向', '')):
                        styles[idx] = 'color:#ff6b6b;'
                    elif '空' in str(row.get('建議方向', '')):
                        styles[idx] = 'color:#35d07f;'
                elif col == "當日漲停價":
                    styles[idx] = 'color:#ff4b4b;font-weight:bold;'
                elif col == "當日跌停價":
                    styles[idx] = 'color:#00e676;font-weight:bold;'
                elif col in ["成交價價差", "5日線價差"]:
                    val = row[col]
                    try:
                        f_val = float(val)
                        if f_val > 0: styles[idx] = 'color: #ff4b4b;'
                        elif f_val < 0: styles[idx] = 'color: #00e676;'
                        else: styles[idx] = 'color: white;'
                    except: pass
                elif col == "風險":
                    if risk_val.startswith('🚫') or risk_val.startswith('🔴'):
                        styles[idx] = 'color: #ff4b4b; font-weight: bold;'
                    elif risk_val.startswith('🔶'):
                        styles[idx] = 'color: #ff9800; font-weight: bold;'
                    elif risk_val.startswith('🟡'):
                        styles[idx] = 'color: #ffeb3b; font-weight: bold;'
                    elif risk_val.startswith('🟢'):
                        styles[idx] = 'color: #00e676;'
                elif col == "VWAP 狀態":
                    if vwap_val.startswith('偏多'):
                        styles[idx] = 'color: #ff4b4b; font-weight: bold;'
                    elif vwap_val.startswith('偏空'):
                        styles[idx] = 'color: #00e676; font-weight: bold;'
                    elif vwap_val.startswith('中性'):
                        styles[idx] = 'color: #ffeb3b;'
                elif col == "訊號狀態":
                    if signal_state.startswith('✅'):
                        styles[idx] = 'color:#ff4b4b;font-weight:bold;'
                    elif signal_state.startswith('🔵'):
                        styles[idx] = 'color:#29b6f6;font-weight:bold;'
                    elif signal_state.startswith('⛔'):
                        styles[idx] = 'color:#ff9800;font-weight:bold;'
                    elif signal_state.startswith('🟡'):
                        styles[idx] = 'color:#ffeb3b;'
                elif col in ["資料狀態", "市場一致"]:
                    value = data_health if col == "資料狀態" else market_alignment
                    if value.startswith('🟢'):
                        styles[idx] = 'color:#00e676;'
                    elif value.startswith(('🔴', '⛔')):
                        styles[idx] = 'color:#ff4b4b;font-weight:bold;'
                    elif value.startswith('🟡'):
                        styles[idx] = 'color:#ffeb3b;'
                elif col == "信心判讀":
                    confidence = str(row.get('信心判讀', ''))
                    if confidence.startswith('🟢'):
                        styles[idx] = 'color:#00e676;font-weight:bold;'
                    elif confidence.startswith(('🟡', '🟠')):
                        styles[idx] = 'color:#ffb300;font-weight:bold;'
                    elif confidence.startswith('🔴'):
                        styles[idx] = 'color:#ff4b4b;font-weight:bold;'
            return styles
            
        styled_df = df_display[input_cols].style.apply(style_tab1_df, axis=1)

        def stock_content_width(column, minimum, maximum=520, full_content=False):
            return _content_column_width(
                df_display.get(column), minimum, maximum, full_content,
            )

        risk_column_config = {}
        if risk_preview_enabled:
            risk_column_config = {
                "建議方向": st.column_config.TextColumn(width=stock_content_width('建議方向', 56, 90), disabled=True, help="系統自動時逐檔判斷；紅色為建議多、綠色為建議空。選擇手動多／空時會標示為手動。"),
                "方向依據": st.column_config.TextColumn(width=stock_content_width('方向依據', 86), disabled=True, help="當沖優先列出分 K、VWAP、開盤區間與量能；隔日／波段列出日 K 均線、前高前低及 K 棒位置。括號為多空條件分數。"),
                "風險": st.column_config.TextColumn("處置／注意", width=stock_content_width('風險', 64, 110), disabled=True, help="官方注意與處置查核結果；不預測漲跌方向。"),
                "訊號狀態": st.column_config.TextColumn(width=stock_content_width('訊號狀態', 56, 140), disabled=True, help="將原條件濃縮為等待、接近、觸發或暫停；原規則仍在明細。"),
                "市場一致": st.column_config.TextColumn(width=stock_content_width('市場一致', 56, 120), disabled=True, help="目前方向是否與近月臺指期環境一致；不改變原排序。"),
                "資料狀態": st.column_config.TextColumn(width=stock_content_width('資料狀態', 56, 150), disabled=True, help="顯示即時、手動／暫存、官方日行情或資料過期。"),
                "買賣價差": st.column_config.TextColumn(
                    width=stock_content_width('買賣價差', 56, 96), disabled=True,
                    help="即時最佳賣價與最佳買價的距離，換算為跳動單位；跳數越少通常代表報價較連續、進出成本較低。無即時買賣價時顯示「—」。"
                ),
                "信心分": st.column_config.ProgressColumn("進場信心", min_value=0, max_value=100, format="%d", width=82, help="綜合方向條件、觸發位置、市場一致與資料狀態；代表條件一致度，不是勝率。"),
                "信心判讀": st.column_config.TextColumn(width=stock_content_width('信心判讀', 48, 96), disabled=True, help="高／中高／中／低；追離進場點、條件失效或資料過期時會降級。"),
                "支撐壓力": st.column_config.TextColumn(width=stock_content_width('支撐壓力', 76), disabled=True, help="當沖採開盤區間與 VWAP；隔日／波段沿用原戰略價位，顯示最接近目前價格的支撐與壓力。"),
            }
            if is_daytrade_mode:
                risk_column_config.update({
                    "VWAP 狀態": st.column_config.TextColumn(width=stock_content_width('VWAP 狀態', 72, 150), disabled=True, help="價格相對成交量加權平均價的位置；上方偏多、下方偏空。"),
                    "開盤區間": st.column_config.TextColumn(width=stock_content_width('開盤區間', 80, 150), disabled=True, help="09:00–09:15 顯示形成中的即時低點－高點；09:15 後固定為完整開盤區間。"),
                    "量能": st.column_config.TextColumn(width=stock_content_width('量能', 52, 96), disabled=True, help="目前累積量相對最近交易日同時段平均量。"),
                    "盤中觸發": st.column_config.TextColumn(width=stock_content_width('盤中觸發', 72), disabled=True, help="僅在盤中條件同時成立時提供觀察提示，不是自動買賣指令。"),
                    "進出場預判": st.column_config.TextColumn(width=stock_content_width('進出場預判', 86, 720, True), disabled=True, help="通過條件後，以開盤區間與 VWAP 推估進場、策略失效離場與第一目標；僅供觀察與回測。"),
                })
            else:
                risk_column_config.update({
                    "乖離": st.column_config.TextColumn(width=stock_content_width('乖離', 52, 96), disabled=True, help="收盤價相對 20 日線的 ATR 距離；數值越大越不宜追價或追空。"),
                    "隔日規則": st.column_config.TextColumn(width=stock_content_width('隔日規則', 72), disabled=True, help="僅在隔日條件成真時才列入評估，不是自動買賣指令。"),
                    "進出場預判": st.column_config.TextColumn(width=stock_content_width('進出場預判', 86, 720, True), disabled=True, help="通過條件後，以昨高／昨低與 ATR 推估進場、策略失效離場與第一目標；僅供觀察與回測。"),
                })

        stock_table_signature = abs(hash((
            tuple(df_display['代號'].astype(str).tolist()), tuple(input_cols)
        )))
        stock_editor_key = (
            f"main_editor_{st.session_state.stock_strategy_editor_revision}_{stock_table_signature}"
        )
        edited_df = st.data_editor(
            styled_df,
            column_config={
                **risk_column_config,
                "移除": st.column_config.CheckboxColumn("刪除", width=40, help="勾選後刪除並自動遞補"),
                "代號": st.column_config.TextColumn(disabled=True, width=_content_column_width(df_display.get("代號"), 44, 62)),
                "名稱": st.column_config.TextColumn(disabled=True, width=_content_column_width(df_display.get("名稱"), 44, 130)),
                "收盤價": st.column_config.TextColumn("成交價", width=_content_column_width(df_display.get("收盤價"), 52, 78), disabled=True),
                "漲跌幅": st.column_config.TextColumn(disabled=True, width=_content_column_width(df_display.get("漲跌幅"), 56, 78)),
                "期貨": st.column_config.TextColumn(width=_content_column_width(df_display.get("期貨"), 44, 70), disabled=True),
                "當日漲停價": st.column_config.TextColumn(width=_content_column_width(df_display.get("當日漲停價"), 54, 78), disabled=True),
                "當日跌停價": st.column_config.TextColumn(width=_content_column_width(df_display.get("當日跌停價"), 54, 78), disabled=True),
                "成交價價差": st.column_config.TextColumn(
                    width=80, disabled=True,
                    help="目前成交價減去昨日收盤價；正值為上漲點數，負值為下跌點數。"
                ),
                "5日線價差": st.column_config.TextColumn(
                    width=80, disabled=True,
                    help="目前成交價減去 5 日均線；正值在均線上方，負值在均線下方。"
                ),
                "狀態": None, # 設定為 None 即可在資料編輯器中隱藏該欄位
                "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px, disabled=False),
            },
            hide_index=True, width='stretch' if risk_preview_enabled else 'content', num_rows="fixed", key=stock_editor_key
        )

        if risk_preview_enabled and risk_details:
            detail_options = {
                f"{row['代號']} {row['名稱']}": str(row['代號'])
                for _, row in df_display.iterrows()
            }
            if detail_options:
                selected_risk_label = st.selectbox("查看策略信心明細", list(detail_options), key="risk_filter_detail_code")
                selected_risk = risk_details[detail_options[selected_risk_label]]
                rule_label = "盤中觸發" if is_daytrade_mode else "隔日規則"
                data_time_text = f"｜盤中資料更新：{selected_risk['data_time']}" if is_daytrade_mode and selected_risk.get('data_time') else ""
                plan_text = selected_risk.get('trade_plan', {}).get('detail', '尚未預判點位。')
                confidence_detail = selected_risk.get('confidence', {})
                direction_detail = selected_risk.get('direction_info', {})
                st.caption(
                    f"{direction_detail.get('label', '方向未判定')}｜{direction_detail.get('basis', '方向依據不足')}｜"
                    f"進場信心 {confidence_detail.get('score', '—')} 分（{confidence_detail.get('label', '—')}）｜"
                    f"{selected_risk['detail']}｜{rule_label}：{selected_risk['rule']}｜進出場預判：{plan_text}{data_time_text}。"
                    "信心分代表條件一致度，不是勝率；僅供策略觀察與回測。"
                )
                selected_code = detail_options[selected_risk_label]
                selected_rows = df_display[df_display['代號'].astype(str) == selected_code]
                if not selected_rows.empty:
                    selected_row = selected_rows.iloc[0]
                    st.caption(
                        f"附加狀態：{selected_row.get('訊號狀態', '—')}｜{selected_row.get('市場一致', '—')}｜"
                        f"{selected_row.get('資料狀態', '—')}｜買賣價差 {selected_row.get('買賣價差', '—')}。"
                    )

            if st.button(
                '📝 記錄目前表格的已觸發股票訊號', key='record_stock_strategy_signals',
                help='一次記錄目前股票表格中所有已觸發或回測確認、且達最低進場信心的個股；不是只記錄單一個股。'
            ):
                records = []
                recordable_rows = df_display[df_display.get('_附加可記錄', False) == True]
                for _, row in recordable_rows.iterrows():
                    plan = parse_trade_plan_numbers(row.get('進出場預判'))
                    score = row.get('信心分')
                    records.append({
                        '市場': '股票', '商品鍵': str(row['代號']), '代碼': str(row['代號']),
                        '名稱': str(row['名稱']), '策略': strategy_mode, '方向': str(row.get('_系統方向', '')),
                        '訊號狀態': str(row.get('訊號狀態', '')), '評分': _safe_number(score),
                        '信心判讀': str(row.get('信心判讀', '')),
                        '進場價': plan['entry'], '停損價': plan['stop'], '目標價': plan['target'],
                        '最新價': _safe_number(row.get('收盤價')),
                        '風險': str(row.get('風險', '')), '資料狀態': str(row.get('資料狀態', '')),
                    })
                added, saved = register_strategy_signals(records)
                if saved:
                    save_data_cache(
                        st.session_state.stock_data, st.session_state.ignored_stocks,
                        st.session_state.all_candidates, st.session_state.saved_notes,
                    )
                    st.toast(f'已新增 {added} 筆股票訊號；重複訊號不另建。', icon='📝')
                else:
                    st.error('訊號紀錄儲存失敗，請確認檔案是否可寫入。')
        
        if not edited_df.empty:
            trigger_rerun = False
            if "移除" in edited_df.columns:
                to_remove = edited_df[edited_df["移除"] == True]
                if not to_remove.empty:
                    remove_codes = to_remove["代號"].unique()
                    
                    # 速度優化：將刪除的股票資料快取起來
                    for c in remove_codes: 
                        st.session_state.ignored_stocks.add(str(c))
                        row_data = st.session_state.stock_data[st.session_state.stock_data["代號"] == c]
                        if not row_data.empty:
                            st.session_state.ignored_data_cache[c] = row_data.iloc[0].to_dict()
                            
                    # --- 新增：資源優化，忽略快取超過 5 檔就刪除最舊的 ---
                    while len(st.session_state.ignored_data_cache) > 5:
                        oldest_key = next(iter(st.session_state.ignored_data_cache))
                        del st.session_state.ignored_data_cache[oldest_key]
                    # ----------------------------------------------------
                    
                    # 從表格資料中剃除
                    st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(remove_codes)]
                    
                    # 🚀 關鍵修改：立刻儲存並 Rerun，讓畫面「瞬間」移除該行，
                    # 把耗時的遞補抓取動作交給最下方的區塊去處理，避免畫面卡死！
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.session_state.stock_strategy_editor_revision += 1
                    st.rerun()

            if trigger_rerun: st.rerun()

        df_curr = st.session_state.stock_data
        if not df_curr.empty:
            upload_count = len(df_curr) if '_source' not in df_curr.columns else len(df_curr[df_curr['_source'] == 'upload'])
            limit = st.session_state.limit_rows
            
            # 若筆數不足且有候補名單，則進行自動遞補
            if upload_count < limit and st.session_state.all_candidates:
                needed = limit - upload_count
                existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                futures_copy = dict(st.session_state.futures_list)
                notes_copy = dict(st.session_state.saved_notes)
                code_map_copy, _ = load_local_stock_names()
                
                cand_to_fetch = []
                for cand in st.session_state.all_candidates:
                     c_code, c_name, c_source, c_extra = str(cand[0]), cand[1], cand[2], cand[3]
                     if c_source != 'upload' or c_code in st.session_state.ignored_stocks or c_code in existing_codes: continue
                     cand_to_fetch.append((c_code, c_name, c_source, c_extra))
                     if len(cand_to_fetch) >= needed: break
                     
                if cand_to_fetch:
                    with st.spinner(f"正在遞補 {len(cand_to_fetch)} 檔股票..."):
                        # 🚀 1. 優先從「背景預載快取」提取 (瞬間完成)
                        ready_results = []
                        remaining_to_fetch = []
                        
                        for cand in cand_to_fetch:
                            t_code = cand[0]
                            if t_code in st.session_state.get('prefetch_cache', {}):
                                ready_results.append(st.session_state.prefetch_cache.pop(t_code))
                            else:
                                remaining_to_fetch.append(cand)
                                
                        if ready_results:
                            st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(ready_results)], ignore_index=True)
                            
                        # 🚀 2. 若快取不足(例如剛開啟網頁還沒預載完)，才即時抓取剩下的
                        if remaining_to_fetch:
                            worker_sj_logged_in = st.session_state.get('sj_logged_in', False)
                            worker_sj_api = st.session_state.get('sj_api')
                            def _replenish_worker(cand):
                                time.sleep(API_REQUEST_GAP_SECONDS)
                                t_code, t_name, t_src, t_extra = cand
                                res = fetch_stock_data_raw(
                                    t_code, t_name, t_extra, futures_copy, notes_copy,
                                    code_map_copy, worker_sj_logged_in, worker_sj_api,
                                )
                                if res: res.update({'_source': t_src, '_order': t_extra, '_source_rank': 1})
                                return res

                            with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                                results = list(executor.map(_replenish_worker, remaining_to_fetch))
                                valid_results = [r for r in results if r]
                                if valid_results:
                                    st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(valid_results)], ignore_index=True)
                        
                        # 修正：強制依據來源優先權進行排序，讓自動遞補的新股票排在查詢的股票之前
                        if '_source_rank' in st.session_state.stock_data.columns:
                            st.session_state.stock_data = st.session_state.stock_data.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)

                        save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                        st.rerun() # 遞補完成，立刻更新畫面

        
        st.markdown("---")
        # 重新配置欄位比例與順序
        col_rt_update, col_btn, col_clear = st.columns([2, 2.5, 2])
        with col_rt_update: btn_rt_update = st.button("⏱️ 即時更新報價", width='stretch', type="primary")
        with col_btn: btn_update = st.button("⚡ 執行更新&儲存手動備註", width='stretch')
        with col_clear: btn_clear_notes = st.button("🧹 清除手動備註", width='stretch', help="清除所有記憶的戰略備註內容")

        if btn_rt_update:
            if st.session_state.get('sj_logged_in', False) and st.session_state.get('sj_api'):
                sj_api = st.session_state.sj_api
                with st.spinner("正在透過永豐API更新報價..."):
                    quote_codes = st.session_state.stock_data['代號'].astype(str).tolist()
                    snapshot_map = fetch_stock_snapshot_map(sj_api, quote_codes)
                    refreshed_quotes, updated_count = merge_realtime_stock_snapshots(
                        st.session_state.stock_data,
                        snapshot_map,
                        points_map=points_map,
                    )
                if updated_count:
                    st.session_state.stock_data = refreshed_quotes
                    tz_tw = pytz.timezone('Asia/Taipei')
                    st.session_state.last_rt_update_time = datetime.now(tz_tw).strftime("%Y/%m/%d %H:%M:%S")
                    update_strategy_signal_outcomes({
                        str(row['代號']): _safe_number(row.get('收盤價'))
                        for _, row in st.session_state.stock_data.iterrows()
                    })
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.session_state.stock_strategy_editor_revision += 1
                    st.toast(f"已用單一批次更新 {updated_count} 檔報價。", icon="⏱️")
                    st.rerun()
                else:
                    st.warning("目前未取得任何有效即時報價，原表格資料未變更。")
            else:
                st.warning("⚠️ 請先登入永豐 API 才能使用即時更新報價功能。")

        if btn_clear_notes:
            st.session_state.saved_notes = {}
            st.toast("手動備註已清除", icon="🧹")
            if not st.session_state.stock_data.empty:
                 for idx, row in st.session_state.stock_data.iterrows():
                     clean_note, _ = generate_note_from_points(row.get('_points', []), "", show_3d_hilo)
                     st.session_state.stock_data.at[idx, '戰略備註'] = clean_note
                     if '_auto_note' in st.session_state.stock_data.columns: st.session_state.stock_data.at[idx, '_auto_note'] = clean_note
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.session_state.stock_strategy_editor_revision += 1
            st.rerun()
        
        if 'last_rt_update_time' in st.session_state:
            st.markdown(f"<div style='text-align: left; color: #888; font-size: 14px; margin-top: 5px; margin-bottom: 10px;'>透過永豐API即時更新報價(更新時間:{st.session_state.last_rt_update_time})</div>", unsafe_allow_html=True)

        if btn_update:
             update_map = edited_df.set_index('代號')[['戰略備註']].to_dict('index')
             for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    new_note = update_map[code]['戰略備註']
                    if str(row['戰略備註']) != str(new_note):
                        b_auto = str(auto_notes_dict.get(code, "")).strip()
                        n_note = str(new_note).strip()
                        st.session_state.saved_notes[code] = n_note[len(b_auto):] if b_auto and n_note.startswith(b_auto) else f"[M]{n_note}"
                    st.session_state.stock_data.at[i, '戰略備註'] = new_note
                st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
             save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
             st.session_state.stock_strategy_editor_revision += 1
             st.rerun()

        st.markdown("### ⚡獨立計算")
        indep_strategy_mode = None
        if risk_preview_enabled:
            if st.session_state.get('indep_strategy_mode') == '當沖預覽':
                st.session_state['indep_strategy_mode'] = '當沖'
            if 'risk_filter_market_data' not in st.session_state:
                st.session_state.risk_filter_market_data = {
                    'attention': {}, 'disposition': [], 'updated': None, 'errors': []
                }
            indep_ctrl1, indep_ctrl2, indep_ctrl3 = st.columns(3)
            with indep_ctrl1:
                indep_strategy_mode = st.radio(
                    "獨立計算模式", ["當沖", "隔日／波段"], horizontal=True,
                    key="indep_strategy_mode",
                    help="09:00–09:15 會取得即時串流＋1 分 K；09:15 後改用 5 分 K，計算 VWAP、開盤區間與量能。"
                )
                if st.session_state.get('indep_risk_direction') in ('多頭', '空頭'):
                    st.session_state['indep_risk_direction'] = '多' if st.session_state['indep_risk_direction'] == '多頭' else '空'
                indep_direction = st.radio(
                    "判斷方向", ["系統自動", "多", "空"], horizontal=True,
                    key="indep_risk_direction",
                    help="系統自動會依每檔股票可用的分 K／日 K、VWAP 與支撐壓力分別判斷多空。"
                )
            with indep_ctrl2:
                indep_min_score = st.slider("最低進場信心", 60, 90, 75, key="indep_risk_min_score")
                indep_max_extension = st.slider("最大乖離（ATR）", 1.0, 3.0, 2.0, 0.1, key="indep_risk_max_extension")
            with indep_ctrl3:
                indep_block_attention = st.checkbox("封鎖注意累計 ≥ 2", value=True, key="indep_risk_block_attention")
                indep_show_only_eligible = st.checkbox("只顯示可操作候選", value=False, key="indep_risk_show_eligible")
                if st.button("🔄 更新注意／處置名單", key="refresh_indep_market_risk_data"):
                    fetch_market_risk_lists.clear()
                    with st.spinner("正在更新上市／上櫃注意與處置名單..."):
                        attention, disposition, disposition_tomorrow, errors = fetch_market_risk_lists()
                    st.session_state.risk_filter_market_data = merge_market_risk_refresh(
                        st.session_state.get('risk_filter_market_data', {}),
                        attention, disposition, disposition_tomorrow, errors,
                    )
            if indep_strategy_mode == "當沖":
                st.caption("VWAP 判讀：偏多＝站上 VWAP（紅色）；偏空＝跌破 VWAP（綠色）。09:00–09:15 使用快照＋1 分 K，之後使用 5 分 K。")
        col_q1, col_q2 = st.columns([5, 1.5])
        with col_q1:
            indep_selection = st.multiselect(
                "🔍 快速查詢 (中文/代號)", 
                options=stock_options, 
                key="indep_search_multiselect", 
                placeholder="輸入 2330 或 台積電..."
            )
        with col_q2:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            btn_indep_run = st.button("🚀 執行分析", key="btn_indep_run", width='stretch')
            
        cached_indep_data = st.session_state.get('stock_independent_raw_results', [])
        if btn_indep_run and not indep_selection:
            st.warning("請先選擇至少一檔股票再執行獨立分析。")
        if (btn_indep_run and indep_selection) or cached_indep_data:
            c_map_q, n_map_q = load_local_stock_names()
            sj_logged = st.session_state.get('sj_logged_in', False)
            sj_api_obj = st.session_state.get('sj_api', None)
            indep_data = list(cached_indep_data)
            if btn_indep_run and indep_selection:
                if not st.session_state.futures_list:
                    st.session_state.futures_list = fetch_futures_list()
                f_set = st.session_state.futures_list
                notes_copy = dict(st.session_state.get('saved_notes', {}))
                indep_now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
                indep_opening_micro = _is_opening_micro_window(indep_now_tw)
                indep_intraday_interval = '1m' if indep_opening_micro else '5m'
                indep_interval_label = '1 分 K' if indep_opening_micro else '5 分 K'
                indep_codes = [item.split(' ', 1)[0] for item in indep_selection]
                indep_snapshot_map = (
                    fetch_stock_snapshot_map(sj_api_obj, indep_codes)
                    if sj_logged and sj_api_obj is not None else {}
                )

                with st.spinner("正在獨立分析..."):
                    def _indep_worker(item):
                        time.sleep(API_REQUEST_GAP_SECONDS)
                        parts = item.split(' ', 1)
                        q_code = parts[0]
                        q_name = parts[1] if len(parts) > 1 else ""
                        result = fetch_stock_data_raw(
                            q_code, q_name, None, f_set, notes_copy,
                            c_map_q, sj_logged, sj_api_obj
                        )
                        if result and risk_preview_enabled and indep_strategy_mode == "當沖" and sj_logged and sj_api_obj is not None:
                            time.sleep(API_REQUEST_GAP_SECONDS)
                            intraday_df = fetch_shioaji_data(
                                sj_api_obj, q_code, interval=indep_intraday_interval, lookback_days=3
                            )
                            snapshot = indep_snapshot_map.get(q_code)
                            daytrade_metrics = calculate_daytrade_metrics(
                                intraday_df, live_snapshot=snapshot, now_tw=indep_now_tw,
                                interval_label=indep_interval_label,
                            )
                            if daytrade_metrics:
                                result.update(daytrade_metrics)
                                if snapshot is not None:
                                    price = _safe_number(getattr(snapshot, 'close', None))
                                    change_rate = snapshot_change_rate(snapshot, price) if price is not None else None
                                    if price is not None and price > 0:
                                        result['收盤價'] = price
                                        if change_rate is not None:
                                            result['漲跌幅'] = change_rate
                                            result['成交價價差'] = price_change_amount(price, change_rate)
                                    result['_quote_bid'] = _safe_number(getattr(snapshot, 'buy_price', None))
                                    result['_quote_ask'] = _safe_number(getattr(snapshot, 'sell_price', None))
                                    result['_quote_time'] = indep_now_tw.strftime('%Y/%m/%d %H:%M:%S')
                        return result

                    with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                        results = list(executor.map(_indep_worker, indep_selection))
                        indep_data = [res for res in results if res]
                if indep_data:
                    st.session_state.stock_independent_raw_results = indep_data
                else:
                    st.warning("本次未取得有效資料，已保留上一份獨立分析結果。")
                    indep_data = list(cached_indep_data)

            if indep_data:
                df_indep = pd.DataFrame(indep_data)
                indep_is_daytrade = risk_preview_enabled and indep_strategy_mode == "當沖"
                indep_risk_details = {}

                if risk_preview_enabled:
                    indep_market_risk_data = st.session_state.risk_filter_market_data
                    indep_attention_counts = indep_market_risk_data.get('attention', {})
                    indep_disposition_codes = indep_market_risk_data.get('disposition', [])
                    indep_market_lists_updated = bool(indep_market_risk_data.get('updated')) and not indep_market_risk_data.get('errors')
                    if indep_is_daytrade and (not sj_logged or sj_api_obj is None):
                        st.info("當沖需要登入永豐 Shioaji 才能取得即時串流與分 K；目前仍會顯示日 K 資料，但盤中條件會標示為資料不足。")
                
                # 重新套用戰略備註與價差邏輯
                for i, row in df_indep.iterrows():
                    pts = row.get('_points', [])
                    manual = st.session_state.saved_notes.get(row['代號'], "")
                    n_full, n_auto = generate_note_from_points(pts, manual, show_3d_hilo)
                    df_indep.at[i, "戰略備註"] = n_full
                    df_indep.at[i, "名稱"] = row['名稱'].replace('🔴 ', '').replace('🟢 ', '').replace('⚪ ', '')
                    price_difference = price_change_amount(row.get('收盤價'), row.get('漲跌幅'))
                    df_indep.at[i, '成交價價差'] = (
                        round(price_difference, 2) if price_difference is not None else None
                    )
                    
                    ma5_val = row.get('_ma5')
                    if pd.isna(ma5_val):
                        for p in pts:
                            if p.get('tag') in ['多', '空', '平']:
                                ma5_val = p.get('val')
                                break
                    if pd.notna(ma5_val):
                        close_p = row.get('收盤價')
                        if pd.notna(close_p) and str(close_p).strip() != "":
                            try: df_indep.at[i, '5日線價差'] = round(float(close_p) - float(ma5_val), 2)
                            except: pass

                if risk_preview_enabled:
                    for i, row in df_indep.iterrows():
                        direction_info = determine_stock_direction(row, indep_is_daytrade, indep_direction)
                        row_direction = direction_info['direction']
                        result = calculate_daytrade_filter_result(
                            row, row_direction, indep_attention_counts, indep_disposition_codes,
                            indep_market_lists_updated, indep_block_attention
                        ) if indep_is_daytrade else calculate_risk_filter_result(
                            row, row_direction, indep_max_extension, indep_attention_counts, indep_disposition_codes,
                            indep_market_lists_updated, indep_block_attention
                        )
                        code = str(row.get('代號', ''))
                        if indep_is_daytrade:
                            daily_risk = calculate_risk_filter_result(
                                row, row_direction, indep_max_extension, indep_attention_counts, indep_disposition_codes,
                                indep_market_lists_updated, indep_block_attention
                            )
                            if not daily_risk['eligible']:
                                result['eligible'] = False
                                if result['rule'].startswith('觸發：'):
                                    result['rule'] = f"不交易：盤前門檻未通過（{daily_risk['rule']}）"
                            df_indep.at[i, '風險'] = daily_risk['risk']
                            df_indep.at[i, 'VWAP 狀態'] = result['vwap_status']
                            df_indep.at[i, '開盤區間'] = result['opening_range']
                            volume_ratio = _as_float(row.get('_daytrade_volume_ratio'))
                            df_indep.at[i, '量能'] = f"{_format_compact_number(volume_ratio, 2)}x" if volume_ratio is not None else "資料不足"
                            df_indep.at[i, '盤中觸發'] = result['rule']
                        else:
                            df_indep.at[i, '風險'] = result['risk']
                            df_indep.at[i, '乖離'] = f"{_format_compact_number(result['extension'], 1, signed=True)} ATR" if result['extension'] is not None else "—"
                            df_indep.at[i, '隔日規則'] = result['rule']
                        trade_plan = build_trade_plan(row, row_direction, indep_is_daytrade, result)
                        if result.get('eligible') and not trade_plan.get('valid', False):
                            result['eligible'] = False
                            result['rule'] = f"觀察：{trade_plan.get('blocking_reason', '進場品質未達門檻')}"
                            if indep_is_daytrade:
                                df_indep.at[i, '盤中觸發'] = result['rule']
                            else:
                                df_indep.at[i, '隔日規則'] = result['rule']
                        result['trade_plan'] = trade_plan
                        result['direction_info'] = direction_info
                        signal_state = classify_signal_state(
                            result['rule'], result['eligible'], result['score'], indep_min_score
                        )
                        data_time = row.get('_daytrade_data_time') if indep_is_daytrade else None
                        required_ready = bool(data_time) if indep_is_daytrade else result.get('extension') is not None
                        quote_time = row.get('_quote_time') or data_time
                        data_health = build_data_health(
                            quote_time, required_ready,
                            live_expected=bool(row.get('_quote_time')) or indep_is_daytrade
                        )
                        bid = _safe_number(row.get('_quote_bid'))
                        ask = _safe_number(row.get('_quote_ask'))
                        reference_price = _safe_number(row.get('收盤價'))
                        tick = get_tick_size(reference_price) if reference_price is not None else 0.01
                        spread_ticks = (
                            (ask - bid) / tick
                            if bid is not None and ask is not None and ask >= bid and tick > 0 else None
                        )
                        market_alignment = calculate_market_alignment(row_direction, market_bias)
                        current_price = (
                            _safe_number(row.get('_daytrade_close')) if indep_is_daytrade else None
                        ) or _safe_number(row.get('收盤價'))
                        confidence = calculate_entry_confidence(
                            result['score'], signal_state, current_price, trade_plan['summary'], row_direction,
                            data_health, market_alignment, result.get('detail', '')
                        )
                        result['confidence'] = confidence
                        indep_risk_details[code] = result
                        df_indep.at[i, '建議方向'] = direction_info['label']
                        df_indep.at[i, '方向依據'] = direction_info['basis']
                        df_indep.at[i, '進出場預判'] = trade_plan['summary']
                        df_indep.at[i, '支撐壓力'] = build_stock_support_resistance(row, indep_is_daytrade)
                        df_indep.at[i, '訊號狀態'] = signal_state
                        df_indep.at[i, '信心分'] = confidence['score']
                        df_indep.at[i, '信心判讀'] = confidence['label']
                        df_indep.at[i, '市場一致'] = market_alignment
                        df_indep.at[i, '資料狀態'] = data_health
                        df_indep.at[i, '買賣價差'] = f'{spread_ticks:.0f}跳' if spread_ticks is not None else '—'
                        df_indep.at[i, '_indep_eligible'] = (
                            bool(trade_plan.get('valid')) and result['eligible']
                            and confidence['score'] >= indep_min_score
                        )

                    if indep_show_only_eligible:
                        df_indep = df_indep[df_indep['_indep_eligible']].reset_index(drop=True)
                        if df_indep.empty:
                            st.warning("目前沒有符合門檻的候選；可降低最低評分、放寬最大乖離，或改看完整結果。")

                    if indep_is_daytrade:
                        input_cols = ["代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "建議方向", "方向依據", "成交價價差", "5日線價差", "風險", "VWAP 狀態", "開盤區間", "量能", "訊號狀態", "信心分", "信心判讀", "支撐壓力", "盤中觸發", "進出場預判", "市場一致", "當日漲停價", "當日跌停價", "期貨", "資料狀態", "買賣價差"]
                    else:
                        input_cols = ["代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "建議方向", "方向依據", "成交價價差", "5日線價差", "風險", "訊號狀態", "信心分", "信心判讀", "乖離", "支撐壓力", "隔日規則", "進出場預判", "市場一致", "當日漲停價", "當日跌停價", "期貨", "資料狀態", "買賣價差"]
                else:
                    input_cols = ["代號", "名稱", "戰略備註", "收盤價", "漲跌幅", "成交價價差", "5日線價差", "當日漲停價", "當日跌停價", "期貨"]
                for col in input_cols:
                    if col not in df_indep.columns: df_indep[col] = None
                    
                cols_to_fmt = ["當日漲停價", "當日跌停價", "成交價價差", "5日線價差"]
                for c in cols_to_fmt:
                    if c in df_indep.columns: df_indep[c] = df_indep[c].apply(fmt_price)

                # 強制轉換為 object 型別，以防寫入字串時發生 TypeError
                if "收盤價" in df_indep.columns: df_indep["收盤價"] = df_indep["收盤價"].astype(object)
                if "漲跌幅" in df_indep.columns: df_indep["漲跌幅"] = df_indep["漲跌幅"].astype(object)

                if "收盤價" in df_indep.columns and "漲跌幅" in df_indep.columns:
                    for i in range(len(df_indep)):
                        try:
                            p = float(df_indep.at[i, "收盤價"])
                            chg = float(df_indep.at[i, "漲跌幅"])
                            df_indep.at[i, "收盤價"] = fmt_price(p)
                            df_indep.at[i, "漲跌幅"] = _signed_percent(chg)
                        except:
                            df_indep.at[i, "收盤價"] = fmt_price(df_indep.at[i, "收盤價"])
                            try: df_indep.at[i, "漲跌幅"] = _signed_percent(float(df_indep.at[i, '漲跌幅']))
                            except: pass

                for col in input_cols:
                    if col != "信心分":
                        df_indep[col] = df_indep[col].map(_blank_display_text)

                # 套用與主表格完全一致的顏色邏輯
                styled_indep = df_indep[input_cols].style.apply(
                    lambda row: style_tab1_df(row, df_indep), axis=1,
                )
                def indep_content_width(column, minimum, maximum=520, full_content=False):
                    return _content_column_width(
                        df_indep.get(column), minimum, maximum, full_content,
                    )

                indep_column_config = {}
                if risk_preview_enabled:
                    indep_column_config.update({
                        "建議方向": st.column_config.TextColumn(width=indep_content_width('建議方向', 56, 90), disabled=True, help="紅色為建議多、綠色為建議空；手動指定時會顯示手動。"),
                        "方向依據": st.column_config.TextColumn(width=indep_content_width('方向依據', 86), disabled=True, help="列出本檔採用的分 K／日 K、VWAP 與支撐壓力判斷。"),
                        "風險": st.column_config.TextColumn("處置／注意", width=indep_content_width('風險', 64, 110), disabled=True, help="官方注意與處置查核結果；不預測漲跌方向。"),
                        "訊號狀態": st.column_config.TextColumn(width=indep_content_width('訊號狀態', 56, 140), disabled=True),
                        "信心分": st.column_config.ProgressColumn("進場信心", min_value=0, max_value=100, format="%d", width=82, help="條件一致度，不是勝率。"),
                        "信心判讀": st.column_config.TextColumn(width=indep_content_width('信心判讀', 48, 96), disabled=True),
                        "支撐壓力": st.column_config.TextColumn(width=indep_content_width('支撐壓力', 76), disabled=True),
                        "市場一致": st.column_config.TextColumn(width=indep_content_width('市場一致', 56, 120), disabled=True),
                        "資料狀態": st.column_config.TextColumn(width=indep_content_width('資料狀態', 56, 150), disabled=True),
                    })
                    if indep_is_daytrade:
                        indep_column_config.update({
                            "VWAP 狀態": st.column_config.TextColumn(width=indep_content_width('VWAP 狀態', 72, 150), disabled=True, help="偏多：站上 VWAP；偏空：跌破 VWAP。"),
                            "開盤區間": st.column_config.TextColumn(width=indep_content_width('開盤區間', 80, 150), disabled=True, help="09:00–09:15 顯示形成中的即時低點－高點；09:15 後固定為完整開盤區間。"),
                            "量能": st.column_config.TextColumn(width=indep_content_width('量能', 52, 96), disabled=True, help="目前累積量相對最近交易日同時段平均量。"),
                            "盤中觸發": st.column_config.TextColumn(width=indep_content_width('盤中觸發', 72), disabled=True, help="僅為盤中觀察提示，不是自動買賣指令。"),
                            "進出場預判": st.column_config.TextColumn(width=indep_content_width('進出場預判', 86, 720, True), disabled=True, help="以開盤區間與 VWAP 推估進場、策略失效離場與第一目標。"),
                        })
                    else:
                        indep_column_config.update({
                            "乖離": st.column_config.TextColumn(width=indep_content_width('乖離', 52, 96), disabled=True),
                            "隔日規則": st.column_config.TextColumn(width=indep_content_width('隔日規則', 72), disabled=True),
                            "進出場預判": st.column_config.TextColumn(width=indep_content_width('進出場預判', 86, 720, True), disabled=True, help="以昨高／昨低與 ATR 推估進場、策略失效離場與第一目標。"),
                        })

                st.dataframe(
                    styled_indep,
                    column_config={
                        **indep_column_config,
                        "代號": st.column_config.TextColumn(width=_content_column_width(df_indep.get("代號"), 44, 62)),
                        "名稱": st.column_config.TextColumn(width=_content_column_width(df_indep.get("名稱"), 44, 130)),
                        "收盤價": st.column_config.TextColumn("成交價", width=_content_column_width(df_indep.get("收盤價"), 52, 78)),
                        "漲跌幅": st.column_config.TextColumn(width=_content_column_width(df_indep.get("漲跌幅"), 56, 78)),
                        "期貨": st.column_config.TextColumn(width=_content_column_width(df_indep.get("期貨"), 44, 70)),
                        "當日漲停價": st.column_config.TextColumn(width=_content_column_width(df_indep.get("當日漲停價"), 54, 78)),
                        "當日跌停價": st.column_config.TextColumn(width=_content_column_width(df_indep.get("當日跌停價"), 54, 78)),
                        "成交價價差": st.column_config.TextColumn(
                            width=indep_content_width('成交價價差', 56, 96),
                            help="目前成交價減去昨日收盤價；正值為上漲點數，負值為下跌點數。"
                        ),
                        "5日線價差": st.column_config.TextColumn(
                            width=indep_content_width('5日線價差', 56, 96),
                            help="目前成交價減去 5 日均線；正值在均線上方，負值在均線下方。"
                        ),
                        "買賣價差": st.column_config.TextColumn(
                            width=indep_content_width('買賣價差', 56, 96),
                            help="即時最佳賣價與最佳買價的距離，換算為跳動單位。"
                        ),
                        "狀態": None, # 設定為 None 隱藏獨立計算結果的狀態欄位
                        "戰略備註": st.column_config.TextColumn("戰略備註", width=note_width_px)
                    },
                    hide_index=True, width='content', key="indep_table_output"
                )

                if indep_risk_details and not df_indep.empty:
                    detail_options = {
                        f"{row['代號']} {row['名稱']}": str(row['代號'])
                        for _, row in df_indep.iterrows()
                    }
                    selected_label = st.selectbox("查看獨立計算信心明細", list(detail_options), key="indep_risk_detail_code")
                    selected_result = indep_risk_details[detail_options[selected_label]]
                    rule_label = "盤中觸發" if indep_is_daytrade else "隔日規則"
                    data_time_text = f"｜盤中資料更新：{selected_result['data_time']}" if indep_is_daytrade and selected_result.get('data_time') else ""
                    plan_text = selected_result.get('trade_plan', {}).get('detail', '尚未預判點位。')
                    confidence_detail = selected_result.get('confidence', {})
                    direction_detail = selected_result.get('direction_info', {})
                    st.caption(
                        f"{direction_detail.get('label', '方向未判定')}｜{direction_detail.get('basis', '方向依據不足')}｜"
                        f"進場信心 {confidence_detail.get('score', '—')} 分（{confidence_detail.get('label', '—')}）｜"
                        f"{selected_result['detail']}｜{rule_label}：{selected_result['rule']}｜"
                        f"進出場預判：{plan_text}{data_time_text}。信心分代表條件一致度，不是勝率。"
                    )

with tab2:
    tab2_1, tab2_2, tab2_3 = st.tabs(
        ["當沖損益室", "波段信用室", "期權交易室"],
        key="profit_room_active_tab", on_change="rerun",
    )
    
    with tab2_1:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            calc_price = st.number_input("基準價格", min_value=0.01, value=max(0.01, float(st.session_state.calc_base_price)), step=0.01, format="%.12g", key="input_base_price")
            if calc_price != st.session_state.calc_base_price:
                st.session_state.calc_base_price = calc_price
                st.session_state.calc_view_price = apply_tick_rules(calc_price)
        with c2: shares = st.number_input("股數", min_value=1, value=1000, step=1000)
        with c3: discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0)
        with c4: min_fee = st.number_input("最低手續費 (元)", min_value=0, value=20, step=1)
        with c5: tick_count = st.number_input("顯示檔數 (檔)", value=10, min_value=1, max_value=50, step=1)
        direction = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)

        stop_col1, stop_col2, _ = st.columns([1, 1, 3])
        with stop_col1:
            day_stop_loss_percent = st.number_input(
                "停損幅度 (%)", value=5.0, min_value=0.0, max_value=100.0,
                step=0.5, format="%.12g", key="day_stop_loss_percent"
            )
        with stop_col2:
            day_is_long = "多" in direction
            day_stop_price = calculate_stop_loss_price(calc_price, day_stop_loss_percent, day_is_long)
            stop_direction = "下跌" if day_is_long else "上漲"
            st.metric("停損價", fmt_price(day_stop_price), f"{stop_direction} {day_stop_loss_percent:g}%")
        
        # --- 新增：目標價快速試算區 ---
        st.markdown("##### 🎯 目標價快速試算")
        col_t1, col_t2 = st.columns([1, 4])
        with col_t1:
            # 將 value 設為 None 並加入 placeholder，實現預設為空值
            target_p = st.number_input("輸入目標價", min_value=0.01, value=None, step=0.5, format="%.12g", key="input_target_price", placeholder="請輸入...")
        with col_t2:
            fee_rate = 0.001425; tax_rate = 0.0015
            base_p = st.session_state.calc_base_price
            is_long = "多" in direction
            
            # 加入防呆判斷：只有當使用者輸入數字時才進行計算
            if target_p is not None:
                if is_long:
                    t_buy = base_p; t_sell = target_p
                    t_buy_fee = max(min_fee, math.floor(t_buy * shares * fee_rate * (discount/10)))
                    t_sell_fee = max(min_fee, math.floor(t_sell * shares * fee_rate * (discount/10)))
                    t_tax = math.floor(t_sell * shares * tax_rate)
                    t_cost = (t_buy * shares) + t_buy_fee
                    t_income = (t_sell * shares) - t_sell_fee - t_tax
                    t_profit = t_income - t_cost
                    t_total_fee = t_buy_fee + t_sell_fee
                else:
                    t_sell = base_p; t_buy = target_p
                    t_sell_fee = max(min_fee, math.floor(t_sell * shares * fee_rate * (discount/10)))
                    t_buy_fee = max(min_fee, math.floor(t_buy * shares * fee_rate * (discount/10)))
                    t_tax = math.floor(t_sell * shares * tax_rate)
                    t_income = (t_sell * shares) - t_sell_fee - t_tax
                    t_cost = (t_buy * shares) + t_buy_fee
                    t_profit = t_income - t_cost
                    t_total_fee = t_buy_fee + t_sell_fee
                
                t_roi = (t_profit / (base_p * shares) * 100) if (base_p * shares) != 0 else 0
                
                t_color = "#ff4b4b" if t_profit > 0 else ("#00e676" if t_profit < 0 else "white")
                diff_val = target_p - base_p
                diff_color = "#ff4b4b" if diff_val > 0 else ("#00e676" if diff_val < 0 else "white")
                
                html_str = f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: {t_color}; font-weight: bold;">{int(t_profit):,} ({_format_compact_number(t_roi, 2, signed=True)}%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">{int(t_total_fee):,}</span></div>
                    <div>交易稅: <span style="color: #cccccc;">{int(t_tax):,}</span></div>
                    <div>價差: <span style="color: {diff_color}; font-weight: bold;">{_format_compact_number(diff_val, 2, signed=True)}</span></div>
                </div>
                """
            else:
                # 空值時的預設顯示狀態
                html_str = f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: white; font-weight: bold;">0 (+0.00%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">0</span></div>
                    <div>交易稅: <span style="color: #cccccc;">0</span></div>
                    <div>價差: <span style="color: white; font-weight: bold;">+0.00</span></div>
                </div>
                """
                
            st.markdown(html_str, unsafe_allow_html=True)
        st.markdown("---")
        
        limit_up, limit_down = calculate_limits(st.session_state.calc_base_price)
        b1, b2, _ = st.columns([1, 1, 6])
        with b1:
            if st.button("🔽 向下", width='stretch'):
                if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
                st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, -tick_count)
                st.rerun()
        with b2:
            if st.button("🔼 向上", width='stretch'):
                if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
                st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, tick_count)
                st.rerun()
        
        ticks_range = range(tick_count, -(tick_count + 1), -1)
        calc_data = []
        base_p = st.session_state.calc_base_price
        if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = base_p
        view_p = st.session_state.calc_view_price
        is_long = "多" in direction
        fee_rate = 0.001425; tax_rate = 0.0015 
        
        for i in ticks_range:
            p = move_tick(view_p, i)
            
            if is_long:
                buy_price = base_p; sell_price = p
                buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
                sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
                tax = math.floor(sell_price * shares * tax_rate)
                cost = (buy_price * shares) + buy_fee
                income = (sell_price * shares) - sell_fee - tax
                profit = income - cost
                total_fee = buy_fee + sell_fee
            else: 
                sell_price = base_p; buy_price = p
                sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
                buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
                tax = math.floor(sell_price * shares * tax_rate)
                income = (sell_price * shares) - sell_fee - tax
                cost = (buy_price * shares) + buy_fee
                profit = income - cost
                total_fee = buy_fee + sell_fee
            roi = 0
            if (base_p * shares) != 0: roi = (profit / (base_p * shares)) * 100
            diff = p - base_p
            diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
            if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
            
            note_type = ""
            if abs(p - limit_up) < 0.001: note_type = "up"
            elif abs(p - limit_down) < 0.001: note_type = "down"
            is_base = (abs(p - base_p) < 0.001)
            
            calc_data.append({
                "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{_format_compact_number(roi, 2, signed=True)}%",
                "手續費": int(total_fee), "交易稅": int(tax), "_profit": profit, "_note_type": note_type, "_is_base": is_base
            })
            
        df_calc = pd.DataFrame(calc_data)
        
        def style_calc_row(row):
            is_base = row['_is_base']
            prof = row['_profit']
            
            if is_base: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
            if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
            if prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
            return ['color: gray'] * len(row)

        if not df_calc.empty:
            table_height = (len(df_calc) + 1) * 35
            st.dataframe(
                df_calc.style.apply(style_calc_row, axis=1), 
                width=425, 
                hide_index=True, 
                height=table_height,
                column_config={"_profit": None, "_note_type": None, "_is_base": None}
            )

    with tab2_2:
        c2_1, c2_2, c2_3, c2_4, c2_5 = st.columns(5)
        with c2_1:
            swing_calc_price = st.number_input("基準價格", min_value=0.01, value=None, step=0.5, format="%.12g", key="input_swing_base_price", placeholder="請輸入...")
        with c2_2: swing_shares = st.number_input("股數", min_value=1, value=1000, step=1000, key="swing_shares")
        with c2_3: swing_discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0, key="swing_discount")
        with c2_4: swing_min_fee = st.number_input("最低手續費 (元)", min_value=0, value=20, step=1, key="swing_min_fee")
        with c2_5: swing_tick_count = st.number_input("顯示檔數 (檔)", value=10, min_value=1, max_value=50, step=1, key="swing_tick_count")
        
        c2_type, c2_margin, c2_rate, c2_days, c2_fee_rate = st.columns(5)
        with c2_type:
            swing_type = st.selectbox("交易選項", ["個股", "融資(多)", "融券(空)"], key="swing_type")
        with c2_margin:
            margin_ratio = st.number_input("融資/券成數(%)", min_value=0.0, max_value=100.0, value=60.0 if swing_type == "融資(多)" else (90.0 if swing_type == "融券(空)" else 0.0), step=10.0, key="margin_ratio")
        with c2_rate:
            annual_rate = st.number_input("年利率(%)", min_value=0.0, value=6.25 if swing_type == "融資(多)" else (0.2 if swing_type == "融券(空)" else 0.0), step=0.1, key="annual_rate")
        with c2_days:
            swing_date_range = st.date_input("選擇區間", value=(datetime.now(tz_tw).date(), datetime.now(tz_tw).date() + timedelta(days=1)), key="swing_date_range")
            if isinstance(swing_date_range, tuple) and len(swing_date_range) == 2:
                swing_days = (swing_date_range[1] - swing_date_range[0]).days
                if swing_days < 1: swing_days = 1
            else:
                swing_days = 1
            st.caption(f"總天數: {swing_days} 天")
        with c2_fee_rate:
            short_fee_rate = st.number_input("借券費率(‱)", min_value=0.0, value=8.0 if swing_type == "融券(空)" else 0.0, step=1.0, key="short_fee_rate")

        swing_stop_col1, swing_stop_col2, _ = st.columns([1, 1, 3])
        with swing_stop_col1:
            swing_stop_loss_percent = st.number_input(
                "停損幅度 (%)", value=5.0, min_value=0.0, max_value=100.0,
                step=0.5, format="%.12g", key="swing_stop_loss_percent"
            )
        with swing_stop_col2:
            swing_is_long = swing_type != "融券(空)"
            swing_stop_price = (
                calculate_stop_loss_price(swing_calc_price, swing_stop_loss_percent, swing_is_long)
                if swing_calc_price is not None else 0.0
            )
            swing_stop_direction = "下跌" if swing_is_long else "上漲"
            st.metric(
                "停損價", fmt_price(swing_stop_price) if swing_calc_price is not None else "—",
                f"{swing_stop_direction} {swing_stop_loss_percent:g}%"
            )

        # --- 目標價快速試算區 ---
        st.markdown("##### 🎯 目標價快速試算")
        col_st1, col_st2 = st.columns([1, 4])
        with col_st1:
            swing_target_p = st.number_input("輸入目標價", min_value=0.01, value=None, step=0.5, format="%.12g", key="swing_target_price", placeholder="請輸入...")
        with col_st2:
            if swing_calc_price is not None and swing_target_p is not None:
                s_base_p = swing_calc_price
                s_fee_rate = 0.001425; s_tax_rate = 0.003
                
                t_buy_price = s_base_p if swing_type in ["個股", "融資(多)"] else swing_target_p
                t_sell_price = swing_target_p if swing_type in ["個股", "融資(多)"] else s_base_p
                
                t_buy_fee = max(swing_min_fee, math.floor(t_buy_price * swing_shares * s_fee_rate * (swing_discount/10)))
                t_sell_fee = max(swing_min_fee, math.floor(t_sell_price * swing_shares * s_fee_rate * (swing_discount/10)))
                t_tax = math.floor(t_sell_price * swing_shares * s_tax_rate)
                t_total_fee = t_buy_fee + t_sell_fee
                
                t_stock_value_buy = t_buy_price * swing_shares
                t_stock_value_sell = t_sell_price * swing_shares
                
                t_profit = 0
                t_interest = 0
                t_borrow_fee = 0
                t_roi = 0
                
                if swing_type == "個股":
                    t_cost = t_stock_value_buy + t_buy_fee
                    t_income = t_stock_value_sell - t_sell_fee - t_tax
                    t_profit = t_income - t_cost
                    t_roi = (t_profit / t_cost * 100) if t_cost > 0 else 0
                elif swing_type == "融資(多)":
                    t_margin_loan = math.floor(t_stock_value_buy * (margin_ratio/100) / 1000) * 1000
                    t_self_prepare = t_stock_value_buy - t_margin_loan + t_buy_fee
                    t_interest = round(t_margin_loan * (annual_rate/100) * (swing_days/365))
                    t_net_sell = t_stock_value_sell - t_sell_fee - t_tax - t_margin_loan - t_interest
                    t_profit = t_net_sell - t_self_prepare
                    t_roi = (t_profit / t_self_prepare * 100) if t_self_prepare > 0 else 0
                elif swing_type == "融券(空)":
                    t_margin_deposit = math.ceil(t_stock_value_sell * (margin_ratio/100) / 100) * 100
                    t_borrow_fee = math.floor(t_stock_value_sell * (short_fee_rate/10000))
                    t_sell_guaranty = t_stock_value_sell - t_sell_fee - t_tax - t_borrow_fee
                    t_interest = round((t_margin_deposit + t_sell_guaranty) * (annual_rate/100) * (swing_days/365))
                    t_buy_cost = t_stock_value_buy + t_buy_fee
                    t_refund = t_margin_deposit + t_sell_guaranty + t_interest - t_buy_cost
                    t_profit = t_refund - t_margin_deposit
                    t_roi = (t_profit / t_margin_deposit * 100) if t_margin_deposit > 0 else 0

                t_color = "#ff4b4b" if t_profit > 0 else ("#00e676" if t_profit < 0 else "white")
                t_diff_val = swing_target_p - s_base_p
                t_diff_color = "#ff4b4b" if t_diff_val > 0 else ("#00e676" if t_diff_val < 0 else "white")
                
                swing_html_str = (
                    f"<div style='font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;'>"
                    f"<div>預估損益: <span style='color: {t_color}; font-weight: bold;'>{int(t_profit):,} ({_format_compact_number(t_roi, 2, signed=True)}%)</span></div>"
                    f"<div>手續費總和: <span style='color: #cccccc;'>{int(t_total_fee):,}</span></div>"
                    f"<div>交易稅: <span style='color: #cccccc;'>{int(t_tax):,}</span></div>"
                )
                if swing_type in ["融資(多)", "融券(空)"]:
                    swing_html_str += f"<div>利息: <span style='color: #cccccc;'>{int(t_interest):,}</span></div>"
                    if swing_type == "融券(空)":
                        swing_html_str += f"<div>借券費: <span style='color: #cccccc;'>{int(t_borrow_fee):,}</span></div>"
                swing_html_str += f"<div>價差: <span style='color: {t_diff_color}; font-weight: bold;'>{_format_compact_number(t_diff_val, 2, signed=True)}</span></div></div>"
                st.markdown(swing_html_str, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: white; font-weight: bold;">0 (+0.00%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">0</span></div>
                    <div>交易稅: <span style="color: #cccccc;">0</span></div>
                    <div>價差: <span style="color: white; font-weight: bold;">+0.00</span></div>
                </div>
                """, unsafe_allow_html=True)
        st.markdown("---")

        if swing_calc_price is not None:
            if swing_calc_price != st.session_state.get('swing_base_price', 0):
                st.session_state.swing_base_price = swing_calc_price
                st.session_state.swing_view_price = apply_tick_rules(swing_calc_price)

            swing_limit_up, swing_limit_down = calculate_limits(st.session_state.swing_base_price)
            sb1, sb2, _ = st.columns([1, 1, 6])
            with sb1:
                if st.button("🔽 向下", key="swing_btn_down", width='stretch'):
                    if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = st.session_state.swing_base_price
                    st.session_state.swing_view_price = move_tick(st.session_state.swing_view_price, -swing_tick_count)
                    st.rerun()
            with sb2:
                if st.button("🔼 向上", key="swing_btn_up", width='stretch'):
                    if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = st.session_state.swing_base_price
                    st.session_state.swing_view_price = move_tick(st.session_state.swing_view_price, swing_tick_count)
                    st.rerun()

            swing_ticks_range = range(swing_tick_count, -(swing_tick_count + 1), -1)
            swing_calc_data = []
            s_base_p = st.session_state.swing_base_price
            if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = s_base_p
            s_view_p = st.session_state.swing_view_price
            s_fee_rate = 0.001425; s_tax_rate = 0.003
            
            for i in swing_ticks_range:
                p = move_tick(s_view_p, i)
                
                buy_price = s_base_p if swing_type in ["個股", "融資(多)"] else p
                sell_price = p if swing_type in ["個股", "融資(多)"] else s_base_p
                
                buy_fee = max(swing_min_fee, math.floor(buy_price * swing_shares * s_fee_rate * (swing_discount/10)))
                sell_fee = max(swing_min_fee, math.floor(sell_price * swing_shares * s_fee_rate * (swing_discount/10)))
                tax = math.floor(sell_price * swing_shares * s_tax_rate)
                total_fee = buy_fee + sell_fee
                
                stock_value_buy = buy_price * swing_shares
                stock_value_sell = sell_price * swing_shares
                
                profit = 0
                interest = 0
                borrow_fee = 0
                maintenance_ratio = 0.0
                call_price = 0.0
                
                if swing_type == "個股":
                    cost = stock_value_buy + buy_fee
                    income = stock_value_sell - sell_fee - tax
                    profit = income - cost
                    roi = (profit / cost * 100) if cost > 0 else 0
                elif swing_type == "融資(多)":
                    margin_loan = math.floor(stock_value_buy * (margin_ratio/100) / 1000) * 1000
                    self_prepare = stock_value_buy - margin_loan + buy_fee
                    interest = round(margin_loan * (annual_rate/100) * (swing_days/365))
                    net_sell = stock_value_sell - sell_fee - tax - margin_loan - interest
                    profit = net_sell - self_prepare
                    maintenance_ratio = (stock_value_sell / margin_loan) * 100 if margin_loan > 0 else 0
                    call_price = (margin_loan * 1.3) / swing_shares if margin_loan > 0 else 0
                    roi = (profit / self_prepare * 100) if self_prepare > 0 else 0
                elif swing_type == "融券(空)":
                    margin_deposit = math.ceil(stock_value_sell * (margin_ratio/100) / 100) * 100
                    borrow_fee = math.floor(stock_value_sell * (short_fee_rate/10000))
                    sell_guaranty = stock_value_sell - sell_fee - tax - borrow_fee
                    interest = round((margin_deposit + sell_guaranty) * (annual_rate/100) * (swing_days/365))
                    buy_cost = stock_value_buy + buy_fee
                    refund = margin_deposit + sell_guaranty + interest - buy_cost
                    profit = refund - margin_deposit
                    maintenance_ratio = ((margin_deposit + sell_guaranty) / stock_value_buy) * 100 if stock_value_buy > 0 else 0
                    call_price = (margin_deposit + sell_guaranty) / (1.3 * swing_shares) if swing_shares > 0 else 0
                    roi = (profit / margin_deposit * 100) if margin_deposit > 0 else 0

                diff = p - s_base_p
                diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
                if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
                
                is_base = (abs(p - s_base_p) < 0.001)
                
                row_data = {
                    "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{_format_compact_number(roi, 2, signed=True)}%",
                    "手續費": int(total_fee), "交易稅": int(tax)
                }
                if swing_type in ["融資(多)", "融券(空)"]:
                    if swing_type == "融券(空)":
                        row_data["借券費"] = int(borrow_fee)
                    row_data["利息"] = int(interest)
                    row_data["維持率%"] = f"{_format_compact_number(maintenance_ratio, 1)}%" if maintenance_ratio > 0 else "-"
                    row_data["強制回補價"] = _format_compact_number(call_price, 2) if call_price > 0 else "-"
                    
                row_data["_profit"] = profit
                row_data["_is_base"] = is_base
                if swing_type in ["融資(多)", "融券(空)"]:
                    row_data["_call"] = True if maintenance_ratio > 0 and maintenance_ratio < 130 else False
                else:
                    row_data["_call"] = False

                swing_calc_data.append(row_data)

            df_swing_calc = pd.DataFrame(swing_calc_data)
            
            def style_swing_row(row):
                is_base = row['_is_base']
                prof = row['_profit']
                is_call = row['_call']
                
                if is_base: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
                if is_call: return ['background-color: #ffcccc; color: #ff0000; font-weight: bold'] * len(row)
                if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
                if prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
                return ['color: gray'] * len(row)

            if not df_swing_calc.empty:
                st.dataframe(
                    df_swing_calc.style.apply(style_swing_row, axis=1), 
                    width='stretch',
                    hide_index=True, 
                    height=(len(df_swing_calc) + 1) * 35,
                    column_config={
                        "成交價": st.column_config.TextColumn(width="small"),
                        "漲跌": st.column_config.TextColumn(width="small"),
                        "預估損益": st.column_config.NumberColumn(width="small"),
                        "報酬率%": st.column_config.TextColumn(width="small"),
                        "手續費": st.column_config.NumberColumn(width="small"),
                        "交易稅": st.column_config.NumberColumn(width="small"),
                        "借券費": st.column_config.NumberColumn(width="small"),
                        "利息": st.column_config.NumberColumn(width="small"),
                        "維持率%": st.column_config.TextColumn(width="small"),
                        "強制回補價": st.column_config.TextColumn(width="small"),
                        "_profit": None, "_is_base": None, "_call": None
                    }
                )

    with tab2_3:
        options_profit_active = bool(tab2.open and tab2_3.open)
        st.markdown("""
        <style>
        .opt-card {
            background-color: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 10px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .opt-label {
            font-size: 13px;
            color: #aaa;
            margin-bottom: 5px;
        }
        .opt-value {
            font-size: 18px;
            font-weight: bold;
        }
        /* 做多與做空選擇文字的動態變色 */
        div[role="radiogroup"] label:has(input[value="🔴 做多 ▲"]:checked) p {
            color: #ff4b4b !important;
            font-weight: bold !important;
        }
        div[role="radiogroup"] label:has(input[value="🟢 做空 ▼"]:checked) p {
            color: #00e676 !important;
            font-weight: bold !important;
        }
        .futures-expiry-reminder {
            display: flex;
            align-items: baseline;
            flex-wrap: wrap;
            gap: 10px;
            padding: 8px 12px;
            margin: 2px 0 12px;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 8px;
        }
        .futures-expiry-contract { font-size: 16px; font-weight: 700; color: #f5f5f5; }
        .futures-expiry-date { font-size: 18px; font-weight: 600; color: #e6e6e6; }
        .futures-expiry-countdown { font-size: 16px; color: #ffcc80; font-weight: 600; }
        .futures-expiry-days { font-size: 22px; font-weight: 800; color: #ff9800; line-height: 1; }
        </style>
        """, unsafe_allow_html=True)

        near_settlement_date = get_near_month_futures_settlement()
        days_to_settlement = (near_settlement_date - datetime.now(pytz.timezone('Asia/Taipei')).date()).days
        st.markdown(
            f"<div class='futures-expiry-reminder'>"
            f"<span class='futures-expiry-contract'>台指期近月結算</span>"
            f"<span class='futures-expiry-date'>{near_settlement_date:%m/%d} 結算</span>"
            f"<span class='futures-expiry-countdown'>⏱ 倒數 <span class='futures-expiry-days'>{days_to_settlement}</span> 日</span>"
            f"</div>",
            unsafe_allow_html=True
        )

        # ---------------- 回呼函數定義 ----------------
        def sync_taifex_margin():
            try:
                url = 'https://openapi.taifex.com.tw/v1/IndexFuturesAndOptionsMargining'
                headers = {
                    'accept': 'application/json',
                    'If-Modified-Since': 'Mon, 26 Jul 1997 05:00:00 GMT',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
                r = requests.get(url, headers=headers, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    res = {}
                    sync_date = ""
                    for item in data:
                        sym_api = item.get("Contract", "").strip()
                        margin_api = item.get("InitialMargin", 0)
                        if isinstance(margin_api, str): margin_api = float(margin_api.replace(',', ''))
                        res[sym_api] = margin_api
                        # 正規化: 無論 API 回傳中文名稱或英文代碼，統一存為英文代碼
                        sym_clean = sym_api.replace(' ', '')
                        if sym_clean in ["MXF", "TMF"] or "微型臺" in sym_clean or "微型台" in sym_clean:
                            res["MXF"] = margin_api
                            res["TMF"] = margin_api
                        elif sym_clean == "MTX" or "小型臺指" in sym_clean or "小型台指" in sym_clean:
                            res["MTX"] = margin_api
                        elif sym_clean == "TX" or sym_clean in ["臺股期貨", "台股期貨", "臺指期貨", "台指期貨"]:
                            res["TX"] = margin_api
                        if not sync_date and "Date" in item:
                            d_str = str(item["Date"])
                            if len(d_str) == 8:
                                sync_date = f"{d_str[:4]}/{d_str[4:6]}/{d_str[6:]}"
                    
                    st.session_state.taifex_margin_data = res
                    if sync_date:
                        st.session_state.taifex_sync_date = sync_date
                    
                    # 立即更新當前選擇的合約保證金數值
                    opt_tx_type = st.session_state.get('opt_tx_type', '大台 (TX)')
                    # 修正期交所 API 的合約代號 (大台 TX, 小台 MTX, 微台 TMF)
                    sym = "TX" if "大台" in opt_tx_type else ("MTX" if "小台" in opt_tx_type else "MXF")
                    if sym in res:
                        st.session_state["margin_display_tx"] = f"{res[sym]:,.0f}"
                    
                    st.toast("已同步期交所最新保證金", icon="✅")
            except Exception as e:
                st.toast(f"取得期交所保證金失敗: {e}", icon="⚠️")

        @st.cache_data(ttl=300, show_spinner=False)
        def fetch_ssf_margin_info():
            """透過期交所 OpenAPI 獲取個股期貨保證金比例與小型合約資訊"""
            margin_map = {}
            has_small_set = set()
            sync_date = ""
            group_level_map = {}   # 新增
            maint_map = {}         # 新增
            try:
                url_pct = "https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining"
                r_pct = requests.get(url_pct, headers={'accept': 'application/json', 'If-Modified-Since': 'Mon, 26 Jul 1997 05:00:00 GMT'}, timeout=5)
                if r_pct.status_code == 200:
                    data = r_pct.json()
                    stock_contracts = {}
                    for item in data:
                        code = str(item.get("UnderlyingSecurityCode", "")).strip()
                        contract = str(item.get("Contract", "")).strip()
                        m_val = str(item.get("InitialMarginRate", "0")).replace('%', '').strip()
                        
                        if code:
                            if code not in stock_contracts:
                                stock_contracts[code] = set()
                            if contract:
                                stock_contracts[code].add(contract)
                            
                            try: margin_map[code] = float(m_val)
                            except: pass
                            maint_val = str(item.get("MaintenanceMarginRate", "0")).replace('%', '').strip()
                            try: maint_map[code] = float(maint_val)
                            except: pass
                            g_val = str(item.get("GroupLevel", "")).strip()
                            if g_val: group_level_map[code] = g_val
                        # 擷取更新日期
                        if not sync_date and "Date" in item:
                            d_str = str(item["Date"])
                            if len(d_str) == 8:
                                sync_date = f"{d_str[:4]}/{d_str[4:6]}/{d_str[6:]}"

                    # 判斷是否含有小型股期 (若同一股號有多個合約代號，即代表有小型股期)
                    for code, contracts in stock_contracts.items():
                        if len(contracts) > 1:
                            has_small_set.add(code)
            except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
                logger.warning("個股期貨保證金暫時無法取得：%s", type(exc).__name__)
            return margin_map, maint_map, group_level_map, has_small_set, sync_date

        def do_clear_opt():
            for k in ['opt_entry_p', 'opt_exit_p', 'opt_sl_p', 'opt_manual_margin_tx', 'opt_manual_margin_opt', 'opt_rt_price', 'opt_ref_price']:
                if k in st.session_state:
                    st.session_state[k] = None
            st.session_state['opt_sf_search'] = None
            if 'opt_custom_margin' in st.session_state:
                del st.session_state['opt_custom_margin']
            st.session_state['opt_lots'] = 1
            st.session_state['opt_dir'] = "🔴 做多 ▲"
            st.session_state['opt_margin_level'] = "級距一 | 13.5% (一般股票)"

        def on_opt_tab_change():
            do_clear_opt()
            # 新增此行：確保切換商品類別(例如切換至台指期)時，自動觸發抓取預設合約的最新價格
            st.session_state.opt_rt_trigger = True
        
        def on_f_contract_change():
            st.session_state.opt_rt_trigger = True
        # --------------------------------------------

        if 'taifex_margin_data' not in st.session_state:
            st.session_state.taifex_margin_data = {}
            if options_profit_active:
                sync_taifex_margin()
        
        # 確保期貨清單有被載入 (穩定來源)
        if options_profit_active and (
            'futures_list' not in st.session_state or not st.session_state.futures_list
        ):
            st.session_state.futures_list = fetch_futures_list()

        # 取得 API 保證金與小型股期資料
        if options_profit_active:
            ssf_margin_map, ssf_maint_map, ssf_group_level_map, has_small_set, ssf_sync_date = fetch_ssf_margin_info()
        else:
            ssf_margin_map, ssf_maint_map = {}, {}
            ssf_group_level_map, has_small_set, ssf_sync_date = {}, set(), ''

        c_map_opt, _ = load_local_stock_names()
        sf_opts = []
        # 改回使用穩定的 futures_list 產生選單，避免 API 異常時選單空白
        for code, status in st.session_state.futures_list.items():
            name = c_map_opt.get(code, code)
            sf_opts.append(f"{code} {name}期貨 (一般 x2000)")
            # 結合網頁抓取的 "(小)" 標記與 API 回傳的 has_small_set 雙重確認
            if "(小)" in status or code in has_small_set:
                sf_opts.append(f"{code} 小型{name}期貨 (小型 x100)")
        sf_opts = sorted(sf_opts)

        col_left, col_right = st.columns([1.1, 1], gap="large")

        with col_left:
            st.markdown("###### ① 合約設定")
            opt_main_tab = st.selectbox(
                "合約類別", 
                ["台指期", "個股期貨", "選擇權"], 
                key="opt_main_tab",
                on_change=on_opt_tab_change
            )

            if opt_main_tab == "台指期":
                opt_tx_type = st.radio("合約規格", ["大台 (TX)", "小台 (MTX)", "微台 (TMF)"], horizontal=True, key="opt_tx_type", on_change=on_f_contract_change)
                mult = 200 if "大台" in opt_tx_type else (50 if "小台" in opt_tx_type else 10)
                tax_rate = 0.00002
            elif opt_main_tab == "個股期貨":
                # 1. 搜尋股期輸入股號只要出現對應的股期就好
                opt_sf_input = st.text_input("搜尋股期 (輸入代號或名稱)", placeholder="例如: 2330", key="opt_sf_input")
                filtered_sf_opts = [opt for opt in sf_opts if opt_sf_input in opt] if opt_sf_input else sf_opts
                
                search_stock_futures = st.selectbox(
                    "選擇對應股期", 
                    options=filtered_sf_opts, 
                    index=0 if filtered_sf_opts else None,
                    key="opt_sf_search",
                    on_change=on_f_contract_change
                )
                is_small = search_stock_futures is not None and "小型" in search_stock_futures
                opt_sub_type = st.radio("合約規格", ["一般 (x2000)", "小型 (x100)"], horizontal=True, index=1 if is_small else 0, on_change=on_f_contract_change)
                mult = 100 if "小型" in opt_sub_type else 2000
                tax_rate = 0.00002
            else:
                st.markdown("<div style='margin-bottom: 10px; font-size: 14px;'>合約規格：選擇權 (x50)</div>", unsafe_allow_html=True)
                mult = 50
                tax_rate = 0.001

            opt_dir = st.radio("部位方向", ["🔴 做多 ▲", "🟢 做空 ▼"], horizontal=True, key="opt_dir")
            opt_lots = st.number_input("口數", min_value=1, value=1, step=1, key="opt_lots")

           # 新增：當最新成交價為空（例如剛切換到期權交易室），自動觸發獲取預設的台指期大台價格
            if st.session_state.get('opt_rt_price') is None:
                st.session_state.opt_rt_trigger = True

            # --- 獲取最新即時價格邏輯 (支援夜盤) ---
            if st.session_state.get('opt_rt_trigger', False):
                st.session_state.opt_rt_trigger = False
                rt_p, ref_p = None, None
                try:
                    sj_logged = st.session_state.get('sj_logged_in', False)
                    sj_api = st.session_state.get('sj_api', None)
                    
                    if opt_main_tab in ["台指期", "個股期貨"] and sj_logged and sj_api:
                        contract = None
                        
                        tz_now = datetime.now(pytz.timezone('Asia/Taipei'))
                        today_str = tz_now.strftime("%Y%m%d")
                        now_time = tz_now.time()
                        
                        def is_valid_contract(c):
                            code_str = getattr(c, 'code', '')
                            if code_str.endswith('R1') or code_str.endswith('R2') or '/' in code_str:
                                return False
                                
                            # 優先取得 delivery_date，若無則取 delivery_month，並清除斜線與破折號以利比對
                            d_date = str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999')))
                            d_date = d_date.replace('/', '').replace('-', '')
                            
                            # 若為 8 碼精確日期 (YYYYMMDD)
                            if len(d_date) == 8:
                                if d_date < today_str:
                                    return False
                                if d_date == today_str and now_time >= dt_time(13, 30):
                                    return False
                            # 若僅有 6 碼年月 (YYYYMM)
                            elif len(d_date) == 6:
                                today_ym = today_str[:6]
                                if d_date < today_ym:
                                    return False
                                if d_date == today_ym:
                                    settle_date = futures_expiry_date(d_date)
                                    if settle_date is not None:
                                        if tz_now.date() > settle_date:
                                            return False
                                        if tz_now.date() == settle_date and now_time >= dt_time(13, 30):
                                            return False
                            return True

                        if opt_main_tab == "台指期":
                            try:
                                if "大台" in opt_tx_type:
                                    contracts = sj_api.Contracts.Futures.TXF
                                elif "小台" in opt_tx_type:
                                    contracts = sj_api.Contracts.Futures.MXF
                                else:
                                    contracts = sj_api.Contracts.Futures.TMF
                                    
                                valid_list = [c for c in contracts if is_valid_contract(c)]
                                if valid_list:
                                    contract = min(valid_list, key=lambda c: str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999'))).replace('/', '').replace('-', ''))
                            except: pass
                        elif opt_main_tab == "個股期貨":
                            code = search_stock_futures.split(" ")[0] if search_stock_futures else ""
                            if code:
                                try:
                                    is_small = False
                                    if 'opt_sub_type' in locals() and isinstance(opt_sub_type, str) and "小型" in opt_sub_type:
                                        is_small = True
                                    candidates = []
                                    for category in sj_api.Contracts.Futures:
                                        for c in category:
                                            if str(getattr(c, 'underlying_code', '')) == str(code):
                                                c_name = getattr(c, 'name', '')
                                                if is_small and "小型" in c_name: candidates.append(c)
                                                elif not is_small and "小型" not in c_name: candidates.append(c)
                                    if not candidates:
                                        for category in sj_api.Contracts.Futures:
                                            for c in category:
                                                if str(getattr(c, 'underlying_code', '')) == str(code): candidates.append(c)
                                    if candidates:
                                        valid_contracts = [c for c in candidates if is_valid_contract(c)]
                                        if valid_contracts:
                                            contract = min(valid_contracts, key=lambda c: str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999'))).replace('/', '').replace('-', ''))
                                except Exception: pass

                        if contract:
                            try:
                                snap = get_stream_quotes(sj_api, [contract])
                                if snap and len(snap) > 0:
                                    s = snap[0]
                                    rt_p = s.close if s.close > 0 else s.open
                                    
                                    # 修正：精準讀取永豐快照的 change_price 反推基準價
                                    change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                                    if change_val is not None and rt_p > 0:
                                        ref_p = rt_p - float(change_val)
                                    else:
                                        # 堅固備援：直接向 API 請求 K 棒，抓取當日 13:45 最後一筆收盤價
                                        tz_tw_loc = pytz.timezone('Asia/Taipei')
                                        now_loc = datetime.now(tz_tw_loc)
                                        start_str = (now_loc - timedelta(days=5)).strftime("%Y-%m-%d")
                                        end_str = now_loc.strftime("%Y-%m-%d")
                                        
                                        try:
                                            kbars = sj_api.kbars(contract, start=start_str, end=end_str)
                                            if kbars and hasattr(kbars, 'ts') and len(kbars.ts) > 0:
                                                df_k = pd.DataFrame({**kbars})
                                                df_k['ts'] = pd.to_datetime(df_k['ts'])
                                                if df_k['ts'].dt.tz is not None:
                                                    df_k['ts'] = df_k['ts'].dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
                                                
                                                day_mask = (df_k['ts'].dt.time >= dt_time(8, 45)) & (df_k['ts'].dt.time <= dt_time(13, 45))
                                                df_day = df_k[day_mask]
                                                if not df_day.empty:
                                                    daily_closes = df_day.groupby(df_day['ts'].dt.date)['Close'].last()
                                                    curr_d = now_loc.date()
                                                    
                                                    # 14:00 前 (含凌晨夜盤與上午日盤)：基準價為「前一交易日」的 13:45
                                                    if now_loc.time() < dt_time(14, 0):
                                                        past_closes = daily_closes[daily_closes.index < curr_d]
                                                        if not past_closes.empty: ref_p = float(past_closes.iloc[-1])
                                                    # 14:00 後 (下午夜盤開始)：基準價為「今日」的 13:45
                                                    else:
                                                        valid_closes = daily_closes[daily_closes.index <= curr_d]
                                                        if not valid_closes.empty: ref_p = float(valid_closes.iloc[-1])
                                        except: pass
                                        
                                        # 若真的都抓不到，最後才回退至快取的參考價
                                        if ref_p is None or ref_p == 0:
                                            ref_p = getattr(contract, 'reference', 0)
                                            
                                    # 防呆：若夜盤尚未有成交量，讓成交價顯示與基準價相同
                                    if rt_p == 0 and ref_p is not None and ref_p > 0:
                                        rt_p = ref_p
                            except Exception: pass
                except Exception: pass
                
                st.session_state.opt_rt_price = rt_p
                st.session_state.opt_ref_price = ref_p
            # ------------------------------------

            c_p1, c_p2 = st.columns(2)
            with c_p1:
                entry_p = st.number_input("進場價 (點)", min_value=0.0, value=None, format="%.12g", placeholder="輸入進場價", key="opt_entry_p")
                
                # --- 顯示最新成交價及重新整理按鈕 ---
                if opt_main_tab in ["台指期", "個股期貨"]:
                    # 修正：注入 CSS 實現 align-items: center 讓按鈕與文字垂直置中平行，並移除 margin 造成的落差
                    st.markdown("""
                    <style>
                    div[data-testid="stHorizontalBlock"]:has(button[title="更新最新價格"]) {
                        align-items: center !important;
                    }
                    div:has(> button[title="更新最新價格"]) button {
                        min-height: 32px !important;
                        font-size: 14px !important;
                        padding: 0px 5px !important;
                        margin: 0px !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)

                    c_rt1, c_rt2 = st.columns([5, 1])
                    with c_rt1:
                        rt_p = st.session_state.get('opt_rt_price', None)
                        ref_p = st.session_state.get('opt_ref_price', None)
                        if rt_p is not None and ref_p is not None:
                            color = "#ff4b4b" if rt_p > ref_p else ("#00e676" if rt_p < ref_p else "white")
                            diff = rt_p - ref_p
                            sign = "+" if diff > 0 else ""
                            # 新增：計算漲跌幅百分比
                            pct_chg = (diff / ref_p * 100) if ref_p else 0.0
                            st.markdown(f"<div style='font-size:20px; margin:0px;'>最新成交價: <span style='color:{color}; font-weight:bold;'>{rt_p:g}</span> <span style='font-size:16px; color:{color};'>({_format_compact_number(diff, 2, signed=True)})({_format_compact_number(pct_chg, 2, signed=True)}%)</span></div>", unsafe_allow_html=True)
                        else:
                            st.markdown("<div style='font-size:20px; margin:0px; color:#aaa;'>最新成交價: 尚未更新</div>", unsafe_allow_html=True)
                    with c_rt2:
                        if st.button("🔄", key="btn_refresh_opt_rt", help="更新最新價格", width='stretch'):
                            st.session_state.opt_rt_trigger = True
                            st.rerun()
                # ------------------------------------

            with c_p2:
                exit_p = st.number_input("出場/目標價 (點)", min_value=0.0, value=None, format="%.12g", placeholder="輸入目標價", key="opt_exit_p")

            st.markdown("###### ⇆ 停損及保證金設定")
            sl_p = st.number_input("停損價 (點) - 用於風報比", min_value=0.0, value=None, format="%.12g", placeholder="輸入停損價", key="opt_sl_p")
            
            # 手續費記憶
            config = load_config()
            if 'saved_opt_fee' not in st.session_state:
                st.session_state.saved_opt_fee = config.get('saved_opt_fee', None)
                
            opt_fee = st.number_input("單邊手續費 (元/口)", min_value=0.0, value=st.session_state.saved_opt_fee, step=1.0, placeholder="輸入手續費 (必填)")
            if opt_fee is not None and opt_fee != st.session_state.saved_opt_fee:
                st.session_state.saved_opt_fee = opt_fee
                config['saved_opt_fee'] = opt_fee
                try:
                    _write_json_atomic(CONFIG_FILE, config)
                except (OSError, TypeError, ValueError):
                    st.warning("手續費設定暫時無法儲存，請稍後再試。")

            actual_margin_req = 0
            margin_display_val = "尚未同步或無資料"
            sync_text = "尚未同步"

            if opt_main_tab == "台指期":
                sync_text = st.session_state.get('taifex_sync_date', '尚未同步')
                opt_tx_type = st.session_state.get('opt_tx_type', '大台 (TX)')
                
                # 修正此處的合約代號對應
                sym = "TX" if "大台" in opt_tx_type else ("MTX" if "小台" in opt_tx_type else "MXF")
                
                fetched_margin = st.session_state.taifex_margin_data.get(sym, 0)
                
                # 偵測合約切換，若切換則強制更新 session_state 裡的值
                if st.session_state.get("last_opt_tx_type") != opt_tx_type:
                    st.session_state["last_opt_tx_type"] = opt_tx_type
                    if fetched_margin > 0:
                        st.session_state["margin_display_tx"] = f"{fetched_margin:,.0f}"
                    else:
                        st.session_state["margin_display_tx"] = ""

                st.markdown("<div style='font-size: 14px; margin-bottom: 5px;'>每口保證金 (原始)</div>", unsafe_allow_html=True)
                c_m1, c_m2 = st.columns([3, 1])
                with c_m1:
                    user_margin = st.text_input("每口保證金", label_visibility="collapsed", key="margin_display_tx")
                    try:
                        actual_margin_req = float(user_margin.replace(',', '').replace(' ', '')) if user_margin else 0
                    except ValueError:
                        actual_margin_req = fetched_margin
                with c_m2:
                    st.button("↺ 重新整理", key="refresh_tx_margin", width='stretch', on_click=sync_taifex_margin)
                if sync_text != "尚未同步":
                    st.markdown(f"<div style='font-size:13px; margin-top: -10px; margin-bottom: 10px;'><span style='color:#00e676;'>✔️</span> <span style='color:#ff4b4b;'>已同步</span> <span style='color:#aaa;'>期交所資料：{sync_text}</span></div>", unsafe_allow_html=True)
                    
            elif opt_main_tab == "個股期貨":
                sync_text = ssf_sync_date if ssf_sync_date else "尚未同步"
                
                sf_code = search_stock_futures.split(" ")[0] if search_stock_futures else ""
                margin_pct = ssf_margin_map.get(sf_code, 0)
                
                # 直接使用 API 的級距百分比計算標準合約保證金，避免套用到特殊調整合約的絕對金額
                calc_margin = 0
                if margin_pct > 0 and entry_p is not None:
                    calc_margin = round(entry_p * mult * (margin_pct / 100.0))
                
                # 偵測合約切換或進場價變更，若切換則強制更新 session_state 裡的值
                current_ssf_state = f"{sf_code}_{entry_p}_{mult}"
                if st.session_state.get("last_ssf_state") != current_ssf_state:
                    st.session_state["last_ssf_state"] = current_ssf_state
                    if calc_margin > 0:
                        st.session_state["margin_display_ssf"] = f"{calc_margin:,.0f}"
                    else:
                        st.session_state["margin_display_ssf"] = ""
# ── 保證金級距資訊卡片 ──────────────────────────
                group_level = ssf_group_level_map.get(sf_code, "")
                maint_pct   = ssf_maint_map.get(sf_code, 0)
                calc_maint  = round(entry_p * mult * (maint_pct / 100.0)) if maint_pct > 0 and entry_p is not None else 0

                if sf_code and (group_level or margin_pct > 0):
                    level_label = f"第 {group_level} 級" if group_level.isdigit() else (group_level if group_level else "—")
                    init_line  = f"原始 <b style='color:#ff9800;'>{calc_margin:,}</b> 元" if calc_margin > 0 else f"原始比例 <b>{margin_pct}%</b>"
                    maint_line = (f"　｜　維持 <b style='color:#4fc3f7;'>{calc_maint:,}</b> 元" if calc_maint > 0
                                  else (f"　｜　維持比例 <b>{maint_pct}%</b>" if maint_pct > 0 else ""))
                    st.markdown(f"""
                    <div style='background:#0d1b2a;border:1px solid #1e3a5f;border-radius:8px;padding:9px 14px;margin-bottom:12px;font-size:13px;'>
                        <span style='color:#00e5ff;font-weight:700;'>📊 保證金級距：{level_label}</span>
                        <span style='color:#888;margin-left:10px;'>原始 {margin_pct}%</span>
                        {"<span style='color:#888;'> ／ 維持 " + str(maint_pct) + "%</span>" if maint_pct > 0 else ""}
                        <div style='margin-top:5px;color:#e0e0e0;'>{init_line}{maint_line}</div>
                    </div>""", unsafe_allow_html=True)
                # ────────────────────────────────────────────────
                st.markdown("<div style='font-size: 14px; margin-bottom: 5px;'>每口保證金 (原始)</div>", unsafe_allow_html=True)
                c_m1, c_m2 = st.columns([3, 1])
                with c_m1:
                    user_margin = st.text_input("每口保證金", label_visibility="collapsed", placeholder=f"API 級距 {margin_pct}%" if margin_pct > 0 else "", key="margin_display_ssf")
                    try:
                        actual_margin_req = float(user_margin.replace(',', '').replace(' ', '')) if user_margin else 0
                    except ValueError:
                        actual_margin_req = calc_margin
                with c_m2:
                    if st.button("↺ 重新整理", key="refresh_ssf_margin", width='stretch'):
                        fetch_ssf_margin_info.clear()
                        st.rerun()
                if sync_text != "尚未同步":
                    st.markdown(f"<div style='font-size:13px; margin-top: -10px; margin-bottom: 10px;'><span style='color:#00e676;'>✔️</span> <span style='color:#ff4b4b;'>已同步</span> <span style='color:#aaa;'>期交所資料：{sync_text}</span></div>", unsafe_allow_html=True)
                    
            else: # 選擇權
                margin_req = st.number_input("每口保證金 (原始)", value=None, step=1000.0, format="%.0f", key="opt_manual_margin_opt", placeholder="買方為權利金，賣方請手動輸入")
                if margin_req is not None:
                    actual_margin_req = margin_req
                elif entry_p is not None:
                    actual_margin_req = entry_p * mult
                else:
                    actual_margin_req = 0
            
            st.markdown("<div style='margin-top: 15px;'></div>", unsafe_allow_html=True)

        with col_right:
            st.markdown("###### 📈 損益結果")

            if entry_p is not None and exit_p is not None and opt_fee is not None:
                pt_diff = (exit_p - entry_p) if "做多" in opt_dir else (entry_p - exit_p)
                gross_pnl = pt_diff * mult * opt_lots

                tax_buy = round(entry_p * mult * tax_rate) * opt_lots
                tax_sell = round(exit_p * mult * tax_rate) * opt_lots
                total_tax = tax_buy + tax_sell
                total_fee = opt_fee * 2 * opt_lots

                net_pnl = gross_pnl - total_tax - total_fee

                pnl_color = "#ff4b4b" if net_pnl > 0 else ("#00e676" if net_pnl < 0 else "white")

                st.markdown(f"""
                <div class="opt-card">
                    <div class="opt-label">預估淨損益 (含手續費與稅)</div>
                    <div class="opt-value" style="color: {pnl_color}; font-size: 24px;">{int(net_pnl):,} 元</div>
                </div>
                """, unsafe_allow_html=True)

                cr1, cr2 = st.columns(2)
                with cr1:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">毛損益</div>
                        <div class="opt-value">{int(gross_pnl):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cr2:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">點差/口</div>
                        <div class="opt-value">{pt_diff:g} 點</div>
                    </div>
                    """, unsafe_allow_html=True)

                cr3, cr4 = st.columns(2)
                with cr3:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">手續費 (雙邊)</div>
                        <div class="opt-value">{int(total_fee):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cr4:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">期交稅 (雙邊)</div>
                        <div class="opt-value">{int(total_tax):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)

                # 刪除原本冗長重複的區塊，只留下這一段：
                if actual_margin_req > 0:
                    roi = net_pnl / (actual_margin_req * opt_lots) * 100
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">預估保證金總額</div>
                        <div class="opt-value">{int(actual_margin_req * opt_lots):,} 元 <span style="font-size: 14px; font-weight: normal; color: #aaa;">(報酬率: {_format_compact_number(roi, 2)}%)</span></div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("<br><div style='font-size:16px; font-weight:bold; color:#ddd; margin-bottom:5px;'>風報比 (R:R)</div>", unsafe_allow_html=True)
                if sl_p is not None:
                    risk_pt = (entry_p - sl_p) if "做多" in opt_dir else (sl_p - entry_p)
                    if risk_pt > 0:
                        reward_pt = pt_diff if pt_diff > 0 else 0
                        rrr = reward_pt / risk_pt if risk_pt != 0 else 0

                        st.markdown(f"<div style='text-align: right; font-size: 20px; font-weight: bold; margin-bottom: 5px;'>1 : {_format_compact_number(rrr, 2)}</div>", unsafe_allow_html=True)

                        total_rr = risk_pt + reward_pt
                        if total_rr > 0:
                            risk_pct = (risk_pt / total_rr) * 100
                            reward_pct = (reward_pt / total_rr) * 100
                        else:
                            risk_pct = 100; reward_pct = 0

                        st.markdown(f"""
                        <div style="width: 100%; height: 8px; display: flex; border-radius: 4px; overflow: hidden; margin-bottom: 5px;">
                            <div style="width: {risk_pct}%; background-color: #00e676;"></div>
                            <div style="width: {reward_pct}%; background-color: #ff4b4b;"></div>
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #aaa;">
                            <span>▼ 風險 {risk_pt:g} 點</span>
                            <span>▲ 報酬 {reward_pt:g} 點</span>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.error("停損價設定錯誤 (做多時停損應低於進場價，做空時應高於進場價)")
                else:
                    st.caption("輸入停損價後即可顯示風報比評估")
                    
                st.markdown("<br><div style='text-align: center; font-size: 12px; color: #888;'>💡 提示：手續費依各券商折扣不同 (大台≈100、小台≈50、微台≈25)<br>保證金請以期交所最新公告為準</div>", unsafe_allow_html=True)
            else:
                st.info("👈 請在左側填寫完整的 **進場價**、**出場/目標價** 與 **單邊手續費** 即可自動開始計算損益與風險。")
                
with tab_fibo:
    
    def format_fibo_tag(key):
        val = str(st.session_state.get(key, '') or '').strip()
        if not val:
            save_fibo_config()
            return

        st.session_state[key] = normalize_fibo_quick_tag(val)
        save_fibo_config()
    
    tab_trade_plan, tab_option_plan, tab_fibo_thermometer, tab_fibo_chart, tab_fibo_manual = st.tabs(
        ["🧭 指數操作計畫", "📅 選擇權操作計畫", "🌡️ 市場溫度計", "📊 費波圖表", "🧮 手動費波"],
        default="🧭 指數操作計畫", key="index_workspace_active_tab", on_change="rerun",
    )

    thermometer_specs = [
        ("加權股價指數", "^TWII"),
        ("臺股期貨", "TWF=F"),
    ]
    thermometer_data = []
    # Streamlit tabs 仍會執行隱藏分頁程式；只在實際開啟需要的分頁時下載兩組溫度資料。
    if tab_fibo.open and (
        tab_trade_plan.open or tab_option_plan.open or tab_fibo_thermometer.open
    ):
        for label, code in thermometer_specs:
            temp_df, source = get_cached_market_temperature_data(code)
            result = calculate_market_temperature(temp_df)
            thermometer_data.append((label, code, temp_df, source, result))

    with tab_trade_plan:
        st.subheader("指數操作計畫")
        st.caption("日線費波決定主方向，5 分 K 提供短波當沖點位；即時報價僅供觀察，不會自動下單或構成投資建議。")
        option_mode_choices = [
            "自動評選（單買／價差）",
            "價差單（限定風險，偏結算）",
            "單買 BC／BP（快進快出）",
        ]
        saved_option_mode = st.session_state.get('trade_plan_option_mode_saved', '')
        if saved_option_mode not in option_mode_choices:
            if str(saved_option_mode).startswith('價差單'):
                saved_option_mode = option_mode_choices[1]
            elif str(saved_option_mode).startswith('單買'):
                saved_option_mode = option_mode_choices[2]
            else:
                saved_option_mode = option_mode_choices[0]
            st.session_state['trade_plan_option_mode_saved'] = saved_option_mode
        index_item = next((item for item in thermometer_data if item[1] == '^TWII'), None)
        futures_item = next((item for item in thermometer_data if item[1] == 'TWF=F'), None)
        plan = calculate_index_trade_plan(
            index_item[2] if index_item else pd.DataFrame(), index_item[4] if index_item else None,
            futures_item[2] if futures_item else pd.DataFrame(), futures_item[4] if futures_item else None,
        )
        if plan is None:
            st.warning("目前缺少足夠的加權或期貨日 K，暫時無法建立操作計畫。")
            if st.button(
                "🔄 重新載入指數日 K", key="retry_index_trade_plan_history",
                width='stretch',
            ):
                clear_index_market_data_cache()
                st.rerun()
        else:
            display_direction, direction_color = {
                '偏多': ('偏多', '#ff4b4b'),
                '偏空': ('偏空', '#00c853'),
            }.get(plan['direction'], ('區間盤整', '#ffc107'))
            profile_col, refresh_col = st.columns([6, 1])
            with profile_col:
                entry_profile = st.radio(
                    "進場靈敏度",
                    ["積極（提早確認）", "穩健（完整確認）"],
                    horizontal=True,
                    key="trade_plan_entry_profile",
                    help="積極模式會擴大費波觀察區，並允許即時價站穩／跌破 VWAP 後提早小部位進場；過熱與反向急漲跌保護仍保留。",
                )
            with refresh_col:
                st.button("↻ 即時更新", key="refresh_trade_plan_live", width='stretch')
            live_snapshot = get_live_futures_snapshot(st.session_state.get('sj_api'), 'TMF')
            live_price = live_snapshot['price'] if live_snapshot else plan['latest']
            live_change = live_snapshot['change'] if live_snapshot else float((futures_item[4] or {}).get('change', 0))
            intraday_state = get_cached_futures_intraday_state(
                st.session_state.get('sj_api'), plan['direction'],
            )
            previous_temperature = None
            if futures_item is not None and len(futures_item[2]) > 20:
                previous_temperature = calculate_market_temperature(futures_item[2].iloc[:-1].copy())
            temperature_delta = (
                float(futures_item[4]['score']) - float(previous_temperature['score'])
                if futures_item and futures_item[4] and previous_temperature else 0.0
            )
            trade_state = evaluate_trade_entry_state(
                plan, live_price, live_change, intraday_state, temperature_delta, entry_profile,
            )
            confirmation_text = intraday_state['confirmation_text']
            if trade_state['can_enter'] and trade_state['execution_direction'] == '偏多':
                position_recommendation = '建議做多｜可快進快出'
                position_color = '#ff4b4b'
            elif trade_state['can_enter'] and trade_state['execution_direction'] == '偏空':
                position_recommendation = '建議做空｜可快進快出'
                position_color = '#00c853'
            else:
                position_recommendation = f"保守不進場｜{trade_state['permission']}"
                position_color = '#ffc107'
            st.markdown(
                f"""<div style='border-left:5px solid {plan['action_color']};background:#151a22;padding:14px 18px;border-radius:7px;margin-bottom:12px'>
                <div style='font-size:14px;color:#b7c0cc'>{plan['market_label']}</div>
                <div style='font-size:15px;font-weight:700;color:{direction_color};margin-top:3px'>趨勢背景：{display_direction}</div>
                <div style='font-size:21px;font-weight:800;color:{trade_state['color']};margin-top:3px'>{trade_state['stage']}｜{trade_state['permission']}</div>
                <div style='font-size:14px;color:#dfe6e9;margin-top:4px'>{plan['alignment_note']}</div>
                </div>""", unsafe_allow_html=True
            )
            st.info(f"**即時判斷：** {trade_state['reason']}")
            p1, p2, p3, p4 = st.columns(4)
            if live_snapshot:
                p1.markdown(
                    f"""<div style='line-height:1.25'>
                    <div style='font-size:14px;font-weight:600;color:#dfe6e9'>最新微台（{live_snapshot['contract_code']}）</div>
                    <div style='font-size:25px;font-weight:700;color:{live_snapshot['color']}'>{live_snapshot['price']:,.0f}</div>
                    <div style='font-size:14px;font-weight:700;color:{live_snapshot['color']}'>{live_snapshot['arrow']} {abs(live_snapshot['change']):,.0f} ({_format_compact_number(live_snapshot['change_pct'], 2, signed=True)}%)</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
            else:
                p1.metric("最新期貨（日 K）", f"{plan['latest']:,.0f}")
            render_index_level_metric(p2, "費波支撐", _format_compact_number(plan['support'], 0))
            render_index_level_metric(p3, "費波壓力", _format_compact_number(plan['resistance'], 0))
            render_index_level_metric(
                p4, "費波區寬", f"± {_format_compact_number(plan['zone_points'], 0)} 點"
            )
            if live_snapshot:
                st.caption(
                    f"微台快照於 {datetime.now(pytz.timezone('Asia/Taipei')).strftime('%H:%M:%S')} 擷取；"
                    "按「即時更新」會更新最新快照，日線歷史快取 3 分鐘、15 分／5 分 K 快取 8 秒，以減少重複下載。"
                )

            index_result = index_item[4] if index_item else None
            if index_result is not None:
                index_price = float(index_result['close'])
                basis = live_price - index_price
                if basis > 0:
                    basis_label, basis_color = "正價差", "#ff4b4b"
                    basis_advice = (
                        "期貨高於加權，反映期貨相對現貨偏強。僅在市場判讀同為偏多、"
                        "且價格回測費波支撐後獲確認時，才考慮順勢做多；不可只因正價差追價。"
                    )
                elif basis < 0:
                    basis_label, basis_color = "逆價差", "#00c853"
                    basis_advice = (
                        "期貨低於加權，反映期貨相對現貨偏弱。僅在市場判讀同為偏空、"
                        "且價格反彈至費波壓力後受壓時，才考慮順勢做空；不可只因逆價差追空。"
                    )
                else:
                    basis_label, basis_color = "平價", "#dfe6e9"
                    basis_advice = "期現價格貼近，價差不提供方向優勢；以費波位置與 15 分 K 確認作為主要進出依據。"

                now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
                updated_at = pd.Timestamp(index_result['updated_at']).date()
                is_cash_snapshot = (
                    updated_at == now_tw.date()
                    and dt_time(9, 0) <= now_tw.time() < dt_time(13, 35)
                    and index_item[3] and '即時串流' in index_item[3]
                )
                basis_reference = "加權盤中快照" if is_cash_snapshot else "最近加權日收盤"
                index_change = float(index_result.get('change', 0) or 0)
                index_change_pct = float(index_result.get('change_pct', 0) or 0)
                index_color = '#ff4b4b' if index_change > 0 else ('#00c853' if index_change < 0 else '#dfe6e9')
                index_arrow = '▲' if index_change > 0 else ('▼' if index_change < 0 else '◆')
                futures_result = (futures_item[4] or {}) if futures_item else {}
                futures_change = float(live_snapshot['change']) if live_snapshot else float(futures_result.get('change', 0) or 0)
                futures_change_pct = float(live_snapshot['change_pct']) if live_snapshot else float(futures_result.get('change_pct', 0) or 0)
                futures_color = '#ff4b4b' if futures_change > 0 else ('#00c853' if futures_change < 0 else '#dfe6e9')
                futures_arrow = '▲' if futures_change > 0 else ('▼' if futures_change < 0 else '◆')
                st.markdown("##### ⚖️ 期現價差判讀")
                b1, b2, b3 = st.columns(3)
                b1.markdown(
                    f"""<div style='line-height:1.25'>
                    <div style='font-size:14px;font-weight:600;color:#dfe6e9'>{basis_reference}</div>
                    <div style='font-size:24px;font-weight:700;color:{index_color}'>{index_price:,.0f}</div>
                    <div style='font-size:14px;font-weight:700;color:{index_color}'>{index_arrow} {abs(index_change):,.0f} ({_format_compact_number(index_change_pct, 2, signed=True)}%)</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                b2.markdown(
                    f"""<div style='line-height:1.25'>
                    <div style='font-size:14px;font-weight:600;color:#dfe6e9'>期貨指數</div>
                    <div style='font-size:24px;font-weight:700;color:{futures_color}'>{live_price:,.0f}</div>
                    <div style='font-size:14px;font-weight:700;color:{futures_color}'>{futures_arrow} {abs(futures_change):,.0f} ({_format_compact_number(futures_change_pct, 2, signed=True)}%)</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                b3.markdown(
                    f"""<div style='line-height:1.25'>
                    <div style='font-size:14px;font-weight:600;color:#dfe6e9'>期貨－加權價差</div>
                    <div style='font-size:24px;font-weight:700;color:{basis_color}'>{basis:+,.0f} 點</div>
                    <div style='font-size:14px;font-weight:700;color:{basis_color}'>{basis_label}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"計算：期貨 {live_price:,.0f} − {basis_reference} {index_price:,.0f} = {basis:+,.0f} 點。"
                    "夜盤因現貨已收盤，價差主要反映夜盤預期與海外市場，不宜單獨作為交易訊號。"
                )
                st.info(f"**操作建議：** {basis_advice}")

            st.divider()
            st.markdown(
                f"#### 🎯 進出依據 <span style='font-size:15px;color:{position_color};font-weight:700'>　{position_recommendation}</span>",
                unsafe_allow_html=True,
            )
            st.caption("以日線費波支撐／壓力與 15 分 K 確認，作為順勢進場、停損與目標依據。")
            st.info(f"**進場確認：** {plan['trigger']}")
            position_count = st.selectbox(
                "預計進場口數",
                options=[1, 2, 3, 4, 5],
                format_func=lambda count: f"{count} 口微型臺指",
                key="trade_plan_position_count",
                help="此設定會連動更新進出依據與短波當沖的停損金額及預估收益。",
            )
            render_index_plan_metric_cards([
                ("觀察進場區", _format_compact_number(trade_state['entry_level'], 0)),
                ("失效／停損", _format_compact_number(trade_state['invalidation'], 0)),
                ("第一目標", _format_compact_number(trade_state['target'], 0)),
                (
                    "預估風報比",
                    f"1 : {_format_compact_number(trade_state['rr_ratio'], 2)}"
                    if trade_state['rr_ratio'] is not None else "—",
                ),
            ])
            entry_risk_level, entry_risk_color, entry_risk_ratio = get_trade_risk_level(
                trade_state['risk_points'], plan['atr'],
            )
            entry_risk_amount = trade_state['risk_points'] * 10 * position_count
            entry_reward_amount = trade_state['reward_points'] * 10 * position_count
            er1, er2, er3, er4 = st.columns(4)
            er1.markdown(
                f"""<div style='line-height:1.2'>
                <div style='font-size:12px;color:#b7c0cc'>計畫風險點數</div>
                <div style='font-size:19px;font-weight:700;color:{entry_risk_color}'>{trade_state['risk_points']:,.0f} 點</div>
                <div style='font-size:12px;font-weight:700;color:{entry_risk_color}'>風險等級：{entry_risk_level}</div>
                </div>""", unsafe_allow_html=True,
            )
            er2.markdown(
                f"""<div style='line-height:1.2'>
                <div style='font-size:12px;color:#b7c0cc'>預估盈利點數</div>
                <div style='font-size:19px;font-weight:700;color:#ff4b4b'>{trade_state['reward_points']:,.0f} 點</div>
                </div>""", unsafe_allow_html=True,
            )
            er3.markdown(
                f"""<div style='line-height:1.2'>
                <div style='font-size:12px;color:#b7c0cc'>{position_count} 口最大風險</div>
                <div style='font-size:19px;font-weight:700;color:{entry_risk_color}'>${entry_risk_amount:,.0f}</div>
                </div>""", unsafe_allow_html=True,
            )
            er4.markdown(
                f"""<div style='line-height:1.2'>
                <div style='font-size:12px;color:#b7c0cc'>{position_count} 口預估收益</div>
                <div style='font-size:19px;font-weight:700;color:#ff4b4b'>${entry_reward_amount:,.0f}</div>
                </div>""", unsafe_allow_html=True,
            )
            st.caption(
                f"風險等級以停損距離相對日 ATR 判定：{_format_compact_number(entry_risk_ratio, 2)} ATR；"
                "微台每點 10 元。"
            )
            if trade_state['can_enter']:
                st.info(f"15 分 K：{confirmation_text} 目前符合「{trade_state['permission']}」條件，仍須依設定停損控制部位。")
            elif plan['direction'] in ('偏多', '偏空'):
                st.warning(f"15 分 K：{confirmation_text} 目前狀態為「{trade_state['permission']}」，不觸發進場。")
            else:
                st.info(f"區間策略：{trade_state['reason']}")

            signal_details = []
            if intraday_state['available']:
                signal_details.append(f"15 分 K VWAP {intraday_state['vwap']:,.0f}")
                signal_details.append(
                    f"本段開盤區間 {intraday_state['opening_low']:,.0f}–{intraday_state['opening_high']:,.0f}"
                )
            signal_details.append(
                f"即時漲跌為日 ATR 的 {_format_compact_number(trade_state['shock_ratio'], 2, signed=True)} 倍"
            )
            signal_details.append(
                f"溫度變化 {_format_compact_number(trade_state['temperature_delta'], 0, signed=True)} 度"
            )
            st.caption("｜".join(signal_details))

            st.divider()
            short_wave_direction, short_direction_note = resolve_short_wave_direction(
                plan, trade_state, intraday_state,
            )
            if short_wave_direction == '偏多':
                short_recommendation, short_color = '短多｜可提早小部位進場', '#ff4b4b'
            elif short_wave_direction == '偏空':
                short_recommendation, short_color = '短空｜可提早小部位進場', '#00c853'
            else:
                short_recommendation, short_color = '等待盤中動能', '#ffc107'
            st.markdown(
                f"#### ⚡ 短波當沖（5 分 K） <span style='font-size:15px;color:{short_color};font-weight:700'>　{short_recommendation}</span>",
                unsafe_allow_html=True,
            )
            st.caption(f"{short_direction_note}。短波採 EMA5 或前根高低點即時觸發，不等待完整 5 分 K 收盤。")
            short_wave = get_cached_short_wave_plan(st.session_state.get('sj_api'), short_wave_direction)
            if short_wave:
                render_index_plan_metric_cards([
                    (
                        "短波進場區",
                        f"{_format_compact_number(short_wave['entry'], 0)} ± {_format_compact_number(short_wave['zone'], 0)}",
                    ),
                    ("短波停損", _format_compact_number(short_wave['stop'], 0)),
                    ("短波目標", _format_compact_number(short_wave['target'], 0)),
                    (
                        "短波風報比",
                        f"1 : {_format_compact_number(short_wave['rr'], 2)}"
                        if short_wave['rr'] is not None else "—",
                    ),
                ])
                st.info(f"**快速進場條件：** {short_wave['trigger']}")
                momentum_label = "已達快速觸發" if short_wave['momentum_ready'] else "接近觸發，等待穿越前根高低點"
                momentum_color = '#ff4b4b' if short_wave_direction == '偏多' else '#00c853'
                st.markdown(
                    f"<div style='font-size:14px;font-weight:700;color:{momentum_color}'>即時狀態：{momentum_label}｜"
                    f"EMA5 {short_wave['ema_fast']:,.0f}｜量比 {_format_compact_number(short_wave['volume_ratio'], 2)}</div>",
                    unsafe_allow_html=True,
                )
                short_risk_level, short_risk_color, short_risk_ratio = get_trade_risk_level(
                    short_wave['risk'], plan['atr'],
                )
                short_risk_amount = short_wave['risk'] * 10 * position_count
                short_reward_amount = short_wave['reward'] * 10 * position_count
                sr1, sr2, sr3, sr4 = st.columns(4)
                sr1.markdown(
                    f"""<div style='line-height:1.2'>
                    <div style='font-size:12px;color:#b7c0cc'>短波風險點數</div>
                    <div style='font-size:18px;font-weight:700;color:{short_risk_color}'>{short_wave['risk']:,.0f} 點</div>
                    <div style='font-size:12px;font-weight:700;color:{short_risk_color}'>風險等級：{short_risk_level}</div>
                    </div>""", unsafe_allow_html=True,
                )
                sr2.markdown(
                    f"""<div style='line-height:1.2'>
                    <div style='font-size:12px;color:#b7c0cc'>短波盈利點數</div>
                    <div style='font-size:18px;font-weight:700;color:#ff4b4b'>{short_wave['reward']:,.0f} 點</div>
                    </div>""", unsafe_allow_html=True,
                )
                sr3.markdown(
                    f"""<div style='line-height:1.2'>
                    <div style='font-size:12px;color:#b7c0cc'>{position_count} 口最大風險</div>
                    <div style='font-size:18px;font-weight:700;color:{short_risk_color}'>${short_risk_amount:,.0f}</div>
                    </div>""", unsafe_allow_html=True,
                )
                sr4.markdown(
                    f"""<div style='line-height:1.2'>
                    <div style='font-size:12px;color:#b7c0cc'>{position_count} 口預估收益</div>
                    <div style='font-size:18px;font-weight:700;color:#ff4b4b'>${short_reward_amount:,.0f}</div>
                    </div>""", unsafe_allow_html=True,
                )
                st.caption(
                    "以最新 6 根 5 分 K、EMA5 與前根高低點計算；短波停損為日 ATR 的 "
                    f"{_format_compact_number(short_risk_ratio, 2)} 倍。"
                    "短波可先於日線進場許可觸發，但只適合小部位快進快出。"
                )
            elif short_wave_direction:
                st.info("即時條件已通過，但尚未取得足夠的 5 分 K，暫不提供短波點位。")
            else:
                st.info("尚未形成可辨識的盤中方向；短波暫不啟用，避免在區間中段雙向追價。")

    with tab_option_plan:
        if tab_option_plan.open:
            st.subheader("選擇權操作計畫")
            st.caption("獨立載入最近到期週三選、週五選或月選；比較價外／平價／價內單買與限定風險價差。")
            option_now = datetime.now(pytz.timezone('Asia/Taipei'))
            data_status = []
            for label, item in (("加權", index_item), ("期貨", futures_item)):
                data_frame = item[2] if item else pd.DataFrame()
                source_name = item[3] if item else "尚未取得資料來源"
                required_columns = {'High', 'Low', 'Close'}
                valid_rows = (
                    len(data_frame.dropna(subset=['High', 'Low', 'Close']))
                    if not data_frame.empty and required_columns.issubset(data_frame.columns) else 0
                )
                latest_date = (
                    pd.Timestamp(data_frame.index[-1]).strftime('%Y/%m/%d')
                    if valid_rows else "無資料"
                )
                data_status.append(f"{label} {valid_rows} 根（最新 {latest_date}；{source_name}）")
            st.caption("計畫資料：" + "｜".join(data_status))
            if is_market_closed_func(option_now.date()):
                st.info(
                    "目前為休市日，計畫會沿用最近完整交易日的日 K；不會建立假日零成交量 K 棒。"
                    "履約價與損益曲線仍可試算，但即時買賣價、價差與流動性應等開盤後重新更新。"
                )
            option_is_closed = is_market_closed_func(option_now.date())
            option_direction = locals().get('short_wave_direction')
            if option_is_closed and plan and plan.get('direction') in ('偏多', '偏空'):
                # 休市時沒有有效的盤中 5 分 K，改採最近完整交易日的日線方向。
                option_direction = plan['direction']
            if plan is None:
                st.info(
                    "目前可用日 K 少於 20 根或高低價欄位不完整，暫時無法建立選擇權操作計畫。"
                    "系統已同時嘗試期貨日 K 與證交所加權指數備援；請檢查上方資料根數與最新日期。"
                )
            elif option_direction not in ('偏多', '偏空'):
                st.info("盤中方向尚未形成，暫不建立 BC／BP 或方向價差；等待 VWAP 與 5 分 K 動能確認。")
            else:
                control_expiry, control_mode, control_money, control_width, control_refresh = st.columns([1.1, 2, 1.25, 1.35, 0.75])
                with control_expiry:
                    expiry_choice = st.selectbox(
                        "到期別", ["最近到期", "週三選", "週五選", "月選"], key="trade_plan_expiry_choice",
                        help="最近到期會比較週三、週五與月選，優先採最早仍可交易的契約。",
                    )
                target_specs = get_txo_target_contract_specs(expiry_choice)
                expected_contract = target_specs[0]['delivery_month'] if target_specs else "依永豐最近到期契約"
                if target_specs:
                    st.caption(
                        f"永豐商品根：`{target_specs[0]['root']}`｜目標契約：`{expected_contract}`｜預定到期日：{target_specs[0]['expiry'].strftime('%Y/%m/%d')}"
                        f"｜剩餘 {(target_specs[0]['expiry'] - datetime.now(pytz.timezone('Asia/Taipei')).date()).days} 天"
                    )
                if st.session_state.get('trade_plan_option_mode_widget') not in option_mode_choices:
                    st.session_state['trade_plan_option_mode_widget'] = st.session_state['trade_plan_option_mode_saved']
                with control_mode:
                    option_mode = st.selectbox("操作方式", option_mode_choices, key="trade_plan_option_mode_widget")
                st.session_state['trade_plan_option_mode_saved'] = option_mode
                moneyness_choices = ["自動評選", "價外", "平價", "價內"]
                saved_moneyness = st.session_state.get('trade_plan_moneyness_saved', moneyness_choices[0])
                if saved_moneyness not in moneyness_choices:
                    saved_moneyness = moneyness_choices[0]
                if st.session_state.get('trade_plan_moneyness_widget') not in moneyness_choices:
                    st.session_state['trade_plan_moneyness_widget'] = saved_moneyness
                with control_money:
                    moneyness_preference = st.selectbox(
                        "履約價偏好", moneyness_choices, key="trade_plan_moneyness_widget",
                        help="自動評選會同時比較價外、平價與價內的模型獲利機率、成本效率及流動性。",
                    )
                st.session_state['trade_plan_moneyness_saved'] = moneyness_preference
                with control_width:
                    spread_width_label = st.selectbox(
                        "價差寬度", ["100 點優先", "50 點"], key="trade_plan_spread_width",
                        help="100 點優先：先找相差 100 點的保護腿，缺少該履約價時自動退回 50 點。",
                    )
                spread_width = 50 if spread_width_label.startswith('50') else 100
                with control_refresh:
                    # Align the button with the lower selectbox control instead of its label.
                    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
                    refresh_option_plan = st.button("↻ 更新", key="refresh_option_plan", width='stretch')

                use_intraday_option_levels = not option_is_closed and short_wave
                option_entry = short_wave['entry'] if use_intraday_option_levels else plan['entry_level']
                option_stop = short_wave['stop'] if use_intraday_option_levels else plan['invalidation']
                option_target = short_wave['target'] if use_intraday_option_levels else plan['target']
                option_price = live_price if not option_is_closed else plan['latest']
                quote_plan = {
                    **plan,
                    'latest': option_price,
                    'direction': option_direction,
                    'entry_level': option_entry,
                    'invalidation': option_stop,
                    'target': option_target,
                }
                quote_signature = (
                    expiry_choice, option_mode, moneyness_preference, spread_width, option_direction,
                    round(float(option_price), 0), round(float(option_target), 0), round(float(option_stop), 0),
                    id(st.session_state.get('sj_api')),
                )
                option_cache = st.session_state.get('_option_plan_quote_cache')
                if refresh_option_plan or not option_cache or option_cache.get('signature') != quote_signature:
                    directional_quote = None
                    spread_quote = None
                    if option_mode.startswith("自動") or option_mode.startswith("單買"):
                        directional_quote = get_txo_directional_quote(
                            st.session_state.get('sj_api'), quote_plan, expiry_choice, moneyness_preference,
                        )
                    if option_mode.startswith("自動") or option_mode.startswith("價差單"):
                        spread_quote = get_txo_spread_quote(
                            st.session_state.get('sj_api'), quote_plan, expiry_choice, spread_width,
                        )
                    option_cache = {
                        'signature': quote_signature, 'directional': directional_quote,
                        'spread': spread_quote, 'updated_at': datetime.now(pytz.timezone('Asia/Taipei')),
                    }
                    st.session_state['_option_plan_quote_cache'] = option_cache
                directional_quote = option_cache.get('directional')
                spread_quote = option_cache.get('spread')
                strategy_view = recommend_txo_strategy(directional_quote, spread_quote, quote_plan)
                st.markdown(
                    f"<div style='border-left:5px solid {strategy_view['color']};background:#151a22;padding:12px 16px;border-radius:7px'>"
                    f"<div style='font-size:13px;color:#b7c0cc'>綜合波動率、量價、流動性與短波方向</div>"
                    f"<div style='font-size:20px;font-weight:800;color:{strategy_view['color']}'>建議：{strategy_view['choice']}</div>"
                    f"<div style='font-size:14px;color:#dfe6e9'>{strategy_view['reason']}</div></div>",
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"方向：{'BC／Call' if option_direction == '偏多' else 'BP／Put'}｜"
                    f"報價更新：{option_cache['updated_at'].strftime('%H:%M:%S')}｜資料只在本分頁開啟或按更新時載入。"
                )
                with st.expander("🧮 模型數字怎麼看"):
                    st.markdown(
                        "- **模型機率**：估算到期時跨過損益兩平點的機率，不是歷史勝率。\n"
                        "- **目標／停損情境**：假設快進快出後仍有約 65% 剩餘時間，估算當時權利金；未含手續費與稅。\n"
                        "- **模型估值差**：理論價減保守買進價。負值常反映買賣價差，不代表這筆交易的期望報酬。\n"
                        "- **單買門檻**：一般單買需目標越過損益兩平、目標情境為正、報酬風險比至少 1.25。\n"
                        "- **放寬試單**：機率／價差較不理想但仍符合最低條件時，只顯示小部位試單；不代表一般單買通過。\n"
                        "- 夜盤以最新期貨代替現貨，期現價差可能造成估算誤差。"
                    )

                display_spread = option_mode.startswith("價差單") or (
                    option_mode.startswith("自動") and strategy_view['choice'] == '價差單'
                )
                option_quote = spread_quote if display_spread else directional_quote
                if option_quote and display_spread:
                    option_title = f"{option_quote['name']}｜{option_quote['delivery_month']}｜{option_quote['expiry'].strftime('%Y/%m/%d')} 到期（剩 {option_quote['dte']} 天）"
                    st.markdown(f"**{option_title}**")
                    spread_width_value = abs(option_quote['short_strike'] - option_quote['long_strike'])
                    render_option_metric_cards("契約組合", [
                        ("賣方價外履約價", f"{_format_compact_number(option_quote['short_strike'], 0)} {option_quote['right']}", '#f5f5f5'),
                        ("保護買方履約價", f"{_format_compact_number(option_quote['long_strike'], 0)} {option_quote['right']}", '#f5f5f5'),
                        ("履約價差", f"{_format_compact_number(spread_width_value, 0)} 點", '#29b6f6'),
                    ])
                    render_option_metric_cards("風險與模型", [
                        ("預估淨權利金", f"${_format_compact_number(option_quote['net_credit'], 0)}" if option_quote['net_credit'] is not None else "報價不足", '#ffb300'),
                        ("最大獲利", f"${_format_compact_number(option_quote['max_profit'], 0)}" if option_quote['max_profit'] is not None else "報價不足", '#ff4b4b'),
                        ("單組最大風險", f"${_format_compact_number(option_quote['max_loss'], 0)}", '#00c853'),
                        ("損益兩平", _format_compact_number(option_quote['breakeven'], 2) if option_quote['breakeven'] is not None else "報價不足", '#f5f5f5'),
                        ("模型勝率", f"{_format_compact_number(option_quote['model_probability'] * 100, 2)}%" if option_quote['model_probability'] is not None else "無法估算", '#29b6f6'),
                        ("模型估值差", f"${_format_compact_number(option_quote['expected_pnl'], 0, signed=True)}" if option_quote['expected_pnl'] is not None else "無法估算", '#cbd5e1'),
                    ])
                    premium_detail = "即時買賣價不足，最大風險先以履約價差 × 50 元估算。"
                    if option_quote['short_premium'] is not None and option_quote['long_premium'] is not None:
                        premium_detail = (
                            f"賣方權利金 {_format_compact_number(option_quote['short_premium'], 2)}、"
                            f"保護買方權利金 {_format_compact_number(option_quote['long_premium'], 2)}，"
                            "以賣方買價與買方賣價保守估算。"
                        )
                    st.caption(
                        f"風險指標：{option_quote['risk_level']}；{premium_detail} 到期週 Gamma 風險高，"
                        "價格有效跌破／突破日線失效點時應優先退出，絕不留裸賣部位。"
                    )
                    st.caption(f"資料來源：{option_quote['source']}（契約與即時串流）")
                elif option_quote:
                    option_title = f"{option_quote['name']}｜{option_quote['delivery_month']}｜{option_quote['expiry'].strftime('%Y/%m/%d')} 到期（剩 {option_quote['dte']} 天）"
                    st.markdown(f"**{option_title}**")
                    render_option_metric_cards("契約與成交", [
                        ("候選履約價" if not option_quote.get('trade_ready', False) else "建議買進", f"{_format_compact_number(option_quote['strike'], 0)} {option_quote['right']}", '#f5f5f5'),
                        ("買進參考價", f"{_format_compact_number(option_quote['premium'], 2)} 點" if option_quote['premium'] is not None else "報價不足", '#ffb300'),
                        ("價內外", f"{option_quote['moneyness']}｜距現價 {_format_compact_number(option_quote['distance_points'], 0)} 點", '#29b6f6'),
                        ("買賣價差", f"{_format_compact_number(option_quote['spread'], 2)} 點" if option_quote['spread'] is not None else "報價不足", '#f5f5f5'),
                        ("流動性", option_quote['liquidity'], '#f5f5f5'),
                    ])
                    render_option_metric_cards("成本、風險與模型", [
                        ("單口權利金成本", f"${_format_compact_number(option_quote['max_loss'], 0)}" if option_quote['max_loss'] is not None else "報價不足", '#00c853'),
                        ("損益兩平", _format_compact_number(option_quote['breakeven'], 2) if option_quote['breakeven'] is not None else "報價不足", '#f5f5f5'),
                        ("模型勝率", f"{_format_compact_number(option_quote['model_probability'] * 100, 2)}%" if option_quote['model_probability'] is not None else "無法估算", '#29b6f6'),
                        ("模型波動率", f"{_format_compact_number(option_quote['model_volatility'] * 100, 2)}%", '#f5f5f5'),
                        ("模型估值差", f"${_format_compact_number(option_quote['model_value_gap'], 0, signed=True)}" if option_quote.get('model_value_gap') is not None else "無法估算", '#cbd5e1'),
                        ("報酬／風險", _format_compact_number(option_quote.get('payoff_ratio'), 2) if option_quote.get('payoff_ratio') is not None else "無法估算", '#29b6f6'),
                        (
                            "目標情境損益",
                            f"${_format_compact_number(option_quote['target_pnl'], 0, signed=True)}",
                            _txo_scenario_color(option_quote.get('target_pnl')),
                        ),
                        (
                            "停損情境損益",
                            f"${_format_compact_number(option_quote['stop_pnl'], 0, signed=True)}",
                            _txo_scenario_color(option_quote.get('stop_pnl')),
                        ),
                    ])
                    if option_quote.get('trade_ready', False):
                        st.warning(
                            f"⚠️ 風險：{option_quote['risk_level']}。只在 5 分 K 確認後進場；"
                            "標的碰到失效點，或權利金回落約 40%，優先退出。"
                        )
                    elif option_quote.get('small_position_ready', False):
                        st.warning(
                            "⚠️ 放寬試單條件：這筆 BC／BP 尚未達一般單買門檻，"
                            "只可用可承受歸零的小部位，5 分 K 未確認或碰到失效點就退出。"
                        )
                    else:
                        st.warning(
                            "⏸️ 本候選未達單買門檻："
                            + "、".join(option_quote.get('quality_notes') or ['條件不足'])
                            + "。先等待，不以候選價直接進場。"
                        )
                    st.caption(
                        f"報價依據：{option_quote.get('premium_basis', '最後成交價')}。單口權利金成本 = 參考價 × 50 元，"
                        "未含手續費與交易稅；買方最大風險即已付權利金。"
                    )
                    st.caption(
                        f"篩選模式：{option_quote['profile']}；目前選為{option_quote['moneyness']}。"
                        f"短波目標{'可涵蓋' if option_quote['target_reachable'] else '尚未涵蓋'}此履約價；"
                        f"波動率來源：{option_quote['volatility_source']}。"
                        "模型勝率是到期超越損益兩平點的估算，不是歷史回測命中率。"
                        f"資料來源：{option_quote['source']}（契約與報價）。"
                    )
                    render_txo_scenario_note(option_quote)
                    if option_quote.get('alternatives'):
                        comparison_rows = []
                        for candidate in option_quote['alternatives']:
                            comparison_rows.append({
                                "價內外": candidate['moneyness'],
                                "履約價": _format_compact_number(candidate['strike'], 0),
                                "買進價": _format_compact_number(candidate['premium'], 2),
                                "模型勝率": f"{_format_compact_number(candidate['model_probability'] * 100, 2)}%" if candidate['model_probability'] is not None else "—",
                                "IV／模型波動率": f"{_format_compact_number(candidate['model_volatility'] * 100, 2)}%",
                                "模型估值差": f"${_format_compact_number(candidate['model_value_gap'], 0, signed=True)}" if candidate.get('model_value_gap') is not None else "—",
                                "報酬／風險": _format_compact_number(candidate.get('payoff_ratio'), 2) if candidate.get('payoff_ratio') is not None else "—",
                                "單買門檻": (
                                    "✅ 通過" if candidate.get('trade_ready')
                                    else "⚠️ 小部位" if candidate.get('small_position_ready')
                                    else "⏸️ 等待"
                                ),
                                "目標損益": f"${_format_compact_number(candidate['target_pnl'], 0, signed=True)}",
                                "停損損益": f"${_format_compact_number(candidate['stop_pnl'], 0, signed=True)}",
                                "流動性": candidate['liquidity'],
                                "綜合分數": _format_compact_number(candidate['score'], 2),
                            })
                        st.markdown("##### 履約價比較")
                        comparison_frame = pd.DataFrame(comparison_rows)
                        st.dataframe(
                            comparison_frame,
                            column_config=compact_table_column_config(comparison_frame),
                            width='stretch', hide_index=True,
                        )
                elif display_spread:
                    st.warning(
                        f"永豐 Shioaji 尚未取得 {expiry_choice}（預期 `{expected_contract}`）的夜盤即時契約／報價，"
                        "本次不提供價差單履約價與權利金建議，避免使用日盤資料造成誤判。"
                    )
                else:
                    operation = "BC（買進 Call）" if quote_plan['direction'] == '偏多' else "BP（買進 Put）"
                    st.warning(
                        f"永豐 Shioaji 尚未取得 {expiry_choice}（預期 `{expected_contract}`） 的可用選擇權契約／報價，暫不推薦單買 {operation}。"
                        "請確認已登入期貨帳戶，並在交易日重新載入永豐契約檔。"
                    )
                if not option_quote:
                    diagnostic = st.session_state.get('txo_contract_diagnostic')
                    if diagnostic:
                        st.caption(f"契約讀取診斷：{diagnostic}")
                else:
                    payoff_chart = build_txo_payoff_chart(option_quote, quote_plan, display_spread)
                    if payoff_chart is not None:
                        st.plotly_chart(
                            payoff_chart, width='stretch',
                            config={'displayModeBar': False, 'scrollZoom': False},
                        )
                        st.caption(
                            "曲線為到期結算損益，每口／每組乘數 50 元；紅色為獲利、綠色為虧損。"
                            "到期前的實際損益仍會受剩餘時間、隱含波動率與買賣價差影響。"
                        )
                    else:
                        st.info("即時權利金或淨收權利金不足，暫時無法繪製可驗證的損益曲線。")

    with tab_fibo_thermometer:
        title_col, refresh_col = st.columns([6, 1])
        with title_col:
            st.subheader("臺灣加權／期貨溫度計")
            st.caption("以 60 日位置、RSI、均線趨勢與 5 日動能合成 0–100 分；期貨納入夜盤至次日日盤的未完成交易日 K。")
        with refresh_col:
            refresh_market_temperature = st.button(
                "🔄 更新", key="refresh_market_temperature", width='stretch'
            )
        if refresh_market_temperature:
            clear_index_market_data_cache()
            st.rerun()

        gauge_cols = st.columns(2)
        summary_rows = []
        for column, (label, code, temp_df, source, result) in zip(gauge_cols, thermometer_data):
            with column:
                if result is None:
                    if code == "TWF=F":
                        st.warning("臺股期貨溫度計需要登入永豐 Shioaji，才能取得含夜盤的完整資料。")
                    else:
                        st.warning("目前無法取得足夠的加權指數資料。")
                    continue

                st.plotly_chart(build_market_temperature_gauge(label, result), width='stretch', config={'displayModeBar': False})
                m1, m2, m3 = st.columns(3)
                m1.markdown(
                    f"""<div style='line-height:1.25'>
                    <div style='font-size:14px;font-weight:600;color:#dfe6e9'>最新</div>
                    <div style='font-size:31px;font-weight:700;color:{result['price_color']}'>{result['close']:,.0f}</div>
                    <div style='font-size:15px;font-weight:700;color:{result['price_color']}'>{result['price_arrow']} {abs(result['change']):,.0f} ({_format_compact_number(result['change_pct'], 2, signed=True)}%)</div>
                    </div>""",
                    unsafe_allow_html=True
                )
                m2.metric("RSI(14)", _format_compact_number(result['rsi'], 1))
                m3.metric("5日動能", f"{_format_compact_number(result['momentum'], 2, signed=True)}%")
                st.markdown(f"**狀態：** <span style='color:{result['color']}; font-size:18px; font-weight:700'>{result['status']}</span>", unsafe_allow_html=True)
                st.info(f"入場觀察：{result['entry']}")
                st.warning(f"出場／風控：{result['exit_rule']}")
                if source:
                    st.caption(f"資料來源：{source}｜最新交易日：{pd.Timestamp(result['updated_at']).strftime('%Y/%m/%d')}")

                summary_rows.append({
                    "市場": label,
                    "溫度": result['score'],
                    "狀態": result['status'],
                    "60日位置": _format_compact_number(result['range_score'], 1),
                    "MA20": f"{result['ma20']:,.0f}",
                    "MA60": f"{result['ma60']:,.0f}",
                })

        if summary_rows:
            st.markdown("#### 判讀摘要")
            temperature_frame = pd.DataFrame(summary_rows)
            st.dataframe(
                temperature_frame,
                column_config=compact_table_column_config(temperature_frame),
                width='stretch', hide_index=True,
            )

        with st.expander("溫度計判讀規則"):
            st.markdown("""
            - 0–39：空方動能較強；40–59：區間盤整；60–100：多方動能較強。
            - 溫度屬於日線「趨勢背景」，不是立即進場訊號；操作計畫會另外確認費波位置、15 分 K、VWAP、開盤區間及 ATR 反轉幅度。
            - 反向漲跌達約 1.5 日 ATR，或溫度快速反轉並突破盤中結構時，會先暫停原趨勢方向，避免在超跌反彈追空或過熱回落追多。
            - 期貨在 15:00 後會建立下一交易日的夜盤未完成日 K，隔日日盤會累加到同一根，因此可直接用於夜盤支撐壓力判讀。
            - 此為規則型技術判讀與風險提示，不構成投資建議；實際交易仍應搭配停損、部位與流動性管理。
            """)

    with tab_fibo_chart:
        code_map_fibo, name_map_fibo = load_local_stock_names()
        fibo_stock_options = []
        for c, n in sorted(code_map_fibo.items()):
            if not st.session_state.get('allow_warrant_search', False) and is_warrant(c):
                continue
            fibo_stock_options.append(f"{n}({c})")
        # 下方 search_list 修改對應變數
        search_list = ["加權股價指數(TAIEX)", "臺股期貨(TX)", "微型臺指期貨(TMF)"] + fibo_stock_options

        def set_fibo_search(val):
            canonical_value = normalize_fibo_quick_tag(val)
            st.session_state.fibo_search_input = canonical_value
            if canonical_value in search_list:
                st.session_state.fibo_selectbox = canonical_value
            st.session_state.fibo_trigger_search = True
            st.session_state.fibo_interval = "1d"

        with st.expander("⚙️ 設定快速標籤"):
            st.info("💡 將圖表字體大小設定獨立：")
            st.session_state.fibo_font_size = st.slider("圖表標籤字體大小", min_value=8, max_value=24, value=st.session_state.fibo_font_size)
            st.write("---")
            cloud_enabled = bool(get_app_secret('gsheet_api_url'))
            fibo_source_labels = {
                'google_sheet': 'Google Sheet', 'local_tag_cache': '伺服器標籤檔',
                'config': '部署設定檔', 'data_cache': '本機資料快取', 'default': '預設標籤',
            }
            fibo_source = fibo_source_labels.get(
                st.session_state.get('_fibo_tags_source', 'default'), '預設標籤'
            )
            if cloud_enabled:
                st.caption(f"☁️ Google Sheet 同步已啟用｜本次標籤來源：{fibo_source}")
                if st.button(
                    "☁️ 從 Google Sheet 重新載入標籤",
                    key="reload_fibo_quick_tags_from_cloud", width='stretch',
                ):
                    restored, message = reload_fibo_tags_from_cloud()
                    if restored:
                        st.toast(message, icon="☁️")
                        st.rerun()
                    else:
                        st.error(f"雲端標籤讀取失敗：{message}")
            else:
                st.warning("尚未設定 Google Sheet 同步；目前快速標籤只保存在伺服器。")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.text_input("快速標籤 1", key="custom_tag_1", on_change=format_fibo_tag, args=("custom_tag_1",))
            c2.text_input("快速標籤 2", key="custom_tag_2", on_change=format_fibo_tag, args=("custom_tag_2",))
            c3.text_input("快速標籤 3", key="custom_tag_3", on_change=format_fibo_tag, args=("custom_tag_3",))
            c4.text_input("快速標籤 4", key="custom_tag_4", on_change=format_fibo_tag, args=("custom_tag_4",))
            c5.text_input("快速標籤 5", key="custom_tag_5", on_change=format_fibo_tag, args=("custom_tag_5",))
            if st.button("💾 儲存快速標籤", key="save_fibo_quick_tags", width='stretch'):
                sync_saved = save_fibo_config()
                if sync_saved:
                    message = "快速標籤已同步到 Google Sheet" if cloud_enabled else "快速標籤已儲存在伺服器"
                    st.toast(message, icon="✅")
                else:
                    st.error("標籤已嘗試儲存，但 Google Sheet 同步失敗；請稍後再試。")

        st.write("📌 **快速查詢標籤** (點擊按鈕直接帶入)")
        btn_labels = [
            ("加權股價指數(TAIEX)", "加權股價指數(TAIEX)"),
            ("臺股期貨(TX)", "臺股期貨(TX)"),
            ("微型臺指期貨(TMF)", "微型臺指期貨(TMF)")
        ]
        for tag in [st.session_state.custom_tag_1, st.session_state.custom_tag_2, st.session_state.custom_tag_3, st.session_state.custom_tag_4, st.session_state.custom_tag_5]:
            canonical_tag = normalize_fibo_quick_tag(tag)
            if canonical_tag:
                btn_labels.append((canonical_tag, canonical_tag))
        
        if btn_labels:
            tag_cols = st.columns(len(btn_labels))
            for i, (label, val) in enumerate(btn_labels):
                if i < len(tag_cols):
                    tag_cols[i].button(label, on_click=set_fibo_search, args=(val,), width='stretch', key=f"btn_fibo_{i}")

        # 移除重複且未過濾的賦值，直接取得當前搜尋框的預設索引值
        current_val = normalize_fibo_quick_tag(st.session_state.fibo_search_input)
        default_index = None
        
        if current_val:
            for i, opt in enumerate(search_list):
                if current_val == opt:
                    default_index = i
                    break

        def selectbox_changed():
            val = st.session_state.fibo_selectbox
            if val:
                st.session_state.fibo_search_input = normalize_fibo_quick_tag(val)
                st.session_state.fibo_interval = "1d"
            else:
                st.session_state.fibo_search_input = ""

        selected_raw = st.selectbox(
            "🔍 搜尋股票 (可直接輸入股號或股名查找，或點擊上方快捷按鈕)",
            options=search_list,
            index=default_index,
            placeholder="請選擇或輸入...",
            key="fibo_selectbox",
            on_change=selectbox_changed
        )
        
        final_target = normalize_fibo_quick_tag(st.session_state.fibo_search_input)
        
        st.write("---")
        st.write("⚙️ **圖表顯示設定**")
        col_m1, col_m2, col_m3, col_m4, col_v, col_w = st.columns([1, 1, 1, 1, 1.5, 2.5])
        s_ma5 = col_m1.checkbox("5MA (橘)", value=True)
        s_ma10 = col_m2.checkbox("10MA (淺藍)", value=True)
        s_ma20 = col_m3.checkbox("20MA (綠)", value=True)
        s_ma60 = col_m4.checkbox("60MA (黃)", value=True)
        s_vol = col_v.checkbox("📊 顯示成交量", value=True)
        ma_w = col_w.slider(
            "均線粗細", min_value=1.0, max_value=5.0, step=0.5,
            key="ma_w", on_change=lambda: save_fibo_config(save_tags=False), label_visibility="collapsed"
        )
        
        ma_flags = {'5': s_ma5, '10': s_ma10, '20': s_ma20, '60': s_ma60}

        st.write("---")
        interval_options = {"5m": "5分", "15m": "15分", "60m": "60分", "1d": "日", "1wk": "週", "1mo": "月"}
        try: default_radio_idx = list(interval_options.keys()).index(st.session_state.fibo_interval)
        except: default_radio_idx = 0 

        interval_col, advice_col = st.columns([1, 3], vertical_alignment="top")
        selected_interval_label = interval_col.radio(
            "⏱️ 選擇時間標籤",
            options=list(interval_options.values()),
            index=default_radio_idx,
            horizontal=True
        )
        advice_placeholder = advice_col.container()
        
        selected_interval = list(interval_options.keys())[list(interval_options.values()).index(selected_interval_label)]
        st.session_state.fibo_interval = selected_interval 
        
        if tab_fibo_chart.open and final_target.strip():
            plot_fibonacci_chart(
                final_target, selected_interval,
                font_size=st.session_state.fibo_font_size,
                ma_flags=ma_flags, ma_width=st.session_state.ma_w,
                show_vol=s_vol, advice_container=advice_placeholder,
            )
        elif tab_fibo_chart.open:
            st.info("請在上方選擇或輸入股票/期貨以顯示圖表。")

    with tab_fibo_manual:
        st.write("📌 **手動輸入高低點，計算費波納契回撤與延伸點位**")
        col_table, col_empty = st.columns([1, 2])
        
        with col_table:
            col_h, col_l = st.columns(2)
            with col_h:
                fibo_high = st.number_input("輸入波段高點：", value=None, step=1.0, format="%.12g")
            with col_l:
                fibo_low = st.number_input("輸入波段低點：", value=None, step=1.0, format="%.12g")
                
            if fibo_high is not None and fibo_low is not None:
                if fibo_high > 0 and fibo_low > 0 and fibo_high > fibo_low:
                    diff = fibo_high - fibo_low
                    ratios_manual = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
                    
                    fibo_data = []
                    for r in ratios_manual:
                        price = fibo_low + (r * diff)
                        calc_price = round(price, 2)
                        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
                        fibo_data.append({
                            "比例": r_label,
                            "計算點位": _format_compact_number(calc_price, 2),
                            "_raw_r": r 
                        })
                    
                    df_fibo = pd.DataFrame(fibo_data)
                    
                    def style_fibo_manual(row):
                        important_ratios = [0.0, 0.382, 0.5, 0.618, 1.0]
                        if row["_raw_r"] in important_ratios:
                            return ['background-color: #ffffcc; color: black; font-weight: bold;'] * len(row)
                        return [''] * len(row)
                        
                    table_height = (len(df_fibo) + 1) * 36
                    styled_fibo = df_fibo.style.apply(style_fibo_manual, axis=1)
                    
                    st.dataframe(
                        styled_fibo, 
                        width='stretch', 
                        height=table_height, 
                        hide_index=True,
                        column_config={"_raw_r": None}
                    )
                else:
                    st.warning("波段高點必須大於波段低點且大於0")
            else:
                st.info("請在上方輸入高低點數值開始計算。")


with tab_db:
    sub_tab1, sub_tab2, sub_tab3 = st.tabs(
        ["三大法人買賣超", "台指期籌碼快訊", "處置股"],
        key="strategy_database_active_tab", on_change="rerun",
    )
    
    with sub_tab1:
        st.markdown("#### 📊 台股三大法人每日買賣超統計")
        
        # 調整預設日期邏輯：若是週六或週日，預設切回上週五
        today_check = datetime.today()
        if today_check.weekday() == 5:
            default_date = today_check - timedelta(days=1)
        elif today_check.weekday() == 6:
            default_date = today_check - timedelta(days=2)
        else:
            default_date = today_check

        selected_date = st.date_input("選擇日期", default_date)
        date_str = selected_date.strftime("%Y%m%d")
        
        # 加入安全攔截，若發生錯誤則預設為 None
        database_institutional_active = bool(tab_db.open and sub_tab1.open)
        try:
            df_inst = (
                get_major_institutional_data(date_str)
                if database_institutional_active else None
            )
        except Exception:
            df_inst = None
            
        if df_inst is not None:
            st.subheader(f"📅 {selected_date.strftime('%Y-%m-%d')} 統計結果")
            
            # 建立一個自訂的格式化函數，將原始數字轉為：千分位(+-XXX.XX億)
            def fmt_with_yi(val):
                try:
                    v = float(val)
                    yi_val = v / 100000000
                    return f"{int(v):,}  ({_format_compact_number(yi_val, 2, signed=True)}億)"
                except:
                    return str(val)
            
            # 將自訂格式套用到買進、賣出、買賣差額三個欄位
            fmt_dict = {col: fmt_with_yi for col in ['買進金額', '賣出金額', '買賣差額']}
            
            try:
                styled_df = df_inst.style.map(color_negative_positive, subset=['買賣差額']).format(fmt_dict)
            except AttributeError:
                styled_df = df_inst.style.applymap(color_negative_positive, subset=['買賣差額']).format(fmt_dict)
            
            # 使用 columns 進行縮排，不讓表格佔滿全螢幕
            col_tbl, _ = st.columns([1.5, 1])
            with col_tbl:
                st.dataframe(styled_df, width='stretch', hide_index=True)
            st.caption("數據來源：[台灣證券交易所 (TWSE)](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)")
        elif database_institutional_active:
            st.warning("該日期目前無資料（可能尚未開市或為休假日或證交所 API 觸發防護防阻）。")
                
        st.markdown("---")
        st.markdown("#### 📈 法人當日買賣超個股")
        inst_tabs = st.tabs(
            ["外資當日買賣超", "投信當日買賣超", "自營商當日買賣超"],
            key="institutional_ranking_active_tab", on_change="rerun",
        )
        with inst_tabs[0]:
            if database_institutional_active and inst_tabs[0].open:
                components.html(
                    fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_D.djhtm"),
                    height=1185, width=800, scrolling=True,
                )
        with inst_tabs[1]:
            if database_institutional_active and inst_tabs[1].open:
                components.html(
                    fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DD.djhtm"),
                    height=1185, width=800, scrolling=True,
                )
        with inst_tabs[2]:
            if database_institutional_active and inst_tabs[2].open:
                components.html(
                    fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DB.djhtm"),
                    height=1185, width=800, scrolling=True,
                )
        
    with sub_tab2:
        st.markdown("#### 📑 永豐期貨盤後籌碼自動化工具")
        
        if st.button("🔄 刷新最新報告清單"):
            get_report_list.clear()        # 只清報告清單
            fetch_and_parse_pdf.clear()   # 只清 PDF 快取
            st.rerun()

        reports_active = bool(tab_db.open and sub_tab2.open)
        reports = get_report_list() if reports_active else []

        if not reports:
            if reports_active:
                st.warning("目前找不到相關報告，請檢查官網是否變動或稍後再試。")
        else:
            latest_report = reports[0]
            st.markdown(f"### 🔥 最新快訊: {latest_report['日期']} | {latest_report['title']}")
            
            with st.spinner("正在下載並自動預覽最新報告..."):
                data = fetch_and_parse_pdf(latest_report['url'])
                
                if data['ratio'] != "N/A" and data['ratio'] != "解析錯誤":
                    val = float(data['ratio'])
                    st.metric("散戶小台多空比", f"{val}%", delta=f"{val}%", delta_color="inverse")
                
                if data.get('images'):
                    col_img, _ = st.columns([1, 2])
                    with col_img:
                        for img in data['images']:
                            st.image(img, width='stretch')
                else:
                    st.warning("⚠️ 無法自動轉譯為圖片，請使用下方連結開啟。")
                    
            st.link_button("📥 點此進入原始報告下載頁面", latest_report['url'])
            
            st.divider()
            st.markdown("#### 📅 歷史報告清單")
            
            # 設定7天前的日期界線 (以台灣時間計算，精準到天)
            tz_tw = pytz.timezone('Asia/Taipei')
            limit_date = pd.Timestamp(datetime.now(tz_tw).date() - timedelta(days=7))
            
            for idx, report in enumerate(reports[1:], 1):
                show_report = False # 預設改為不顯示
                
                try:
                    # 改用 pandas 處理日期，可自動適應 - 或 / 等不同格式
                    rep_date = pd.to_datetime(report['日期'])
                    if rep_date.tzinfo is not None: 
                        rep_date = rep_date.tz_localize(None)
                        
                    if rep_date >= limit_date:
                        show_report = True
                except:
                    # 只有在明確寫 "近期發布" 時才例外顯示
                    if report['日期'] == "近期發布":
                        show_report = True
                        
                if show_report:
                    with st.expander(f"📅 {report['日期']} | {report['title']}"):
                        st.write(f"連結: [點此查看原始 PDF]({report['url']})")

    with sub_tab3:
        st.markdown("#### 🚨 處置股預測與公告")
        
        # 初始化處置股獨立重整計數器
        if 'disposal_refresh_idx' not in st.session_state:
            st.session_state.disposal_refresh_idx = 0
            
        # 新增重新整理按鈕
        if st.button("🔄 重新整理處置股", key="btn_refresh_disposal"):
            st.session_state.disposal_refresh_idx += 1
            st.rerun()
            
        # 將計數器加入網址參數，藉由改變網址強制 iframe 重新載入
        refresh_url = f"https://cmfaren.github.io/dispositionforecast/?t={st.session_state.disposal_refresh_idx}"
        
        if tab_db.open and sub_tab3.open:
            st.iframe(
                refresh_url,
                height=800,
                width='stretch',
            )

with tab3:
    # ==========================================
    # 新增：自訂行事曆存取與 UI 介面
    # ==========================================
    CAL_OVERRIDE_FILE = "cal_override.json"
    CAL_PREFERENCES_FILE = "cal_preferences.json"
    CALENDAR_EVENT_OPTIONS = [
        "台股開休市",
        "台股突發休市公告",
        "FOMC 利率決議",
        "美國 CPI",
        "美國核心 PCE",
        "美國 GDP",
        "美國 ISM 製造業指數",
        "美國大非農",
        "美國小非農 ADP",
        "美國初領失業金",
        "美股四巫日",
        "富台指期結算",
        "MSCI 季度調整",
        "指定股票財報",
        "台股月營收",
        "美股季度／年度營收",
    ]
    LEGACY_US_HIGH_IMPACT_EVENTS = [
        "FOMC 利率決議", "美國 CPI", "美國大非農", "美國小非農 ADP", "美國初領失業金"
    ]
    US_HIGH_IMPACT_EVENTS = [
        "FOMC 利率決議", "美國 CPI", "美國核心 PCE", "美國 GDP",
        "美國 ISM 製造業指數", "美國大非農", "美國小非農 ADP", "美國初領失業金",
    ]
    US_HIGH_IMPACT_DESCRIPTIONS = {
        "FOMC 利率決議": "聯準會利率決策與政策聲明",
        "美國 CPI": "消費者物價指數，觀察通膨",
        "美國核心 PCE": "BEA 個人消費支出物價指數（排除食品與能源），Fed 重點通膨指標",
        "美國 GDP": "BEA 國內生產毛額的初值、修正值與終值",
        "美國 ISM 製造業指數": "ISM 製造業 PMI，觀察製造業景氣擴張或收縮",
        "美國大非農": "非農就業人數與失業率",
        "美國小非農 ADP": "ADP 民間就業預估",
        "美國初領失業金": "每週首次申請失業救濟人數",
    }
    TAIWAN_COMPANY_GROUP = "台股公司營收與財報"
    US_COMPANY_GROUP = "美股公司營收與財報"
    SETTLEMENT_REBALANCE_GROUP = "國際結算與指數調整"
    SETTLEMENT_REBALANCE_EVENTS = ["美股四巫日", "富台指期結算", "MSCI 季度調整"]
    SETTLEMENT_REBALANCE_DESCRIPTIONS = {
        "美股四巫日": "3／6／9／12 月衍生性商品集中到期；遇美股休市提前",
        "富台指期結算": "SGX 富台指期每月倒數第二個臺灣交易日結算",
        "MSCI 季度調整": "2／5／8／11 月最後臺灣交易日收盤執行指數調整",
    }
    LEGACY_CALENDAR_GROUP_OPTIONS = [
        "台股市場與休市", "美國高影響總經", TAIWAN_COMPANY_GROUP, US_COMPANY_GROUP,
    ]
    CALENDAR_GROUP_OPTIONS = LEGACY_CALENDAR_GROUP_OPTIONS + [SETTLEMENT_REBALANCE_GROUP]
    CALENDAR_GROUP_EVENTS = {
        "台股市場與休市": ["台股開休市", "台股突發休市公告"],
        "美國高影響總經": US_HIGH_IMPACT_EVENTS,
        TAIWAN_COMPANY_GROUP: ["台股公司財報", "台股月營收"],
        US_COMPANY_GROUP: ["美股公司財報", "美股季度／年度營收"],
        SETTLEMENT_REBALANCE_GROUP: SETTLEMENT_REBALANCE_EVENTS,
    }

    def normalize_calendar_preferences(saved=None):
        """將檔案或既有 Streamlit session 的舊版事件設定升級為目前格式。"""
        default_preferences = {
            "groups": CALENDAR_GROUP_OPTIONS.copy(),
            "macro_events": US_HIGH_IMPACT_EVENTS.copy(),
            "settlement_events": SETTLEMENT_REBALANCE_EVENTS.copy(),
            "tickers": "2330.TW",
        }
        if not isinstance(saved, dict):
            return default_preferences

        groups_were_saved = "groups" in saved
        raw_groups = saved.get("groups", [])
        if isinstance(raw_groups, str):
            raw_groups = [raw_groups]
        migrated_groups = []
        for item in raw_groups if isinstance(raw_groups, (list, tuple, set)) else []:
            if item in {"個股財報與營收", "公司財報／營收摘要"}:
                migrated_groups.extend([TAIWAN_COMPANY_GROUP, US_COMPANY_GROUP])
            else:
                migrated_groups.append(item)
        raw_groups = migrated_groups
        # 舊版若使用全部預設群組，自動加入新版的結算／季調群組；自訂群組則維持原設定。
        if (
            set(LEGACY_CALENDAR_GROUP_OPTIONS).issubset(set(raw_groups))
            and SETTLEMENT_REBALANCE_GROUP not in raw_groups
        ):
            raw_groups.append(SETTLEMENT_REBALANCE_GROUP)
        groups = list(dict.fromkeys(
            item for item in raw_groups
            if isinstance(item, str) and item in CALENDAR_GROUP_OPTIONS
        )) if isinstance(raw_groups, (list, tuple, set)) else []
        if not groups and not groups_were_saved:
            legacy_events = saved.get("events", [])
            if isinstance(legacy_events, str):
                legacy_events = [legacy_events]
            legacy_events = {
                item for item in legacy_events if isinstance(item, str)
            } if isinstance(legacy_events, (list, tuple, set)) else set()
            groups = [
                group for group, events in CALENDAR_GROUP_EVENTS.items()
                if legacy_events.intersection(events)
            ] or CALENDAR_GROUP_OPTIONS.copy()

        macro_events_were_saved = "macro_events" in saved
        raw_macro_events = saved.get("macro_events", saved.get("events", []))
        if isinstance(raw_macro_events, str):
            raw_macro_events = [raw_macro_events]
        raw_macro_event_set = set(raw_macro_events or [])
        # 舊版若採用完整預設清單，自動帶入新版新增的 GDP／核心 PCE／ISM；自訂子集合則維持原選擇。
        if set(LEGACY_US_HIGH_IMPACT_EVENTS).issubset(raw_macro_event_set):
            raw_macro_event_set.update(US_HIGH_IMPACT_EVENTS)
        macro_events = [
            item for item in US_HIGH_IMPACT_EVENTS if item in raw_macro_event_set
        ]
        if not macro_events and not macro_events_were_saved:
            macro_events = US_HIGH_IMPACT_EVENTS.copy()
        settlement_events_were_saved = "settlement_events" in saved
        raw_settlement_events = saved.get("settlement_events", saved.get("events", []))
        if isinstance(raw_settlement_events, str):
            raw_settlement_events = [raw_settlement_events]
        settlement_events = [
            item for item in SETTLEMENT_REBALANCE_EVENTS if item in set(raw_settlement_events or [])
        ]
        if not settlement_events and not settlement_events_were_saved:
            settlement_events = SETTLEMENT_REBALANCE_EVENTS.copy()
        tickers = saved.get("tickers", default_preferences["tickers"])
        if tickers is None:
            tickers = default_preferences["tickers"]
        return {
            "groups": groups,
            "macro_events": macro_events,
            "settlement_events": settlement_events,
            "tickers": str(tickers),
        }

    def load_calendar_preferences():
        if not os.path.exists(CAL_PREFERENCES_FILE):
            return normalize_calendar_preferences()
        try:
            with open(CAL_PREFERENCES_FILE, "r", encoding="utf-8") as file:
                saved = json.load(file)
            return normalize_calendar_preferences(saved)
        except (OSError, ValueError, TypeError):
            return normalize_calendar_preferences()

    def save_calendar_preferences(groups, macro_events, tickers, events, settlement_events=None):
        if settlement_events is None:
            settlement_events = st.session_state.get("calendar_preferences", {}).get(
                "settlement_events", SETTLEMENT_REBALANCE_EVENTS
            )
        try:
            _write_json_atomic(CAL_PREFERENCES_FILE, {
                "groups": groups,
                "macro_events": macro_events,
                "settlement_events": settlement_events,
                "events": events,
                "tickers": tickers,
                "calendar_events_version": 6,
            }, indent=2)
        except OSError:
            pass
    
    def load_cal_overrides():
        if os.path.exists(CAL_OVERRIDE_FILE):
            try:
                with open(CAL_OVERRIDE_FILE, "r", encoding="utf-8") as f:
                    df = pd.DataFrame(json.load(f))
                    if not df.empty and "日期" in df.columns:
                        df["日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.date
                        return df
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        
        # 預設給予一列空白資料，並確保日期欄位擁有正確的空值型別 (NaT)
        df = pd.DataFrame([{"日期": pd.NaT, "事件名稱": "", "文字顏色": "白色"}])
        df["日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.date
        return df
        
    def save_cal_overrides(df):
        try:
            # 確保日期格式為字串以利 JSON 儲存
            df_save = df.copy()
            df_save['日期'] = df_save['日期'].astype(str)
            _write_json_atomic(CAL_OVERRIDE_FILE, _json_safe(df_save.to_dict(orient="records")), indent=2)
        except (OSError, TypeError, ValueError):
            pass

    if 'cal_overrides' not in st.session_state:
        st.session_state.cal_overrides = load_cal_overrides()
    if 'calendar_preferences' not in st.session_state:
        st.session_state.calendar_preferences = load_calendar_preferences()
    else:
        # Streamlit Cloud 部署新版程式時會沿用既有 session，需在讀取欄位前即時遷移。
        st.session_state.calendar_preferences = normalize_calendar_preferences(st.session_state.calendar_preferences)
    if 'company_event_snapshot' not in st.session_state:
        st.session_state.company_event_snapshot = load_company_event_snapshot()
    else:
        st.session_state.company_event_snapshot = normalize_company_event_snapshot(st.session_state.company_event_snapshot)

    with st.expander("🛠️ 自訂與校正行事曆事件"):
        st.info("若發現系統預設日期或時間有誤，可在此手動新增或覆寫事件（例如：提前休市、自訂總經數據時間）。")
        edited_cal_df = st.data_editor(
            st.session_state.cal_overrides,
            num_rows="dynamic",
            column_config={
                "日期": st.column_config.DateColumn("日期 (YYYY-MM-DD)", required=True),
                "事件名稱": st.column_config.TextColumn("事件名稱", required=True),
                "文字顏色": st.column_config.SelectboxColumn(
                    "文字顏色",
                    options=["白色", "紅色", "綠色", "黃色", "藍色", "橘色", "紫紅色"],
                    default="白色"
                )
            },
            key="cal_override_editor",
            width='stretch'
        )
        
        if st.button("💾 儲存行事曆設定", key="btn_save_cal_override"):
            st.session_state.cal_overrides = edited_cal_df
            save_cal_overrides(edited_cal_df)
            st.toast("自訂行事曆已儲存！", icon="✅")
            st.rerun()
    st.markdown("---")

    with st.expander("🌐 網路同步與追蹤事件", expanded=False):
        st.caption("選擇要顯示的事件；公司資料只讀取獨立分頁的最近快照，不會在切換月份時重新查詢。")
        if 'calendar_event_groups' in st.session_state:
            migrated_widget_groups = normalize_calendar_preferences({
                'groups': st.session_state.calendar_event_groups,
            })['groups']
            # Widget state may only be migrated before the widget is created.
            # Reset stale legacy values so the normalized saved preference becomes its default.
            if migrated_widget_groups != st.session_state.calendar_event_groups:
                del st.session_state.calendar_event_groups
        selected_event_groups = st.multiselect(
            "追蹤類別",
            options=CALENDAR_GROUP_OPTIONS,
            default=st.session_state.calendar_preferences["groups"],
            key="calendar_event_groups",
        )

        if "美國高影響總經" in selected_event_groups:
            selected_macro_events = st.multiselect(
                "美國高影響總經（勾選要呈現的事件）",
                options=US_HIGH_IMPACT_EVENTS,
                default=st.session_state.calendar_preferences.get(
                    "macro_events", US_HIGH_IMPACT_EVENTS
                ),
                key="calendar_macro_events",
            )
            with st.expander("高影響總經包含什麼？", expanded=False):
                st.markdown("\n".join(
                    f"- **{event_name}**：{US_HIGH_IMPACT_DESCRIPTIONS[event_name]}"
                    for event_name in US_HIGH_IMPACT_EVENTS
                ))
        else:
            selected_macro_events = []

        if SETTLEMENT_REBALANCE_GROUP in selected_event_groups:
            selected_settlement_events = st.multiselect(
                "國際結算與指數調整（勾選要呈現的事件）",
                options=SETTLEMENT_REBALANCE_EVENTS,
                default=st.session_state.calendar_preferences.get(
                    "settlement_events", SETTLEMENT_REBALANCE_EVENTS
                ),
                key="calendar_settlement_events",
            )
            with st.expander("結算與季調包含什麼？", expanded=False):
                st.markdown("\n".join(
                    f"- **{event_name}**：{SETTLEMENT_REBALANCE_DESCRIPTIONS[event_name]}"
                    for event_name in SETTLEMENT_REBALANCE_EVENTS
                ))
        else:
            selected_settlement_events = []

        selected_event_types = []
        for group in selected_event_groups:
            if group not in {"美國高影響總經", SETTLEMENT_REBALANCE_GROUP}:
                selected_event_types.extend(CALENDAR_GROUP_EVENTS[group])
        selected_event_types.extend(selected_macro_events)
        selected_event_types.extend(selected_settlement_events)
        st.caption(
            "呈現模式：完整（只顯示已勾選事件）｜目前顯示：" + (
                "｜".join(selected_event_groups) if selected_event_groups else "未選擇自動事件"
            ) + (
                "｜總經：" + "、".join(selected_macro_events)
                if selected_macro_events else ""
            ) + (
                "｜結算季調：" + "、".join(selected_settlement_events)
                if selected_settlement_events else ""
            )
        )
        snapshot = st.session_state.company_event_snapshot
        if {TAIWAN_COMPANY_GROUP, US_COMPANY_GROUP}.intersection(selected_event_groups):
            if snapshot.get("updated_at"):
                st.caption(f"公司事件快照：{snapshot['updated_at']}｜追蹤：{snapshot.get('tickers', '—')}")
            else:
                st.info("尚無公司事件快照；請到「公司營收與財報」分頁同步一次。")
        update_col, save_col = st.columns(2)
        with update_col:
            if st.button("🔄 更新市場行事曆", key="refresh_calendar_network"):
                fetch_twse_holiday_events.clear()
                fetch_twse_temporary_closure_events.clear()
                fetch_fomc_events.clear()
                fetch_bls_cpi_events.clear()
                fetch_bea_release_schedule_html.clear()
                fetch_bea_core_pce_events.clear()
                fetch_bea_gdp_events.clear()
                fetch_ism_manufacturing_events.clear()
                fetch_bls_employment_events.clear()
                fetch_tradingview_us_calendar.clear()
                fetch_adp_employment_events.clear()
                build_us_initial_claims_events.clear()
                st.toast("已更新市場與總經行事曆；公司資料請在獨立分頁同步。", icon="🔄")
                st.rerun()
        with save_col:
            if st.button("💾 儲存追蹤設定", key="save_calendar_preferences"):
                st.session_state.calendar_preferences = {
                    "groups": selected_event_groups,
                    "macro_events": selected_macro_events,
                    "settlement_events": selected_settlement_events,
                    "tickers": st.session_state.calendar_preferences.get("tickers", "2330.TW"),
                }
                save_calendar_preferences(
                    selected_event_groups,
                    selected_macro_events,
                    st.session_state.calendar_preferences["tickers"],
                    selected_event_types,
                    selected_settlement_events,
                )
                st.toast("追蹤設定已儲存。", icon="✅")

    st.caption("行事曆資料來源：TWSE、Federal Reserve、U.S. BLS、U.S. BEA、ISM、U.S. Department of Labor、ADP、Cboe、SGX、MSCI，以及 TradingView 免費經濟日曆備援；公司營收與財報顯示獨立分頁的最近快照。")

    def change_month(delta):
        st.session_state.cal_month += delta
        if st.session_state.cal_month > 12:
            st.session_state.cal_month = 1
            st.session_state.cal_year += 1
        elif st.session_state.cal_month < 1:
            st.session_state.cal_month = 12
            st.session_state.cal_year -= 1
        
        if 'sel_year_box' in st.session_state: del st.session_state['sel_year_box']
        if 'sel_month_box' in st.session_state: del st.session_state['sel_month_box']

    col_sel_y, col_sel_m = st.columns(2)
    with col_sel_y:
        current_calendar_year = now_tw.year
        year_start = min(2024, st.session_state.cal_year, current_calendar_year - 2)
        year_end = max(current_calendar_year + 8, st.session_state.cal_year)
        year_options = list(range(year_start, year_end + 1))
        current_year_idx = year_options.index(st.session_state.cal_year)
        new_year = st.selectbox("年份", year_options, index=current_year_idx, key='sel_year_box')
        if new_year != st.session_state.cal_year:
            st.session_state.cal_year = new_year
            st.rerun()

    with col_sel_m:
        current_month_idx = st.session_state.cal_month - 1
        new_month = st.selectbox("月份", range(1, 13), index=current_month_idx, key='sel_month_box')
        if new_month != st.session_state.cal_month:
            st.session_state.cal_month = new_month
            st.rerun()

    sel_year = st.session_state.cal_year
    sel_month = st.session_state.cal_month

    col_prev, col_header, col_next = st.columns([1, 8, 1])
    with col_prev: st.button("◀️", on_click=change_month, args=(-1,), width='stretch')
    with col_next: st.button("▶️", on_click=change_month, args=(1,), width='stretch')
    with col_header: st.markdown(f"<div class='calendar-header'>{sel_year}/{sel_month:02}</div>", unsafe_allow_html=True)

    # 主分頁切換會觸發 rerun；只有行事曆實際開啟時才讀網路來源，
    # 避免股票／期貨按鈕每次 rerun 都連帶執行整套行事曆請求。
    calendar_network_active = bool(tab3.open)
    calendar_base_sources, calendar_base_errors = (
        fetch_calendar_base_sources(sel_year)
        if calendar_network_active else ({}, [])
    )
    calendar_last_success = st.session_state.setdefault(
        '_calendar_source_last_success', {}
    )
    calendar_year_cache = calendar_last_success.setdefault(str(sel_year), {})
    calendar_base_sources, retained_base_sources = merge_calendar_last_success(
        calendar_base_sources, calendar_year_cache.get('base', {}),
    )
    if any(calendar_base_sources.values()):
        calendar_year_cache['base'] = calendar_base_sources
    twse_holiday_events = calendar_base_sources.get('holidays', [])
    twse_temporary_events = calendar_base_sources.get('temporary', [])
    current_holidays = {
        (pd.Timestamp(event["date"]).month, pd.Timestamp(event["date"]).day): event["title"]
        for event in twse_holiday_events if event["closed"]
    }
    if not current_holidays:
        current_holidays = get_holidays(sel_year)
    temporary_closures = {
        pd.Timestamp(event["date"]).date(): event for event in twse_temporary_events
    }
    taiwan_closed_dates = {
        date(sel_year, month, day)
        for (month, day), holiday_name in current_holidays.items()
        if holiday_name != "封關日"
    }
    taiwan_closed_dates.update(temporary_closures.keys())

    # 將使用者選擇的資料來源合併成統一事件格式；台股突發休市永遠覆蓋交易日狀態。
    network_events = list(twse_temporary_events)
    calendar_source_counts = {}

    def add_network_source(label, events):
        event_list = list(events or [])
        calendar_source_counts[label] = len(event_list)
        network_events.extend(event_list)

    if calendar_network_active and "台股開休市" in selected_event_types:
        add_network_source('台股開休市', twse_holiday_events)
    selected_calendar_sources, selected_calendar_errors = (
        fetch_selected_calendar_sources(
            sel_year, selected_event_types, taiwan_closed_dates,
        ) if calendar_network_active else ({}, [])
    )
    selected_calendar_sources, retained_selected_sources = merge_calendar_last_success(
        selected_calendar_sources, calendar_year_cache.get('selected', {}),
    )
    if any(selected_calendar_sources.values()):
        calendar_year_cache['selected'] = selected_calendar_sources
    for source_label, source_events in selected_calendar_sources.items():
        add_network_source(source_label, source_events)
    if calendar_base_errors or selected_calendar_errors:
        logger.warning(
            'Calendar source refresh was partial: %s',
            calendar_base_errors + selected_calendar_errors,
        )
    if retained_base_sources or retained_selected_sources:
        st.caption(
            "部分行事曆來源暫時無回應，已沿用本次工作階段的最後成功資料："
            + "、".join(retained_base_sources + retained_selected_sources)
            + "。"
        )

    company_snapshot = normalize_company_event_snapshot(st.session_state.company_event_snapshot)
    st.session_state.company_event_snapshot = company_snapshot
    earnings_events = list(company_snapshot.get("earnings", {}).get("events", []))
    legacy_market_by_name = {}
    for resolved_text in company_snapshot.get("earnings", {}).get("resolved", []):
        match = re.search(r"→\s*(.*?)（([^（）]+)）", str(resolved_text))
        if match:
            legacy_market_by_name[match.group(1).strip()] = (
                "台股" if re.fullmatch(r"\d{4,6}\.(?:TW|TWO)", match.group(2).strip(), re.I)
                else "美股"
            )

    def company_earnings_for_market(market_name):
        matched = []
        for event in earnings_events:
            event_market = event.get("market")
            if not event_market:
                ticker = str(event.get("ticker", ""))
                if ticker:
                    event_market = (
                        "台股" if re.fullmatch(r"\d{4,6}\.(?:TW|TWO)", ticker, re.I) else "美股"
                    )
                else:
                    company_name = str(event.get("title", "")).replace(" 財報預估日", "").strip()
                    event_market = legacy_market_by_name.get(company_name)
            if event_market == market_name:
                matched.append(event)
        return matched

    if TAIWAN_COMPANY_GROUP in selected_event_groups:
        taiwan_earnings_events = company_earnings_for_market("台股")
        taiwan_revenue_events = list(company_snapshot.get("taiwan_revenue", {}).get("events", []))
        network_events.extend(taiwan_earnings_events)
        network_events.extend(taiwan_revenue_events)
        visible_company_events = [
            event for event in taiwan_earnings_events + taiwan_revenue_events
            if str(event.get('date', '')).startswith(f'{sel_year}-{sel_month:02d}-')
        ]
        st.caption(
            f"台股公司事件快照：財報 {len(taiwan_earnings_events)} 筆、月營收 {len(taiwan_revenue_events)} 筆；"
            f"本月顯示 {len(visible_company_events)} 筆。"
        )
        if not taiwan_earnings_events and not taiwan_revenue_events:
            st.info("目前快照沒有可顯示的台股財報／月營收；請到「公司營收與財報」按「同步公司資料」。")
    if US_COMPANY_GROUP in selected_event_groups:
        network_events.extend(company_earnings_for_market("美股"))
        network_events.extend(company_snapshot.get("us_revenue", {}).get("events", []))
    if "美國高影響總經" in selected_event_groups:
        source_labels_by_option = {
            "FOMC 利率決議": "FOMC",
            "美國 CPI": "CPI",
            "美國核心 PCE": "核心 PCE",
            "美國 GDP": "GDP",
            "美國 ISM 製造業指數": "ISM 製造業",
            "美國大非農": "大非農",
            "美國小非農 ADP": "小非農 ADP",
            "美國初領失業金": "初領失業金",
        }
        selected_source_labels = [
            source_labels_by_option[event_name]
            for event_name in selected_macro_events if event_name in source_labels_by_option
        ]
        us_count_text = '｜'.join(
            f"{label} {calendar_source_counts.get(label, 0)} 筆"
            for label in selected_source_labels
        )
        st.caption(f"{sel_year} 高影響事件來源檢查：{us_count_text}。事件已換算為台灣日期與時間。")
        missing_core_sources = [
            label for label in selected_source_labels
            if calendar_source_counts.get(label, 0) == 0
        ]
        if missing_core_sources:
            st.warning("以下資料來源本次未讀到資料：" + '、'.join(missing_core_sources) + "；可按『更新市場行事曆』重試。")

    def get_us_events(y, m):
        events = {}
        def add_evt(d, txt, color):
            if d not in events: events[d] = []
            events[d].append(f"<div style='color:{color}; font-size:0.8em; margin-top:2px; font-weight:bold;'>{txt}</div>")
        
        cal_temp = calendar.Calendar(firstweekday=6)
        month_days_list = list(cal_temp.itermonthdays2(y, m))
        
        # (1) 美股固定休市日期與節日對照表
        us_holiday_names = {
            2024: {
                (1,1): "元旦", (1,15): "馬丁路德金紀念日", (2,19): "總統日", (3,29): "耶穌受難日", 
                (5,27): "陣亡將士紀念日", (6,19): "六月節", (7,4): "獨立紀念日", (9,2): "勞動節", 
                (11,28): "感恩節", (12,25): "聖誕節"
            },
            2025: {
                (1,1): "元旦", (1,20): "馬丁路德金紀念日", (2,17): "總統日", (4,18): "耶穌受難日", 
                (5,26): "陣亡將士紀念日", (6,19): "六月節", (7,4): "獨立紀念日", (9,1): "勞動節", 
                (11,27): "感恩節", (12,25): "聖誕節"
            },
            2026: {
                (1,1): "元旦", (1,19): "馬丁路德金紀念日", (2,16): "總統日", (4,3): "耶穌受難日", 
                (5,25): "陣亡將士紀念日", (6,19): "六月節", (7,3): "獨立紀念日", (9,7): "勞動節", 
                (11,26): "感恩節", (12,25): "聖誕節"
            }
        }
        if y in us_holiday_names:
            for (hm, hd), name in us_holiday_names[y].items():
                if hm == m: 
                    add_evt(hd, f"美股{name}休市", "#B0BEC5")
        
        # (2) 美國非農人數公布 (自動判斷美股休市提前與夏令時間)
        for d, wd in month_days_list:
            if d != 0 and wd == 4:
                year_holidays = us_holiday_names.get(y, {})
                # 檢查該週五是否恰逢美股休市（例如 2026/7/3 獨立紀念日）
                if (m, d) in year_holidays:
                    # 提前至週四公布，且因必為夏令時間，固定顯示 20:30
                    add_evt(d - 1, "美國非農 (20:30)", "#00E5FF")
                else:
                    # 正常週五公布，依月份區分夏令(3~11月: 20:30) 與 冬令(12~2月: 21:30)
                    time_str = "20:30" if 3 <= m <= 11 else "21:30"
                    add_evt(d, f"美國非農 ({time_str})", "#00E5FF")
                break
                
        # (3) 四巫日 (3,6,9,12月 第三個週五)
        if m in [3, 6, 9, 12]:
            fridays = [d for d, wd in month_days_list if d != 0 and wd == 4]
            if len(fridays) >= 3:
                add_evt(fridays[2], "四巫日", "#FF00FF") # 洋紅色
                
        # (4) 13F報告 (通常在2,5,8,11月的14日或15日，以估算值為主)
        if m == 2: add_evt(14, "13F報告", "#FFD700") # 金黃色
        elif m == 5: add_evt(15, "13F報告", "#FFD700")
        elif m == 8: add_evt(14, "13F報告", "#FFD700")
        elif m == 11: add_evt(14, "13F報告", "#FFD700")
        
        # (5) MSCI季調 (2,5,8,11月最後一個交易日生效)
        if m in [2, 5, 8, 11]:
            weekdays = [d for d, wd in month_days_list if d != 0 and wd < 5]
            if weekdays:
                add_evt(weekdays[-1], "MSCI季調生效", "#FF9800") # 橘色

        # (6) FOMC (台灣時間，大多為週四凌晨 02:00 或 03:00，整理2024-2026日期)
        fomc_dates = {
            2024: [(2,1), (3,21), (5,2), (6,13), (8,1), (9,19), (11,8), (12,19)],
            2025: [(1,30), (3,20), (5,8), (6,19), (7,31), (9,18), (10,30), (12,11)],
            2026: [(1,29), (3,19), (5,7), (6,18), (7,30), (9,17), (10,29), (12,10)]
        }
        if y in fomc_dates:
            for fm, fd in fomc_dates[y]:
                if fm == m: 
                    time_str = "02:00" if 3 <= m <= 11 else "03:00"
                    add_evt(fd, f"FOMC ({time_str})", "#FF4500")
                
        # (7) 美國CPI公布 (台灣時間 20:30 或 21:30，整理2024-2026日期)
        cpi_dates = {
            2024: [(1,11), (2,13), (3,12), (4,10), (5,15), (6,12), (7,11), (8,14), (9,11), (10,10), (11,13), (12,11)],
            2025: [(1,15), (2,12), (3,12), (4,9), (5,14), (6,11), (7,16), (8,13), (9,10), (10,15), (11,12), (12,10)],
            2026: [(1,14), (2,11), (3,11), (4,15), (5,13), (6,10), (7,15), (8,12), (9,16), (10,15), (11,12), (12,16)]
        }
        if y in cpi_dates:
            for cm, cd in cpi_dates[y]:
                if cm == m: 
                    time_str = "20:30" if 3 <= m <= 11 else "21:30"
                    add_evt(cd, f"美國CPI ({time_str})", "#00FA9A")
        
        return events

    # 不再以程式內的固定日期作為主要來源；保留函數僅相容既有程式結構。
    us_events = {}
    network_event_dict = {}
    for event in network_events:
        try:
            event_date = parse_calendar_event_date(event.get("date"))
            if event_date and event_date.year == sel_year and event_date.month == sel_month:
                network_event_dict.setdefault(event_date, []).append(event)
        except (KeyError, TypeError, ValueError):
            continue

    def is_market_closed_func(d_date):
        if d_date.weekday() >= 5: return True
        if d_date in temporary_closures: return True
        name = current_holidays.get((d_date.month, d_date.day), "")
        if name and name != "封關日": return True
        return False

    real_settlements = {} 
    def calculate_month_settlements(y, m):
        cal_obj = calendar.Calendar(firstweekday=6)
        days_in_month = cal_obj.itermonthdays(y, m)
        d_list = [d for d in days_in_month if d != 0]
        
        w_count, f_count = 0, 0
        month_raw_wed, month_raw_fri = [], []
        
        for d in d_list:
            curr = date(y, m, d)
            if curr.weekday() == 2: w_count += 1; month_raw_wed.append((curr, w_count))
            if curr.weekday() == 4: f_count += 1; month_raw_fri.append((curr, f_count))
                
        monthly_raw = month_raw_wed[2][0] if len(month_raw_wed) >= 3 else None
        real_monthly_date = None
        if monthly_raw:
            check = monthly_raw
            while is_market_closed_func(check): check += timedelta(days=1)
            real_monthly_date = check
        
        local_results = []
        if monthly_raw: local_results.append((monthly_raw, 'M', f"{m:02}月", real_monthly_date))

        for dt, idx in month_raw_wed:
            if dt != monthly_raw: local_results.append((dt, 'W', f"{str(y)[2:]}{m:02}W{idx}", real_monthly_date))
        for dt, idx in month_raw_fri: local_results.append((dt, 'F', f"{str(y)[2:]}{m:02}F{idx}", real_monthly_date))
        return local_results

    current_month_data = calculate_month_settlements(sel_year, sel_month)
    if sel_month == 1: prev_y, prev_m = sel_year - 1, 12
    else: prev_y, prev_m = sel_year, sel_month - 1
    prev_month_data = calculate_month_settlements(prev_y, prev_m)
    
    all_raw_data = prev_month_data + current_month_data
    
    for raw_date, s_type, s_code, m_date in all_raw_data:
        check_date = raw_date
        while is_market_closed_func(check_date):
            check_date += timedelta(days=1)
            if (check_date - raw_date).days > 30: break
        
        if check_date.year == sel_year and check_date.month == sel_month:
            if s_type == 'F' and check_date == m_date: continue
            if check_date not in real_settlements: real_settlements[check_date] = []
            real_settlements[check_date].append((s_type, s_code))

    cal_obj = calendar.Calendar(firstweekday=6)
    month_days = cal_obj.monthdayscalendar(sel_year, sel_month)

    # --- 新增：將自訂事件整理為字典以利快速渲染 ---
    override_dict = {}
    if not st.session_state.cal_overrides.empty:
        for _, r in st.session_state.cal_overrides.iterrows():
            try:
                if pd.notna(r["日期"]) and str(r["日期"]).strip():
                    d_obj = pd.to_datetime(r["日期"]).date()
                    if d_obj.year == sel_year and d_obj.month == sel_month:
                        if d_obj not in override_dict:
                            override_dict[d_obj] = []
                        color_map = {"白色": "#FFFFFF", "紅色": "#FF4B4B", "綠色": "#00E676", "黃色": "#FFD700", "藍色": "#00E5FF", "橘色": "#FF9800", "紫紅色": "#FF00FF"}
                        raw_col = str(r["文字顏色"]).strip() if pd.notna(r["文字顏色"]) else "白色"
                        col = color_map.get(raw_col, "#FFFFFF")
                        event_name = html.escape(str(r["事件名稱"]))
                        override_dict[d_obj].append(
                            f"<div style='color:{col}; font-size:0.8em; margin-top:2px; font-weight:bold;'>{event_name}</div>"
                        )
            except (KeyError, TypeError, ValueError):
                continue
    def calendar_day_content(curr_date):
        """建立單日事件內容，桌機月曆與手機清單共用，避免兩種版面日期不一致。"""
        holiday_name = current_holidays.get((curr_date.month, curr_date.day), "")
        content_html = []
        if holiday_name:
            if holiday_name and holiday_name != "封關日": content_html.append(f"<div class='holiday-tag'>{html.escape(holiday_name)}</div>")
            if holiday_name == "封關日": content_html.append(f"<div style='color:#ff9800; font-size:0.8em;'>{html.escape(holiday_name)}</div>")

        for event in network_event_dict.get(curr_date, []):
            if event.get("closed") and not event.get("temporary"):
                continue
            if event.get("temporary"):
                detail = html.escape(str(event.get("detail", "")))
                content_html.append(
                    "<div style='background:#B71C1C;color:#fff;padding:3px;margin-top:3px;"
                    "font-size:.78em;font-weight:900;border-radius:3px'>"
                    f"{html.escape(str(event.get('title', '台股突發休市')))}</div>"
                )
                if detail:
                    content_html.append(f"<div style='color:#FFCDD2;font-size:.72em;margin-top:2px'>{detail}</div>")
            else:
                color = "#FF7043" if event.get("source") == "Federal Reserve" else "#00E5FF"
                if "大非農" in event.get("title", ""):
                    color = "#FF5252"
                elif "小非農" in event.get("title", ""):
                    color = "#FFB74D"
                elif event.get("impact") == "routine":
                    color = "#B0BEC5"
                elif "CPI" in event.get("title", "") or "核心 PCE" in event.get("title", ""):
                    color = "#00FA9A"
                elif "GDP" in event.get("title", ""):
                    color = "#42A5F5"
                elif "ISM" in event.get("title", ""):
                    color = "#AB47BC"
                elif "四巫日" in event.get("title", ""):
                    color = "#FF00FF"
                elif "富台" in event.get("title", ""):
                    color = "#FFB74D"
                elif "MSCI" in event.get("title", ""):
                    color = "#FF9800"
                if "財報" in event.get("title", ""):
                    color = "#FFD700"
                if event.get("market") == "台股" and isinstance(event.get("revenue"), dict):
                    revenue = event.get("revenue", {})
                    mom, yoy = str(revenue.get("mom", "--")), str(revenue.get("yoy", "--"))
                    content_html.append(
                        "<div style='font-size:.8em;margin-top:2px;font-weight:bold'>"
                        f"<span style='color:#00E676'>{html.escape(str(revenue.get('company', '月營收事件')))} 月營收</span> "
                        f"<span style='color:{_percent_color(mom)}'>MoM{html.escape(mom)}</span>／"
                        f"<span style='color:{_percent_color(yoy)}'>YoY{html.escape(yoy)}</span></div>"
                    )
                elif event.get("source") == "Yahoo Finance（季度／年度營收）":
                    revenue = event.get("revenue", {})
                    qoq, yoy = str(revenue.get("qoq", "--")), str(revenue.get("yoy", "--"))
                    content_html.append(
                        "<div style='font-size:.8em;margin-top:2px;font-weight:bold'>"
                        f"<span style='color:#FFD700'>{html.escape(str(revenue.get('company', '美股營收事件')))} 季營收</span> "
                        f"<span style='color:{_percent_color(qoq)}'>QoQ{html.escape(qoq)}</span>／"
                        f"<span style='color:{_percent_color(yoy)}'>YoY{html.escape(yoy)}</span></div>"
                    )
                else:
                    content_html.append(
                        f"<div style='color:{color};font-size:.8em;margin-top:2px;font-weight:bold'>"
                        f"{html.escape(str(event.get('title', '未命名事件')))}</div>"
                    )
                if event.get("source") == "Yahoo Finance":
                    time_note = str(event.get("detail", "")).split("；", 1)[0].replace("Yahoo Finance｜", "")
                    content_html.append(
                        f"<div style='color:#B0BEC5;font-size:.72em;margin-top:1px'>{html.escape(time_note)}</div>"
                    )

        content_html.extend(override_dict.get(curr_date, []))
        infos = sorted(real_settlements.get(curr_date, []), key=lambda item: 0 if item[0] == 'M' else 1)
        for settlement_type, settlement_code in infos:
            if settlement_type == 'M':
                content_html.append(f"<div class='settle-m'>台指期{settlement_code}結算<br>月選結算</div>")
            elif settlement_type == 'W':
                content_html.append(f"<div class='settle-w'>週選(三) {settlement_code}</div>")
            elif settlement_type == 'F':
                content_html.append(f"<div class='settle-f'>週選(五) {settlement_code}</div>")
        return ''.join(content_html)

    desktop_cells = ["<div class='calendar-week-head'>週</div>"]
    desktop_cells.extend(
        f"<div class='calendar-day-head'>{weekday}</div>" for weekday in ["日", "一", "二", "三", "四", "五", "六"]
    )
    mobile_cells = []
    weekday_names = ["週一", "週二", "週三", "週四", "週五", "週六", "週日"]
    def calendar_display_week(curr_date):
        """Sunday-start calendar rows use the following Monday's ISO week consistently."""
        if curr_date.weekday() == 6:
            curr_date += timedelta(days=1)
        return curr_date.isocalendar().week

    for week in month_days:
        first_valid_day = next((day for day in week if day), None)
        # 月曆從週日開始，但週次從週一開始；優先取該列週一作為週次基準。
        iso_anchor_day = week[1] if len(week) > 1 and week[1] else first_valid_day
        iso_week = calendar_display_week(date(sel_year, sel_month, iso_anchor_day))
        desktop_cells.append(f"<div class='cal-box cal-week'>W{iso_week}</div>")
        for day in week:
            if not day:
                desktop_cells.append("<div class='cal-box cal-empty'></div>")
                continue
            curr_date = date(sel_year, sel_month, day)
            day_content = calendar_day_content(curr_date)
            bg_class = "cal-closed" if is_market_closed_func(curr_date) else "cal-open"
            today_class = "today-border" if curr_date == now_tw.date() else ""
            desktop_cells.append(
                f"<div class='cal-box {bg_class} {today_class}'><div class='cal-date'>{day}</div>{day_content}</div>"
            )
            mobile_cells.append(
                f"<div class='calendar-mobile-day {bg_class} {today_class}'>"
                f"<div class='calendar-mobile-date'><span>{sel_month}/{day} {weekday_names[curr_date.weekday()]}</span>"
                f"<span class='calendar-mobile-week'>W{calendar_display_week(curr_date)}</span></div>"
                f"{day_content}</div>"
            )

    st.markdown(
        f"<div class='calendar-desktop-grid'>{''.join(desktop_cells)}</div>"
        f"<div class='calendar-mobile-list'>{''.join(mobile_cells)}</div>",
        unsafe_allow_html=True,
    )



with tab_company:
    st.markdown("""
    <style>
    .company-room-title {
        margin: 0.1rem 0 0.25rem;
        color: #f5f7fa;
        font-size: 1.35rem;
        line-height: 1.35;
        font-weight: 700;
    }
    </style>
    <div class='company-room-title'>🏢 公司營收與財報</div>
    """, unsafe_allow_html=True)
    st.caption("這個分頁採手動同步；只有按下按鈕時才查詢 MOPS／TWSE／Yahoo。行事曆只讀取完成後的摘要快照。")

    company_ticker_input = st.text_input(
        "追蹤公司或代碼（用逗號分隔，例如 2408, 台積電, META, Google, Tesla）",
        value=st.session_state.calendar_preferences.get("tickers", "2330.TW"),
        key="company_data_tickers",
    )
    preview_inputs = [item.strip() for item in company_ticker_input.split(",") if item.strip()]
    if preview_inputs:
        resolved_preview = [resolve_earnings_ticker(item) for item in preview_inputs]
        st.caption("辨識結果：" + "； ".join(
            f"{item['input']} → {item['display_name']}（{item['candidates'][0]}）" for item in resolved_preview
        ))

    sync_col, status_col = st.columns([1, 2.2])
    with sync_col:
        sync_company_data = st.button("🔄 同步公司資料", key="sync_company_financial_data", width="stretch")
    with status_col:
        current_snapshot = st.session_state.company_event_snapshot
        if current_snapshot.get("updated_at"):
            st.caption(f"最近同步：{current_snapshot['updated_at']}｜{current_snapshot.get('tickers', '')}")
        else:
            st.caption("尚未建立快照；行事曆目前不會顯示公司事件。")
    company_sync_notice = st.session_state.pop('_company_sync_notice', '')
    if company_sync_notice:
        st.warning(company_sync_notice)

    if sync_company_data:
        ticker_symbols = tuple(dict.fromkeys(item.upper() for item in preview_inputs))
        if not ticker_symbols:
            st.warning("請至少輸入一家公司或代碼。")
        else:
            if len(ticker_symbols) > COMPANY_SYNC_MAX_TICKERS:
                st.warning(
                    f"為避免雲端記憶體超載，本次只同步前 {COMPANY_SYNC_MAX_TICKERS} 家；"
                    "其餘請分批同步。"
                )
            fetch_earnings_events.clear()
            fetch_twse_monthly_revenue_rows.clear()
            fetch_mops_company_monthly_revenue.clear()
            fetch_finmind_monthly_revenue_rows.clear()
            fetch_cnyes_revenue_announcement_date.clear()
            fetch_google_news_revenue_announcement_date.clear()
            fetch_taiwan_monthly_revenue_events.clear()
            fetch_us_revenue_events.clear()
            with st.spinner("正在同步財報日期與營收資料；完成後行事曆會直接讀取快照……"):
                company_sections, company_sync_errors, used_tickers = (
                    fetch_company_event_sections(ticker_symbols)
                )
            previous_snapshot = normalize_company_event_snapshot(
                st.session_state.company_event_snapshot
            )
            earnings_result = company_sections.get(
                'earnings', previous_snapshot.get('earnings', {'events': []})
            )
            taiwan_revenue_result = company_sections.get(
                'taiwan_revenue', previous_snapshot.get('taiwan_revenue', {'events': []})
            )
            us_revenue_result = company_sections.get(
                'us_revenue', previous_snapshot.get('us_revenue', {'events': []})
            )
            combined_events = (
                earnings_result.get("events", [])
                + taiwan_revenue_result.get("events", [])
                + us_revenue_result.get("events", [])
            )
            new_snapshot = {
                "updated_at": datetime.now(pytz.timezone("Asia/Taipei")).strftime("%Y/%m/%d %H:%M"),
                "tickers": company_ticker_input,
                "events": combined_events,
                "earnings": earnings_result,
                "taiwan_revenue": taiwan_revenue_result,
                "us_revenue": us_revenue_result,
            }
            # Persisted corrections are supplied during sync; public-source
            # reconciliation still occurs inside the helper for stale values.
            new_snapshot['revenue_date_overrides'] = dict(
                st.session_state.company_event_snapshot.get('revenue_date_overrides', {})
            )
            new_snapshot = apply_revenue_announcement_date_overrides(new_snapshot)
            st.session_state.company_event_snapshot = new_snapshot
            # The date-input widgets are created later in this rerun. Clear their
            # previous values so a newly fetched 8/4 cannot keep displaying 8/3.
            for state_key in list(st.session_state.keys()):
                if str(state_key).startswith('revenue_date_'):
                    del st.session_state[state_key]
            company_sync_ok = save_company_event_snapshot(new_snapshot)
            st.session_state.calendar_preferences["tickers"] = company_ticker_input
            save_calendar_preferences(
                st.session_state.calendar_preferences.get("groups", CALENDAR_GROUP_OPTIONS),
                st.session_state.calendar_preferences.get("macro_events", US_HIGH_IMPACT_EVENTS),
                company_ticker_input,
                selected_event_types,
            )
            if company_sync_errors:
                st.session_state['_company_sync_notice'] = (
                    "部分來源未完成，已保留該區塊上次成功資料："
                    + "；".join(company_sync_errors)
                )
            st.toast(
                f"公司資料已同步 {len(used_tickers)} 家，行事曆摘要快照已更新。"
                + ("已同步 Google Sheet。" if company_sync_ok and get_app_secret('gsheet_api_url') else ""),
                icon="✅",
            )
            st.rerun()

    st.markdown("""
    <style>
    .revenue-metric-card { padding: 0.15rem 0 0.55rem; min-height: 6rem; }
    .revenue-metric-label { font-size: 0.78rem; font-weight: 700; color: #F5F5F5; }
    .revenue-metric-value { font-size: 1.55rem; line-height: 1.45; font-weight: 650; color: #FFFFFF; white-space: nowrap; }
    .revenue-metric-delta { display: inline-block; margin-top: 0.18rem; font-size: 0.72rem; font-weight: 750; background: rgba(128,128,128,.16); border-radius: .6rem; padding: .08rem .34rem; }
    </style>
    """, unsafe_allow_html=True)
    snapshot = st.session_state.company_event_snapshot
    revenue_events = list(snapshot.get('taiwan_revenue', {}).get('events', []))
    if revenue_events:
        with st.expander("🗓️ 月營收公告日校正（可直接修改日期）", expanded=False):
            st.caption(
                "MOPS 單一公司頁未提供可驗證的公告日；此處填入公司官網／公告確認的日期後，"
                "再按下方套用按鈕，行事曆與下次同步都會沿用校正值。"
            )
            correction_values = {}
            with st.form('revenue_announcement_date_form'):
                for event in revenue_events:
                    revenue = event.get('revenue', {}) if isinstance(event.get('revenue'), dict) else {}
                    code = str(event.get('ticker', '')).strip()
                    revenue_month = str(revenue.get('revenue_month', '')).strip()
                    company = str(revenue.get('company', event.get('title', ''))).strip()
                    correction_key = f"{code}:{revenue_month}"
                    current_date = parse_calendar_event_date(event.get('date')) or date.today()
                    info_col, date_col = st.columns([3, 2], vertical_alignment='center')
                    info_col.markdown(
                        f"**{company}（{code}）**｜營收月份 `{revenue_month}`｜"
                        f"目前：`{current_date.isoformat()}`"
                    )
                    correction_values[correction_key] = date_col.date_input(
                        f"{company}實際公告日",
                        value=current_date,
                        key=f"revenue_date_{code}_{revenue_month}",
                        label_visibility='collapsed',
                    )
                apply_revenue_dates = st.form_submit_button(
                    '套用實際公告日到行事曆', width='stretch',
                )
            if apply_revenue_dates:
                corrections = {
                    key: value.isoformat()
                    for key, value in correction_values.items()
                    if isinstance(value, date)
                }
                corrected_snapshot = apply_revenue_announcement_date_overrides(snapshot, corrections)
                corrected_snapshot['updated_at'] = datetime.now(
                    pytz.timezone('Asia/Taipei')
                ).strftime('%Y/%m/%d %H:%M:%S')
                st.session_state.company_event_snapshot = corrected_snapshot
                cloud_saved = save_company_event_snapshot(corrected_snapshot)
                if get_app_secret('gsheet_api_url') and not cloud_saved:
                    st.session_state['_revenue_date_save_notice'] = (
                        '日期已套用於目前程式，但 Google Sheet 同步失敗，請稍後再按一次。'
                    )
                else:
                    st.session_state['_revenue_date_save_notice'] = (
                        '已套用實際公告日，行事曆會依校正日期顯示。'
                    )
                st.rerun()
            notice = st.session_state.pop('_revenue_date_save_notice', '')
            if notice:
                st.success(notice)
    summary_cols = st.columns(3)
    summary_cols[0].metric("財報事件", len(snapshot.get("earnings", {}).get("events", [])))
    summary_cols[1].metric("台股月營收", len(snapshot.get("taiwan_revenue", {}).get("events", [])))
    summary_cols[2].metric("美股營收", len(snapshot.get("us_revenue", {}).get("events", [])))
    render_company_event_snapshot(snapshot)
