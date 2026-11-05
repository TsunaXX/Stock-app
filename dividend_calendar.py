"""Dividend calendar normalization; never infer an unannounced payment date."""
import re
from datetime import date
from decimal import Decimal, InvalidOperation

KINDS = ('除息日', '股息發放日')


def codes(text):
    result = []
    for value in re.split(r'[,，\s]+', str(text).strip()):
        if not value:
            continue
        value = re.sub(r'\.(TW|TWO)$', '', value.upper())
        if not re.fullmatch(r'\d{4,6}[A-Z]?', value):
            raise ValueError('請輸入台灣股票或 ETF 代碼，以逗號分隔。')
        if value not in result:
            result.append(value)
    if len(result) > 30:
        raise ValueError('每份清單最多追蹤 30 檔。')
    return result


def announced_date(value):
    parts = re.findall(r'\d+', str(value or ''))
    if len(parts) == 1 and len(parts[0]) in (7, 8):
        raw = parts[0]
        parts = [raw[:-4], raw[-4:-2], raw[-2:]]
    if len(parts) != 3:
        return ''
    year, month, day = map(int, parts)
    if year < 1911:
        year += 1911
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return ''


def events(code, name, period, ex, payment, amount, source, url):
    try:
        amount = Decimal(str(amount).replace(',', ''))
        amount = format(amount.normalize(), 'f') if amount.is_finite() and amount > 0 else ''
    except (InvalidOperation, ValueError):
        amount = ''
    result = []
    for kind, raw in zip(KINDS, (ex, payment)):
        stamp = announced_date(raw)
        if stamp:
            result.append({'event_id':f'dividend:{code}:{period}:{kind}', 'ticker':code,
                           'date':stamp, 'kind':kind, 'title':f'{name}({code}) {kind}' + (f' {amount}元' if amount else ''),
                           'detail':f'{source}｜每股／單位 {amount or "待公告"}元',
                           'source':source,'source_url':url,'closed':False,'category':'dividends'})
    return result


def etf_events(payload, url):
    if not isinstance(payload, dict) or payload.get('status') != 'ok' or not isinstance(payload.get('data'), list):
        raise ValueError('ETF 官方來源格式錯誤')
    result = []
    for row in payload['data']:
        if not isinstance(row, list) or len(row) < 6:
            raise ValueError('ETF 官方欄位不足')
        code, name, ex, basis, payment, amount = row[:6]
        period = announced_date(basis) or announced_date(ex)
        if period:
            result.extend(events(str(code), name, period, ex, payment, amount, 'TWSE ETF 官方公告', url))
    return result


def finmind_events(payload, code, name):
    if not isinstance(payload, dict) or payload.get('status') != 200 or not isinstance(payload.get('data'), list):
        raise ValueError('股利備援來源未就緒')
    result = []
    for row in payload['data']:
        if str(row.get('stock_id')) != code:
            continue
        amount = Decimal(str(row.get('CashEarningsDistribution') or 0)) + Decimal(str(row.get('CashStatutorySurplus') or 0))
        period = row.get('year') or row.get('CashExDividendTradingDate')
        result.extend(events(code, name, period, row.get('CashExDividendTradingDate'),
                             row.get('CashDividendPaymentDate'), amount, 'FinMind 股利備援',
                             'https://finmind.github.io/tutor/TaiwanMarket/Fundamental/'))
    return result


def merge_events(previous, incoming):
    """Each period/kind is independent; failed sources cannot erase history."""
    merged = {e['event_id']:e for e in previous if isinstance(e, dict) and e.get('event_id')}
    for event in incoming:
        if event.get('fetched_at','') >= merged.get(event['event_id'],{}).get('fetched_at',''):
            merged[event['event_id']] = event
    return list(merged.values())


def snapshot(value):
    value = value if isinstance(value, dict) else {}
    selected = codes(','.join(value.get('codes', [])))
    items = value.get('events', [])
    return {**value, 'codes':selected,
            'kinds':[kind for kind in value.get('kinds', KINDS) if kind in KINDS],
            'events':[e for e in items if isinstance(e,dict) and e.get('event_id')
                      and e.get('kind') in KINDS and announced_date(e.get('date'))]}


def selected_events(items, selected, kinds):
    # Prefer the official ETF record when a fallback reports the same date/kind.
    result = {}
    for event in sorted(items, key=lambda e: '官方' in e.get('source', '')):
        if event.get('ticker') in selected and event.get('kind') in kinds:
            result[(event['ticker'], event['date'], event['kind'])] = event
    return list(result.values())
