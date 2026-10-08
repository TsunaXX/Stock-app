"""Evidence-based simulation on existing quotes; one local writer, atomic cloud merge."""
import hashlib
import io
import json
import math
import queue
import sqlite3
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = 'V1.0'
TZ = ZoneInfo('Asia/Taipei')
ACTIVE = {'等待進場', '模擬持倉', '監控中斷', '資料不足'}
DEFAULT_COSTS = {'stock_shares': 1000, 'stock_discount': 2.8, 'stock_min_fee': 20,
                 'futures_contracts': 1, 'futures_fee': 50, 'slippage_ticks': 1}


def normalize_costs(values):
    values = values if isinstance(values,dict) else {}
    result = {}
    for key,default in DEFAULT_COSTS.items():
        value = number(values.get(key, default))
        minimum = .1 if key == 'stock_discount' else (1 if key in ('stock_shares','futures_contracts') else 0)
        maximum = 10 if key == 'stock_discount' else 10000000
        value = value if value is not None and minimum <= value <= maximum else default
        result[key] = value if key == 'stock_discount' else int(value)
    return result


def stamp(value):
    try:
        dt = pd.Timestamp(value)
        if pd.isna(dt):
            return None
        return dt.tz_localize('Asia/Taipei') if dt.tzinfo is None else dt.tz_convert('Asia/Taipei')
    except (ValueError, TypeError, OverflowError):
        return None


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def trade_id(signal):
    # Decimal spellings and different device receipt times cannot change identity.
    fields = [signal.get(k, '') for k in ('交易日', '市場', '商品鍵', '策略', '策略版本', '方向')]
    fields += [format(float(signal[k]), '.10g') for k in ('進場價', '停損價', '目標價')]
    return hashlib.sha256('|'.join(map(str, fields)).encode()).hexdigest()


def merge_trades(remote, local):
    merged = {}
    for row in list(remote or []) + list(local or []):
        if not isinstance(row, dict) or not row.get('交易ID'):
            continue
        row = dict(row)
        old = merged.get(row['交易ID'])
        if old:
            incoming = row
            newer = str(incoming.get('來源時間', '')) > str(old.get('來源時間', ''))
            terminal = incoming.get('狀態') == '已平倉'
            old_terminal = old.get('狀態') == '已平倉'
            if terminal and old_terminal:
                chosen = min((old, incoming), key=lambda r: (bool(r.get('資料缺口')), str(r.get('結案時間', r.get('來源時間',''))), str(r.get('進場時間',''))))
            else:
                chosen = incoming if (newer and not old_terminal) or (terminal and not old_terminal) else old
            row = dict(chosen)
            row['資料缺口'] = bool(row.get('資料缺口')) if row.get('狀態') == '已平倉' else bool(old.get('資料缺口') or incoming.get('資料缺口'))
            if old.get('模擬進場價') is not None and incoming.get('模擬進場價') is not None and old['模擬進場價'] != incoming['模擬進場價']:
                row['資料缺口'] = True
                row['異常原因'] = '跨裝置模擬成交證據不一致，排除有效績效'
            if old.get('成本設定') != incoming.get('成本設定'):
                row['資料缺口'] = True
                row['異常原因'] = '跨裝置成本設定不一致，排除有效績效'
            first = min((old, incoming), key=lambda r: str(r.get('建立時間', '')))
            for key in ('建立時間', '指標快照', '成本設定', '訊號價', '市場環境', '觸發條件', '條件快照'):
                if key in first:
                    row[key] = first[key]
            events = {e['事件ID']: e for item in (old, incoming) for e in item.get('事件', []) if e.get('事件ID')}
            row['事件'] = sorted(events.values(), key=lambda e: e['時間'])
        merged[row['交易ID']] = row
    return list(merged.values())


def event(record, state, ts, reason=''):
    record['狀態'] = state
    record['來源時間'] = ts
    record['異常原因'] = reason
    evidence = '|'.join([record['交易ID'], state, ts, reason])
    eid = hashlib.sha256(evidence.encode()).hexdigest()
    if not any(e.get('事件ID') == eid for e in record.setdefault('事件', [])):
        record['事件'].append({'事件ID': eid, '時間': ts, '狀態': state, '原因': reason})


def new_trade(signal):
    entry,stop,target = (number(signal.get(k)) for k in ('進場價','停損價','目標價'))
    long = signal.get('方向') in ('多頭','偏多')
    if None in (entry,stop,target) or min(entry,stop,target) <= 0 or not (stop < entry < target if long else target < entry < stop):
        raise ValueError('模擬計畫缺少有效進場、停損或目標')
    if signal.get('方向') not in ('多頭','偏多','空頭','偏空') or signal.get('策略') not in ('當沖','波段') or not signal.get('策略版本') or stamp(signal.get('交易日')) is None:
        raise ValueError('訊號方向、策略版本或交易日無效')
    if not signal.get('商品鍵') or signal.get('市場') not in ('股票','期貨') or stamp(signal.get('來源時間')) is None:
        raise ValueError('訊號商品或來源時間無效')
    row = {**signal, '交易ID': trade_id(signal), '紀錄類型': '自動', '資料缺口': False,
           'MFE(R)': 0.0, 'MAE(R)': 0.0, '事件': []}
    row['成本設定'] = normalize_costs(row.get('成本設定'))
    row['建立時間'] = row['來源時間']
    event(row, '等待進場', row['來源時間'])
    return row


def advance(record, quote):
    """Only ordered observed prices establish fills. Gaps never count as verified wins."""
    row = json.loads(json.dumps(record, ensure_ascii=False))
    if row['狀態'] not in ACTIVE:
        return row
    ts, prior = stamp(quote.get('來源時間')), stamp(row.get('來源時間'))
    price = number(quote.get('價格'))
    if ts is None or (prior is not None and ts < prior):
        return row
    if prior is not None and ts == prior and row['狀態'] != '等待進場' and quote.get('序號') == row.get('最後序號'):
        return row
    text = ts.isoformat()
    stream = quote.get('串流識別')
    old_stream = row.get('串流識別')
    if old_stream is not None and stream is not None and old_stream != stream and (old_stream.split(':')[0] == stream.split(':')[0] or not quote.get('銜接確認')):
        row['資料缺口'] = True
        event(row, '監控中斷', text, '串流重新連線；中斷區間未確認')
    if stream is not None:
        row['串流識別'] = stream
    seq = quote.get('序號')
    old_seq = row.get('串流序號', {}).get(stream) if stream else row.get('最後序號')
    if seq is not None and old_seq is not None and seq > old_seq + 1:
        row['資料缺口'] = True
        event(row, '監控中斷', text, '成交證據佇列缺漏，不能確認停利停損先後')
    if seq is not None:
        row['最後序號'] = seq
        if stream:
            row.setdefault('串流序號', {})[stream] = seq
    if not quote.get('有效', False) or price is None or price <= 0:
        if row['狀態'] != '資料不足':
            row['資料缺口'] = True
            event(row, '資料不足', text, '行情過期、來源失效或成交證據不足')
        return row
    gap_limit = max(30, float(quote.get('更新秒數', 5)) * 3)
    rest_bridge = row.get('策略') == '波段' and quote.get('休市銜接') and stream == old_stream and seq is not None and old_seq is not None and seq == old_seq + 1
    if prior is not None and (ts - prior).total_seconds() > gap_limit and not rest_bridge:
        row['資料缺口'] = True
        event(row, '監控中斷', text, '兩次可驗證行情之間有缺口；不回填當時進出場')
    if row['狀態'] in ('監控中斷', '資料不足'):
        event(row, '模擬持倉' if row.get('模擬進場價') is not None else '等待進場', text,
              '恢復追蹤；缺口仍保留並排除有效績效')
    if row['策略'] == '當沖' and row['交易日'] != quote.get('交易日'):
        row['資料缺口'] = bool(row.get('模擬進場價') is not None or row.get('資料缺口'))
        event(row, '訊號失效', text, '當沖交易日已結束；缺少收盤成交證據時不估算損益')
        return row
    long = row['方向'] in ('多頭', '偏多')
    sign = 1 if long else -1
    entry, stop, target = (float(row[k]) for k in ('進場價', '停損價', '目標價'))
    tick, multiplier = number(row.get('跳動點')), number(row.get('乘數'))
    if tick is None or tick <= 0 or multiplier is None or multiplier <= 0:
        event(row, '資料不足', text, '缺少有效跳動點或契約乘數，無法驗證成本')
        row['資料缺口'] = True
        return row
    slip = tick * row['成本設定']['slippage_ticks']
    if row['狀態'] == '等待進場':
        invalid = price <= stop if long else price >= stop
        if invalid or (row.get('策略') == '當沖' and str(row.get('交易日')) != str(quote.get('交易日'))):
            event(row, '訊號失效', text, '未進場前已越過停損或交易日已結束')
            return row
        if sign * (price - entry) < 0:
            row['來源時間'] = text
            return row
        fill = price + sign * slip
        if sign * (target - fill) <= 0:
            event(row, '訊號失效', text, '首個成交證據已越過目標；不得回填計畫價成交')
            return row
        row['模擬進場價'], row['進場時間'], row['最新價'] = fill, text, price
        event(row, '模擬持倉', text)
        return row  # The entry sample cannot also prove a later exit.
    fill = row['模擬進場價']
    risk = abs(fill - stop)
    r = sign * (price - fill) / risk if risk else 0
    row['MFE(R)'] = max(row['MFE(R)'], r)
    row['MAE(R)'] = max(row['MAE(R)'], -r)
    row['來源時間'], row['最新價'] = text, price
    stopped = price <= stop if long else price >= stop
    targeted = price >= target if long else price <= target
    # OHLC extrema never establish the order of stop and target.
    high, low = number(quote.get('最高')), number(quote.get('最低'))
    ambiguous = high is not None and low is not None and high >= max(stop, target) and low <= min(stop, target)
    if ambiguous:
        row['資料缺口'] = True
        event(row, '監控中斷', text, '同一根 K 同時觸及停利停損，先後順序未確認')
        return row
    cutoff = ts.time()
    reached_close = (cutoff.hour == 13 and cutoff.minute >= (30 if row['市場'] == '股票' else 45)) or (row['市場'] == '期貨' and cutoff.hour == 5)
    close_session = quote.get('收盤確認', False) and reached_close and row['策略'] == '當沖'
    if stopped or targeted or close_session:
        exit_price = price - sign * slip
        costs = row['成本設定']
        qty = costs['stock_shares'] if row['市場'] == '股票' else costs['futures_contracts']
        gross = sign * (exit_price - fill) * multiplier * qty
        if row['市場'] == '股票':
            fee = sum(max(costs['stock_min_fee'], math.floor(p * qty * .001425 * costs['stock_discount'] / 10))
                      for p in (fill, exit_price))
            sell = exit_price if long else fill
            tax = math.floor(sell * qty * (.0015 if row['策略'] == '當沖' else .003))
        else:
            fee = costs['futures_fee'] * qty * 2
            rate = row.get('期交稅率')
            if rate is None:
                event(row, '資料不足', text, '此商品未確認期交稅率')
                row['資料缺口'] = True
                return row
            tax = sum(round(p * multiplier * qty * rate) for p in (fill, exit_price))
        row.update({'模擬出場價': exit_price, '結案時間': text, '毛損益': round(gross, 2),
                    '交易成本': round(fee + tax, 2), '模擬損益': round(gross - fee - tax, 2),
                    '結果(R)': round(sign * (exit_price - fill) / risk, 4),
                    '出場原因': '停損' if stopped else ('停利' if targeted else '收盤')})
        event(row, '已平倉', text)
    return row


def performance(records, state=None):
    data = pd.DataFrame(records)
    closed = [r for r in records if r.get('狀態') == '已平倉' and not r.get('資料缺口')
              and number(r.get('模擬損益')) is not None]
    closed.sort(key=lambda r: (r.get('結案時間', ''), r.get('交易ID', '')))
    # Only newly completed trades are folded. Cloud corrections/removals rebuild this filtered view.
    state = {} if state is None else state
    fingerprints = [json.dumps({k:r.get(k) for k in ('交易ID','結案時間','模擬損益','市場','策略','策略版本','方向','市場環境','指標快照','條件快照')}, ensure_ascii=False, sort_keys=True) for r in closed]
    previous = state.get('fingerprints', [])
    if fingerprints[:len(previous)] != previous:
        state.clear()
        previous = []
    state.setdefault('wins', 0); state.setdefault('losses', 0)
    state.setdefault('profit', 0.); state.setdefault('loss', 0.)
    state.setdefault('equity', 0.); state.setdefault('peak', 0.); state.setdefault('drawdown', 0.)
    state.setdefault('groups', {})
    added = closed[len(previous):]
    for r in added:
        value = float(r['模擬損益'])
        state['wins'] += value > 0; state['losses'] += value < 0
        state['profit'] += max(0, value); state['loss'] += min(0, value)
        state['equity'] += value
        state['peak'] = max(state['peak'], state['equity'])
        state['drawdown'] = max(state['drawdown'], state['peak'] - state['equity'])
        labels = [{k:r.get(k, '未取得') for k in keys} for keys in (('市場','策略','策略版本'), ('方向','市場環境'))]
        labels += [{'指標':k} for k,v in r.get('指標快照', {}).items() if v is not None]
        labels += [{'進場條件':k,'判定':'符合' if v else '未符合'} for k,v in r.get('條件快照', {}).items() if isinstance(v,bool)]
        for label in labels:
            key = json.dumps(label, ensure_ascii=False, sort_keys=True)
            group = state['groups'].setdefault(key, {**label, '有效交易數':0, '獲勝':0, '累積模擬損益':0.})
            group['有效交易數'] += 1; group['獲勝'] += value > 0; group['累積模擬損益'] += value
    state['fingerprints'], state['added'] = fingerprints, len(added)
    count, wins, losses = len(closed), state['wins'], state['losses']
    metrics = {'有效交易數': count, '獲勝': wins, '虧損': losses,
               '勝率(%)': wins / count * 100 if count else None,
               '累積模擬損益': state['equity'], '平均獲利': state['profit'] / wins if wins else None,
               '平均虧損': state['loss'] / losses if losses else None,
               '獲利因子': state['profit'] / abs(state['loss']) if losses else None,
               '每筆期望值': state['equity'] / count if count else None, '最大回撤': state['drawdown'],
               '訊號有效率(%)': count / len(records) * 100 if records else None,
               '資料完整率(%)': sum(not r.get('資料缺口', False) for r in records) / len(records) * 100 if records else None}
    groups = [{**g, '勝率(%)':g['獲勝']/g['有效交易數']*100} for g in state['groups'].values()]

    return metrics, pd.DataFrame(groups), data


def excel_report(records):
    metrics, groups, data = performance(records)
    events = [{**e, '交易ID': r['交易ID']} for r in records for e in r.get('事件', [])]
    for key in ('指標快照', '成本設定', '事件', '條件快照'):
        if key in data:
            data[key] = data[key].map(lambda x: json.dumps(x, ensure_ascii=False))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for name, frame in [('總覽', pd.DataFrame([metrics])), ('策略與指標', groups),
                            ('歷史明細', data), ('事件', pd.DataFrame(events))]:
            frame.to_excel(writer, sheet_name=name, index=False)
            sheet = writer.sheets[name]
            sheet.freeze_panes = 'A2'
            sheet.auto_filter.ref = sheet.dimensions
            for column in sheet.columns:
                sheet.column_dimensions[column[0].column_letter].width = min(45, max(12, len(str(column[0].value or '')) * 2 + 4))
                for cell in column:
                    if cell.data_type == 'f':
                        cell.data_type = 's'  # Names/conditions are data, never executable Excel formulas.
    return output.getvalue()


class ModelTracker:
    """One bounded writer per app process; SQLite outbox survives restart, monthly cloud scopes merge."""
    def __init__(self, path, url='', sync_seconds=30, start=True, market_open=None):
        self.path, self.url, self.sync_seconds = str(path), url, sync_seconds
        self.market_open = market_open or (lambda row: True)
        self.queue = queue.Queue(maxsize=512)
        self.status = '尚未同步' if url else '僅本機保存；尚未設定 Google Sheet'
        self.synced_at = None
        self.error = ''
        self.revision = 0
        self.closed = False
        self.overloaded = False
        self.last_activity = time.monotonic()
        self.last_gap_check = 0
        self.leases = {}
        self.primary_streams = {}
        self.source_quotes = {}
        self.last_sync = 0
        self.month_versions = {}
        self.hydrated = not bool(url)
        with self.db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, month TEXT, slot TEXT, state TEXT, body TEXT, dirty INTEGER)')
            db.execute('CREATE INDEX IF NOT EXISTS trades_month ON trades(month)')
            db.execute('CREATE INDEX IF NOT EXISTS trades_state ON trades(state,slot)')
            for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉')").fetchall():
                row = json.loads(raw[0])
                previous_time = row['來源時間']
                row['資料缺口'] = True
                event(row, '監控中斷', datetime.now(TZ).isoformat(), '服務重新啟動；未確認區間不回填')
                row['來源時間'] = previous_time
                self.store(db, row)
        self.thread = None
        if start:
            self.thread = threading.Thread(target=self.run, name='model-tracker', daemon=True)
            self.thread.start()

    def db(self):
        return sqlite3.connect(self.path, timeout=5)

    def submit(self, signals, quotes):
        self.last_activity = time.monotonic()
        for key, quote in quotes.items():
            self.leases[key] = (self.last_activity, max(30, float(quote.get('更新秒數',5)) * 3))
        try:
            self.queue.put_nowait((signals, quotes))
            return True
        except queue.Full:
            self.overloaded = True
            self.error = '記錄佇列已滿；保留資料缺口，不阻塞行情'
            return False

    def records(self, start=None, end=None):
        with self.db() as db:
            rows = db.execute('SELECT body FROM trades WHERE month >= ? AND month <= ? ORDER BY month,id',
                              (start or '000000', end or '999999')).fetchall()
        return [json.loads(r[0]) for r in rows]

    def store(self, db, row, dirty=1):
        db.execute('INSERT INTO trades VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,body=excluded.body,dirty=MAX(trades.dirty,excluded.dirty)',
                   (row['交易ID'], row['交易日'][:7].replace('-', '').replace('/', ''),
                    '|'.join(str(row.get(k,'')) for k in ('商品鍵','市場','策略','策略版本','方向')),
                    row['狀態'], json.dumps(row, ensure_ascii=False), dirty))

    def process(self, signals, quotes):
        selected = {}
        now = time.monotonic()
        for key, quote in quotes.items():
            stream = quote.get('串流識別')
            if not stream:
                selected[key] = quote
                continue
            sources = self.source_quotes.setdefault(key, {})
            sources[stream] = (now, quote)
            for old in list(sources):
                if now - sources[old][0] > 120:
                    sources.pop(old)
            primary = self.primary_streams.get(key, stream)
            if primary.split(':')[0] == stream.split(':')[0] and primary != stream:
                primary = stream  # Same connection has a new transport/buffer epoch.
            chosen = sources.get(primary)
            if chosen is None or not chosen[1].get('有效') or now - chosen[0] > 30:
                candidates = [(name, value) for name,value in sources.items() if value[1].get('有效') and now-value[0] <= 30]
                if candidates:
                    primary, chosen = max(candidates, key=lambda item: str(item[1][1].get('來源時間','')))
            self.primary_streams[key] = primary
            selected[key] = chosen[1] if chosen else quote
        quotes = selected
        with self.db() as db:
            if self.overloaded:
                for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉','監控中斷','資料不足')").fetchall():
                    row = json.loads(raw[0])
                    row['資料缺口'] = True
                    event(row, '監控中斷', datetime.now(TZ).isoformat(), '記錄佇列滿載，成交證據未確認')
                    self.store(db, row)
                self.overloaded = False
            for signal in signals:
                row = new_trade(signal)
                existing = db.execute('SELECT body FROM trades WHERE id=?', (row['交易ID'],)).fetchone()
                # One active simulation per instrument/mode/direction/version; a changing plan is not a new fill.
                slot = '|'.join(str(row.get(k,'')) for k in ('商品鍵','市場','策略','策略版本','方向'))
                open_rows = db.execute("SELECT body FROM trades WHERE slot=? AND state IN ('等待進場','模擬持倉','監控中斷','資料不足')", (slot,)).fetchall()
                same_open = any(r.get('狀態') in ACTIVE and (row['策略'] != '當沖' or r.get('交易日') == row['交易日']) and all(r.get(k) == row.get(k) for k in ('商品鍵', '市場', '策略', '策略版本', '方向'))
                                for r in (json.loads(x[0]) for x in open_rows))
                if not existing and not same_open:
                    self.store(db, row)
                    self.revision += 1
            for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉','監控中斷','資料不足')").fetchall():
                row = json.loads(raw[0])
                quote = quotes.get(row['市場'] + '|' + row['商品鍵'])
                if quote is None or (quote.get('休市') and not quote.get('收盤確認')) or row['狀態'] not in ACTIVE:
                    continue
                updated = row
                evidence = quote.get('成交證據', [])
                quote = dict(quote)
                quote['銜接確認'] = any(stamp(t.get('來源時間')) == stamp(row.get('來源時間')) and t.get('價格') == row.get('最新價',row.get('訊號價')) for t in evidence)
                for tick in evidence:
                    if str(tick.get('來源時間', '')) < str(updated['來源時間']):
                        continue
                    updated = advance(updated, {**quote, **tick})
                if not evidence or not quote.get('有效'):
                    updated = advance(updated, quote)
                if updated != row:
                    self.store(db, updated)
                    self.revision += 1

    def mark_gaps(self):
        now = time.monotonic()
        with self.db() as db:
            for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉')").fetchall():
                row = json.loads(raw[0])
                if row['策略'] == '波段' and not self.market_open(row):
                    continue
                lease = self.leases.get(row['市場'] + '|' + row['商品鍵'])
                if lease is not None and now - lease[0] <= lease[1]:
                    continue
                row['資料缺口'] = True
                event(row, '監控中斷', datetime.now(TZ).isoformat(), '監控停止、登入中斷或頁面離開；未確認區間不回填')
                self.store(db,row)
                self.revision += 1

    def remote_get(self, scope):
        response = requests.get(self.url, params={'scope': scope}, timeout=8)
        response.raise_for_status()
        payload = response.json()
        if payload.get('success') is not True:
            raise ValueError(payload.get('error', 'Google Sheet 讀取失敗'))
        data = payload.get('data') or {}
        if not isinstance(data, dict):
            raise ValueError('Google Sheet 儲存格式不符，保留待同步')
        return data

    def remote_save(self, scope, data):
        response = requests.post(self.url, json={'scope': scope, 'data': json.dumps(data, ensure_ascii=False),
                                                'updated_at': datetime.now(TZ).isoformat()}, timeout=8)
        response.raise_for_status()
        payload = response.json()
        if payload.get('success') is not True:
            raise ValueError(payload.get('error', 'Google Sheet 寫入失敗'))
        actual = self.remote_get(scope)
        if actual.get('model_schema') != 1:
            raise ValueError('Apps Script 尚未部署策略追蹤原子合併 V1.0；資料保留待同步')
        return actual

    def sync(self):
        if not self.url:
            return
        catalog = self.remote_get('strategy_signals')
        if catalog.get('model_schema') != 1:
            raise ValueError('Apps Script 尚未部署策略追蹤原子合併 V1.0；資料保留待同步')
        with self.db() as db:
            pending = [r[0] for r in db.execute('SELECT DISTINCT month FROM trades WHERE dirty=1')]
        versions = catalog.get('model_versions', {})
        if not isinstance(versions, dict) or not isinstance(catalog.get('model_months', []), list) or any(not isinstance(m,str) or len(m) != 6 or not m.isdigit() or not 1 <= int(m[4:]) <= 12 for m in catalog.get('model_months', [])):
            raise ValueError('Google Sheet 月份索引無效；保留待同步')
        months = sorted(set(pending + [m for m in catalog.get('model_months', [])
                                      if not self.hydrated or versions.get(m) != self.month_versions.get(m)]))
        for month in months:
            scope = 'strategy_signals:' + month
            remote = self.remote_get(scope).get('model_trades', [])
            if not isinstance(remote,list):
                raise ValueError('Google Sheet 交易紀錄格式無效')
            for row in remote:
                if not isinstance(row,dict) or new_trade(row)['交易ID'] != row.get('交易ID') or row.get('狀態') not in ACTIVE | {'已平倉','訊號失效'}:
                    raise ValueError('Google Sheet 交易ID或狀態無效；保留待同步')
            local = self.records(month, month)
            merged = merge_trades(remote, local)
            if month in pending:
                actual = self.remote_save(scope, {'model_schema': 1, 'model_trades': merged})
                saved = {r['交易ID']: r for r in actual.get('model_trades', [])}
                if any(r['交易ID'] not in saved or merge_trades([saved[r['交易ID']]], [r])[0] != saved[r['交易ID']] for r in merged):
                    raise ValueError('交易事件回讀不一致；保留待同步並重試')
                catalog_check = self.remote_get('strategy_signals')
                if month not in catalog_check.get('model_months', []):
                    raise ValueError('月份索引回讀不一致；保留待同步')
                merged = list(saved.values())
            with self.db() as db:
                for row in merged:
                    self.store(db, row, dirty=0)
                if month in pending:
                    db.execute('UPDATE trades SET dirty=0 WHERE month=?', (month,))
            self.revision += 1
            self.month_versions[month] = versions.get(month)
        self.hydrated = True
        self.synced_at = datetime.now(TZ).strftime('%Y/%m/%d %H:%M:%S')
        self.status, self.error = '已回讀驗證', ''

    def run(self):
        while not self.closed:
            try:
                signals, quotes = self.queue.get(timeout=.25)
                if not self.hydrated and self.url:
                    # Preserve samples while storage is unavailable; their original timestamps remain evidence.
                    if time.monotonic() - self.last_sync >= self.sync_seconds:
                        self.last_sync = time.monotonic()
                        try:
                            self.sync()
                        except Exception as exc:
                            self.error, self.status = str(exc), '待同步／本機保留'
                    self.hydrated = True  # Cloud merge still deduplicates restored signals by stable ID.
                self.process(signals, quotes)
            except queue.Empty:
                pass
            except Exception as exc:
                self.error = '本機記錄失敗：' + type(exc).__name__
            if time.monotonic() - self.last_gap_check >= 5:
                self.last_gap_check = time.monotonic()
                try:
                    self.mark_gaps()
                except Exception as exc:
                    self.error = '資料缺口記錄待重試：' + type(exc).__name__
            if self.url and time.monotonic() - self.last_activity < 120 and time.monotonic() - self.last_sync >= self.sync_seconds:
                self.last_sync = time.monotonic()
                try:
                    self.sync()
                except Exception as exc:
                    self.error, self.status = str(exc), '待同步／本機保留'

    def close(self):
        self.closed = True
        if self.thread:
            self.thread.join(timeout=1)
