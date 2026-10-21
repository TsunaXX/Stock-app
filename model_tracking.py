"""Evidence-based simulation on existing quotes; one local writer, atomic cloud merge."""
import hashlib
import io
import json
import math
import queue
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, ROUND_FLOOR
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = 'V1.1'
PROTECTION_POLICY = 'balanced-1R-v1'
TZ = ZoneInfo('Asia/Taipei')
ACTIVE = {'等待進場', '模擬持倉', '監控中斷', '資料不足'}
DEFAULT_COSTS = {'stock_shares': 1000, 'stock_discount': 2.8, 'stock_min_fee': 20,
                 'futures_contracts': 1, 'futures_fee': 10, 'slippage_ticks': 1}


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


def option_tick(price):
    """TXO premium ticks, not the underlying index tick."""
    return next((tick for ceiling, tick in ((10, .1), (50, .5), (500, 1), (1000, 5))
                 if price < ceiling), 10)


def option_fills(record, quote, opening):
    prices = quote.get('權利金證據', {})
    fills = {}
    for leg in record.get('選擇權腿', []):
        evidence = prices.get(leg['代碼'], {})
        price, source, trigger_time = number(evidence.get('價格')), stamp(evidence.get('來源時間')), stamp(quote.get('來源時間'))
        if (price is None or price <= 0 or source is None or trigger_time is None
                or not 0 <= (trigger_time - source).total_seconds() <= 2):
            return None
        sign = leg['買賣'] * (1 if opening else -1)
        fill = max(0, price + sign * option_tick(price) * record['成本設定']['slippage_ticks'])
        step = option_tick(fill)
        fills[leg['代碼']] = round((math.ceil(fill / step - 1e-9) if sign > 0 else math.floor(fill / step + 1e-9)) * step, 2)
    if len(fills) == 2:
        credit = -sum(leg['買賣'] * fills[leg['代碼']] for leg in record['選擇權腿'])
        raw_credit = -sum(leg['買賣'] * prices[leg['代碼']]['價格'] for leg in record['選擇權腿'])
        width = number(record.get('限定風險寬度'))
        if width is None or not 0 <= raw_credit <= width or (opening and not 0 <= credit < width):
            return None
    return fills


def trade_id(signal):
    # Decimal spellings and different device receipt times cannot change identity.
    fields = [signal.get(k, '') for k in ('交易日', '市場', '商品鍵', '策略', '策略版本', '方向')]
    if signal.get('選擇權腿'):
        fields.append(json.dumps(signal['選擇權腿'], sort_keys=True, ensure_ascii=False))
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
            same_stream = incoming.get('串流識別') == old.get('串流識別')
            newer = (str(incoming.get('來源時間', '')), (number(incoming.get('最後序號')) or 0) if same_stream else 0, len(incoming.get('事件', []))) > (
                str(old.get('來源時間', '')), (number(old.get('最後序號')) or 0) if same_stream else 0, len(old.get('事件', [])))
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
        restore_profit_protection(row)
        merged[row['交易ID']] = row
    return list(merged.values())


def event(record, state, ts, reason=''):
    record['狀態'] = state
    record['來源時間'] = ts
    record['異常原因'] = reason
    protection = {key: record[key] for key in ('保利價', '保利淨額', '保利最大浮盈', '初始風險') if key in record} if reason.startswith('保利') else {}
    evidence = '|'.join([record['交易ID'], state, ts, reason]) + (json.dumps(protection, sort_keys=True) if protection else '')
    eid = hashlib.sha256(evidence.encode()).hexdigest()
    if not any(e.get('事件ID') == eid for e in record.setdefault('事件', [])):
        record['事件'].append({'事件ID': eid, '時間': ts, '狀態': state, '原因': reason, **protection})


def new_trade(signal):
    entry,stop,target = (number(signal.get(k)) for k in ('進場價','停損價','目標價'))
    long = signal.get('方向') in ('多頭','偏多')
    if None in (entry,stop,target) or min(entry,stop,target) <= 0 or not (stop < entry < target if long else target < entry < stop):
        raise ValueError('模擬計畫缺少有效進場、停損或目標')
    if signal.get('方向') not in ('多頭','偏多','空頭','偏空') or signal.get('策略') not in ('當沖','波段') or not signal.get('策略版本') or stamp(signal.get('交易日')) is None:
        raise ValueError('訊號方向、策略版本或交易日無效')
    if not signal.get('商品鍵') or signal.get('市場') not in ('股票','期貨') or stamp(signal.get('來源時間')) is None:
        raise ValueError('訊號商品或來源時間無效')
    legs = signal.get('選擇權腿')
    if legs is not None and (not isinstance(legs, list) or len(legs) not in (1, 2)
            or any(not isinstance(leg, dict) or not leg.get('代碼') or leg.get('買賣') not in (-1, 1) for leg in legs)
            or len({leg['代碼'] for leg in legs}) != len(legs)
            or sum(leg['買賣'] for leg in legs) != (1 if len(legs) == 1 else 0)
            or (len(legs) == 2 and number(signal.get('限定風險寬度')) not in (50, 100))
            or signal.get('乘數') != 50 or stamp(signal.get('到期時間')) is None):
        raise ValueError('選擇權腿、乘數或到期時間無效')
    row = {**signal, '交易ID': trade_id(signal), '紀錄類型': '自動', '資料缺口': False,
           'MFE(R)': 0.0, 'MAE(R)': 0.0, '事件': []}
    row['成本設定'] = normalize_costs(row.get('成本設定'))
    if row.get('保利規則') == PROTECTION_POLICY:
        row['保利狀態'] = '待進場'
    row['建立時間'] = row['來源時間']
    event(row, '等待進場', row['來源時間'])
    return row


def entry_quality_reason(row, price):
    """Check executable prices and both-side costs; never alter an existing position."""
    if not row.get('進場品質檢查') or row.get('選擇權腿') or row.get('策略') != '當沖':
        return ''
    sign = 1 if row['方向'] in ('多頭', '偏多') else -1
    entry, stop, target = (float(row[k]) for k in ('進場價', '停損價', '目標價'))
    if sign * (price - entry) > abs(entry - stop) * .25:
        return '已偏離原進場點超過 0.25R，等待回測'
    costs = normalize_costs(row.get('成本設定'))
    slip = row['跳動點'] * costs['slippage_ticks']
    fill, exit_target, exit_stop = price + sign * slip, target - sign * slip, stop - sign * slip
    qty = costs['stock_shares'] if row['市場'] == '股票' else costs['futures_contracts']
    multiplier = row['乘數']
    def net(exit_price):
        if row['市場'] == '股票':
            fee = sum(max(costs['stock_min_fee'], math.floor(p * qty * .001425 * costs['stock_discount'] / 10)) for p in (fill, exit_price))
            tax = math.floor((exit_price if sign > 0 else fill) * qty * .0015)
        else:
            rate = number(row.get('期交稅率'))
            if rate is None:
                return None
            fee = costs['futures_fee'] * qty * 2
            tax = sum(round(p * multiplier * qty * rate) for p in (fill, exit_price))
        return sign * (exit_price - fill) * multiplier * qty - fee - tax
    reward, loss = net(exit_target), net(exit_stop)
    if reward is None or loss is None:
        return '未確認交易成本，等待資料'
    if reward <= 0 or loss >= 0 or reward / -loss < 1:
        return '扣除雙邊滑價與成本後風報比不足 1，等待回測'
    return ''


def trade_pnl(row, exit_price, fills=None):
    """One cost definition for simulated exits and the cost-covering protection floor."""
    costs, legs = row['成本設定'], row.get('選擇權腿')
    qty = costs['stock_shares'] if row['市場'] == '股票' else costs['futures_contracts']
    multiplier, entry = row['乘數'], row['模擬進場價']
    sign = 1 if row['方向'] in ('多頭', '偏多') else -1
    gross = sign * (exit_price - entry) * multiplier * qty
    if row['市場'] == '股票':
        fee = sum(max(costs['stock_min_fee'], math.floor(p * qty * .001425 * costs['stock_discount'] / 10)) for p in (entry, exit_price))
        tax = math.floor((exit_price if sign > 0 else entry) * qty * (.0015 if row['策略'] == '當沖' else .003))
    elif legs:
        if fills is None:
            return None
        gross = sum(leg['買賣'] * (fills[leg['代碼']] - row['進場權利金'][leg['代碼']]) for leg in legs) * multiplier * qty
        fee = costs['futures_fee'] * qty * 2 * len(legs)
        tax = sum(int((Decimal(str(p)) * Decimal(str(multiplier)) * qty * Decimal('.001')).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
                  for values in (row['進場權利金'], fills) for p in values.values())
    else:
        rate = number(row.get('期交稅率'))
        if rate is None:
            return None
        fee = costs['futures_fee'] * qty * 2
        tax = sum(round(p * multiplier * qty * rate) for p in (entry, exit_price))
    return {'毛損益': round(gross, 2), '交易成本': round(fee + tax, 2), '模擬損益': round(gross - fee - tax, 2)}


def protection_tick(row, price):
    if row['市場'] == '股票':
        return next((step for ceiling, step in ((10, .01), (50, .05), (100, .1), (500, .5), (1000, 1)) if price < ceiling), 5)
    return row['跳動點']


def restore_profit_protection(row):
    """The existing cloud event union also carries monotonic protection across devices."""
    if row.get('保利規則') != PROTECTION_POLICY or row.get('狀態') not in ACTIVE:
        return
    for item in row.get('事件', []):
        if item.get('時間', '') > row.get('來源時間', ''):
            continue
        for key in ('保利價', '保利淨額', '保利最大浮盈'):
            value = number(item.get(key))
            if value is not None:
                old = number(row.get(key))
                row[key] = value if old is None else (min(old, value) if key == '保利價' and row['方向'] in ('空頭', '偏空') else max(old, value))
        if '初始風險' in item:
            row.setdefault('初始風險', item['初始風險'])
        if item.get('保利價') is not None or item.get('保利淨額') is not None:
            row.setdefault('保利啟動時間', item['時間'])
            row['保利更新時間'] = max(row.get('保利更新時間', ''), item['時間'])
    if row.get('保利價') is not None or row.get('保利淨額') is not None:
        row['保利狀態'] = '保利中'
    if row.get('初始風險', 0) > 0:
        row['保利最大浮盈(R)'] = round(row.get('保利最大浮盈', 0) / row['初始風險'], 4)


def update_profit_protection(row, price, text, pnl=None):
    """Only observed post-entry prices can raise protection; initial risk never changes."""
    if row.get('保利規則') != PROTECTION_POLICY or row.get('資料缺口'):
        return
    legs = row.get('選擇權腿')
    sign = 1 if row['方向'] in ('多頭', '偏多') else -1
    entry, risk = row['模擬進場價'], row['初始風險']
    if legs:
        if pnl is None:
            row['保利狀態'] = '權利金待確認'
            return
        peak = max(row.get('保利最大浮盈', 0), pnl['模擬損益'])
        row['保利最大浮盈'] = peak
        row['保利最大浮盈(R)'] = round(peak / risk, 4)
        if peak < risk:
            return
        candidate = max(0, peak - risk)
        old = row.get('保利淨額')
        row['保利淨額'] = max(old or 0, round(candidate, 2))
        changed = old != row['保利淨額']
    else:
        peak = max(row.get('保利最大浮盈', 0), sign * (price - entry))
        if peak == row.get('保利最大浮盈') and row.get('保利價') is not None:
            return
        row['保利最大浮盈'] = peak
        row['保利最大浮盈(R)'] = round(peak / risk, 4)
        if peak < risk:
            return
        slip = row['跳動點'] * row['成本設定']['slippage_ticks']
        # Solve the exact existing fee/tax definition, including adverse exit slippage.
        lo, hi = 0., peak
        for _ in range(40):
            distance = (lo + hi) / 2
            result = trade_pnl(row, entry + sign * (distance - slip))
            if result is None:
                row['保利狀態'] = '成本待確認'
                return
            if result['模擬損益'] >= 0:
                hi = distance
            else:
                lo = distance
        candidate = entry + sign * max(hi, peak - risk)
        step = Decimal(str(protection_tick(row, candidate)))
        candidate = float((Decimal(str(candidate)) / step).to_integral_value(rounding=ROUND_CEILING if sign > 0 else ROUND_FLOOR) * step)
        # A cost floor beyond the observed market is not an executable protection level.
        if sign * (price - candidate) <= 0:
            row['保利狀態'] = '成本待確認'
            return
        old = row.get('保利價')
        row['保利價'] = candidate if old is None else (max(old, candidate) if sign > 0 else min(old, candidate))
        changed = old != row['保利價']
    row['保利狀態'] = '保利中'
    if changed:
        row.setdefault('保利啟動時間', text)
        row['保利更新時間'] = text
        event(row, '模擬持倉', text, '保利啟動' if old is None else '保利移動')


def advance(record, quote):
    """Only ordered observed prices establish fills. Gaps never count as verified wins."""
    row = json.loads(json.dumps(record, ensure_ascii=False))
    if row['狀態'] not in ACTIVE or row.get('到期未確認'):
        return row
    ts, prior = stamp(quote.get('來源時間')), stamp(row.get('來源時間'))
    price = number(quote.get('價格'))
    if ts is None or (prior is not None and ts < prior):
        return row
    restore_profit_protection(row)
    if row.get('到期時間') and ts >= stamp(row['到期時間']):
        row['到期未確認'] = row.get('模擬進場價') is not None
        row['資料缺口'] = bool(row['到期未確認'] or row.get('資料缺口'))
        event(row, '資料不足' if row['到期未確認'] else '訊號失效', ts.isoformat(),
              '契約已到期；沒有結算或到期前平倉證據，不估算損益')
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
              '恢復追蹤；缺口仍保留並排除有效績效' if row.get('資料缺口') else '串流成交證據已就緒')
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
    option_legs = row.get('選擇權腿')
    slip = 0 if option_legs else tick * row['成本設定']['slippage_ticks']
    if row['狀態'] == '等待進場':
        invalid = price <= stop if long else price >= stop
        if invalid or (row.get('策略') == '當沖' and str(row.get('交易日')) != str(quote.get('交易日'))):
            event(row, '訊號失效', text, '未進場前已越過停損或交易日已結束')
            return row
        if sign * (price - entry) < 0:
            row['來源時間'] = text
            return row
        if (not quote.get('進場許可', True) or
                (quote.get('許可方向') and quote['許可方向'] != row['方向']) or
                (quote.get('進場許可時間') and ts < stamp(quote['進場許可時間']))):
            row['來源時間'] = text
            return row
        if row.get('進場品質檢查') and (
            quote.get('進場品質許可') is False or
            (quote.get('進場品質時間') and ts < stamp(quote['進場品質時間'])) or
            (quote.get('進場品質方向') and quote['進場品質方向'] != row['方向'])
        ):
            row['來源時間'] = text
            row['進場等待原因'] = '目前條件未放行同向進場，保留原點位追蹤'
            return row
        fill = price + sign * slip
        if sign * (target - fill) <= 0:
            event(row, '訊號失效', text, '首個成交證據已越過目標；不得回填計畫價成交')
            return row
        quality_reason = entry_quality_reason(row, price)
        if quality_reason:
            # Keep the original levels and wait; do not relabel an old loss or move the target.
            row['來源時間'] = text
            row['進場等待原因'] = quality_reason
            return row
        row.pop('進場等待原因', None)
        if option_legs:
            fills = option_fills(row, quote, True)
            if fills is None:
                row['資料缺口'] = True
                event(row, '資料不足', text, '觸及進場但缺少同時段的各腿權利金成交證據')
                return row
            row['進場權利金'] = fills
            row['觸發進場點位'] = price
            fill = sum(leg['買賣'] * fills[leg['代碼']] for leg in option_legs)
        row['模擬進場價'], row['進場時間'], row['最新價'] = fill, text, price
        if row.get('保利規則') == PROTECTION_POLICY:
            qty = row['成本設定']['stock_shares'] if row['市場'] == '股票' else row['成本設定']['futures_contracts']
            risk = ((fill if len(option_legs) == 1 else row['限定風險寬度'] + fill) * multiplier * qty
                    if option_legs else abs(fill - stop))
            row.update({'初始風險': risk, '初始風險單位': '元' if option_legs else '點',
                        '保利狀態': '未啟動', '保利最大浮盈': 0., '保利最大浮盈(R)': 0.})
        event(row, '模擬持倉', text)
        return row  # The entry sample cannot also prove a later exit.
    fill = row['模擬進場價']
    trigger_fill = row.get('觸發進場點位', fill)
    risk = abs(trigger_fill - stop)
    r = sign * (price - trigger_fill) / risk if risk else 0
    row['MFE(R)'] = max(row['MFE(R)'], r)
    row['MAE(R)'] = max(row['MAE(R)'], -r)
    row['來源時間'], row['最新價'] = text, price
    stopped = price <= stop if long else price >= stop
    targeted = price >= target if long else price <= target
    # OHLC extrema never establish the order of stop and target.
    high, low = number(quote.get('最高')), number(quote.get('最低'))
    observed_stop = (number(row.get('保利價')) or stop) if row.get('保利規則') == PROTECTION_POLICY and not option_legs else stop
    ambiguous = high is not None and low is not None and high >= max(observed_stop, target) and low <= min(observed_stop, target)
    if ambiguous:
        row['資料缺口'] = True
        event(row, '監控中斷', text, '同一根 K 同時觸及停利停損，先後順序未確認')
        return row
    exit_fills = option_fills(row, quote, False) if option_legs else None
    exit_price = (sum(leg['買賣'] * exit_fills[leg['代碼']] for leg in option_legs)
                  if exit_fills is not None else price - sign * slip)
    pnl = trade_pnl(row, exit_price, exit_fills)
    if row.get('保利規則') == PROTECTION_POLICY and option_legs and exit_fills is None:
        row['資料缺口'] = True
        event(row, '資料不足', text, '保利追蹤缺少同時段的各腿權利金成交證據')
        return row
    protected = False
    if row.get('保利規則') == PROTECTION_POLICY:
        if option_legs:
            floor = number(row.get('保利淨額'))
            protected = floor is not None and pnl is not None and pnl['模擬損益'] <= floor
        else:
            floor = number(row.get('保利價'))
            protected = floor is not None and sign * (price - floor) <= 0
    cutoff = ts.time()
    reached_close = (cutoff.hour == 13 and cutoff.minute >= (30 if row['市場'] == '股票' else 45)) or (row['市場'] == '期貨' and cutoff.hour == 5)
    close_session = quote.get('收盤確認', False) and reached_close and row['策略'] == '當沖'
    if stopped or targeted or protected or close_session:
        if option_legs:
            if exit_fills is None:
                row['資料缺口'] = True
                event(row, '資料不足', text, '觸及出場但缺少同時段的各腿權利金成交證據')
                return row
            row['出場權利金'] = exit_fills
        if pnl is None:
            event(row, '資料不足', text, '此商品未確認期交稅率')
            row['資料缺口'] = True
            return row
        if protected:
            row['保利狀態'] = '保利出場'
        row.update({**pnl, '模擬出場價': exit_price, '結案時間': text,
                    '結果(R)': round(sign * (price - trigger_fill) / risk, 4) if option_legs else round(sign * (exit_price - fill) / risk, 4),
                    '出場原因': '保利' if protected else ('停損' if stopped else ('停利' if targeted else '收盤'))})
        event(row, '已平倉', text)
    elif row.get('保利規則') == PROTECTION_POLICY:
        update_profit_protection(row, price, text, pnl)
    return row


def performance(records, state=None):
    data = pd.DataFrame(records)
    closed = [r for r in records if r.get('狀態') == '已平倉' and not r.get('資料缺口')
              and number(r.get('模擬損益')) is not None]
    closed.sort(key=lambda r: (r.get('結案時間', ''), r.get('交易ID', '')))
    # Only newly completed trades are folded. Cloud corrections/removals rebuild this filtered view.
    state = {} if state is None else state
    fingerprints = [json.dumps({k:r.get(k) for k in ('交易ID','結案時間','模擬損益','市場','策略','策略版本','方向','市場環境','指標快照','條件快照','計畫來源','計畫類型')}, ensure_ascii=False, sort_keys=True) for r in closed]
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
        labels.append({'計畫來源': r.get('計畫來源', '股期戰略室'), '計畫類型': r.get('計畫類型', r.get('策略'))})
        labels += [{'指標':k} for k,v in r.get('指標快照', {}).items() if v is not None]
        labels += [{'進場條件':k,'判定':'符合' if v else '未符合'} for k,v in r.get('條件快照', {}).items() if isinstance(v,bool)]
        for label in labels:
            key = json.dumps(label, ensure_ascii=False, sort_keys=True)
            group = state['groups'].setdefault(key, {**label, '有效交易數':0, '獲勝':0, '累積模擬損益':0.})
            group['有效交易數'] += 1; group['獲勝'] += value > 0; group['累積模擬損益'] += value
    state['fingerprints'], state['added'] = fingerprints, len(added)
    count, wins, losses = len(closed), state['wins'], state['losses']
    insufficient = sum(bool(r.get('資料缺口') or r.get('到期未確認') or
                            r.get('狀態') in ('資料不足', '監控中斷')) for r in records)
    metrics = {'全部訊號數': len(records), '資料不足數': insufficient,
               '有效交易數': count, '獲勝': wins, '虧損': losses,
               '勝率(%)': wins / count * 100 if count else None,
               '累積模擬損益': state['equity'], '平均獲利': state['profit'] / wins if wins else None,
               '平均虧損': state['loss'] / losses if losses else None,
               '獲利因子': state['profit'] / abs(state['loss']) if losses else None,
               '每筆期望值': state['equity'] / count if count else None, '最大回撤': state['drawdown'],
               '訊號有效率(%)': count / len(records) * 100 if records else None,
               '資料完整率(%)': sum(not r.get('資料缺口', False) and r.get('狀態') != '資料不足' for r in records) / len(records) * 100 if records else None}
    groups = [{**g, '勝率(%)':g['獲勝']/g['有效交易數']*100} for g in state['groups'].values()]

    return metrics, pd.DataFrame(groups), data


def excel_report(records):
    metrics, groups, data = performance(records)
    events = [{**e, '交易ID': r['交易ID']} for r in records for e in r.get('事件', [])]
    for key in ('指標快照', '成本設定', '事件', '條件快照', '選擇權腿', '進場權利金', '出場權利金'):
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


def database_busy(exc):
    return (isinstance(exc, sqlite3.OperationalError)
            and (getattr(exc, 'sqlite_errorcode', 0) & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))


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
        self.stop = threading.Event()
        self.overloaded = False
        self.last_activity = time.monotonic()
        self.last_gap_check = 0
        self.leases = {}
        self.primary_streams = {}
        self.source_quotes = {}
        self.entry_permissions = {}
        self.last_sync = 0
        self.month_versions = {}
        self.sync_positions = {}
        self.hydrated = not bool(url)
        self.write_lock = threading.RLock()
        with closing(self.db()) as db:
            db.execute('PRAGMA journal_mode=WAL')
        with self.write_db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, month TEXT, slot TEXT, state TEXT, body TEXT, dirty INTEGER)')
            db.execute('CREATE INDEX IF NOT EXISTS trades_month ON trades(month)')
            db.execute('CREATE INDEX IF NOT EXISTS trades_state ON trades(state,slot)')
            db.execute('CREATE INDEX IF NOT EXISTS trades_slot ON trades(slot)')
            for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉')").fetchall():
                row = json.loads(raw[0])
                previous_time = row['來源時間']
                row['資料缺口'] = True
                event(row, '監控中斷', datetime.now(TZ).isoformat(), '服務重新啟動；未確認區間不回填')
                row['來源時間'] = previous_time
                self.store(db, row)
        self.thread = None
        self.sync_thread = None
        if start:
            self.thread = threading.Thread(target=self.run, name='model-tracker', daemon=True)
            self.thread.start()
            if self.url:
                self.sync_thread = threading.Thread(target=self.run_sync, name='model-sync', daemon=True)
                self.sync_thread.start()

    def db(self):
        return sqlite3.connect(self.path, timeout=5)

    @contextmanager
    def write_db(self):
        # Only short local transactions share this lock; HTTP and UI reads never hold it.
        with self.write_lock, closing(self.db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            yield db

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
        with closing(self.db()) as db, db:
            rows = db.execute('SELECT body FROM trades WHERE month >= ? AND month <= ? ORDER BY month,id',
                              (start or '000000', end or '999999')).fetchall()
        return [json.loads(r[0]) for r in rows]

    def active_plans(self, source, day):
        month = day[:7].replace('-', '')
        with closing(self.db()) as db, db:
            rows = db.execute("SELECT body FROM trades WHERE month=? AND state IN ('等待進場','模擬持倉','監控中斷','資料不足')", (month,)).fetchall()
        return [row for raw in rows if (row := json.loads(raw[0])).get('計畫來源') == source and row.get('交易日') == day]

    def protection_plans(self, market, keys, mode, day, source=None):
        """Bounded current-plan reads by indexed instrument, without loading performance history."""
        result = {}
        with closing(self.db()) as db:
            for key in keys:
                prefix = f'{key}|{market}|{mode}|'
                raw = db.execute("""SELECT body FROM trades WHERE slot>=? AND slot<?
                    AND json_extract(body, '$.保利規則')=? AND json_extract(body, '$.計畫來源') IS ?
                    AND ((?='波段' AND state IN ('等待進場','模擬持倉','監控中斷','資料不足')) OR json_extract(body, '$.交易日')=?)
                    ORDER BY (state IN ('模擬持倉','監控中斷','資料不足') AND json_extract(body, '$.模擬進場價') IS NOT NULL) DESC,
                    rowid DESC LIMIT 1""", (prefix, prefix + '\uffff', PROTECTION_POLICY, source, mode, day)).fetchone()
                if raw:
                    result[str(key)] = json.loads(raw[0])
        return result

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
        with self.write_db() as db:
            if self.overloaded:
                for raw in db.execute("SELECT body FROM trades WHERE state IN ('等待進場','模擬持倉','監控中斷','資料不足')").fetchall():
                    row = json.loads(raw[0])
                    row['資料缺口'] = True
                    event(row, '監控中斷', datetime.now(TZ).isoformat(), '記錄佇列滿載，成交證據未確認')
                    self.store(db, row)
                    self.revision += 1
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
                if quote.get('策略') and quote['策略'] != row['策略'] and row.get('模擬進場價') is None:
                    continue
                if (quote.get('暖機等待') and row.get('最後序號') is None and row.get('模擬進場價') is None
                        and (not row.get('到期時間') or stamp(quote['來源時間']) < stamp(row['到期時間']))):
                    if row['狀態'] != '資料不足':
                        event(row, '資料不足', row['來源時間'], '等待第一組串流成交證據，尚未確認進場')
                        self.store(db, row)
                        self.revision += 1
                    continue
                updated = row
                evidence = quote.get('成交證據', [])
                quote = dict(quote)
                prior_time, prior_price = row.get('來源時間'), row.get('最新價', row.get('訊號價'))
                # Stream timestamps normally match verbatim; parse only cross-format bridges.
                quote['銜接確認'] = any(t.get('來源時間') == prior_time and t.get('價格') == prior_price for t in evidence)
                if not quote['銜接確認']:
                    prior_stamp = stamp(prior_time)
                    quote['銜接確認'] = any(t.get('價格') == prior_price and stamp(t.get('來源時間')) == prior_stamp for t in evidence)
                permission = self.entry_permissions.get((row['市場'] + '|' + row['商品鍵'], row['策略']), {})
                permission_time = stamp(permission.get('進場品質時間'))
                current_time = stamp(quote.get('進場品質時間'))
                continuous = (permission.get('有效') and permission.get('交易日') == quote.get('交易日')
                              and permission.get('串流識別') == quote.get('串流識別')
                              and permission_time is not None and current_time is not None
                              and 0 <= (current_time - permission_time).total_seconds() <= max(30, float(quote.get('更新秒數', 5)) * 3))
                for tick in evidence:
                    if str(tick.get('來源時間', '')) < str(updated['來源時間']):
                        continue
                    sample = {**quote, **tick}
                    # A new decision applies from its publication time, never to earlier observed fills.
                    tick_time = stamp(tick.get('來源時間')) if continuous and updated.get('模擬進場價') is None else None
                    if tick_time is not None and permission_time <= tick_time < current_time:
                        sample.update({field: permission[field] for field in ('進場品質許可', '進場品質方向', '進場品質時間')})
                    updated = advance(updated, sample)
                if not evidence or not quote.get('有效'):
                    updated = advance(updated, quote)
                if updated != row:
                    self.store(db, updated)
                    self.revision += 1
        for key, quote in quotes.items():
            if quote.get('策略') and quote.get('進場品質時間'):
                identity = (key, quote['策略'])
                previous_time = stamp(self.entry_permissions.get(identity, {}).get('進場品質時間'))
                current_time = stamp(quote['進場品質時間'])
                if current_time is not None and (previous_time is None or current_time >= previous_time):
                    self.entry_permissions[identity] = {
                        field: quote.get(field) for field in ('進場品質許可', '進場品質方向', '進場品質時間', '有效', '交易日', '串流識別')}

    def mark_gaps(self):
        now = time.monotonic()
        with self.write_db() as db:
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

    def remote_get(self, scope, trade_ids=None):
        for attempt in range(3):
            try:
                params = {'scope': scope, '_ts': time.time_ns()}
                if trade_ids is not None:
                    params['trade_ids'] = ','.join(trade_ids)
                response = requests.get(self.url, params=params,
                                        headers={'Cache-Control': 'no-cache, no-store, max-age=0', 'Pragma': 'no-cache'},
                                        timeout=(5, 35))
                response.raise_for_status()
                break
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status is not None and status not in (429, 500, 502, 503, 504):
                    raise
                if attempt == 2:
                    reason = f'HTTP {status}' if status else type(exc).__name__
                    raise RuntimeError(f'Google Sheet 暫時無法讀取（{scope}／{reason}，已嘗試 3 次）；保留待同步並自動重試') from exc
                time.sleep(2 ** attempt)
        payload = response.json()
        if payload.get('success') is not True:
            raise ValueError(payload.get('error', 'Google Sheet 讀取失敗'))
        data = payload.get('data') or {}
        if not isinstance(data, dict):
            raise ValueError('Google Sheet 儲存格式不符，保留待同步')
        return data

    def remote_save(self, scope, data, delta=False):
        response = requests.post(self.url, json={'scope': scope, 'data': json.dumps(data, ensure_ascii=False),
                                                'updated_at': datetime.now(TZ).isoformat()}, timeout=(5, 35))
        response.raise_for_status()
        payload = response.json()
        if payload.get('success') is not True:
            raise ValueError(payload.get('error', 'Google Sheet 寫入失敗'))
        actual = self.remote_get(scope, [r['交易ID'] for r in data['model_trades']]) if delta else self.remote_get(scope)
        if actual.get('model_schema') != 1:
            raise ValueError('Apps Script 尚未部署策略追蹤原子合併 V1.0；資料保留待同步')
        if delta:
            version = str(payload.get('model_version', ''))
            previous = str(payload.get('model_previous_version', ''))
            read_version = str(actual.get('model_version', ''))
            if not version.isdigit() or not read_version.isdigit() or int(read_version) < int(version) or (previous and not previous.isdigit()):
                raise ValueError('交易同步版本回讀無效；保留待同步')
            actual['_committed_version'], actual['_previous_version'] = version, previous
        return actual

    def sync(self):
        if not self.url:
            return
        catalog = self.remote_get('strategy_signals')
        if catalog.get('model_schema') != 1:
            raise ValueError('Apps Script 尚未部署策略追蹤原子合併 V1.0；資料保留待同步')
        with closing(self.db()) as db, db:
            pending = [r[0] for r in db.execute('SELECT DISTINCT month FROM trades WHERE dirty=1')]
        versions = catalog.get('model_versions', {})
        if not isinstance(versions, dict) or not isinstance(catalog.get('model_months', []), list) or any(not isinstance(m,str) or len(m) != 6 or not m.isdigit() or not 1 <= int(m[4:]) <= 12 for m in catalog.get('model_months', [])):
            raise ValueError('Google Sheet 月份索引無效；保留待同步')
        if catalog.get('model_sync') == 2:
            return self.sync_delta(catalog, pending)
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
            local_versions = {r['交易ID']: r for r in local}
            merged = merge_trades(remote, local)
            if month in pending:
                actual = self.remote_save(scope, {'model_schema': 1, 'model_trades': merged})
                saved_rows = actual.get('model_trades', [])
                # Apps Script collation can order identical ISO event times differently.
                saved = {r['交易ID']: r for r in merge_trades(saved_rows, saved_rows)}
                if any(r['交易ID'] not in saved or merge_trades([saved[r['交易ID']]], [r])[0] != saved[r['交易ID']] for r in merged):
                    raise ValueError('交易事件回讀不一致；保留待同步並重試')
                catalog_check = self.remote_get('strategy_signals')
                if month not in catalog_check.get('model_months', []):
                    raise ValueError('月份索引回讀不一致；保留待同步')
                merged = list(saved.values())
            self.merge_synced_rows(merged, local_versions if month in pending else {})
            self.month_versions[month] = versions.get(month)
        self.hydrated = True
        self.synced_at = datetime.now(TZ).strftime('%Y/%m/%d %H:%M:%S')
        self.status, self.error = '已回讀驗證', ''

    def merge_synced_rows(self, rows, sent=None):
        sent = sent or {}
        for row in rows:
            if not isinstance(row, dict) or new_trade(row)['交易ID'] != row.get('交易ID') or row.get('狀態') not in ACTIVE | {'已平倉','訊號失效'}:
                raise ValueError('Google Sheet 交易ID或狀態無效；保留待同步')
        for offset in range(0, len(rows), 20):
            with self.write_db() as db:
                for row in rows[offset:offset + 20]:
                    raw = db.execute('SELECT body FROM trades WHERE id=?', (row['交易ID'],)).fetchone()
                    current = json.loads(raw[0]) if raw else None
                    merged = merge_trades([row], [current] if current else [])[0]
                    self.store(db, merged, dirty=0)
                    if current == sent.get(row['交易ID']) and merged == row:
                        db.execute('UPDATE trades SET dirty=0 WHERE id=?', (row['交易ID'],))
            self.revision += 1

    def sync_delta(self, catalog, pending):
        versions = catalog.get('model_versions', {})
        for month in sorted(set(pending + catalog.get('model_months', []))):
            scope = 'strategy_signals:' + month
            if not self.hydrated or versions.get(month) != self.month_versions.get(month):
                remote = self.remote_get(scope)
                if not isinstance(remote.get('model_trades', []), list):
                    raise ValueError('Google Sheet 交易紀錄格式無效')
                self.merge_synced_rows(remote.get('model_trades', []))
                self.month_versions[month] = remote.get('model_version') or versions.get(month)
            with closing(self.db()) as db, db:
                # Rotate across dirty rows so frequently changing positions cannot starve later trades.
                rows = db.execute('SELECT rowid,body FROM trades WHERE month=? AND dirty=1 ORDER BY (rowid<=?),rowid LIMIT 20',
                                  (month, self.sync_positions.get(month, 0))).fetchall()
            batch, size = [], 0
            for rowid, body in rows:
                length = len(body.encode('utf-8'))
                if batch and size + length > 256_000:
                    break
                batch.append((rowid, json.loads(body)))
                size += length
            if not batch:
                continue
            sent = {row['交易ID']: row for _, row in batch}
            actual = self.remote_save(scope, {'model_schema': 1, 'model_trades': list(sent.values())}, delta=True)
            saved = actual.get('model_trades', [])
            if not isinstance(saved, list):
                raise ValueError('Google Sheet 交易紀錄格式無效；保留待同步')
            saved = {r['交易ID']: r for r in merge_trades(saved, saved)}
            if any(key not in saved or merge_trades([saved[key]], [row])[0] != saved[key] for key, row in sent.items()):
                raise ValueError('交易事件回讀不一致；保留待同步並重試')
            self.merge_synced_rows(list(saved.values()), sent)
            # A concurrent device may have written between our read and POST; fetch it next cycle.
            if actual['_previous_version'] == (self.month_versions.get(month) or ''):
                self.month_versions[month] = actual['_committed_version']
            self.sync_positions[month] = batch[-1][0]
        self.hydrated = True
        self.synced_at = datetime.now(TZ).strftime('%Y/%m/%d %H:%M:%S')
        with closing(self.db()) as db, db:
            remaining = db.execute('SELECT COUNT(*) FROM trades WHERE dirty=1').fetchone()[0]
        self.status, self.error = (f'已回讀驗證；尚有 {remaining} 筆待同步' if remaining else '已回讀驗證'), ''

    def run(self):
        pending = None
        busy_message = '本機資料庫忙碌；保留原批次並自動重試'
        while not self.closed:
            try:
                if pending is None:
                    pending = self.queue.get(timeout=.25)
                self.process(*pending)
                pending = None
                if self.error == busy_message or (self.error == '記錄佇列已滿；保留資料缺口，不阻塞行情' and self.queue.empty()):
                    self.error = ''
            except queue.Empty:
                pass
            except Exception as exc:
                if database_busy(exc):
                    self.error = busy_message
                    self.stop.wait(.25)
                    continue
                pending = None
                self.error = '本機記錄失敗：' + type(exc).__name__
            if time.monotonic() - self.last_gap_check >= 5:
                self.last_gap_check = time.monotonic()
                try:
                    self.mark_gaps()
                except Exception as exc:
                    self.error = '資料缺口記錄待重試：' + type(exc).__name__

    def run_sync(self):
        # Slow HTTP/retries must never stop the ordered local evidence writer.
        while not self.stop.is_set():
            if time.monotonic() - self.last_sync >= self.sync_seconds:
                self.last_sync = time.monotonic()
                try:
                    with closing(self.db()) as db, db:
                        pending = db.execute('SELECT 1 FROM trades WHERE dirty=1 LIMIT 1').fetchone()
                    if pending or time.monotonic() - self.last_activity < 120:
                        self.sync()
                except Exception as exc:
                    self.error = '本機資料庫忙碌；保留待同步並自動重試' if database_busy(exc) else str(exc)
                    self.status = '待同步／本機保留'
                finally:
                    self.last_sync = time.monotonic()
            self.stop.wait(.25)

    def close(self):
        self.closed = True
        self.stop.set()
        for thread in (self.thread, self.sync_thread):
            if thread:
                thread.join(timeout=1)
