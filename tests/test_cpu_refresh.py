"""CPU regressions must retain complete execution evidence and trading sessions."""
from contextlib import closing
from datetime import datetime
import ast
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import model_tracking as model
from test_core_calculations import load_app_symbols
from test_model_tracking import signal


def test_full_buffer_bridge_does_not_reparse_old_ticks(tmp_path, monkeypatch):
    tracker = model.ModelTracker(tmp_path / 'buffer.sqlite', start=False)
    base = pd.Timestamp('2026-10-08T09:30:10+08:00')
    row = model.advance(model.new_trade(signal()), {
        '價格': 100., '來源時間': base.isoformat(), '交易日': '2026-10-08',
        '有效': True, '序號': 4096, '串流識別': 'desktop:1'})
    with closing(tracker.db()) as db, db:
        tracker.store(db, row)
    ticks = [{'價格': 100., '來源時間': (base + pd.Timedelta(milliseconds=i - 4096)).isoformat(),
              '序號': i, '串流識別': 'desktop:1'} for i in range(51, 4147)]
    calls = []
    original_stamp = model.stamp
    def counted(value):
        calls.append(value)
        return original_stamp(value)
    monkeypatch.setattr(model, 'stamp', counted)
    tracker.process([], {'股票|2330': {**ticks[-1], '有效': True, '交易日': '2026-10-08', '成交證據': ticks}})
    result = tracker.records()[0]
    assert result['最後序號'] == 4146 and result['狀態'] == '模擬持倉'
    assert not result['資料缺口'] and len(ticks) == 4096
    assert len(calls) < 200  # Only the new executions and bridge need timestamp parsing.


@pytest.mark.parametrize('bridge_time,bridge_price,expected_gap', [
    ('2026-10-08T09:30:10+08:00', 100., False),
    ('2026-10-08T01:30:10+00:00', 100., False),
    ('2026-10-08T09:30:10.000000+08:00', 100., False),
    ('2026-10-08T09:30:10+08:00', 101., True),
    ('2026-10-08T09:30:09+08:00', 100., True),
])
def test_bridge_keeps_timezone_price_and_device_validation(tmp_path, bridge_time, bridge_price, expected_gap):
    tracker = model.ModelTracker(tmp_path / 'bridge.sqlite', start=False)
    row = model.advance(model.new_trade(signal()), {
        '價格': 100., '來源時間': '2026-10-08T09:30:10+08:00',
        '交易日': '2026-10-08', '有效': True, '序號': 1, '串流識別': 'desktop:1'})
    with closing(tracker.db()) as db, db:
        tracker.store(db, row)
    ticks = [{'價格': bridge_price, '來源時間': bridge_time, '序號': 1, '串流識別': 'mobile:1'},
             {'價格': 100., '來源時間': '2026-10-08T09:30:11+08:00', '序號': 2, '串流識別': 'mobile:1'}]
    tracker.process([], {'股票|2330': {**ticks[-1], '交易日': '2026-10-08', '有效': True, '成交證據': ticks}})
    result = tracker.records()[0]
    assert result['資料缺口'] is expected_gap
    assert result['最後序號'] == 2 and result['模擬進場價'] == row['模擬進場價']


@pytest.fixture(scope='module')
def futures_calculations():
    ns = load_app_symbols('calculate_futures_strategy_levels', 'get_futures_trading_date',
        'get_futures_tick_size', 'FUTURES_FIXED_TICK_SIZES', 'round_futures_price',
        'clamp_futures_intraday_levels', '_safe_number', 'fmt_price')
    ns['is_market_closed_func'] = lambda d: d.weekday() >= 5
    return ns


@pytest.mark.parametrize('hour,timezone,day_only', [
    (10, False, False), (16, False, False), (2, True, False),
    (10, True, True), (16, False, True), (5, False, False),
])
def test_futures_sessions_and_atr_match_full_history(futures_calculations, monkeypatch, hour, timezone, day_only):
    ns = futures_calculations
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = ns['pytz'].timezone('Asia/Taipei').localize(cls(2026, 10, 12, hour))
            return value if tz else value.replace(tzinfo=None)
    monkeypatch.setitem(ns, 'datetime', Clock)
    monkeypatch.setitem(ns, 'is_market_closed_func', lambda d: d.weekday() >= 5 or d.isoformat() == '2026-10-09')
    index = pd.date_range('2026-10-01 08:45', '2026-10-12 16:00', freq='7min')
    if day_only:
        index = index[(index.hour >= 9) & (index.hour < 13)]
    close = 20000 + np.arange(len(index)) * .1
    data = pd.DataFrame({'Open': close - 1, 'High': close + 2, 'Low': close - 2,
                         'Close': close, 'Volume': np.arange(len(index)) + 1.}, index=index)
    keys = index.normalize() + pd.to_timedelta((index.hour >= 15).astype(int), unit='D')
    dates = keys.map({k: ns['get_futures_trading_date'](k.to_pydatetime()).date() for k in keys.unique()})
    labels = pd.Series(list(zip(dates, np.where((index.hour >= 15) | (index.hour < 5), 'night', 'day'))), index=index)
    kind = 'night' if hour >= 15 or hour < 5 else 'day'
    matching = labels[labels.map(lambda value: value[1] == kind)]
    selected = matching.iloc[-1] if not matching.empty else labels.iloc[-1]
    expected_session = data.loc[labels.map(lambda value: value == selected)]
    observed = []
    original_guard = ns['vwap_guard']
    def capture(frame, *args):
        observed.append(frame)
        return original_guard(frame, *args)
    monkeypatch.setitem(ns, 'vwap_guard', capture)
    row = {'期貨代碼': 'TX', '商品類型': '指數', '收盤價': close[-1], '開盤價': close[0],
           '當日高': close[-1] + 2, '當日低': close[0] - 2, '當日漲停價': 23000., '當日跌停價': 18000.}
    source = data.copy()
    if timezone:
        source.index = source.index.tz_localize('Asia/Taipei').tz_convert('UTC')
    result = ns['calculate_futures_strategy_levels'](row, '當沖', '自動', source)
    pd.testing.assert_frame_equal(observed[0], expected_session, check_freq=False)
    tr = pd.concat([data['High'] - data['Low'], (data['High'] - data['Close'].shift()).abs(),
                    (data['Low'] - data['Close'].shift()).abs()], axis=1).max(axis=1)
    typical = (expected_session['High'] + expected_session['Low'] + expected_session['Close']) / 3
    assert result['ATR'] == tr.tail(14).mean()
    assert result['VWAP'] == pytest.approx((typical * expected_session['Volume']).sum() / expected_session['Volume'].sum())
    pd.testing.assert_frame_equal(data, source.tz_convert('Asia/Taipei').tz_localize(None) if timezone else source, check_freq=False)


@pytest.mark.parametrize('count', [1, 2, 14, 15, 16])
def test_short_history_atr_keeps_first_and_previous_close(futures_calculations, count):
    ns = futures_calculations
    close = np.arange(count) * 10. + 20000
    data = pd.DataFrame({'Open': close, 'High': close + 2, 'Low': close - 2,
                         'Close': close, 'Volume': 100.},
                        index=pd.date_range('2026-10-20 15:00', periods=count, freq='min'))
    result = ns['calculate_futures_strategy_levels']({'期貨代碼': 'TX', '商品類型': '指數',
        '當日漲停價': 23000., '當日跌停價': 18000.}, '當沖', '自動', data)
    assert result['ATR'] == pytest.approx((4 + (count - 1) * 12) / count if count <= 14 else 12)


def test_cpu_only_change_keeps_existing_model_version_but_future_rules_change_it():
    ns = load_app_symbols('calculate_futures_strategy_levels', 'enrich_futures_strategy_rows',
                          'calculate_entry_confidence', 'model_tracking_samples')
    source = Path(__file__).parents[1] / 'app.py'
    node = next(n for n in ast.parse(source.read_text(encoding='utf-8')).body
                if isinstance(n, ast.FunctionDef) and n.name == 'get_model_rules_version')
    node.decorator_list = []
    ns.update(inspect=inspect, MODEL_VERSION=model.VERSION)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), source, 'exec'), ns)
    expected = model.VERSION + '-d8631cd521'
    assert ns['get_model_rules_version'](False) == expected
    def future_strategy(*args):
        return 'Different rules require a new version'
    ns['calculate_futures_strategy_levels'] = future_strategy
    assert ns['get_model_rules_version'](False) != expected
