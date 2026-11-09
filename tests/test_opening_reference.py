import ast
from contextlib import closing
from types import SimpleNamespace

from streamlit.testing.v1 import AppTest

from model_tracking import ModelTracker, new_trade
from test_core_calculations import APP_PATH, load_app_symbols
from test_model_tracking import signal


def test_blocked_chase_preserves_original_stock_levels_when_waiting():
    ns = load_app_symbols('build_guarded_stock_trade_plan', 'build_trade_plan',
                          'parse_trade_plan_numbers', 'has_trade_prediction_signal',
                          '_as_float', '_safe_number', 'get_tick_size', 'get_taiwan_tick_size',
                          'move_tick', 'round_to_tick', 'fmt_price')
    ns['st'] = SimpleNamespace(session_state={})
    row = {'代號': '2330', '_daytrade_data_time': '2026/11/09 09:20:00',
           '_daytrade_close': 113, '_daytrade_vwap': 109, '_daytrade_or_high': 114,
           '_daytrade_or_low': 108, '_交易日漲停價': 116, '_交易日跌停價': 94}
    plan = ns['build_guarded_stock_trade_plan']
    blocked = plan(row, '多頭', True, {'eligible': True})
    assert '可接受進場 ≤112' in blocked['summary']
    assert not blocked['valid'] and blocked['tracking_only']
    waiting = plan({**row, '_daytrade_data_time': '2026-11-09T09:21:00+08:00',
                    '_daytrade_close': 112}, '多頭', True, {'eligible': False})
    assert waiting['summary'].startswith('多·觀望·沿用｜可接受進場 ≤112｜停 109｜目 116')
    assert waiting['summary'].endswith('｜保 —')
    assert not ns['has_trade_prediction_signal'](waiting['summary'])
    assert not plan(row, '多頭', True, {}, room='stock_independent').get('tracking_only')
    assert not plan({**row, '_daytrade_data_time':'2026-11-10'}, '多頭', True, {}).get('tracking_only')


def test_latest_saved_reference_is_scoped_and_read_only(tmp_path):
    tracker = ModelTracker(tmp_path/'reference.sqlite', start=False)
    def saved(key, when, **fields):
        return new_trade(signal(商品鍵=key, 交易日=when[:10].replace('/', '-'),
                                來源時間=when, **fields))
    records = [
        saved('2330', '2026-11-05T10:00:00+08:00'),
        saved('2330', '2026-11-06T09:00:00+08:00', 進場價=101),
        saved('2330', '2026-11-06T12:00:00+08:00', 進場價=102, 策略='波段'),
        saved('2330', '2026-11-09T09:10:00+08:00', 進場價=103),
        saved('2330', '2026-11-06T13:00:00+08:00', 市場='期貨', 計畫來源='指數操作計畫', 進場價=104),
        saved('2330', '2026-11-06T13:10:00+08:00', 市場='期貨', 進場價=105),
        saved('TX:202611', '2026-11-06T13:00:00+08:00', 市場='期貨'),
        saved('TX:202612', '2026-11-06T13:15:00+08:00', 市場='期貨', 進場價=102),
        saved('1815', '2026-11-02T08:29:00+08:00'),
    ]
    with tracker.write_db() as db:
        for index, record in enumerate(records):
            record['交易ID'] = str(index)
            tracker.store(db, record)
    before = tracker.records()
    times = ('2026-11-02T08:30:00+08:00', '2026-11-09T08:30:00+08:00')
    query = load_app_symbols('opening_reference_plans')['opening_reference_plans']
    with closing(tracker.db()) as db:
        assert query(db, '股票', ['2330', '1815'], '當沖', *times) == {
            '2330': ('2026-11-06T09:00:00+08:00', '多頭', 101, 95, 110),
        }
        assert query(db, '期貨', ['2330', 'TX:202611'], '當沖', *times)['2330'][2] == 105
        assert query(db, '期貨', ['TX:202611'], '當沖', *times)['TX:202611'][2] == 100
    assert tracker.records() == before


def test_opening_reference_uses_cached_legacy_tracker_and_yields_to_live_plan(tmp_path):
    db_path = tmp_path / 'legacy.sqlite'
    tracker = ModelTracker(db_path, start=False)
    with tracker.write_db() as db:
        tracker.store(db, new_trade(signal(交易日='2026-11-06', 來源時間='2026-11-06T09:00:00+08:00')))
    tree = ast.parse(APP_PATH.read_text(encoding='utf-8'))
    names = {'opening_reference_plans', 'apply_opening_reference'}
    render = '\n'.join(ast.unparse(node) for node in tree.body
                       if isinstance(node, ast.FunctionDef) and node.name in names)
    script = f"database = {str(db_path)!r}\n" + """
import sqlite3
from contextlib import closing
from sqlite3 import OperationalError
import pandas as pd
import pytz
import streamlit as st
from datetime import datetime as real_datetime, timedelta
from types import SimpleNamespace
class datetime:
    @staticmethod
    def now(tz):
        return tz.localize(real_datetime(2026, 11, 9, 8, 30))
def model_market_open(row, now):
    return market_open
get_app_secret = lambda key: ''
@st.cache_resource
def get_model_tracker(url):
    def connect():
        if st.session_state.get('unavailable', False):
            raise OperationalError('temporary unavailable')
        return sqlite3.connect(database)
    return SimpleNamespace(db=connect, hydrated=True)
fmt_price = lambda value: f'{value:g}'
cached_tracker = get_model_tracker('')
assert not hasattr(cached_tracker, 'opening_reference_plans')
""" + render + """
market_open = st.checkbox('已開盤')
st.checkbox('模擬讀取失敗', key='unavailable')
frame = pd.DataFrame([{'代號':'2330','進出場預判':'—','訊號狀態':'等待'},
                      {'代號':'1815','進出場預判':'空·已進場｜進 100｜停 105｜保 98｜目 90'}])
apply_opening_reference(frame,True,'當沖')
st.dataframe(frame)
"""
    app = AppTest.from_string(script).run()
    assert not app.exception
    displayed = app.dataframe[0].value['進出場預判']
    assert displayed.iloc[0] == '進 100｜停 95｜目 110｜保啟≈105'
    assert displayed.iloc[1].startswith('空·已進場')
    assert app.dataframe[0].value['訊號狀態'].iloc[0] == '⚪ 開盤參考'
    app.checkbox(key='unavailable').set_value(True).run()
    assert not app.exception and app.dataframe[0].value['進出場預判'].iloc[0] == '—'
    assert '開盤參考暫時無法讀取' in app.caption[0].value
    app.checkbox(key='unavailable').set_value(False).run()
    assert not app.exception and app.dataframe[0].value['進出場預判'].iloc[0] == displayed.iloc[0]
    app.checkbox[0].set_value(True).run()
    assert not app.exception and app.dataframe[0].value['進出場預判'].iloc[0] == '—'
