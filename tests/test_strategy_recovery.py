"""Trading-session colors, expired login, and prediction-driven model records."""
from collections import deque
from datetime import datetime, timedelta
from types import SimpleNamespace
import threading
from pathlib import Path

import pandas as pd
import pytz

from test_core_calculations import load_app_symbols
from test_model_tracking import real_stock_adapter, stock_rows_with_evidence
from model_tracking import ModelTracker, performance


def limit_helpers():
    ns = load_app_symbols('strategy_row_limit_state', '_safe_number', 'parse_strategy_data_time',
                          'stock_limit_state', 'futures_limit_state', 'calculate_limits', 'get_tick_size',
                          'apply_tick_rules', 'round_futures_price', 'get_futures_tick_size',
                          'FUTURES_FIXED_TICK_SIZES', 'model_market_open', 'intraday_auto_window_open',
                          'latest_completed_stock_trading_date')
    ns['is_market_closed_func'] = lambda d: d.weekday() >= 5 or d.isoformat() == '2026-10-09'
    return ns


def test_name_color_uses_active_quote_reference_and_latest_completed_session():
    ns = limit_helpers()
    now = pytz.timezone('Asia/Taipei').localize(datetime(2026, 10, 12, 10))
    row = {'收盤價':110., '漲跌幅':0., '_quote_time':'2026/10/12 10:00:00',
           '_交易日漲停價':110., '當日漲停價':110., '_交易日跌停價':90.}
    state = ns['strategy_row_limit_state']
    assert state(row, now_tw=now) == ''  # Friday's limit-up is Monday's unchanged quote.
    assert state({**row, '收盤價':121., '漲跌幅':10.}, now_tw=now) == 'up'
    assert state({**row, '收盤價':99., '漲跌幅':-10.}, now_tw=now) == 'down'
    assert state({**row, '_quote_time':'2026/10/08 13:30:00', '漲跌幅':10.}, now_tw=now) == ''
    assert state({**row, '_quote_time':'2026/10/12 08:00:00', '漲跌幅':10.}, now_tw=now) == ''
    # Before Monday's open the last actual session is Thursday (Friday is a holiday).
    prior = {**row, '_quote_time':'2026/10/08 13:30:00', '漲跌幅':10.}
    assert state(prior, now_tw=now.replace(hour=8)) == 'up'
    assert state({**prior, '_data_stale':True}, now_tw=now.replace(hour=8)) == ''
    # After today's close, tomorrow's forecast columns cannot suppress today's limit.
    assert state({**row, '收盤價':121., '漲跌幅':10., '當日漲停價':133.,
                  '_quote_time':'2026/10/12 13:30:00'}, now_tw=now.replace(hour=16)) == 'up'


def test_futures_name_color_rejects_day_session_quote_during_night_session():
    ns = limit_helpers()
    now = pytz.timezone('Asia/Taipei').localize(datetime(2026, 10, 12, 16))
    row = {'收盤價':110., '漲跌幅':0., '報價時間':'2026/10/12 16:00:00',
           '期貨代碼':'TX', '商品類型':'指數', '當日漲停價':110., '交易時段':'日盤+夜盤'}
    state = ns['strategy_row_limit_state']
    assert state(row, False, now) == ''
    assert state({**row, '漲跌幅':10.}, False, now) == 'up'
    assert state({**row, '漲跌幅':10., '報價時間':'2026/10/12 13:45:00'}, False, now) == ''
    assert state({**row, '收盤價':90., '漲跌幅':-10.}, False, now) == 'down'
    assert state({**row, '漲跌幅':10.}, False, now.replace(day=13, hour=1)) == 'up'


def test_expired_stock_contract_login_prompts_instead_of_crashing():
    ns = load_app_symbols('get_shioaji_stock_contract', '_is_shioaji_auth_error', 'check_shioaji_connection')
    class AuthError(Exception):
        pass
    class Expired:
        def __getitem__(self, code):
            raise AuthError('Not authenticated')
    state = {'lock':threading.RLock()}
    ns['_stream_state'] = lambda api:state
    warnings, invalidated = [], []
    ns['st'] = SimpleNamespace(session_state=SimpleNamespace(), warning=warnings.append)
    ns['invalidate_shioaji_connection'] = lambda:invalidated.append(True)
    api = SimpleNamespace(Contracts=SimpleNamespace(Stocks=Expired()))
    assert ns['get_shioaji_stock_contract'](api, '3624') is None
    assert not ns['check_shioaji_connection'](api)
    assert invalidated == [True] and '已斷線需重新連線' in warnings[0]
    assert ns['st'].session_state.sj_connection_error == 'AuthError'
    contract = object()
    assert ns['get_shioaji_stock_contract'](SimpleNamespace(Contracts=SimpleNamespace(Stocks={'3624':contract})), '3624') is contract
    assert ns['get_shioaji_stock_contract'](SimpleNamespace(Contracts=SimpleNamespace(Stocks={})), 'missing') is None


def test_displayed_3624_prediction_is_recorded_without_confidence_or_account_trades(tmp_path):
    ns = real_stock_adapter()
    rows, config = stock_rows_with_evidence(ns, 1)
    rows.loc[0, ['代號','名稱','收盤價']] = ['3624','光頡',99.5]
    rows.loc[0, '進出場預判'] = '進 100｜停 95｜目 110'
    rows.loc[0, '_系統方向'] = '多頭'
    rows.loc[0, '信心分'] = 1
    rows.loc[0, '_附加可記錄'] = False
    config['min_score'] = 100
    state = ns['_stream_state'](None)
    now = datetime.now(pytz.timezone('Asia/Taipei'))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    ns['datetime'] = Clock
    tracker = ModelTracker(tmp_path/'prediction.sqlite', start=False)
    for i, price in enumerate((99.5, 100.5, 110.5)):
        now += timedelta(seconds=1)
        rows.loc[0, '_quote_time'] = now.isoformat()
        rows.loc[0, '收盤價'] = price
        buffer = state['model_tick_buffers'].setdefault('3624', deque(maxlen=4096))
        buffer.append({'來源時間':now.isoformat(),'價格':price,'序號':i+1,'串流識別':'test:0','有效':True})
        signals, quotes = ns['model_tracking_samples'](rows, True, '當沖', '多', config, 5, object())
        assert len(signals) == 1 and signals[0]['商品鍵'] == '3624'
        tracker.process(signals, quotes)
        assert len(tracker.records()) == 1
        assert tracker.records()[0]['狀態'] == ('等待進場','模擬持倉','已平倉')[i]
    assert performance(tracker.records())[0]['有效交易數'] == 1


def test_prediction_without_ticks_is_listed_but_not_counted_as_a_win(tmp_path):
    ns = real_stock_adapter()
    rows, config = stock_rows_with_evidence(ns, 1)
    state = ns['_stream_state'](None)
    state['model_tick_buffers'].clear()
    rows['進出場預判'] = '進 100.5｜停 95｜目 110'
    rows['_系統方向'] = '多頭'
    now = datetime.now(pytz.timezone('Asia/Taipei'))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    ns['datetime'] = Clock
    signals, quotes = ns['model_tracking_samples'](rows, True, '當沖', '多', config, 5, object())
    assert len(signals) == 1 and not quotes['股票|2330']['有效']
    tracker = ModelTracker(tmp_path/'waiting.sqlite', start=False)
    tracker.process(signals, quotes)
    assert tracker.records()[0]['狀態'] == '資料不足'
    assert not tracker.records()[0]['資料缺口']
    assert performance(tracker.records())[0]['有效交易數'] == 0
    state['model_tick_buffers']['2330'] = deque(maxlen=4096)
    for i, price in enumerate((100.5, 110.5)):
        now += timedelta(seconds=1)
        rows.loc[0, ['收盤價', '_quote_time']] = [price, now.isoformat()]
        state['model_tick_buffers']['2330'].append({
            '來源時間': now.isoformat(), '價格': price, '序號': i + 1, '串流識別': 'test:0', '有效': True})
        signals, quotes = ns['model_tracking_samples'](rows, True, '當沖', '多', config, 5, object())
        tracker.process(signals, quotes)
    assert len(tracker.records()) == 1 and tracker.records()[0]['狀態'] == '已平倉'
    assert performance(tracker.records())[0]['有效交易數'] == 1


def test_model_only_background_follows_published_plan_without_market_polling():
    ns = real_stock_adapter()
    ns.update(load_app_symbols('record_model_table'))
    rows, config = stock_rows_with_evidence(ns, 1)
    rows['進出場預判'] = '進 100.5｜停 95｜目 110'
    captured, submissions = {}, []
    class Worker:
        def configure(self, room, signature, seed, seconds, update, ready, release):
            captured.update(room=room, seed=seed, update=update, release=release)
        def remove(self, room):
            captured['removed'] = room
    worker, api = Worker(), object()
    ns.update(weakref=__import__('weakref'), data_version=lambda x:str(x),
              get_intraday_background=lambda:worker, check_shioaji_connection=lambda a:True,
              model_tracking_config=lambda *a:(config,'多'), get_app_secret=lambda k:'',
              get_model_tracker=lambda u:SimpleNamespace(submit=lambda s,q:submissions.append((s,q))),
              intraday_auto_settings=lambda room:(False,5,None,None),
              get_shioaji_stock_contract=lambda a,c:SimpleNamespace(code=c),
              sync_strategy_stream_scope=lambda *a:None,
              fresh_strategy_stream_quote=lambda q:False)
    ns['st'] = SimpleNamespace(session_state={'sj_api':api,'sj_logged_in':True})
    def stream_quotes(a, contracts, snapshot_fallback):
        assert not snapshot_fallback  # Cache only; never poll snapshots, ticks, or kbars.
        return []
    ns['get_stream_quotes'] = stream_quotes
    ns['record_model_table'].__globals__.update(ns)
    ns['record_model_table']('stock', rows, True, '當沖')
    assert captured['room'] == 'stock_model' and submissions[0][0]
    captured['update'](captured['seed'])
    assert len(submissions) == 2 and submissions[1][0][0]['進場價'] == 100.5
    old_release = captured['release']
    rows['進出場預判'] = '進 101｜停 95｜目 110'
    ns['record_model_table']('stock', rows, True, '當沖')
    old_release()
    state = ns['_stream_state'](api)
    assert state['model_scope_versions']  # A replaced job cannot clear the new scope's evidence.
    captured['release']()
    assert not state['model_scope_versions']


def test_stock_history_warmup_auth_error_keeps_table_visible_in_app_test():
    import ast
    import textwrap
    from streamlit.testing.v1 import AppTest
    source = (Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8')
    names = {'get_shioaji_stock_contract','_is_shioaji_auth_error','check_shioaji_connection','invalidate_shioaji_connection'}
    definitions = '\n'.join(ast.unparse(n) for n in ast.parse(source).body
                            if isinstance(n, ast.FunctionDef) and n.name in names)
    marker = source.index('# Warm the manual button')
    start = source.rfind("                if st.session_state.get('sj_logged_in'", 0, marker)
    warmup = textwrap.dedent(source[start:source.index('                edited_df =', marker)])
    setup = """
import streamlit as st
import pandas as pd, threading
from types import SimpleNamespace
class AuthError(Exception):
    pass
class Expired:
    def __getitem__(self, code):
        raise AuthError('Not authenticated')
state = {'lock':threading.RLock()}
_stream_state = lambda api: state
clear_market_stream = lambda api: None
get_shared_market_data_cache = lambda: ({'shioaji_usage':{}}, threading.RLock())
init_shioaji_connection = SimpleNamespace(clear=lambda:None)
get_strategy_intraday_history = lambda *a: (_ for _ in ()).throw(AssertionError('No history request after authentication failure'))
st.session_state.sj_logged_in = True
st.session_state.sj_api = SimpleNamespace(Contracts=SimpleNamespace(Stocks=Expired()))
st.session_state.stock_main_visible_codes = ['3624']
"""
    app = AppTest.from_string(setup + definitions + '\n' + warmup + "\nst.dataframe(pd.DataFrame([{'名稱':'光頡','收盤價':100.}]))", default_timeout=30).run()
    assert not app.exception and len(app.dataframe) == 1
    assert not app.session_state['sj_logged_in']
    assert any('已斷線需重新連線' in message.value for message in app.warning)


def test_swing_prediction_label_uses_supported_model_mode():
    ns = real_stock_adapter()
    rows, config = stock_rows_with_evidence(ns, 1)
    rows['進出場預判'] = '進 100.5｜停 95｜目 110'
    rows['_系統方向'] = '多頭'
    records, _ = ns['model_tracking_samples'](rows, True, '隔日／波段', '多', config, 5, object())
    assert len(records) == 1 and records[0]['策略'] == '波段'
