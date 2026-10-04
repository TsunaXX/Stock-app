import threading
import ast
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from market_automation import (BackgroundJobs, attach_macro_results, data_version,
                               index_scenario, macro_results, merge_company_sections, merge_macro_results, vwap_guard)
from test_core_calculations import load_app_symbols


def bars(closes, volumes=None):
    return pd.DataFrame({'Open': closes, 'Close': closes,
                         'High': [v + .1 for v in closes], 'Low': [v - .1 for v in closes],
                         'Volume': volumes or [100] * len(closes)},
                        index=pd.date_range('2026-10-05 09:20', periods=len(closes), freq='min'))


def test_band_retains_direction_but_never_admits_new_entry():
    assert vwap_guard(None, 100.05, 100, .1)['side'] is None
    prior = vwap_guard(None, 100.5, 100, .1)['side']
    result = vwap_guard(None, 99.95, 100, .1, prior)
    assert result['side'] == 'long'
    assert not result['ready']
    assert vwap_guard(None, 99.5, 100, .1, prior)['side'] == 'short'


def test_buffer_follows_tick_and_short_volatility():
    data = bars([100, 103, 99, 104, 100])
    quiet = vwap_guard(None, 102, 100, .05)
    volatile = vwap_guard(data, 102, 100, .05)
    assert volatile['buffer'] > quiet['buffer']
    assert vwap_guard(None, 102, 100, 1)['buffer'] == 2


def test_chop_veto_and_immediate_escape_do_not_wait_for_bar_close():
    data = bars([100.2, 99.8] * 5)
    result = vwap_guard(data, 100.2, 100, .05)
    assert result['crosses'] >= 3
    assert not result['ready']
    assert '反覆穿越' in result['reason']
    # Same current minute's stream price escapes the previous ten-minute range.
    data.iloc[-1, data.columns.get_loc('Close')] = 101
    data.iloc[-1, data.columns.get_loc('High')] = 101
    assert vwap_guard(data, 101, 100, .05)['ready']


@pytest.mark.parametrize('values,price,average,side', [
    ([102, 101.8, 101.6, 101.4, 101.2, 101, 100.8, 100.6], 102, 101, 'long'),
    ([98, 98.2, 98.4, 98.6, 98.8, 99, 99.2, 99.4], 98, 99, 'short'),
])
def test_opposing_vwap_slope_rejects_entry(values, price, average, side):
    result = vwap_guard(bars(values), price, average, .05)
    assert result['side'] == side and not result['ready']
    assert '斜率反向' in result['reason']


def test_recent_crossings_expire_without_holding_up_fresh_trend():
    data = bars([100.2, 99.8] * 5 + list(range(101, 116)))
    assert vwap_guard(data, 116, 105, .05)['ready']


def test_missing_numeric_values_do_not_form_a_direction():
    assert not vwap_guard(None, float('nan'), 100, .1)['ready']


def test_background_job_is_nonblocking_singleflight_and_keeps_last_success():
    jobs = BackgroundJobs()
    started, finish = threading.Event(), threading.Event()
    calls = []
    def loader():
        calls.append(1)
        started.set()
        finish.wait(2)
        return {'version': 1}
    try:
        assert jobs.poll('data', loader) is None
        assert started.wait(1)
        for _ in range(5):
            assert jobs.poll('data', loader) is None
        assert len(calls) == 1
        finish.set()
        jobs.jobs['data']['future'].result(2)
        assert jobs.poll('data', loader) == {'version': 1}
        def fail():
            raise ValueError('partial network failure')
        assert jobs.poll('data', fail, interval=0) == {'version': 1}
        try:
            jobs.jobs['data']['future'].result(2)
        except ValueError:
            pass
        assert jobs.poll('data', loader) == {'version': 1}
    finally:
        finish.set()
        jobs.pool.shutdown(wait=True)


def test_company_updates_preserve_selection_and_partial_company_success():
    old = {'calendar_companies': ['2330'], 'revenue_date_overrides': {'2330:11508': '2026-09-09'},
           'taiwan_revenue': {'events': [
               {'ticker': '2330', 'date': '2026-09-09', 'revenue': {'revenue_month': '11508', 'yoy': '10%'}},
               {'ticker': '1815', 'date': '2026-09-10', 'revenue': {'revenue_month': '11508'}}]}}
    new = merge_company_sections(old, {'taiwan_revenue': {'events': [
        {'ticker': '2330', 'date': '2026-09-09', 'revenue': {'revenue_month': '11508', 'yoy': '12%'}}]}})
    assert new['calendar_companies'] == ['2330']
    assert new['revenue_date_overrides'] == old['revenue_date_overrides']
    assert len(new['events']) == 2
    assert new['events'][0]['revenue']['yoy'] == '12%'
    assert merge_company_sections(new, {'taiwan_revenue': {'events': []}})['events'] == new['events']


def test_company_older_revision_cannot_replace_newer_eps():
    newer = {'financials': {'events': [{'event_id': 'eps:2330:115:2', 'data_asof': '20261005', 'title': 'EPS 10'}]}}
    incoming = {'financials': {'events': [{'event_id': 'eps:2330:115:2', 'data_asof': '20261004', 'title': 'EPS 9'}]}}
    assert merge_company_sections(newer, incoming)['events'][0]['title'] == 'EPS 10'


def test_delayed_company_result_only_fills_missing_events_without_replacing_newer_revision():
    current = {'taiwan_revenue': {'events': [
        {'event_id': 'rev:2330:11508', 'title': 'YoY +12%'}]}}
    delayed = {'taiwan_revenue': {'events': [
        {'event_id': 'rev:2330:11508', 'title': 'YoY +10%'},
        {'event_id': 'rev:1815:11508', 'title': 'YoY +5%'}]}}
    events = merge_company_sections(current, delayed, prefer_existing=True)['events']
    assert [e['title'] for e in events] == ['YoY +12%', 'YoY +5%']


@pytest.mark.parametrize('include_analysis', [False, True])
def test_futures_quote_refresh_preserves_strategy_or_checks_each_rows_prior_stop(include_analysis):
    ns = load_app_symbols('update_futures_live_rows', '_safe_number', 'snapshot_change_rate',
                          '_stream_datetime', 'set_futures_row_values', 'parse_trade_plan_numbers')
    rows = pd.DataFrame([{'期貨代碼': code, '契約月份': '202610', '收盤價': 100,
        '開盤價': 100, '當日高': 101, '當日低': 99, '當日成交口數': 10,
        '原始保證金率': 13., '維持保證金率': 10., '乘數': 2000., 'VWAP': 100.,
        '進出場點位': '保留原策略', '_last_intraday_stop': stop,
        '_last_intraday_direction': '偏多', '_last_intraday_session': 'night'}
        for code, stop in [('CDF', 101), ('DHF', 99)]])
    ns['filter_active_futures_rows'] = lambda r: (r.copy(), [])
    ns['resolve_shioaji_futures_contract'] = lambda api, code, month: SimpleNamespace(code=code)
    ns['get_stream_quotes'] = lambda api, contracts, **kw: [SimpleNamespace(
        close=100.5, open=100, high=101, low=99, total_volume=11, change_price=0,
        avg_price=100.2, updated_at=pd.Timestamp.now(tz='Asia/Taipei')) for _ in contracts]
    ns['get_futures_tick_size'] = lambda *a: .5
    ns['round_futures_price'] = lambda price, *a: price
    def history(*a, **kw):
        assert include_analysis, 'Quote-only refresh must not request history'
        return pd.DataFrame()
    def analysis(*a):
        assert include_analysis, 'Quote-only refresh must not recalculate strategy'
        return {'方向': '偏多', '進出場點位': '等待｜均價緩衝區內',
                '_daytrade_guard_session': 'night'}
    ns['get_strategy_intraday_history'], ns['calculate_futures_strategy_levels'] = history, analysis
    ns['fmt_price'] = lambda value: str(value)
    updated, count = ns['update_futures_live_rows'](rows, object(), '當沖', '自動', include_analysis)
    assert count == 2 and list(updated['收盤價']) == [100.5, 100.5]
    if include_analysis:
        assert '前次預判停損已碰觸' in updated.iloc[0]['進出場點位']
        assert '前次預判停損已碰觸' not in updated.iloc[1]['進出場點位']
    else:
        assert list(updated['進出場點位']) == ['保留原策略'] * 2
        assert list(updated['VWAP']) == [100.] * 2


def test_zero_macro_actual_is_published_and_compared_in_same_units():
    rows = [{'date': '2026-10-05T12:30:00Z', 'ticker': 'ECONOMICS:USNFP',
             'title': 'Non Farm Payrolls', 'actual': 0, 'forecast': 100, 'previous': None, 'scale': 'K'}]
    result = macro_results(rows)
    sources = {'大非農': [{'date': '2026-10-05', 'title': '美國大非農／失業率', 'detail': '官方排程'}]}
    merged = attach_macro_results(sources, result, pd.Timestamp('2026-10-05T21:00:00+08:00'))
    text = merged['大非農'][0]['detail']
    assert '已公布' in text and '實際 0K' in text and '低於預期' in text and '前值 未取得' in text
    missing = macro_results([{**rows[0], 'actual': None}])
    assert attach_macro_results(merged, missing, pd.Timestamp('2026-10-05T21:05:00+08:00'))['大非農'][0]['release_results'][0]['actual'] == 0
    assert '待公布' in attach_macro_results(sources, result, pd.Timestamp('2026-10-05T19:00:00+08:00'))['大非農'][0]['detail']
    assert data_version(result) == data_version(result)


def test_different_pce_series_remain_separate_and_missing_forecast_is_not_zero():
    rows = [{'date': '2026-10-05T12:30:00Z', 'ticker': ticker, 'title': title,
             'actual': .2, 'forecast': None, 'previous': .1} for ticker, title in (
                 ('ECONOMICS:USCPCEPIMM', 'Core PCE Price Index MoM'),
                 ('ECONOMICS:USCPCEPIAC', 'Core PCE Price Index YoY'))]
    result = attach_macro_results({'核心 PCE': [{'date': '2026-10-05'}]}, macro_results(rows),
                                  pd.Timestamp('2026-10-05T21:00:00+08:00'))
    event = result['核心 PCE'][0]
    assert len(event['release_results']) == 2
    assert '預期未取得' in event['detail']


def test_macro_month_cache_retains_published_zero_after_partial_or_empty_response():
    row = {'date': '2026-10-05T12:30:00Z', 'ticker': 'ECONOMICS:USNFP',
           'title': 'Non Farm Payrolls', 'actual': 0, 'forecast': 100}
    known = macro_results([row])
    partial = macro_results([{**row, 'actual': None}])
    assert merge_macro_results(known, partial) == known
    assert merge_macro_results(known, {}) == known
    revised = merge_macro_results(known, macro_results([{**row, 'actual': 10}]))
    assert revised[('大非農', '2026-10-05')][0]['actual'] == 10


def test_index_stop_is_checked_before_near_support_or_breakout():
    plan = {'support': 20000, 'resistance': 20500, 'zone_points': 20,
            'invalidation': 19980, 'direction': '偏多'}
    assert '失效' in index_scenario(plan, 19970)
    assert '接近支撐' in index_scenario(plan, 20010)
    assert '突破壓力' in index_scenario(plan, 20530)
    assert plan['support'] == 20000


def test_extra_company_scopes_survive_normalization_and_calendar_only_selection():
    ns = load_app_symbols('empty_company_event_snapshot', 'normalize_company_event_snapshot',
                          'apply_revenue_announcement_date_overrides', 'selected_company_calendar_snapshot', 'company_calendar_key')
    snap = {'calendar_companies': ['2330'], 'disclosures': {'events': [
        {'ticker': '2330', 'category': 'disclosures', 'date': '2026-10-05', 'title': '公告', 'source': 'MOPS'},
        {'ticker': '1815', 'category': 'disclosures', 'date': '2026-10-05', 'title': '另一公告'}]},
        'financials': {'events': [{'ticker': '2330', 'category': 'financials', 'date': '', 'title': 'EPS'}]}}
    normalized = ns['apply_revenue_announcement_date_overrides'](snap)
    assert len(normalized['events']) == 3
    assert normalized['taiwan_revenue']['events'] == []
    selected = ns['selected_company_calendar_snapshot'](normalized)
    assert len(selected['disclosures']['events']) == 1
    assert selected['financials']['events'][0]['date'] == ''


def test_stock_guard_keeps_direction_and_stop_notice_even_while_waiting():
    ns = load_app_symbols('determine_stock_direction', '_as_float', 'build_guarded_stock_trade_plan', '_safe_number')
    ns['st'] = SimpleNamespace(session_state={})
    ns['build_trade_plan'] = lambda *args: {'summary': '—', 'valid': False}
    ns['fmt_price'] = lambda v: f'{v:g}'
    row = {'代號': '2330', '_daytrade_close': 99, '_daytrade_vwap': 100, '_daytrade_or_high': 101,
           '_daytrade_or_low': 98, '_daytrade_guard_side': 'long', '_daytrade_guard_ready': False,
           '_daytrade_guard_reason': '緩衝區內', '_daytrade_data_time': '2026/10/05 10:00:00'}
    direction = ns['determine_stock_direction'](row, True)
    assert direction['direction'] == '多頭' and '觀望' in direction['label']
    ns['st'].session_state['_stock_last_intraday_plans'] = {
        '2330': {'session': '2026/10/05', 'stop': 99.5, 'direction': '多頭'}}
    result = ns['build_guarded_stock_trade_plan'](row, '多頭', True, {'eligible': False})
    assert '停損已碰觸' in result['summary']
    row['_daytrade_data_time'] = '2026/10/06 10:00:00'
    assert '停損已碰觸' not in ns['build_guarded_stock_trade_plan'](row, '多頭', True, {})['summary']


def test_option_timer_tracks_at_most_three_candidates_and_two_legs_without_queries():
    ns = load_app_symbols('refresh_option_candidate_cache')
    contracts = [SimpleNamespace(code=f'C{i}') for i in range(8)]
    directional = {'alternatives': [{'contract': c} for c in contracts[:6]],
                   'expiry': pd.Timestamp('2026-10-07').date(), 'profile': '自動評選', 'trade_ready': True}
    spread = {'short_contract': contracts[6], 'long_contract': contracts[7]}
    ns['st'] = SimpleNamespace(session_state={'sj_api': object(), '_option_plan_quote_cache': {
        'directional': directional, 'spread': spread, 'updated_at': pd.Timestamp('2026-10-05')}})
    scoped = []
    ns['sync_strategy_stream_scope'] = lambda api, items, room: scoped.extend(items)
    ns['get_live_futures_snapshot'] = lambda api, product, stream_only: None
    ns['get_txo_snapshot_quotes'] = lambda api, items, snapshot_fallback: [{'fresh': False} for c in items]
    ns['rank_txo_directional_candidates'] = lambda *args: []
    ns['refresh_option_candidate_cache']({'direction': '偏多', 'latest': 20000})
    assert [c.code for c in scoped] == ['C0', 'C1', 'C2', 'C6', 'C7']
    cache = ns['st'].session_state['_option_plan_quote_cache']
    assert not cache['directional']['trade_ready'] and cache['spread']['quote_stale']
    assert cache['updated_at'] == pd.Timestamp('2026-10-05')


def test_index_intraday_resample_reuses_history_and_never_downloads_on_timer():
    ns = load_app_symbols('get_cached_index_minutes')
    ns['get_near_futures_contract'] = lambda *args: object()
    ns['ensure_market_stream_subscription'] = lambda *args: True
    history = bars([100 + i / 10 for i in range(20)])
    ns['get_strategy_intraday_history'] = lambda api, contract, asset, wait: history
    result = ns['get_cached_index_minutes'](object(), '5min')
    assert len(result) == 4 and result['Volume'].sum() == 2000


def test_index_tracking_fragment_ui_keeps_daily_plan_when_live_price_changes():
    from streamlit.testing.v1 import AppTest
    tree = ast.parse((Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8'))
    definitions = '\n'.join(ast.unparse(n) for n in tree.body if isinstance(n, ast.FunctionDef)
                            and n.name in {'get_stable_index_trade_plan', 'render_index_scenario_tracking'})
    source = '''
import streamlit as st
import pandas as pd
import pytz
import time
from datetime import datetime,date
from market_automation import data_version,index_scenario
_post_close_target_date = lambda: (None,date(2026,10,2))
calculate_market_temperature = lambda data: {}
def calculate_index_trade_plan(*args):
    st.session_state['builds'] = st.session_state.get('builds',0)+1
    return {'support':20000,'resistance':20500,'zone_points':20,'invalidation':19980,'direction':'偏多'}
def get_live_futures_snapshot(api, product, stream_only=False):
    assert stream_only
    return {'price':st.session_state.get('live',20010),'updated':datetime(2026,10,5,10)}
_stream_datetime = lambda value: value
''' + definitions + '''
data = pd.DataFrame({'High':[20100,20200,st.session_state.get('live',20010)],
                     'Low':[19800,19900,19800],'Close':[20000,20100,20010]},
                     index=pd.to_datetime(['2026-10-01','2026-10-02','2026-10-05']))
plan = get_stable_index_trade_plan(data,{},data,{})
enabled = st.toggle('盤中情境追蹤',key='auto',value=False)
st.session_state.setdefault('_index_scenario_live',{})['checked'] = 0
render_index_scenario_tracking(plan,enabled,5)
'''
    app = AppTest.from_string(source).run()
    assert not app.exception and app.session_state['builds'] == 1
    app.toggle(key='auto').set_value(True).run()
    assert not app.exception and '接近支撐' in app.info[0].value
    app.session_state['live'] = 19970
    app.run()
    assert not app.exception and '失效' in app.info[0].value
    assert app.session_state['builds'] == 1


def test_company_event_ui_renders_eps_and_disclosures_without_network():
    from streamlit.testing.v1 import AppTest
    tree = ast.parse((Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'render_company_event_snapshot')
    source = '''
import streamlit as st
import pandas as pd
snapshot = {'earnings':{},'taiwan_revenue':{},'us_revenue':{},'financials':{'events':[
 {'date':'','title':'台積電 EPS 10','detail':'實際公告日未取得','source':'上市官方公開資料'}]},
 'disclosures':{'events':[{'date':'2026-10-05','title':'台積電 重大訊息','detail':'公告內容','source':'上市官方公开資料'}]}}
''' + ast.unparse(node) + '\nrender_company_event_snapshot(snapshot)'
    app = AppTest.from_string(source).run()
    assert not app.exception
    frame = app.dataframe[0].value
    assert 'EPS 10' in frame.iloc[0]['公司／事件']
    assert frame.iloc[0]['日期'] == '公告日未取得'
    assert '重大訊息' in frame.iloc[1]['公司／事件']


def test_full_index_option_page_controls_can_toggle_without_losing_cached_plan(tmp_path):
    from unittest.mock import patch
    import requests
    from streamlit.testing.v1 import AppTest
    source = (Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8')
    setup = '''
st.session_state.main_workspace_active_tab = '📈 指數操盤室'
st.session_state.index_workspace_active_tab = '📅 選擇權操作計畫'
fixture = pd.DataFrame({'Open':[20000.]*90,'High':[20500.]*90,'Low':[19500.]*90,
                        'Close':[20100.]*90,'Volume':[1000.]*90},
                       index=pd.date_range('2026-07-05',periods=90,freq='D'))
get_cached_market_temperature_data = lambda *a, **kw: (fixture.copy(),'測試快取')
get_stable_index_trade_plan = lambda *a: {'market_label':'測試市場','alignment_note':'完整日 K',
    'direction':'偏多','action':'多方觀察','action_color':'#ff4b4b','latest':20100.,
    'swing_low':19500.,'swing_high':20500.,'support':20000.,'resistance':20500.,'entry_level':20000.,
    'invalidation':19900.,'target':20500.,'zone_points':20.,'trigger':'測試條件','atr':100.,
    'risk_points':100.,'reward_points':500.,'realized_volatility':.25,'latest_volume_ratio':1.,
    'rr_ratio':5.,'micro_risk_1':1000.,'micro_risk_2':2000.,'option_name':'價差',
    'short_strike':19900,'long_strike':19800,'max_spread_risk_before_credit':5000.}
get_live_futures_snapshot = lambda *a, **kw: None
get_cached_futures_intraday_state = lambda *a, **kw: {'available':False,'confirmation_text':'背景分 K',
    'confirmed':False,'is_up_bar':False,'is_down_bar':False,'bullish_break':False,'bearish_break':False,
    'vwap':None,'opening_high':None,'opening_low':None,'latest':None}
get_cached_short_wave_plan = lambda *a, **kw: None
resolve_short_wave_direction = lambda *a: ('偏多','測試方向')
select_txo_expiry = lambda *a, **kw: ([],None,'測試來源')
get_txo_directional_quote = lambda *a, **kw: None
get_txo_spread_quote = lambda *a, **kw: None
render_postclose_maintenance = lambda: None
'''
    anchor = 'tab1, tab_fibo, tab2, tab_db, tab_company, tab3 = st.tabs(['
    source = source.replace(anchor, 'CONFIG_FILE = ' + repr(str(tmp_path / 'config.json')) + '\n' + setup + '\n' + anchor)
    with patch('requests.get', side_effect=requests.ConnectionError('offline UI fixture')), \
         patch('requests.post', side_effect=requests.ConnectionError('offline UI fixture')), \
         patch('yfinance.download', return_value=pd.DataFrame()), \
         patch('yfinance.Ticker', return_value=SimpleNamespace(history=lambda *a, **kw: pd.DataFrame(), fast_info={}, info={})):
        app = AppTest.from_string(source, default_timeout=120).run()
        assert not app.exception
        assert app.toggle(key='option_auto_enabled').value is False
        app.toggle(key='option_auto_enabled').set_value(True).run()
        assert not app.exception
        app.number_input(key='option_auto_seconds').set_value(7).run()
        assert not app.exception and app.toggle(key='option_auto_enabled').value is True
        app.toggle(key='option_auto_enabled').set_value(False).run()
        assert not app.exception
        assert app.session_state['_option_plan_quote_cache']['signature'][0] == '最近到期'


def test_background_sheet_merge_preserves_remote_selection_dates_and_own_timestamp():
    ns = load_app_symbols('sync_company_background_snapshot', 'empty_company_event_snapshot',
                          'normalize_company_event_snapshot', 'apply_revenue_announcement_date_overrides',
                          '_cache_payload_timestamp')
    ns['GOOGLE_SCOPE_COMPANY'] = 'company_events'
    ns['get_data_cache_sync_lock'] = threading.RLock
    remote = {'tickers': '2330', 'calendar_companies': [], 'updated_at': '2026-10-05T11:00:00+08:00',
              'financials': {'events': [{'event_id':'eps','category':'financials','ticker':'2330',
                                        'date':'','data_asof':'20261005','title':'EPS 10'}]}}
    local = ns['normalize_company_event_snapshot']({**remote, 'calendar_companies':['2330'],
        'updated_at':'2026/10/04 10:00:00', 'financials':{'events':[
            {'event_id':'eps','category':'financials','ticker':'2330','date':'','data_asof':'20261004','title':'EPS 9'}]}})
    ns['_fetch_remote_scope'] = lambda *a, **kw: (remote, None)
    writes = []
    ns['_save_remote_scope'] = lambda url, scope, data, **kw: (writes.append((scope, data, kw)) or (True, None))
    assert ns['sync_company_background_snapshot']('https://test.invalid', local)
    _, data, options = writes[0]
    assert data['calendar_companies'] == []
    assert data['financials']['events'][0]['title'] == 'EPS 10'
    assert data['updated_at'] == remote['updated_at'] and options['verify'] is True
    ns['_fetch_remote_scope'] = lambda *a, **kw: (None, 'read failed')
    assert not ns['sync_company_background_snapshot']('https://test.invalid', local)
    assert len(writes) == 1
