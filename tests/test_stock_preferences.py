"""Stock preferences survive widget cleanup and preserve current source order."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from test_core_calculations import APP_PATH, load_app_symbols


def test_preferences_and_source_order_merge_independently_of_quotes():
    ns = load_app_symbols('merge_stock_preferences', 'merge_postclose_scope')
    remote = {'stock_data_updated_at': '2026-10-15T14:00:00+08:00',
              'display_settings': {'limit_rows': 5, 'hide_non_stock': False, 'updated_at': '2026-10-14'},
              'stock_source_kind': 'official', 'stock_source_updated_at': '2026-10-14',
              'all_candidates': [['8111', '立碁', 'upload', 0]]}
    local = {'stock_data_updated_at': '2026-10-13',
             'display_settings': {'limit_rows': 6, 'hide_non_stock': True, 'updated_at': '2026-10-15'},
             'stock_source_kind': 'official', 'stock_source_updated_at': '2026-10-15',
             'all_candidates': [['6668', '中揚光', 'upload', 0]]}
    for merge in (ns['merge_stock_preferences'], lambda a, b: ns['merge_postclose_scope'](a, b, 'stock')):
        result = merge(remote, local)
        assert result['display_settings'] == local['display_settings']
        assert result['all_candidates'] == local['all_candidates']
        assert result['stock_source_updated_at'] == '2026-10-15'
        assert merge(local, remote)['display_settings'] == local['display_settings']
        legacy = {**remote, 'stock_source_updated_at': '', 'stock_data_updated_at': '2026-10-20'}
        assert merge(local, legacy)['all_candidates'] == local['all_candidates']
        cleared = {**local, 'all_candidates': [], 'stock_source_updated_at': '2026-10-16'}
        assert merge(local, cleared)['all_candidates'] == []


def test_refill_uses_loaded_official_order_and_keeps_file_source_separate():
    ns = load_app_symbols('stock_source_candidates', 'current_stock_source_candidates')
    class State(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
    frame = pd.DataFrame({'代號': ['2330', '6668', '8111'], '名稱': ['台積電', '中揚光', '立碁']})
    state = State(_stock_source_kind='official', _stock_source_date='2026-10-14',
                  turnover_ranking_date='2026-10-15', turnover_ranking_df=frame,
                  all_candidates=[('8111', '立碁', 'upload', 0)],
                  prefetch_cache={'6668': {'代號': '8111'}}, stock_data=pd.DataFrame(),
                  ignored_stocks=set(), saved_notes={})
    saves = []
    ns.update(st=SimpleNamespace(session_state=state), save_data_cache=lambda *a, **kw: saves.append(kw))
    result = ns['current_stock_source_candidates']()
    assert [c[0] for c in result] == ['2330', '6668', '8111']
    assert not state.prefetch_cache and saves == [{'sync_cloud': False}]
    ns['current_stock_source_candidates']()
    assert len(saves) == 1
    state.turnover_ranking_date = '2026-10-13'
    state.turnover_ranking_df = pd.DataFrame({'代號': ['8111'], '名稱': ['立碁']})
    assert ns['current_stock_source_candidates']() == result
    state._stock_source_kind = 'file'
    state.all_candidates = [('1815', '富喬', 'upload', 0)]
    assert ns['current_stock_source_candidates']() == state.all_candidates
    mixed = pd.DataFrame({'代號': ['="2330"', '0050', '00679B', '123456', '2330', '標題'],
                          '名稱': ['台積電', 'ETF', '債券', '權證', '重複', '無效']})
    assert [c[0] for c in ns['stock_source_candidates'](mixed, True)] == ['2330']


def test_display_widgets_autosave_across_hidden_tabs_and_new_devices(tmp_path):
    from streamlit.testing.v1 import AppTest
    names = {'normalize_stock_display_settings', 'get_stock_display_settings',
             'merge_stock_preferences', '_newer_timestamped_state', '_state_updated_at',
             'persist_stock_display_settings', 'render_stock_strategy_controls'}
    tree = ast.parse(APP_PATH.read_text(encoding='utf-8'))
    definitions = '\n'.join(ast.unparse(n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names)
    source = '''
import streamlit as st
import pandas as pd
import pytz, threading, json
from datetime import datetime
from pathlib import Path
''' + f'STOCK_STRATEGY_CACHE_FILE=Path({str(tmp_path / "settings.json")!r})\n' + '''
_RUNTIME_FILE_LOCK=threading.RLock()
def _read_json_cache_file(path):
    return json.loads(path.read_text()) if path.exists() else {}
initial=_read_json_cache_file(STOCK_STRATEGY_CACHE_FILE).get('display_settings', {})
st.session_state.setdefault('_stock_display_settings', initial)
st.session_state.setdefault('stock_data',pd.DataFrame())
st.session_state.setdefault('ignored_stocks',set())
st.session_state.setdefault('all_candidates',[])
st.session_state.setdefault('saved_notes',{})
st.session_state.setdefault('limit_rows',5)
st.session_state.setdefault('stock_hide_non_stock',True)
def save_data_cache(*a, **kw):
    assert kw['sync_cloud'] is False
    STOCK_STRATEGY_CACHE_FILE.write_text(json.dumps({'display_settings':get_stock_display_settings()}))
def save_config(*a, **kw):
    raise AssertionError('Manual saving is obsolete')
''' + definitions + '''
hide=st.toggle('隱藏設定')
if not hide:
    render_stock_strategy_controls()
'''
    app = AppTest.from_string(source).run()
    migrated = json.loads((tmp_path / 'settings.json').read_text())['display_settings']
    assert migrated['updated_at'] and migrated['limit_rows'] == 5
    app.number_input(key='limit_rows_input').set_value(6).run()
    app.checkbox(key='stock_hide_non_stock').uncheck().run()
    assert not app.exception and not any(b.key == 'save_stock_room_settings' for b in app.button)
    assert json.loads((tmp_path / 'settings.json').read_text())['display_settings']['limit_rows'] == 6
    app.toggle[0].set_value(True).run()
    app.toggle[0].set_value(False).run()
    assert not app.exception and app.number_input(key='limit_rows_input').value == 6
    assert app.checkbox(key='stock_hide_non_stock').value is False
    second = AppTest.from_string(source).run()
    assert second.number_input(key='limit_rows_input').value == 6
    assert second.checkbox(key='stock_hide_non_stock').value is False
    second.number_input(key='limit_rows_input').set_value(8).run()
    app.checkbox(key='stock_hide_non_stock').check().run()
    restarted = AppTest.from_string(source).run()
    assert not restarted.exception and restarted.number_input(key='limit_rows_input').value == 8
    assert restarted.checkbox(key='stock_hide_non_stock').value is True


@pytest.mark.parametrize(('kind', 'expected'), [('official', '6668'), ('file', '8111')])
def test_full_stock_table_increase_uses_current_selected_source(tmp_path, kind, expected):
    from unittest.mock import patch
    import requests
    from streamlit.testing.v1 import AppTest
    setup = f"STOCK_STRATEGY_CACHE_FILE={str(tmp_path / 'stock.json')!r}\n" + f"fixture_kind={kind!r}\n" + '''
st.session_state.stock_data = pd.DataFrame([
    {'代號':code,'名稱':code,'收盤價':100,'漲跌幅':0,'狀態':'','期貨':'無','戰略備註':'固定',
     '_ma5':99,'_points':[],'_source':src,'_source_rank':1 if src=='upload' else 2,'_order':i}
    for i,(code,src) in enumerate([(c,'upload') for c in ['2330','2317','2454','2303','2308']]+[('1815','search')])])
st.session_state.ignored_stocks=set()
st.session_state._stock_display_settings={'limit_rows':6,'hide_non_stock':True,'updated_at':'2026-10-15'}
st.session_state._stock_source_kind=fixture_kind
st.session_state._stock_source_date='2026-10-14'
st.session_state._stock_source_updated_at='2026-10-14'
st.session_state.cloud_url_input=''
st.session_state.all_candidates=[('8111','立碁','upload',0)]
st.session_state.prefetch_cache={'8111':{'代號':'8111','名稱':'立碁','收盤價':100,'漲跌幅':0,
    '狀態':'','期貨':'無','戰略備註':'固定','_ma5':99,'_points':[]}}
st.session_state.turnover_ranking_date='2026-10-15'
st.session_state.turnover_ranking_df=pd.DataFrame({'代號':['2330','2317','2454','2303','2308','6668','8111'],
                                                 '名稱':['台積電','鴻海','聯發科','聯電','台達電','中揚光','立碁']})
st.session_state._refill_calls=[]
refill_calls=st.session_state._refill_calls
def fixture_fetch(code,name,*a,**kw):
    refill_calls.append(code)
    return {'代號':code,'名稱':name,'收盤價':100,'漲跌幅':0,'狀態':'','期貨':'無','戰略備註':'固定','_ma5':99,'_points':[]}
st.session_state._fixture_fetch=fixture_fetch
'''
    source = APP_PATH.read_text(encoding='utf-8')
    anchor = 'tab1, tab_fibo, tab2, tab_db, tab_company, tab3 = st.tabs(['
    source = source.replace(anchor, "if not st.session_state.get('_source_fixture'):\n" +
                            ''.join('    ' + line + '\n' for line in setup.strip().splitlines()) +
                            "    st.session_state['_source_fixture']=True\n" +
                            "fetch_stock_data_raw=st.session_state._fixture_fetch\n" +
                            "save_data_cache=lambda *a,**kw:True\n" +
                            "render_postclose_maintenance=lambda:None\n" +
                            "render_opening_direction_prompt=lambda:None\n" +
                            "get_app_secret=lambda key,default=None:default\n" + anchor)
    with patch('requests.get', side_effect=requests.ConnectionError('offline')), \
         patch('requests.post', side_effect=requests.ConnectionError('offline')), \
         patch('yfinance.download', return_value=pd.DataFrame()), \
         patch('yfinance.Ticker', return_value=SimpleNamespace(history=lambda *a, **kw:pd.DataFrame(), fast_info={}, info={})):
        app = AppTest.from_string(source, default_timeout=120).run()
    assert not app.exception
    assert app.session_state['stock_data']['代號'].tolist() == ['2330','2317','2454','2303','2308',expected,'1815']
    assert app.session_state['_refill_calls'] == (['6668'] if kind == 'official' else [])
