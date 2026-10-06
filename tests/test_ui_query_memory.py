"""Saved selections survive widget cleanup and independent market-data versions."""
import ast
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from test_core_calculations import load_app_symbols


def test_restore_queries_uses_own_version_including_explicit_empty_list():
    ns = load_app_symbols('load_data_cache', 'normalize_stock_quick_search_state',
                          '_newer_timestamped_state', '_state_updated_at')
    local = {'stock_data': [{'代號':'1815'}], 'stock_data_updated_at':'2026-10-02',
             'quick_search_state': {'main':['2408 南亞科'], 'independent':['2330 台積電'],
                                    'updated_at':'2026-10-06T10:00:00+08:00'},
             'postclose_sync_pending': True}
    remote = {'stock_data': [{'代號':'2330'}], 'stock_data_updated_at':'2026-10-06',
              'quick_search_state': {'main':['1815 富喬'], 'updated_at':'2026-10-05'}}
    state, writes = {}, []
    ns.update(st=SimpleNamespace(session_state=state), STOCK_STRATEGY_CACHE_FILE='stock.json',
              _read_json_cache_file=lambda _: local, get_app_secret=lambda _: 'configured',
              GOOGLE_SCOPE_STOCK='stock_strategy', _fetch_remote_scope_cached=lambda *a, **k: (remote, ''),
              _cache_payload_timestamp=lambda d: pd.Timestamp(d['stock_data_updated_at']),
              _write_json_atomic=lambda p, d, **kw: writes.append(dict(d)), _json_safe=lambda v: v,
              normalize_stock_display_settings=lambda v: v, load_search_cache=lambda: ['stale'])
    assert ns['load_data_cache']()[0]['代號'].tolist() == ['2330']
    assert state['_stock_quick_search_state'] == local['quick_search_state']
    assert writes[-1]['quick_search_state'] == local['quick_search_state']
    local['quick_search_state'] = {'main':[], 'independent':[], 'updated_at':'2026-10-06T11:00:00+08:00'}
    ns['load_data_cache']()
    assert state['_stock_quick_search_state']['main'] == []
    assert writes[-1]['quick_search_state'] == local['quick_search_state']


def test_hidden_stock_widget_cannot_erase_other_selector_or_resurrect_removed_queries():
    ns = load_app_symbols('get_stock_quick_search_state', 'normalize_stock_quick_search_state',
                          'persist_stock_quick_search_state', 'merge_postclose_scope',
                          '_newer_timestamped_state', '_state_updated_at')
    class State(dict):
        __getattr__ = dict.__getitem__
    saved = {'main':['2408 南亞科'], 'independent':['2330 台積電'], 'updated_at':'2026-10-05'}
    state = State(_stock_quick_search_state=saved, search_multiselect=[], indep_search_multiselect=[],
                  stock_data=pd.DataFrame(), ignored_stocks=set(), all_candidates=[], saved_notes={})
    writes = []
    ns.update(st=SimpleNamespace(session_state=state), save_search_cache=lambda v: None,
              save_data_cache=lambda *a, **kw: writes.append(ns['get_stock_quick_search_state']()) or True)
    assert ns['get_stock_quick_search_state']() == saved
    ns['persist_stock_quick_search_state']('main')
    assert writes[-1]['main'] == [] and writes[-1]['independent'] == ['2330 台積電']
    merged = ns['merge_postclose_scope']({'quick_search_state': saved},
                                        {'quick_search_state':writes[-1]}, 'stock')
    assert merged['quick_search_state'] == writes[-1]


def test_query_widgets_restore_hidden_tabs_and_use_stable_futures_keys(tmp_path):
    from streamlit.testing.v1 import AppTest
    names = {'normalize_stock_quick_search_state', 'get_stock_quick_search_state',
             'persist_stock_quick_search_state', 'render_stock_quick_search', 'render_futures_quick_add'}
    tree = ast.parse((Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8'))
    definitions = '\n'.join(ast.unparse(n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names)
    source = """
import streamlit as st
import pandas as pd
import json,pytz
from datetime import datetime
from pathlib import Path
""" + f"path = Path({str(tmp_path / 'selection.json')!r})\n" + """
saved = json.loads(path.read_text()) if path.exists() else {}
st.session_state.setdefault('_stock_quick_search_state', saved.get('stock', {}))
st.session_state.setdefault('futures_strategy_manual', saved.get('futures', []))
st.session_state.setdefault('futures_strategy_ignored', set())
st.session_state.setdefault('futures_strategy_editor_revision', 0)
st.session_state.setdefault('stock_data', pd.DataFrame())
st.session_state.setdefault('ignored_stocks', set())
st.session_state.setdefault('all_candidates', [])
st.session_state.setdefault('saved_notes', {})
build_stock_search_options = lambda: ['2330 台積電']
save_search_cache = lambda v: None
def save_data_cache(*a, **kw):
    path.write_text(json.dumps({'stock':st.session_state['_stock_quick_search_state'],
                               'futures':st.session_state.futures_strategy_manual}))
    return True
def persist(**kw):
    assert kw['sync_cloud'] is False
    save_data_cache()
""" + definitions + """
if not st.session_state.get('hide', False):
    render_stock_quick_search('main')
    with st.expander('⚙️ 期貨篩選、策略與顯示設定'):
        place = st.container()
    volume = st.session_state.get('volume', 100)
    data = pd.DataFrame([{'契約鍵':'CDF:202610','期貨代碼':'CDF','契約月份':'202610',
                          '名稱':'台積電期貨','當日成交口數':volume}])
    render_futures_quick_add(data, place, persist)
"""
    app = AppTest.from_string(source).run()
    app.multiselect(key='search_multiselect').set_value(['2330 台積電']).run()
    app.multiselect(key='futures_quick_add').set_value(['CDF:202610']).run()
    app.session_state['volume'] = 200
    app.run()
    assert not app.exception and app.multiselect(key='futures_quick_add').value == ['CDF:202610']
    assert any(m.key == 'futures_quick_add' for m in app.expander[0].multiselect)
    app.session_state['hide'] = True
    app.run()
    app.session_state['hide'] = False
    app.run()
    assert not app.exception and app.multiselect(key='search_multiselect').value == ['2330 台積電']
    assert app.multiselect(key='futures_quick_add').value == ['CDF:202610']
    restarted = AppTest.from_string(source).run()
    assert not restarted.exception and restarted.multiselect(key='search_multiselect').value == ['2330 台積電']
    assert restarted.multiselect(key='futures_quick_add').value == ['CDF:202610']
    restarted.multiselect(key='search_multiselect').set_value([]).run()
    restarted.multiselect(key='futures_quick_add').set_value([]).run()
    again = AppTest.from_string(source).run()
    assert again.multiselect(key='search_multiselect').value == []
    assert again.multiselect(key='futures_quick_add').value == []


def test_futures_compaction_retains_all_queries_and_background_sync_selection_version():
    ns = load_app_symbols('compact_futures_strategy_state', '_safe_number', 'merge_postclose_scope',
                          '_prefer_futures_state_section', '_parse_futures_state_time', '_state_updated_at', '_newer_timestamped_state')
    ns['_json_safe'] = lambda v: v
    keys = [f'F{i}:202610' for i in range(60)]
    state = {'manual':keys, 'selection_updated_at':'2026-10-06', 'universe':[]}
    assert ns['compact_futures_strategy_state'](state)['manual'] == keys
    older = {'manual':['CDF:202610'], 'ignored':[], 'selection_updated_at':'2026-10-05'}
    assert ns['merge_postclose_scope'](older, state, 'futures')['manual'] == keys
    cleared = {**state, 'manual':[], 'selection_updated_at':'2026-10-07'}
    assert ns['merge_postclose_scope'](state, cleared, 'futures')['manual'] == []


def test_eps_money_formats_thousands_exactly_and_only_financial_fields():
    f = load_app_symbols('format_company_event_detail')['format_company_event_detail']
    event = {'category':'financials', 'detail':'營業收入 123456.000；稅後淨利 -12.34560；資料版本 20261006'}
    assert f(event) == '營業收入 1億2千萬345萬6000元；稅後淨利 -1萬2345.6元；資料版本 20261006'
    event['detail'] = '營業收入 0.000；稅後淨利 未取得；EPS 1.2500'
    assert f(event) == '營業收入 0元；稅後淨利 未取得；EPS 1.2500'
    assert f({'category':'disclosures', 'detail':'營業收入 100；單位：億元'}) == '營業收入 100；單位：億元'
    already = {'category':'financials','detail':'營業收入 1億2千萬元；稅後淨利 2萬元'}
    assert f(already) == already['detail']


def test_holiday_labels_distinguish_observed_and_actual_dates():
    ns = load_app_symbols('market_holiday_title', 'get_holidays')
    f = ns['market_holiday_title']
    detail = '國慶日為10月10日適逢星期六，於10月9日（星期五）補假。'
    assert f(pd.Timestamp('2026-10-09'), '國慶日', detail) == '國慶日 補假'
    assert f(pd.Timestamp('2026-10-10'), '國慶日', detail) == '國慶日'
    fallback = ns['get_holidays'](2026)
    assert fallback[(10,26)] == '臺灣光復暨金門古寧頭大捷紀念日 補假'
    assert fallback[(10,25)] == '臺灣光復暨金門古寧頭大捷紀念日'
