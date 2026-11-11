"""Cold-start recovery and durable independent-analysis snapshots."""
import copy
import logging
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from test_core_calculations import APP_PATH, load_app_symbols


def cache_helpers():
    return load_app_symbols(
        'load_data_cache', 'restore_stock_cache_to_session', 'restore_stock_quick_search',
        'normalize_stock_quick_search_state', 'get_stock_quick_search_state',
        'normalize_stock_display_settings', 'get_stock_display_settings', 'merge_stock_preferences',
        '_newer_timestamped_state', '_state_updated_at', '_cache_payload_timestamp',
        '_prefer_newer_cache_payload', 'merge_postclose_scope',
    )


def configure_cache(ns, state, disk, remote):
    ns.update(
        st=SimpleNamespace(session_state=state, rerun=lambda: state.update(rerun=True)),
        STOCK_STRATEGY_CACHE_FILE='stock.json', DATA_CACHE_FILE='legacy.json',
        GOOGLE_SCOPE_STOCK='stock_strategy', _RUNTIME_FILE_LOCK=threading.RLock(),
        _read_json_cache_file=lambda path: copy.deepcopy(disk.get(path, {})),
        _write_json_atomic=lambda path, data, **kw: disk.update({path: copy.deepcopy(data)}),
        get_app_secret=lambda name: 'configured',
        _fetch_remote_scope_cached=lambda *a, **kw: remote,
        _is_valid_data_cache_payload=lambda data: False, _json_safe=lambda data: data,
        load_search_cache=lambda: [], logger=logging.getLogger('restore-test'),
    )


def test_failed_cold_start_retries_the_whole_stock_snapshot_without_market_requests():
    ns, state, disk = cache_helpers(), {}, {}
    configure_cache(ns, state, disk, (None, 'timeout'))
    ns['restore_stock_cache_to_session']()
    assert state['stock_data'].empty and state['_stock_query_restore_pending']
    assert not disk  # A timeout must not create an authoritative empty snapshot.
    snapshot = {
        'stock_data': [{'代號': '2330', '名稱': '台積電', '收盤價': 100}],
        'stock_data_updated_at': '2026-11-11T09:00:00+08:00',
        'all_candidates': [['2330', '台積電', 'upload', 0]],
        'saved_notes': {'2330': '保留備註'}, 'ignored_stocks': ['2317'],
        'quick_search_state': {'main': ['2330 台積電'], 'independent': [],
                               'updated_at': '2026-11-11T09:00:00+08:00'},
    }
    state['_postclose_maintenance'] = {
        'query_restore_task': SimpleNamespace(done=lambda: True, result=lambda: (snapshot, None)),
    }
    ns['restore_stock_quick_search']()
    assert state['stock_data']['代號'].tolist() == ['2330']
    assert state['saved_notes'] == snapshot['saved_notes']
    assert state['ignored_stocks'] == {'2317'}
    assert state['all_candidates'] == snapshot['all_candidates']
    assert not state['_stock_query_restore_pending'] and state['rerun']
    assert not state['_stock_cache_refresh_pending']
    assert disk['stock.json']['stock_data'] == snapshot['stock_data']


@pytest.mark.parametrize('rows,source,version', [
    ([{'代號': '1815'}], 'file', '2026-11-11T10:00:00+08:00'),
    ([], 'none', '2026-11-11T10:00:00+08:00'),
])
def test_delayed_startup_read_does_not_replace_analysis_or_explicit_clear(rows, source, version):
    ns, disk = cache_helpers(), {}
    state = {'stock_data': pd.DataFrame(rows), '_stock_source_kind': source,
             '_stock_data_updated_at': version, '_stock_query_restore_pending': True}
    configure_cache(ns, state, disk, (None, 'timeout'))
    state['_postclose_maintenance'] = {'query_restore_task': SimpleNamespace(
        done=lambda: True, result=lambda: ({'stock_data': [{'代號': '2330'}]}, None))}
    ns['restore_stock_quick_search']()
    assert state['stock_data'].to_dict('records') == rows
    assert state['_stock_data_updated_at'] == version


def test_background_sync_preserves_newer_full_snapshot_and_versioned_empty():
    ns = cache_helpers()
    remote = {'stock_data': [{'代號': '2330'}], 'stock_data_updated_at': '2026-11-10'}
    local = {'stock_data': [{'代號': '1815'}], 'stock_data_updated_at': '2026-11-11',
             'saved_notes': {'1815': '使用者備註'}, 'ignored_stocks': ['2330']}
    merged = ns['merge_postclose_scope'](remote, local, 'stock')
    assert merged['stock_data'] == local['stock_data']
    assert merged['saved_notes'] == local['saved_notes']
    assert ns['merge_postclose_scope'](local, remote, 'stock')['stock_data'] == local['stock_data']
    cleared = {**local, 'stock_data': [], 'stock_data_updated_at': '2026-11-12'}
    assert ns['merge_postclose_scope'](local, cleared, 'stock')['stock_data'] == []


def test_independent_snapshots_merge_per_room_without_erasing_newer_clear():
    ns = cache_helpers()
    stock = {'rows': [{'代號': '2330', '_strategy_data_as_of': '2026/11/10'}]}
    futures = {'rows': [{'契約鍵': 'TMF:202611'}], 'trading_date': '2026-11-11'}
    first = {'display_settings': {'independent_analysis': {
        'values': {'stock': stock}, 'field_updated_at': {'stock': '2026-11-10'},
    }}}
    second = {'display_settings': {'independent_analysis': {
        'values': {'futures': futures}, 'field_updated_at': {'futures': '2026-11-11'},
    }}}
    merged = ns['merge_stock_preferences'](first, second)
    saved = ns['normalize_stock_display_settings'](merged['display_settings'])
    assert saved['independent_analysis']['values'] == {'stock': stock, 'futures': futures}
    cleared = {'display_settings': {'independent_analysis': {
        'values': {'stock': {'rows': []}}, 'field_updated_at': {'stock': '2026-11-12'},
    }}}
    final = ns['merge_stock_preferences'](merged, cleared)['display_settings']
    assert final['independent_analysis']['values'] == {'stock': {'rows': []}, 'futures': futures}


def test_preferences_only_read_keeps_the_available_local_stock_snapshot():
    ns, state = cache_helpers(), {}
    saved = {'stock_data': [{'代號': '2330'}], 'stock_data_updated_at': '2026-11-10'}
    disk = {'stock.json': saved}
    configure_cache(ns, state, disk, ({'updated_at': '2026-11-12', 'display_settings': {}}, None))
    assert ns['load_data_cache']()[0]['代號'].tolist() == ['2330']


@pytest.mark.parametrize('saved', [False, True])
def test_stock_independent_area_exists_without_main_rows(tmp_path, saved):
    from unittest.mock import patch
    import requests
    from streamlit.testing.v1 import AppTest

    source = APP_PATH.read_text(encoding='utf-8')
    fixture = """
st.session_state.stock_data = pd.DataFrame()
st.session_state.sj_logged_in = False
st.session_state.manual_logout = True
st.session_state.all_candidates = []
get_app_secret = lambda key, default=None: default
render_opening_direction_prompt = lambda: None
render_postclose_maintenance = lambda: None
saved_independent_analysis = lambda room: fixture_snapshot if room == 'stock' else {}
"""
    fixture = ("CONFIG_FILE = " + repr(str(tmp_path / 'config.json')) + "\n"
               "STOCK_STRATEGY_CACHE_FILE = " + repr(str(tmp_path / 'stock.json')) + "\n"
               "fixture_snapshot = " + repr({'rows': []}) + "\n" + fixture)
    if saved:
        fixture += """
fixture_snapshot = {'strategy_mode':'隔日／波段', 'rows': [{'代號':'2330','名稱':'台積電','收盤價':100,'漲跌幅':0,
    '_points':[],'_ma5':99,'_strategy_close':100,'戰略備註':'','期貨':'無','狀態':'',
    '_strategy_data_as_of':latest_completed_stock_trading_date().strftime('%Y/%m/%d')}]}
"""
    anchor = 'tab1, tab_fibo, tab2, tab_db, tab_company, tab3 = st.tabs(['
    source = source.replace(anchor, fixture + '\n' + anchor)
    with patch('requests.get', side_effect=requests.ConnectionError('offline')), \
         patch('requests.post', side_effect=requests.ConnectionError('offline')), \
         patch('yfinance.download', return_value=pd.DataFrame()), \
         patch('yfinance.Ticker', return_value=SimpleNamespace(
             history=lambda *a, **kw: pd.DataFrame(), fast_info={}, info={})):
        app = AppTest.from_string(source, default_timeout=90).run()
        assert not app.exception
        assert any('獨立計算' in heading.value for heading in app.markdown)
        assert app.button(key='btn_indep_run').label == '🚀 執行分析'
        if saved:
            assert app.session_state['stock_independent_raw_results'][0]['代號'] == '2330'
            assert app.radio(key='indep_strategy_mode').value == '隔日／波段'
            assert len(app.dataframe) >= 1
