"""Official feed and background post-close regression checks."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from test_core_calculations import load_app_symbols


def helpers(*extra):
    return load_app_symbols('_as_float', '_ranking_number', '_ranking_market_date',
                            '_ranking_clamp', 'postclose_risk_version', 'enrich_futures_ranking_fields', '_safe_number', *extra)


def context():
    return {'date': '20261002', 'errors': [], 'source_dates': {
        name: '20261002' for name in ('上市法人', '上市融資券', '上櫃法人', '上櫃融資券',
                                     '上市估值', '上櫃估值', '期貨法人')},
        'stocks': {'1815': {'foreign_net': 0, 'trust_net': 0, 'dealer_net': 0}}, 'scales': {}}


def test_chip_readiness_accepts_zero_but_rejects_missing_or_wrong_date():
    ns = helpers('ranking_context_issues')
    rows = pd.DataFrame([{'代號': '1815'}])
    check = ns['ranking_context_issues']
    data = context()
    assert check(data, rows, 'stock', date(2026, 10, 2)) == []
    data['stocks']['1815']['trust_net'] = None
    assert '1815 法人籌碼缺項' in check(data, rows, 'stock', date(2026, 10, 2))
    data['stocks']['1815']['margin_delta'] = None
    assert '1815 融資券缺項' in check(data, rows, 'stock', date(2026, 10, 2))
    data['source_dates']['上櫃法人'] = '20261001'
    assert '上櫃法人尚未就緒' in check(data, rows, 'stock', date(2026, 10, 2))


def test_official_source_rejects_empty_error_and_mixed_dates():
    ns = helpers('fetch_ranking_source')
    payload = [[]]
    ns['requests'] = SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: payload[0]))
    ns['_TPEX_ORIGIN'] = 'https://www.tpex.org.tw/'
    fetch = ns['fetch_ranking_source']
    for invalid in ([], {'stat': 'ERROR', 'date': '20261002'},
                    [{'Date': '1151002', 'Code': '2330'}, {'Date': '1151001', 'Code': '2317'}]):
        payload[0] = invalid
        with pytest.raises(ValueError):
            fetch('twse_valuation', 'https://example.test/', None, '20261002')
    payload[0] = [{'Date': '1151002', 'Code': '2330'}]
    assert fetch('twse_valuation', 'https://example.test/', None, '20261002') == payload[0]


def test_tpex_parser_preserves_zero_and_missing_margin():
    ns = helpers('fetch_post_close_stock_ranking_context')
    ns.update(TWSE_MONTHLY_REVENUE_URL='revenue', TPEX_MONTHLY_REVENUE_URL='revenue')
    data = {
        'tpex_institutional': [{'Date': '1151002', 'SecuritiesCompanyCode': '1815',
            'TotalDifference': '12,000',
            'Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference': 0,
            'ForeignInvestorsInclude MainlandAreaInvestors-Difference': '999',
            'SecuritiesInvestmentTrustCompanies-Difference': '12000', 'Dealers-Difference': '0'}],
        'tpex_margin': [{'Date': '1151002', 'SecuritiesCompanyCode': '1815',
            'MarginPurchaseBalance': '--', 'MarginPurchaseBalancePreviousDay': '2',
            'ShortSaleBalance': '0', 'ShortSaleBalancePreviousDay': '0'}],
    }
    ns['fetch_ranking_source'] = lambda name, *args: data.get(name, [])
    row = ns['fetch_post_close_stock_ranking_context']('20261002', 'stock')['stocks']['1815']
    assert row['foreign_net'] == 0  # excludes foreign dealers, and 0 is not missing
    assert row['trust_net'] == 12000
    assert row['margin_delta'] is None
    assert row['short_delta'] == 0


def test_maintenance_freeze_and_postclose_boundaries():
    ns = helpers('_post_close_target_date', 'postclose_maintenance_window')
    ns['is_market_closed_func'] = lambda d: d.weekday() >= 5
    window = ns['postclose_maintenance_window']
    assert window('2026-10-05 08:29:59')[1:] == (date(2026, 10, 2), True, True)
    assert window('2026-10-05 08:30:00')[2:] == (False, False)
    assert window('2026-10-05 14:30:00')[1:] == (date(2026, 10, 5), True, False)
    assert window('2026-10-05 20:59:59')[3] is False
    assert window('2026-10-05 21:00:00')[1:] == (date(2026, 10, 5), True, True)
    assert window('2026-10-04 10:00:00')[1:] == (date(2026, 10, 2), True, True)


def test_snapshot_readiness_tracks_both_modes_and_visible_contract_month():
    ns = helpers('postclose_scope', 'postclose_snapshot_ready')
    rows = pd.DataFrame([{'期貨代碼': 'TX', '契約月份': '202610'}])
    snapshot = {'complete': True, 'target_date': '2026-10-02', 'scope': ['TX:202610']}
    check = ns['postclose_snapshot_ready']
    assert check({'daytrade': snapshot, 'swing': snapshot}, rows, 'futures', date(2026, 10, 2))
    assert not check({'daytrade': snapshot}, rows, 'futures', date(2026, 10, 2))
    rows['契約月份'] = '202611'
    assert not check({'daytrade': snapshot, 'swing': snapshot}, rows, 'futures', date(2026, 10, 2))


def test_background_job_uses_closed_prices_and_keeps_room_failures_independent():
    ns = helpers('build_postclose_job', 'ranking_context_issues', 'postclose_scope')
    captured = []
    ns['fetch_post_close_stock_ranking_context'] = lambda *a, **kw: context()
    def score(rows, mode, **kwargs):
        captured.append(rows.iloc[0].to_dict())
        return [{'code': '1815', 'score': 80, 'reason': '籌碼'}]
    ns['build_strategy_ranking_entries'] = score
    ns['fetch_postclose_futures_rows'] = lambda target: (_ for _ in ()).throw(ValueError('not ready'))
    stocks = pd.DataFrame([{'代號': '1815', '收盤價': 120, '漲跌幅': 10,
                           '_strategy_close': 100, '_strategy_change_rate': 0, '_ma5': 99,
                           '_strategy_data_as_of': '2026/10/02', '_daytrade_vwap': 119}])
    futures = pd.DataFrame([{'期貨代碼': 'TX', '契約鍵': 'TX:202610', '契約月份': '202610'}])
    result = ns['build_postclose_job']({'stock': stocks, 'futures': futures}, date(2026, 10, 2),
                                      False, False, {'updated': '2026/10/02', 'errors': []})
    assert set(result['rankings']['stock']) == {'daytrade', 'swing'}
    assert 'futures' not in result['rankings']
    assert result['errors']['futures'] == 'not ready'
    assert captured[0]['收盤價'] == 100 and captured[0]['漲跌幅'] == 0
    assert '_daytrade_vwap' not in captured[0]
    assert stocks.iloc[0]['收盤價'] == 120


def test_failed_context_cannot_overwrite_existing_snapshot():
    ns = helpers('refresh_strategy_ranking_snapshots', 'ranking_context_issues')
    old = {'daytrade': {'entries': [{'code': '1815', 'score': 80}]}}
    state = {'stock_strategy_ranking_snapshots': old}
    ns['st'] = SimpleNamespace(session_state=state)
    ns['ranking_snapshot_refresh_allowed'] = lambda *a: True
    ns['_state_updated_at'] = lambda *a: None
    ns['_post_close_target_date'] = lambda *a: (None, date(2026, 10, 2))
    ns['is_market_closed_func'] = lambda *a: True
    ns['resolve_post_close_ranking_context'] = lambda *a, **kw: {'errors': ['上櫃法人 timeout']}
    assert not ns['refresh_strategy_ranking_snapshots'](pd.DataFrame([{'代號': '1815'}]), 'stock', analysis=True)
    assert state['stock_strategy_ranking_snapshots'] is old
    assert state['_stock_ranking_waiting']

def test_background_completion_respects_freeze_and_newer_manual_snapshot():
    import ast
    import time
    from test_core_calculations import APP_PATH
    ns = helpers('_post_close_target_date', 'postclose_maintenance_window', 'postclose_scope', 'check_intraday_auto_timer', 'supplement_frozen_stock_rankings')
    tree = ast.parse(APP_PATH.read_text(encoding='utf-8'))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name == 'render_postclose_maintenance')
    node.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), APP_PATH, 'exec'), ns)
    ns['is_market_closed_func'] = lambda day: day.weekday() >= 5
    ns['time'] = time
    current = [pd.Timestamp('2026-10-05 08:30:00', tz='Asia/Taipei')]
    def timestamp(value):
        return pd.Timestamp(value)
    timestamp.now = lambda **kw: current[0]
    ns['pd'] = SimpleNamespace(Timestamp=timestamp)
    old = {'daytrade': {'updated_at': 'new-manual', 'entries': [1]}}
    pending = SimpleNamespace(done=lambda: True, result=lambda: {
        'rankings': {'stock': {'daytrade': {'updated_at': 'background', 'scope': ['1815']}}}, 'errors': {}})
    state = {'stock_strategy_ranking_snapshots': old, '_postclose_maintenance': {
        'sync_restored': True, 'seed_loaded': True, 'future': pending, 'target': '2026-10-02', 'versions': {'stock': {'daytrade': 'old-manual'}}}}
    ns['st'] = SimpleNamespace(session_state=state, caption=lambda *a: None)
    ns['render_postclose_maintenance']()
    assert state['stock_strategy_ranking_snapshots'] is old
    assert 'future' not in state['_postclose_maintenance']
    current[0] = pd.Timestamp('2026-10-04 21:00:00', tz='Asia/Taipei')
    state['_postclose_maintenance'].update(future=pending, phase='2026-10-02:2026-10-04:postclose', scopes={})
    ns['render_postclose_maintenance']()
    assert state['stock_strategy_ranking_snapshots'] is old

def test_tpex_eps_supports_official_english_company_code():
    ns = helpers('fetch_ranking_source')
    payload = [{'SecuritiesCompanyCode': '1815', '基本每股盈餘': '2.24'}]
    ns['requests'] = SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: payload))
    ns['_TPEX_ORIGIN'] = 'unused'
    assert ns['fetch_ranking_source']('tpex_eps', 'https://example.test', None, '20261002') == payload


def test_saved_futures_scope_keeps_contract_month_and_ignores_removed_rows():
    ns = helpers('postclose_futures_seed_rows')
    saved = {'universe': [{'期貨代碼': 'TX', '契約鍵': 'TX:202610'},
                          {'期貨代碼': 'TX', '契約鍵': 'TX:202611'},
                          {'期貨代碼': 'TMF', '契約鍵': 'TMF:202610'}],
             'strategy_ranking_snapshots': {'daytrade': {'scope': ['TX:202610', 'TMF:202610']}},
             'ignored': ['TMF:202610']}
    assert ns['postclose_futures_seed_rows'](saved)['契約鍵'].tolist() == ['TX:202610']

def test_maintenance_fragment_does_not_block_controls_while_worker_waits():
    import ast
    from test_core_calculations import APP_PATH
    from streamlit.testing.v1 import AppTest
    source = APP_PATH.read_text(encoding='utf-8')
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == 'render_postclose_maintenance')
    function_source = '@st.fragment(run_every=60)\n' + ast.get_source_segment(source, node)
    app_source = '''
import streamlit as st
import pandas as pd
import time
from datetime import date, time as dt_time
from types import SimpleNamespace
check_intraday_auto_timer = lambda room: None
supplement_frozen_stock_rankings = lambda *a: None
postclose_maintenance_window = lambda now: (pd.Timestamp('2026-10-02 21:00'), date(2026, 10, 2), True, True)
st.session_state.setdefault('_postclose_maintenance', {'sync_restored': True, 'seed_loaded': True, 'future': SimpleNamespace(done=lambda: False)})
''' + function_source + '''
with st.expander('選股條件', expanded=True):
    st.toggle('測試控制', key='control')
render_postclose_maintenance()
'''
    app = AppTest.from_string(app_source).run()
    assert not app.exception
    assert '背景補齊缺項中' in app.caption[0].value
    app.toggle(key='control').set_value(True).run()
    assert not app.exception
    assert app.toggle(key='control').value
    assert '背景補齊缺項中' in app.caption[0].value

def test_background_job_builds_four_modes_from_complete_sources():
    ns = helpers('build_postclose_job', 'ranking_context_issues', 'postclose_scope')
    ns['fetch_post_close_stock_ranking_context'] = lambda *a, **kw: context()
    ns['build_strategy_ranking_entries'] = lambda rows, mode, **kw: [{'code': 'ok', 'score': 80}]
    stocks = pd.DataFrame([{'代號': '1815', '_strategy_close': 100, '_strategy_change_rate': 0,
                           '_ma5': 99, '_strategy_data_as_of': '2026/10/02'}])
    futures = pd.DataFrame([{'期貨代碼': 'TX', '契約鍵': 'TX:202610', '契約月份': '202610'}])
    ns['fetch_postclose_futures_rows'] = lambda target: (futures, {})
    result = ns['build_postclose_job']({'stock': stocks, 'futures': futures}, date(2026, 10, 2),
                                      False, False, {'updated': '2026/10/02', 'errors': []})
    assert result['errors'] == {}
    assert set(result['rankings']) == {'stock', 'futures'}
    for asset in result['rankings'].values():
        assert set(asset) == {'daytrade', 'swing'}
        assert all(snapshot['complete'] and snapshot['source_date'] == '20261002' for snapshot in asset.values())

def test_background_cloud_sync_merges_each_scope_and_requires_readback(tmp_path):
    import json
    import threading
    ns = helpers('sync_postclose_scopes')
    stock_path, futures_path = tmp_path / 'stock.json', tmp_path / 'futures.json'
    stock_path.write_text(json.dumps({'updated_at': 'stock-new'}), encoding='utf-8')
    futures_path.write_text(json.dumps({'updated_at': 'futures-new'}), encoding='utf-8')
    ns.update(STOCK_STRATEGY_CACHE_FILE=stock_path, FUTURES_STATE_CACHE_FILE=futures_path,
              GOOGLE_SCOPE_STOCK='stock_strategy', GOOGLE_SCOPE_FUTURES='futures_strategy',
              _RUNTIME_FILE_LOCK=threading.RLock(), get_data_cache_sync_lock=lambda: threading.RLock())
    ns['_fetch_remote_scope'] = lambda url, scope, **kw: ({'from_remote': scope}, '')
    ns['merge_postclose_scope'] = lambda remote, local, asset: {**remote, **local}
    writes = []
    def save(url, scope, payload, **kwargs):
        writes.append((scope, payload, kwargs))
        return scope == 'stock_strategy', ''
    ns['_save_remote_scope'] = save
    ns['_write_json_atomic'] = lambda path, data, **kw: path.write_text(json.dumps(data), encoding='utf-8')
    assert ns['sync_postclose_scopes']('configured-url', ['stock', 'futures']) == ['stock']
    assert [row[1]['updated_at'] for row in writes] == ['stock-new', 'futures-new']
    assert all(row[1]['from_remote'] == row[0] and row[2]['verify'] is True for row in writes)

def test_background_merge_preserves_remote_selections_and_newer_ranking():
    ns = helpers('merge_postclose_scope', '_newer_timestamped_state', '_state_updated_at')
    remote = {'stock_data': [{'代號': '2330'}], 'quick_search_state': {'codes': ['2330']},
              'strategy_ranking_snapshots': {'daytrade': {'updated_at': '2026-10-02T22:00:00+08:00', 'score': 99}}}
    local = {'stock_data': [{'代號': '1815'}], 'quick_search_state': {'codes': ['1815']},
             'strategy_ranking_snapshots': {'daytrade': {'updated_at': '2026-10-02T21:30:00+08:00', 'score': 10},
                                            'swing': {'updated_at': '2026-10-02T21:30:00+08:00', 'score': 80}}}
    result = ns['merge_postclose_scope'](remote, local, 'stock')
    assert result['stock_data'] == remote['stock_data']
    assert result['quick_search_state'] == remote['quick_search_state']
    assert result['strategy_ranking_snapshots']['daytrade']['score'] == 99
    assert result['strategy_ranking_snapshots']['swing']['score'] == 80
