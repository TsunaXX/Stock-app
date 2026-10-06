"""Missing visible stocks are added without rescoring frozen entries or blocking UI."""
from datetime import date
from types import SimpleNamespace
import threading

import pandas as pd
import pytest

from test_core_calculations import load_app_symbols


class State(dict):
    __getattr__ = dict.__getitem__


def test_frozen_supplement_only_scores_missing_rows_saves_all_and_never_changes_existing_scores():
    ns = load_app_symbols('supplement_frozen_stock_rankings', 'postclose_scope', 'build_postclose_job',
                          'run_postclose_job', '_ranking_market_date', '_ranking_number', '_as_float', 'postclose_risk_version', 'strategy_ranking_weights', 'merge_market_risk_refresh')
    now = pd.Timestamp('2026-10-06 10:00:00')
    original = [{'code': str(i), 'score': 80 - i, 'reason': 'frozen', 'coverage': 100} for i in range(5)]
    snapshots = {mode: {'target_date': '2026-10-05', 'updated_at': '2026-10-06T08:00:00+08:00',
                        'complete': True, 'weights': ns['strategy_ranking_weights']('stock', '當沖' if mode == 'daytrade' else '波段'),
                        'entries': original.copy()} for mode in ('daytrade', 'swing')}
    rows = pd.DataFrame([{'代號': str(i), '_strategy_close': 100, '_strategy_change_rate': 0,
                         '_strategy_data_as_of': '2026/10/05', '_ma5': 99, '收盤價': 999,
                         '_daytrade_vwap': 998} for i in range(56)])
    state = State(stock_strategy_ranking_snapshots=snapshots, _postclose_visible_stock=rows,
                  stock_data=rows, ignored_stocks=[], all_candidates=[], saved_notes={},
                  risk_filter_market_data={})
    reruns, saved, scored = [], [], []
    risk_calls = []
    def fetch_risk():
        risk_calls.append(True)
        return {'1815': 1}, [], [], {'1815': '上櫃'}, []
    ns['fetch_market_risk_lists'] = fetch_risk
    ns['st'] = SimpleNamespace(session_state=state, rerun=lambda: reruns.append(True))
    ns['is_market_closed_func'] = lambda day: False
    ns['_post_close_target_date'] = lambda *a: (now, date(2026, 10, 5))
    ns['fetch_post_close_stock_ranking_context'] = lambda *a, **kw: {'source_dates': {}}
    ns['ranking_context_issues'] = lambda *a: []
    def score(data, mode, **kwargs):
        scored.append(data.copy())
        assert set(data['代號']) == {str(i) for i in range(5, 56)}
        assert set(data['收盤價']) == {100} and '_daytrade_vwap' not in data
        return [{'code': row['代號'], 'score': 95, 'reason': 'closed'} for _, row in data.iterrows()]
    ns['build_strategy_ranking_entries'] = score
    ns['save_data_cache'] = lambda *a, **kw: saved.append(kw)
    ns['get_app_secret'] = lambda key: 'https://test.invalid/'
    def submit(function, *args):
        result = function(*args)
        return SimpleNamespace(done=lambda: True, result=lambda: result)
    worker = {'slot': threading.BoundedSemaphore(1), 'executor': SimpleNamespace(submit=submit)}
    ns['get_postclose_worker'] = lambda: worker
    maintenance = {}
    supplement = ns['supplement_frozen_stock_rankings']
    supplement(now, maintenance)
    assert state['stock_strategy_ranking_snapshots'] is snapshots and not reruns
    supplement(now, maintenance)
    assert state['stock_strategy_ranking_snapshots']['swing']['entries'] == original
    assert len(state['stock_strategy_ranking_snapshots']['daytrade']['entries']) == 56
    supplement(now, maintenance)
    supplement(now, maintenance)
    for updated in state['stock_strategy_ranking_snapshots'].values():
        assert len(updated['entries']) == 56
        assert [e for e in updated['entries'] if e['code'] in {str(i) for i in range(5)}] == original
        assert updated['target_date'] == '2026-10-05' and updated['supplemented']
    assert saved == [{'sync_cloud': False}, {'sync_cloud': False}] and maintenance['pending_sync']['stock']
    assert risk_calls == [True] and state['risk_filter_market_data']['updated']
    assert state['risk_filter_market_data']['attention'] == {'1815': 1}
    supplement(now, maintenance)
    assert len(scored) == 2


def test_supplement_discards_delayed_results_and_retries_failures_without_changing_snapshot():
    ns = load_app_symbols('supplement_frozen_stock_rankings', 'postclose_scope', 'strategy_ranking_weights')
    old = {'daytrade': {'target_date': '2026-10-05', 'updated_at': 'a', 'entries': []}}
    state = State(stock_strategy_ranking_snapshots=old, _postclose_visible_stock=pd.DataFrame([{'代號':'2330'}]))
    reruns = []
    ns['st'] = SimpleNamespace(session_state=state, rerun=lambda: reruns.append(True))
    now = pd.Timestamp('2026-10-06 10:00')
    ns['is_market_closed_func'] = lambda day: False
    ns['_post_close_target_date'] = lambda *a: (now, date(2026, 10, 5))
    from market_automation import data_version
    signature = ('2026-10-05', ('2330',), data_version(old))
    pending = SimpleNamespace(done=lambda: True, result=lambda: {'errors': {'stock':'法人缺項'}, 'rankings':{}})
    maintenance = {'supplement': {'signature':signature, 'future':pending}}
    ns['supplement_frozen_stock_rankings'](now, maintenance)
    assert state['stock_strategy_ranking_snapshots'] is old
    assert state['_stock_ranking_waiting'] == ['法人缺項']
    assert state['_stock_daytrade_ranking_waiting'] == ['法人缺項'] and reruns == [True]
    assert maintenance['supplement_retry'][0] == signature
    state['stock_strategy_ranking_snapshots'] = {'daytrade': dict(old['daytrade'], updated_at='manual-new')}
    maintenance['supplement'] = {'signature':signature, 'future':pending}
    state['_stock_ranking_waiting'] = []
    ns['supplement_frozen_stock_rankings'](now, maintenance)
    assert state['stock_strategy_ranking_snapshots']['daytrade']['updated_at'] == 'manual-new'
    assert state['_stock_ranking_waiting'] == []


def test_postclose_stock_fetch_requests_exact_historical_date_and_rejects_wrong_day():
    ns = load_app_symbols('fetch_postclose_stock_row', '_ranking_market_date')
    calls = []
    response = {'_strategy_data_as_of': '2026/10/05'}
    def fetch(*a, **kw):
        calls.append(kw)
        return response
    ns['fetch_stock_data_raw'] = fetch
    assert ns['fetch_postclose_stock_row']('2330', '台積電', '20261005') == response
    assert calls == [{'include_live_quote': False, 'strategy_target_date': '20261005'}]
    response['_strategy_data_as_of'] = '2026/10/06'
    with pytest.raises(ValueError, match='日 K 尚未就緒'):
        ns['fetch_postclose_stock_row']('2330', '台積電', '20261005')


def test_legacy_formula_with_stale_date_migrates_daytrade_only_and_keeps_swing_frozen():
    ns = load_app_symbols('supplement_frozen_stock_rankings', 'postclose_scope', 'strategy_ranking_weights')
    now = pd.Timestamp('2026-10-06 10:00')
    old = {mode: {'target_date': '2026-10-02', 'entries': [{'code': '1815', 'score': 80}]}
           for mode in ('daytrade', 'swing')}
    rows = pd.DataFrame([{'代號': '1815'}, {'代號': '2330'}])
    state = State(stock_strategy_ranking_snapshots=old, _postclose_visible_stock=rows,
                  stock_data=rows, ignored_stocks=[], all_candidates=[], saved_notes={})
    ns['st'] = SimpleNamespace(session_state=state, rerun=lambda: None)
    ns['is_market_closed_func'] = lambda day: False
    ns['_post_close_target_date'] = lambda *a: (now, date(2026, 10, 5))
    calls = []
    def submit(*args):
        calls.append(args)
        updated = {'target_date': '2026-10-05', 'weights': ns['strategy_ranking_weights']('stock', '當沖'),
                   'entries': [{'code': code, 'score': 90} for code in rows['代號']]}
        return SimpleNamespace(done=lambda: True, result=lambda: {'rankings': {'stock': {'daytrade': updated}}, 'errors': {}})
    ns['get_postclose_worker'] = lambda: {'slot': threading.BoundedSemaphore(1), 'executor': SimpleNamespace(submit=submit)}
    ns['run_postclose_job'] = ns['build_postclose_job'] = lambda *a: None
    ns['save_data_cache'] = lambda *a, **kw: None
    ns['get_app_secret'] = lambda *a: ''
    maintenance = {}
    ns['supplement_frozen_stock_rankings'](now, maintenance)
    assert calls[0][4] == date(2026, 10, 5) and calls[0][-1] == ('當沖',)
    assert set(calls[0][3]['stock']['代號']) == {'1815', '2330'}
    ns['supplement_frozen_stock_rankings'](now, maintenance)
    assert state['stock_strategy_ranking_snapshots']['swing'] is old['swing']
    assert state['stock_strategy_ranking_snapshots']['daytrade']['target_date'] == '2026-10-05'
    ns['supplement_frozen_stock_rankings'](now, maintenance)
    assert len(calls) == 1  # No stale swing rebuild, and no repeat migration.


def test_cloud_supplements_merge_same_day_additions_without_rolling_back_scores():
    ns = load_app_symbols('_newer_timestamped_state', '_state_updated_at')
    old = {'target_date':'2026-10-05', 'updated_at':'2026-10-06T10:00:00+08:00', 'supplemented':True,
           'entries':[{'code':'2330','score':80}, {'code':'1815','score':70}]}
    new = {'target_date':'2026-10-05', 'updated_at':'2026-10-06T10:01:00+08:00', 'supplemented':True,
           'entries':[{'code':'2330','score':85}, {'code':'1727','score':60}]}
    merged = ns['_newer_timestamped_state'](old, new)
    assert merged['entries'] == [{'code':'2330','score':85}, {'code':'1815','score':70}, {'code':'1727','score':60}]
    assert len(old['entries']) == len(new['entries']) == 2
    new['target_date'] = '2026-10-06'
    assert ns['_newer_timestamped_state'](old, new)['entries'] == new['entries']
    new['target_date'] = old['target_date']
    new['weights'] = {'technical': .6, 'chips': .4, 'fundamental': 0}
    assert ns['_newer_timestamped_state'](old, new)['entries'] == new['entries']
    old['updated_at'] = '2026-10-06T12:00:00+08:00'
    assert ns['_newer_timestamped_state'](old, new)['entries'] == new['entries']
    assert ns['_newer_timestamped_state'](new, old)['entries'] == new['entries']
