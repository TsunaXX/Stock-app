"""Session-owned index snapshots: cache-only ticks, stable signals and bounded lifetime."""
import threading
import time
import weakref
from types import SimpleNamespace

import pandas as pd
import pytest

from market_automation import IntradayBackground, BackgroundJobs
from test_core_calculations import load_app_symbols
from test_intraday_background import wait_result


def harness():
    ns = load_app_symbols('index_operation_snapshot', 'calculate_operation_snapshot',
                          'get_stable_index_trade_plan', 'index_operation_model_specs')
    api = object()
    state = {'lock': threading.RLock()}
    session = {'sj_api': api, 'sj_logged_in': True}
    worker = IntradayBackground(lambda: True)
    builds, records, subscriptions = [], [], {}
    def forbidden(*args, **kwargs):
        raise AssertionError('Worker must not access Streamlit state or broker polling')
    class UIState(dict):
        def get(self, *args):
            assert threading.current_thread() is threading.main_thread()
            return super().get(*args)
        def setdefault(self, *args):
            assert threading.current_thread() is threading.main_thread()
            return super().setdefault(*args)
        def __setitem__(self, *args):
            assert threading.current_thread() is threading.main_thread()
            return super().__setitem__(*args)
    session = UIState(session)
    price, connected, market = [20100.], [True], [True]
    def live(*args, **kwargs):
        assert kwargs['stream_only']
        return {'price': price[0], 'change': 100., 'contract_code': 'TMFJ6',
                'updated': pd.Timestamp.now(tz='Asia/Taipei')} if connected[0] else None
    def daily(*args):
        builds.append(1)
        return {'direction':'偏多', 'latest':20100., 'atr':100., 'trigger':'原策略',
                'entry_level':20000., 'invalidation':19900., 'target':20500.}
    def record(source, specs, api, seconds, *, tracker, cursors):
        assert tracker is not None
        records.append((source, [dict(spec) for spec in specs if spec]))
        cursors['kept'] = True
    def sync(api, contracts, scope):
        subscriptions[scope] = {c.code for c in contracts}
    ns.update(st=SimpleNamespace(session_state=session), weakref=weakref,
        get_intraday_background=lambda: worker, get_public_maintenance_jobs=lambda: SimpleNamespace(poll=lambda *a, **kw:None),
        _stream_state=lambda api:state, get_model_tracker=lambda url:object(), get_app_secret=lambda k:'',
        load_model_tracking_costs=lambda: {}, get_operation_model_version=lambda s:'unchanged-rules',
        get_near_futures_contract=lambda api, product:SimpleNamespace(code=product),
        get_taiex_contract=lambda api:SimpleNamespace(code='IX0001'), sync_strategy_stream_scope=sync,
        model_market_open=lambda *a:market[0], parse_strategy_data_time=lambda v:pd.Timestamp(v).tz_localize(None),
        merge_market_temperature_snapshot=lambda data, *a, **kw:data,
        _post_close_target_date=lambda:(None,pd.Timestamp('2026-10-16').date()),
        calculate_market_temperature=lambda data:{'score':50.,'change':100.}, calculate_index_trade_plan=daily,
        get_live_futures_snapshot=live,
        get_cached_futures_intraday_state=lambda *a, **kw:{'vwap':20000.},
        evaluate_trade_entry_state=lambda plan, price, *a:{'can_enter':True, 'entry_level':20000.,
            'invalidation':19900., 'target':20500., 'price':price},
        resolve_short_wave_direction=lambda *a:('偏多','原方向'), get_cached_short_wave_plan=lambda *a, **kw:None,
        operation_model_spec=lambda source, kind, *a, **kw:{'商品鍵':'TMFJ6|'+kind, '進場許可':a[5]},
        record_operation_models=record)
    data = pd.DataFrame({'Close':[20000.,20100.]}, index=pd.to_datetime(['2026-10-15','2026-10-16']))
    seed = [('加權','^TWII',data,'快取',{'score':50.}), ('期貨','TWF=F',data,'快取',{'score':50.})]
    return ns, worker, session, state, seed, price, connected, market, builds, records, subscriptions


def test_first_visit_hidden_updates_return_and_repeated_visits_do_not_duplicate():
    ns, worker, session, state, seed, price, connected, market, builds, records, subscriptions = harness()
    try:
        assert not worker.jobs  # A never-opened page has no job.
        ns['index_operation_snapshot'](seed, .02)
        first = wait_result(worker, 'index_plan', lambda r:r['rows'].get('live_price') == 20100.)
        job = worker.jobs['index_plan']
        price[0] = 20200.
        # No page/fragment call during this wait: hidden page continues server-side.
        latest = wait_result(worker, 'index_plan', lambda r:r['rows'].get('live_price') == 20200.)
        def no_recalculation(*args, **kwargs):
            raise AssertionError('Returning must read the snapshot')
        ns['calculate_operation_snapshot'] = no_recalculation
        for _ in range(10):
            snapshot = ns['index_operation_snapshot']([], .02)
            assert snapshot['live_price'] == 20200.
            assert worker.jobs['index_plan'] is job
        assert len(builds) == 1 and len(worker.jobs) == 1
        assert len([scope for scope,codes in subscriptions.items() if codes]) == 1
        assert records and session['_index_operation_cursors']['kept']
        assert latest['rows']['plan'] == first['rows']['plan']
    finally:
        worker.close()
        worker.thread.join(2)
    assert all(not codes for codes in subscriptions.values())


def test_disconnected_market_closed_and_expired_quote_keep_last_levels_without_permission():
    ns, worker, session, state, seed, price, connected, market, builds, records, subs = harness()
    try:
        ns['index_operation_snapshot'](seed, .02)
        first = wait_result(worker, 'index_plan', lambda r:r['rows'].get('plan'))
        with state['lock']:
            state['connection_down'] = True
        market[0] = False
        paused = ns['index_operation_snapshot']([], .02)
        assert paused['plan'] == first['rows']['plan']
        assert not paused['trade_state']['can_enter']
        # A result is a detached copy, so disabling its permission cannot corrupt the worker.
        assert worker.result('index_plan')['rows']['trade_state']['can_enter']
        with state['lock']:
            state['connection_down'] = False
        market[0] = True
        connected[0] = False
        stale = wait_result(worker, 'index_plan', lambda r: not r['rows'].get('fresh', True))
        assert stale['rows']['live_snapshot']['updated'] == first['rows']['live_snapshot']['updated']
        assert not stale['rows']['trade_state']['can_enter']
        connected[0] = True
        price[0] = 20300.
        assert wait_result(worker, 'index_plan', lambda r:r['rows'].get('live_price') == 20300.)
    finally:
        worker.close()


def test_setting_changes_reuse_cursors_and_release_only_the_old_scope():
    ns, worker, session, state, seed, *_, subscriptions = harness()
    finish = threading.Event()
    try:
        ns['index_operation_snapshot'](seed, .02)
        wait_result(worker, 'index_plan')
        old_cursor = session['_index_operation_cursors']
        calculate = ns['calculate_operation_snapshot']
        def blocked(*args, **kwargs):
            assert finish.wait(2)
            return calculate(*args, **kwargs)
        ns['calculate_operation_snapshot'] = blocked
        session['trade_plan_entry_profile'] = '穩健（完整確認）'
        pending = ns['index_operation_snapshot']([], .03)
        assert not pending['trade_state']['can_enter']
        finish.set()
        assert wait_result(worker, 'index_plan', lambda r:r['rows'].get('plan'))
        assert session['_index_operation_cursors'] is old_cursor and old_cursor['kept']
        assert len([codes for codes in subscriptions.values() if codes]) == 1
        current = worker.jobs['index_plan']
        session.pop('trade_plan_entry_profile')
        session.pop('trade_plan_position_count')
        ns['index_operation_snapshot']([], .03)
        assert worker.jobs['index_plan'] is current
        assert session['trade_plan_entry_profile'] == '穩健（完整確認）'
    finally:
        finish.set()
        worker.close()


def test_two_sessions_are_independent_and_ended_session_releases_its_subscription():
    active, released = [True], []
    one = IntradayBackground(lambda: active[0], disconnected_grace=.01)
    two = IntradayBackground(lambda: True)
    def update(snapshot):
        return {'price':snapshot.get('price',0)+1}, 1
    try:
        for i,worker in enumerate((one,two)):
            worker.configure('index_plan','same',{},.01,update,lambda:True,lambda i=i:released.append(i))
        wait_result(one,'index_plan'); wait_result(two,'index_plan')
        active[0] = False
        one.thread.join(2)
        assert one.closed and not two.closed and released == [0]
        assert wait_result(two,'index_plan',lambda r:r['rows']['price'] > 1)
    finally:
        one.close(); two.close()


def test_slow_index_job_backs_off_but_failed_updates_never_replace_complete_snapshot():
    worker = IntradayBackground(lambda: True)
    fail = [False]
    def update(snapshot):
        if fail[0]:
            snapshot['levels'][0] = 999
            raise ConnectionError('temporary')
        time.sleep(.02)
        return {'levels':[20000], 'source':'original'}, 1
    try:
        worker.configure('index_plan','one',{},.001,update,lambda:True,lambda:None,low_priority=True)
        good = wait_result(worker,'index_plan')
        assert worker.jobs['index_plan']['next'] - time.monotonic() > .04
        fail[0] = True
        failed = wait_result(worker,'index_plan',lambda r:r['error'])
        assert failed['rows'] == good['rows'] and failed['updated_at'] == good['updated_at']
    finally:
        worker.close()


def test_background_history_shares_successes_and_retries_partial_failures_without_erasing():
    ns = load_app_symbols('refresh_index_background_history')
    shared, lock = {'temperature':{}}, threading.RLock()
    ns['get_shared_market_data_cache'] = lambda:(shared,lock)
    ns['calculate_market_temperature'] = lambda data:{'score':50}
    calls, failed = [], [False]
    api = object()
    frame = pd.DataFrame({'Close':[100.]})
    seed = [('加權','^TWII',frame,'last-good',{}),('期貨','TWF=F',frame,'last-good',{})]
    def fetch(code, **kwargs):
        assert kwargs == {'api':api, 'logged_in':True}
        calls.append(code)
        return (pd.DataFrame() if failed[0] and code == 'TWF=F' else frame.copy()),'official'
    ns['fetch_market_temperature_data'] = fetch
    result = ns['refresh_index_background_history'](api,seed)
    assert len(result) == 2 and len(calls) == 2
    ns['refresh_index_background_history'](api,seed)
    assert len(calls) == 2
    for cached in shared['temperature'].values():
        cached['saved_at'] -= 181
    failed[0] = True
    with pytest.raises(RuntimeError):
        ns['refresh_index_background_history'](api,seed)
    assert not shared['temperature'][('TWF=F',180,True,id(api))]['df'].empty


def test_background_model_specs_and_recorder_are_identical_to_foreground(tmp_path):
    from test_operation_model_tracking import adapter, tick, sample, NOW
    from model_tracking import ModelTracker
    ns, state = adapter()
    live = {'contract_code':'TMFJ6', 'price':20000.,'updated':NOW.isoformat()}
    args = ('指數操作計畫','日線／15分K',live,'偏多',20000.,19950.,20200.,True,1,'default',{},'原策略')
    foreground = ns['operation_model_spec'](*args)
    costs = ns['load_model_tracking_costs']()
    ns['load_model_tracking_costs'] = lambda: (_ for _ in ()).throw(AssertionError('background config read'))
    background = ns['operation_model_spec'](*args,cost_settings=costs,version='test-指數操作計畫')
    assert background == foreground
    tick(state,'TMFJ6',20000.,0)
    tracker = ModelTracker(tmp_path/'index.sqlite',start=False)
    # An explicit tracker/cursor must bypass all Streamlit and credential access.
    ns['st'] = SimpleNamespace()
    class Sink:
        def active_plans(self,*a): return []
        def submit(self,signals,quotes): tracker.process(signals,quotes); return True
    cursors = {}
    ns['record_operation_models']('指數操作計畫',[background],object(),5,tracker=Sink(),cursors=cursors)
    ns['record_operation_models']('指數操作計畫',[background],object(),5,tracker=Sink(),cursors=cursors)
    assert len(tracker.records()) == 1 and cursors


def test_worker_fetch_errors_and_empty_minutes_do_not_touch_session_state():
    ns = load_app_symbols('fetch_shioaji_data','get_futures_intraday_state',
                          '_list_shioaji_futures_contracts','remember_shioaji_auth_error','_is_shioaji_auth_error')
    ns['st'] = SimpleNamespace()
    ns['get_taiex_contract'] = lambda api:SimpleNamespace(code='IX0001')
    ns['time'] = SimpleNamespace(sleep=lambda seconds:None)
    api = SimpleNamespace(kbars=lambda **kw:None)
    assert ns['fetch_shioaji_data'](api,'^TWII',lookback_days=1,background=True).empty
    def fail(**kw): raise ConnectionError('temporary')
    api.kbars = fail
    assert ns['fetch_shioaji_data'](api,'^TWII',lookback_days=1,background=True).empty
    ns['get_cached_index_minutes'] = lambda *a:pd.DataFrame(columns=['Open','High','Low','Close'])
    assert not ns['get_futures_intraday_state'](api,'偏多',logged_in=True)['available']
    ns['sj'] = None
    stream = {'lock':threading.RLock()}
    ns['_stream_state'] = lambda api:stream
    def auth(*args): raise RuntimeError('Not authenticated')
    def forbidden(*args): raise AssertionError('background cannot invalidate UI login')
    ns['invalidate_shioaji_connection'] = forbidden
    api.contracts = SimpleNamespace(list=auth)
    assert ns['_list_shioaji_futures_contracts'](api) == []
    assert stream['connection_down'] and stream['auth_error'] == 'RuntimeError'



def test_shared_contract_ownership_survives_one_session_leaving():
    ns = load_app_symbols('sync_strategy_stream_scope')
    stream = {'lock':threading.RLock(), 'subscriptions':{}}
    removed = []
    ns['_stream_state'] = lambda api:stream
    def subscribe(api,contract):
        stream['subscriptions'].setdefault(contract.code,{'contract':contract})
        return True
    ns['ensure_market_stream_subscription'] = subscribe
    ns['_unsubscribe_market_stream'] = lambda api,state,code,metadata:removed.append(code)
    contract = SimpleNamespace(code='TMFJ6')
    sync = ns['sync_strategy_stream_scope']
    sync(object(),[contract],'index_device_one')
    sync(object(),[contract],'index_device_two')
    sync(object(),[],'index_device_one')
    assert removed == []
    assert 'TMFJ6' in stream['strategy_owned_subscriptions']['index_device_two']
    sync(object(),[],'index_device_two')
    assert removed == ['TMFJ6']


def test_background_daily_overlay_reads_owned_near_contract_without_extra_subscription():
    ns = load_app_symbols('merge_market_temperature_snapshot')
    contract = SimpleNamespace(code='TXFJ6',reference=20000.)
    ns['get_near_futures_contract'] = lambda api,product:contract
    ns['get_futures_trading_date'] = lambda now:pd.Timestamp(now.date())
    ns['fresh_strategy_stream_quote'] = lambda quote:True
    calls = []
    def quotes(api,contracts,**kwargs):
        calls.append((contracts[0].code,kwargs))
        return [SimpleNamespace(close=20100.,open=20000.,high=20110.,low=19990.,total_volume=100,
                                change_price=100.,change_rate=.5)]
    ns['get_stream_quotes'] = quotes
    frame = pd.DataFrame({'Open':[20000.],'High':[20050.],'Low':[19900.],'Close':[20000.],'Volume':[99.]},
                         index=[pd.Timestamp.now(tz='Asia/Taipei').tz_localize(None).normalize()])
    result = ns['merge_market_temperature_snapshot'](frame,object(),'TWF=F',stream_only=True,subscribe=False)
    assert result['Close'].iloc[-1] == 20100.
    assert calls == [('TXFJ6',{'snapshot_fallback':False,'subscribe':False})]


def test_failed_slow_source_preserves_snapshot_but_keeps_observing_existing_positions():
    ns, worker, session, state, seed, price, connected, market, builds, records, subscriptions = harness()
    jobs, failing = BackgroundJobs(), [False]
    ns['get_public_maintenance_jobs'] = lambda:SimpleNamespace(poll=lambda key,loader,**kw:jobs.poll(key,loader,interval=0))
    def refresh(api,seed):
        if failing[0]: raise ConnectionError('daily source unavailable')
        return seed
    ns['refresh_index_background_history'] = refresh
    try:
        ns['index_operation_snapshot'](seed,.02)
        good = wait_result(worker,'index_plan',lambda r:r['rows'].get('plan'))
        live = ns['get_live_futures_snapshot']
        ns['get_live_futures_snapshot'] = lambda *a, **kw:good['rows']['live_snapshot']
        failing[0] = True
        failed = wait_result(worker,'index_plan',lambda r:r.get('error'))
        assert failed['rows']['live_price'] == good['rows']['live_price'] and failed['updated_at'] == good['updated_at']
        assert records[-1][1] and all(not spec['進場許可'] and not spec['新訊號'] for spec in records[-1][1])
        failing[0] = False
        ns['get_live_futures_snapshot'] = live
        price[0] = 20200.
        assert wait_result(worker,'index_plan',lambda r:r['rows'].get('live_price') == 20200. and not r['error'])
    finally:
        worker.close()
        jobs.pool.shutdown(wait=True)
