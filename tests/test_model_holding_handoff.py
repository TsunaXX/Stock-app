"""Only confirmed model positions retain subscribed evidence after navigation."""
from datetime import timedelta
from types import SimpleNamespace
import threading
import weakref

import pytest

from model_tracking import ModelTracker
from test_core_calculations import load_app_symbols
from test_model_tracking import signal
from test_operation_model_tracking import NOW, adapter, option, sample, spec, tick


class Worker:
    lock = threading.RLock()
    closed = False

    def __init__(self):
        self.jobs = {}

    def configure(self, room, signature, seed, seconds, update, ready, release, **kwargs):
        self.jobs[room] = {'signature': signature, 'update': update, 'release': release}

    def remove(self, room):
        self.jobs.pop(room)['release']()


def harness(tmp_path, state, ns=None):
    helpers = load_app_symbols('retain_open_model_positions', 'held_model_quotes', 'sync_strategy_stream_scope')
    worker = Worker()
    tracker = ModelTracker(tmp_path / 'hold.sqlite', start=False)
    api, unsubscribed = object(), []
    now = [NOW]
    class Clock(type(NOW)):
        @classmethod
        def now(cls, tz=None):
            return now[0].astimezone(tz)
    def unsubscribe(api, state, code, metadata):
        unsubscribed.append(code)
        state['subscriptions'].pop(code)
    def unexpected_subscribe(*args):
        raise AssertionError('Normal handoff reuses the active subscriptions')
    helpers.update(
        _stream_state=lambda api: state, _unsubscribe_market_stream=unsubscribe,
        ensure_market_stream_subscription=unexpected_subscribe, weakref=weakref, datetime=Clock,
        model_market_open=lambda *args: True, get_futures_trading_date=lambda now: now,
        get_intraday_background=lambda: worker, get_app_secret=lambda key: '',
        get_model_tracker=lambda url: tracker,
        st=SimpleNamespace(session_state={'sj_logged_in': True, '_intraday_background': worker}),
    )
    if ns is not None:
        helpers['operation_model_samples'] = ns['operation_model_samples']
    return helpers, worker, tracker, api, now, unsubscribed


def own(state, scope, codes):
    state.setdefault('strategy_stream_scopes', {})[scope] = set(codes)
    metadata = {code: {'contract': SimpleNamespace(code=code), 'status': 'active'} for code in codes}
    state.setdefault('strategy_owned_subscriptions', {})[scope] = metadata.copy()
    state.setdefault('subscriptions', {}).update(metadata)


@pytest.mark.parametrize('exit_price,expected_gap', [(110., False), (94., False), (94., True)])
def test_stock_tab_switch_keeps_entered_trade_but_never_fills_waiting_plan(tmp_path, exit_price, expected_gap):
    state = {'lock': threading.RLock(), 'model_tick_buffers': {}}
    ns, worker, tracker, api, now, unsubscribed = harness(tmp_path, state)
    own(state, 'stock', ['2330', '2317'])
    tick(state, '2330', 100., 0, epoch='api:0:1')
    ticks = state['model_tick_buffers']['2330']
    date = NOW.date().isoformat()
    opening = {'交易日': date, '來源時間': NOW.isoformat()}
    buy = signal(**opening)
    waiting = signal(**opening, 策略版本='V1.0-other', 進場價=108., 目標價=115.)
    tracker.process([buy, waiting], {'股票|2330': {**ticks[-1], '交易日': date}})
    assert {row['狀態'] for row in tracker.records()} == {'模擬持倉', '等待進場'}

    ns['retain_open_model_positions'](api, '股期戰略室', stock=True)
    ns['retain_open_model_positions'](api, '股期戰略室', stock=True)
    assert len(worker.jobs) == 1
    room, job = next(iter(worker.jobs.items()))
    ns['sync_strategy_stream_scope'](api, [], 'stock')
    assert unsubscribed == ['2317']
    assert state['model_tick_buffers']['2330'] is ticks
    assert job['update']({}) == ({}, 0)

    now[0] += timedelta(seconds=1)
    tick(state, '2330', exit_price, 1, epoch='api:1:1' if expected_gap else 'api:0:1')
    assert job['update']({}) == ({}, 1)
    assert job['update']({}) == ({}, 0)
    tracker.process(*tracker.queue.get_nowait())
    trades = {row['狀態']: row for row in tracker.records()}
    assert set(trades) == {'已平倉', '等待進場'}
    assert trades['已平倉']['出場原因'] == ('停利' if exit_price == 110. else '停損')
    assert trades['已平倉']['資料缺口'] is expected_gap
    assert trades['等待進場'].get('模擬進場價') is None
    assert (('股票|2330', trades['已平倉']['交易ID']) in tracker.leases
            and '股票|2330' not in tracker.leases)

    job['update']({})
    assert room not in worker.jobs
    assert unsubscribed == ['2317', '2330']
    assert not state['model_tick_buffers']


@pytest.mark.parametrize('use_option', [False, True])
def test_operation_handoff_keeps_original_contracts_and_real_exit_evidence(tmp_path, use_option):
    operation, state = adapter()
    ns, worker, tracker, api, now, unsubscribed = harness(tmp_path, state, operation)
    plan = spec(operation, option(True), spread=True) if use_option else spec(operation)
    state['strategy_stream_scopes'].clear()
    scope = 'options_models' if use_option else f'index_{id(worker)}_previous'
    if use_option:
        own(state, 'index', ['TMFJ6'])
        own(state, scope, ['P1', 'P2', 'C1'])
        tick(state, 'P1', 30., 0, epoch='api:0:1')
        tick(state, 'P2', 12., 0, epoch='api:0:1')
    else:
        own(state, scope, ['TMFJ6', 'TXF'])
    stream_code = 'TMFJ6' if use_option else 'TMF202610'
    state['aliases']['TMFJ6'] = stream_code
    tick(state, stream_code, 20000., 0, epoch='api:0:1')
    tracker.process(*sample(operation, [plan], 0))
    entered = tracker.records()[0]
    assert entered['狀態'] == '模擬持倉'

    ns['retain_open_model_positions'](api, plan['計畫來源'])
    assert len(worker.jobs) == 1
    ns['sync_strategy_stream_scope'](api, [], scope)
    if use_option:
        ns['sync_strategy_stream_scope'](api, [], 'index')
        assert set(state['subscriptions']) == {'TMFJ6', 'P1', 'P2'}
        tick(state, 'P1', 16., 1, epoch='api:0:1')
        tick(state, 'P2', 8., 1, epoch='api:0:1')
    else:
        assert set(state['subscriptions']) == {'TMFJ6'}

    now[0] += timedelta(seconds=1)
    tick(state, stream_code, 20201., 1, epoch='api:0:1')
    job = next(iter(worker.jobs.values()))
    assert job['update']({}) == ({}, 1)
    signals, quotes = tracker.queue.get_nowait()
    assert not signals and next(iter(quotes.values()))['僅追蹤持倉'] == (entered['交易ID'],)
    tracker.process(signals, quotes)
    closed, = tracker.records()
    assert closed['交易ID'] == entered['交易ID']
    assert closed['出場原因'] == '停利' and not closed['資料缺口']
    job['update']({})
    assert not worker.jobs and not state['subscriptions']
