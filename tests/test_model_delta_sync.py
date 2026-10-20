"""Bounded sync preserves history, concurrent devices and pending evidence."""
import json
import threading
import time
from types import SimpleNamespace

import pytest

from model_tracking import ModelTracker, merge_trades, new_trade
from test_model_tracking import signal, quote


def cloud(monkeypatch, tracker, history=()):
    rows = {r['交易ID']: r for r in history}
    state = {'version': '1' if rows else '', 'posts': [], 'reads': [], 'before_post': None,
             'after_post': None, 'fail': False, 'damage': False}

    def get(url, params, **kwargs):
        ids = params.get('trade_ids')
        state['reads'].append(ids)
        if params['scope'] == 'strategy_signals':
            data = {'model_schema': 1, 'model_sync': 2, 'model_months': ['202610'] if rows else [],
                    'model_versions': {'202610': state['version']} if rows else {}}
        else:
            selected = [r for key, r in rows.items() if ids is None or key in ids.split(',')]
            data = {'model_schema': 1, 'model_trades': [] if state['damage'] and ids else selected,
                    'model_version': state['version']}
        body = json.loads(json.dumps({'success': True, 'data': data}))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)

    def post(url, **kwargs):
        data = json.loads(kwargs['json']['data'])
        state['posts'].append(data)
        if state['before_post']:
            callback, state['before_post'] = state['before_post'], None
            callback(rows, state)
        previous = state['version']
        rows.update({r['交易ID']: r for r in merge_trades(list(rows.values()), data['model_trades'])})
        state['version'] = str(int(previous or 0) + 1)
        version = state['version']
        if state['after_post']:
            callback, state['after_post'] = state['after_post'], None
            callback(rows, state)
        if state['fail']:
            raise TimeoutError('committed but response lost')
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {
            'success': True, 'model_version': version, 'model_previous_version': previous})

    monkeypatch.setattr('model_tracking.requests.get', get)
    monkeypatch.setattr('model_tracking.requests.post', post)
    return rows, state


def dirty(tracker):
    with tracker.db() as db:
        return db.execute('SELECT COUNT(*) FROM trades WHERE dirty=1').fetchone()[0]


def test_same_timestamp_merge_preserves_newer_tick_and_entry():
    from model_tracking import advance
    waiting = new_trade(signal())
    holding = advance(waiting, quote(100, 序號=1))
    later = advance(holding, quote(101, 序號=2))
    for first, second in ((waiting, holding), (holding, waiting)):
        assert merge_trades([first], [second])[0]['狀態'] == '模擬持倉'
    for first, second in ((later, holding), (holding, later)):
        assert merge_trades([first], [second])[0]['最後序號'] == 2


def test_delta_sends_only_changes_and_reads_history_once(monkeypatch, tmp_path):
    history = [new_trade(signal(商品鍵=str(10000+i))) for i in range(1000)]
    tracker = ModelTracker(tmp_path/'delta.sqlite', 'fake', start=False)
    remote, state = cloud(monkeypatch, tracker, history)
    tracker.sync()
    assert len(tracker.records()) == 1000 and dirty(tracker) == 0
    state['reads'].clear()
    tracker.process([signal()], {'股票|2330': quote(100)})
    tracker.sync()
    assert len(state['posts']) == 1 and len(state['posts'][0]['model_trades']) == 1
    assert state['reads'] == [None, new_trade(signal())['交易ID']]
    assert dirty(tracker) == 0 and len(remote) == len(tracker.records()) == 1001
    state['reads'].clear()
    tracker.sync()
    assert state['reads'] == [None] and len(state['posts']) == 1


@pytest.mark.parametrize('timing', ['before_post', 'after_post'])
def test_other_device_write_is_not_hidden_by_our_commit_token(monkeypatch, tmp_path, timing):
    tracker = ModelTracker(tmp_path/'race.sqlite', 'fake', start=False)
    remote, state = cloud(monkeypatch, tracker)
    tracker.sync()
    other = new_trade(signal(商品鍵='2408'))

    def other_device(rows, state):
        rows[other['交易ID']] = other
        state['version'] = str(int(state['version'] or 0) + 1)

    state[timing] = other_device
    tracker.process([signal()], {'股票|2330': quote(100)})
    tracker.sync()
    tracker.sync()
    assert len(tracker.records()) == len(remote) == 2 and dirty(tracker) == 0
    assert {r['商品鍵'] for r in tracker.records()} == {'2330', '2408'}


@pytest.mark.parametrize('failure', ['fail', 'damage'])
def test_timeout_or_incomplete_readback_keeps_pending_then_retries(monkeypatch, tmp_path, failure):
    tracker = ModelTracker(tmp_path/'retry.sqlite', 'fake', start=False)
    remote, state = cloud(monkeypatch, tracker)
    tracker.sync()
    tracker.process([signal()], {'股票|2330': quote(100)})
    state[failure] = True
    with pytest.raises((ValueError, TimeoutError)):
        tracker.sync()
    assert dirty(tracker) == 1
    state[failure] = False
    tracker.sync()
    assert len(remote) == len(tracker.records()) == 1 and dirty(tracker) == 0


def test_changed_during_upload_stays_dirty_and_hot_rows_do_not_starve(monkeypatch, tmp_path):
    tracker = ModelTracker(tmp_path/'hot.sqlite', 'fake', start=False)
    remote, state = cloud(monkeypatch, tracker)
    tracker.sync()
    tracker.process([signal(商品鍵=str(i)) for i in range(41)], {})

    def change_during_upload(rows, state):
        tracker.process([], {'股票|0': quote(100)})

    state['after_post'] = change_during_upload
    tracker.sync()
    assert dirty(tracker) == 22 and len(remote) == 20
    assert '待同步' in tracker.status
    tracker.sync()
    assert len(remote) == 40
    tracker.sync()
    assert len(remote) == 41 and dirty(tracker) == 0
    assert len(state['posts'][-1]['model_trades']) == 2
    assert next(r for r in tracker.records() if r['商品鍵'] == '0')['狀態'] == '模擬持倉'


def test_pending_retries_after_quote_activity_expires(tmp_path):
    tracker = ModelTracker(tmp_path/'idle.sqlite', 'fake', start=False, sync_seconds=.01)
    tracker.process([signal()], {})
    tracker.last_activity = time.monotonic() - 300
    retried = threading.Event()
    tracker.sync = retried.set
    tracker.sync_thread = threading.Thread(target=tracker.run_sync, daemon=True)
    tracker.sync_thread.start()
    try:
        assert retried.wait(1)
    finally:
        tracker.close()


def test_large_trade_is_not_dropped_and_next_batch_continues(monkeypatch, tmp_path):
    tracker = ModelTracker(tmp_path/'large.sqlite', 'fake', start=False)
    remote, state = cloud(monkeypatch, tracker)
    tracker.sync()
    tracker.process([signal(指標快照={'說明': 'x'*260000}), signal(商品鍵='2408')], {})
    tracker.sync()
    assert len(state['posts'][0]['model_trades']) == 1 and dirty(tracker) == 1
    tracker.sync()
    assert len(remote) == 2 and dirty(tracker) == 0
