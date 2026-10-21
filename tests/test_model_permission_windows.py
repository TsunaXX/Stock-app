"""A later condition must not erase entries/exits observed after a published permit."""
import pytest

from model_tracking import ModelTracker, performance
from test_model_tracking import signal, quote


def sample(price, second, allowed=True, mode='當沖', stream='test:0'):
    tick = quote(price, second, 序號=second + 1, 串流識別=stream)
    return {**tick, '策略': mode, '進場品質許可': allowed, '進場品質方向': '多頭',
            '進場品質時間': tick['來源時間'], '成交證據': [tick]}


@pytest.mark.parametrize('market', ['股票', '期貨'])
@pytest.mark.parametrize('exit_price', [94, 111])
def test_previous_published_permission_covers_intervening_entry_and_exit(tmp_path, market, exit_price):
    tracker = ModelTracker(tmp_path / 'window.sqlite', start=False)
    key = market + '|2330'
    tracker.process([signal(市場=market, 乘數=1000 if market == '期貨' else 1, 期交稅率=.00002, 進場品質檢查=True)], {key: sample(99.5, 0)})
    latest = sample(exit_price, 3, False)
    latest['成交證據'] = [sample(99.5, 0)['成交證據'][0], sample(100, 1)['成交證據'][0],
                        sample(exit_price, 2)['成交證據'][0], latest['成交證據'][0]]
    tracker.process([], {key: latest})
    row, = tracker.records()
    assert row['狀態'] == '已平倉' and row['進場時間'].startswith('2026-10-08T09:30:01')
    assert not row['資料缺口'] and performance([row])[0]['有效交易數'] == 1
    assert (row['模擬損益'] > 0) is (exit_price == 111)
    tracker.process([], {key: latest})
    assert tracker.records() == [row]


@pytest.mark.parametrize('change', ['no_prior', 'denied', 'reconnect', 'expired'])
def test_no_backdated_entry_without_valid_prior_permission(tmp_path, change):
    tracker = ModelTracker(tmp_path / 'deny.sqlite', start=False)
    if change != 'no_prior':
        tracker.process([signal(進場品質檢查=True)], {'股票|2330': sample(99.5, 0, change != 'denied')})
    latest = sample(99.5, 3, False, stream='new:0' if change == 'reconnect' else 'test:0')
    latest['成交證據'] = [sample(100, 1)['成交證據'][0], latest['成交證據'][0]]
    if change == 'expired':
        latest['進場品質時間'] = '2026-10-08T09:31:03+08:00'
    tracker.process([signal(來源時間=latest['來源時間'], 進場品質檢查=True)] if change == 'no_prior' else [], {'股票|2330': latest})
    row, = tracker.records()
    assert row.get('模擬進場價') is None
    assert performance([row])[0]['有效交易數'] == 0


def test_swing_updates_do_not_erase_daytrade_permission_or_advance_waiting_row(tmp_path):
    tracker = ModelTracker(tmp_path / 'modes.sqlite', start=False)
    tracker.process([signal(進場品質檢查=True)], {'股票|2330': sample(99.5, 0)})
    tracker.process([], {'股票|2330': sample(100, 1, False, '波段')})
    assert tracker.records()[0]['來源時間'].startswith('2026-10-08T09:30:00')
    latest = sample(94, 3, False)
    latest['成交證據'] = [sample(100, 1)['成交證據'][0], sample(94, 2)['成交證據'][0], latest['成交證據'][0]]
    tracker.process([], {'股票|2330': latest})
    assert tracker.records()[0]['狀態'] == '已平倉'


def test_database_retry_does_not_publish_permission_before_commit(tmp_path):
    import sqlite3
    from contextlib import closing
    tracker = ModelTracker(tmp_path / 'retry.sqlite', start=False)
    tracker.process([signal(進場品質檢查=True)], {'股票|2330': sample(99.5, 0)})
    before = dict(tracker.entry_permissions)
    tracker.db = lambda: sqlite3.connect(tracker.path, timeout=.02)
    latest = sample(94, 3, False)
    latest['成交證據'] = [sample(100, 1)['成交證據'][0], sample(94, 2)['成交證據'][0], latest['成交證據'][0]]
    with closing(tracker.db()) as lock:
        lock.execute('BEGIN IMMEDIATE')
        with pytest.raises(sqlite3.OperationalError):
            tracker.process([], {'股票|2330': latest})
        assert tracker.entry_permissions == before
        lock.rollback()
    tracker.process([], {'股票|2330': latest})
    assert tracker.records()[0]['狀態'] == '已平倉'
