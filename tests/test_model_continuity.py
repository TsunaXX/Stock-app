"""Do not exclude verifiable fills due to stale waiting prices or queued evidence."""
import time

import pytest

from model_tracking import ModelTracker, performance
from test_model_tracking import signal, quote


@pytest.mark.parametrize('bridge_price, expected_gap', [(99., False), (98., True)])
@pytest.mark.parametrize('exit_price', [111., 94.])
def test_waiting_price_bridges_devices_before_entry(tmp_path, bridge_price, expected_gap, exit_price):
    tracker = ModelTracker(tmp_path / 'continuity.sqlite', start=False)
    first = quote(99., 0, 序號=1, 串流識別='desktop:0')
    tracker.process([signal()], {'股票|2330': {**first, '成交證據': [first]}})
    # The former primary has stopped; the mobile buffer includes the last observed price.
    tracker.source_quotes.clear()
    ticks = [quote(bridge_price, 0, 序號=10, 串流識別='phone:0'),
             quote(100., 1, 序號=11, 串流識別='phone:0'),
             quote(exit_price, 2, 序號=12, 串流識別='phone:0')]
    tracker.process([], {'股票|2330': {**ticks[-1], '成交證據': ticks}})
    row = tracker.records()[0]
    assert row['狀態'] == '已平倉' and row['資料缺口'] is expected_gap
    assert performance([row])[0]['有效交易數'] == (0 if expected_gap else 1)


def test_pending_evidence_is_drained_before_monitor_timeout(tmp_path):
    tracker = ModelTracker(tmp_path / 'queued.sqlite', start=False)
    first = quote(100., 0, 序號=1, 串流識別='desktop:0')
    tracker.process([signal()], {'股票|2330': {**first, '成交證據': [first]}})
    last = quote(111., 1, 序號=2, 串流識別='desktop:0')
    tracker.submit([], {'股票|2330': {**last, '成交證據': [first, last]}})
    tracker.leases['股票|2330'] = (time.monotonic() - 60, 30)
    tracker.mark_gaps()
    assert not tracker.records()[0]['資料缺口']
    tracker.process(*tracker.queue.get_nowait())
    assert performance(tracker.records())[0]['有效交易數'] == 1


def test_empty_queue_still_marks_a_real_monitoring_timeout(tmp_path):
    tracker = ModelTracker(tmp_path / 'stopped.sqlite', start=False)
    first = quote(100., 0, 序號=1, 串流識別='desktop:0')
    tracker.process([signal()], {'股票|2330': {**first, '成交證據': [first]}})
    tracker.mark_gaps()
    assert tracker.records()[0]['資料缺口']
    assert performance(tracker.records())[0]['有效交易數'] == 0
