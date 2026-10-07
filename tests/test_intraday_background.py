import threading
import time

import pandas as pd

from market_automation import IntradayBackground


def wait_result(worker, room, predicate=lambda result: True):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        result = worker.result(room)
        if result is not None and predicate(result):
            return result
        time.sleep(0.005)
    raise AssertionError('Background result was not published')


def test_server_timer_keeps_running_without_browser_ticks_and_cleans_up():
    price, active, allowed = [100], [True], [True]
    updated, released = threading.Event(), threading.Event()
    calls = []
    worker = IntradayBackground(lambda: active[0], disconnected_grace=0.02)

    def update(rows):
        calls.append(time.monotonic())
        if len(calls) == 1:
            raise ConnectionError('temporary')
        rows.loc[0, '收盤價'] = price[0]
        updated.set()
        return rows, 1

    try:
        seed = pd.DataFrame([{'代號': '2330', '收盤價': 99, '戰略備註': '固定', '_quote_time': '來源時間'}])
        worker.configure('stock', 'one', seed, 0.02, update, lambda: allowed[0], released.set)
        assert updated.wait(2)
        first = wait_result(worker, 'stock', lambda r: r['count'] == 1)
        assert first['rows'].iloc[0]['收盤價'] == 100
        assert first['rows'].iloc[0]['戰略備註'] == '固定'
        assert first['rows'].iloc[0]['_quote_time'] == '來源時間'
        # No reconfiguration, UI fragment, browser timer, or quote polling is called.
        updated.clear()
        price[0] = 101
        assert updated.wait(2)
        assert wait_result(worker, 'stock', lambda r: r['rows'].iloc[0]['收盤價'] == 101)
        unchanged_at = worker.result('stock')['updated_at']
        updated.clear()
        assert updated.wait(2)
        assert worker.result('stock')['updated_at'] == unchanged_at
        allowed[0] = False
        time.sleep(0.35)
        assert worker.result('stock')['outside']
        assert worker.result('stock')['updated_at'] == unchanged_at
        allowed[0] = True
        price[0] = 102
        assert wait_result(worker, 'stock', lambda r: r['rows'].iloc[0]['收盤價'] == 102)
        active[0] = False
        assert released.wait(2)
        worker.thread.join(2)
        assert worker.closed and not worker.thread.is_alive()
    finally:
        worker.close()


def test_cancelled_slow_job_does_not_block_ui_or_overwrite_new_scope():
    entered, finish, newest, released = (threading.Event() for _ in range(4))
    worker = IntradayBackground(lambda: True)
    seed = pd.DataFrame([{'代號': '2330', '收盤價': 100}])

    def slow(rows):
        entered.set()
        assert finish.wait(2)
        return rows.assign(**{'收盤價': 999}), 1

    def replacement(rows):
        newest.set()
        return rows, 1

    try:
        worker.configure('stock', 'old', seed, 10, slow, lambda: True, released.set)
        assert entered.wait(2)
        # Reads and configuration stay responsive while the updater is blocked.
        assert worker.result('stock') is None
        worker.configure('stock', 'new', seed.assign(**{'代號': '1815'}), 10,
                         replacement, lambda: True, lambda: None)
        finish.set()
        assert released.wait(2) and newest.wait(2)
        result = wait_result(worker, 'stock')
        assert result['rows'].iloc[0].to_dict() == {'代號': '1815', '收盤價': 100}
        worker.remove('stock')
        assert worker.result('stock') is None
    finally:
        finish.set()
        worker.close()
