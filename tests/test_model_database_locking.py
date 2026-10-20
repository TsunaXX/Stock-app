"""Real SQLite contention must preserve ordered evidence and pending cloud writes."""
import sqlite3
import threading
import time
from contextlib import closing

import pytest

from model_tracking import ModelTracker, database_busy, new_trade, performance
from test_model_tracking import signal, quote


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    assert predicate()


def test_cloud_import_and_local_writer_do_not_compete_for_sqlite_lock(tmp_path):
    tracker = ModelTracker(tmp_path / 'concurrent.sqlite', start=False)
    tracker.db = lambda: sqlite3.connect(tracker.path, timeout=.03)
    entered, release, writer_started, writer_done = [threading.Event() for _ in range(4)]
    failures, transactions = [], []
    original_store, original_db = tracker.store, tracker.db

    def db():
        conn = original_db()
        if threading.current_thread().name == 'cloud-import':
            conn.set_trace_callback(lambda sql: transactions.append(sql) if sql.startswith('BEGIN') else None)
        return conn

    def store(conn, row, dirty=1):
        original_store(conn, row, dirty)
        if threading.current_thread().name == 'cloud-import' and not entered.is_set():
            entered.set()
            assert release.wait(5)

    def run(function):
        try:
            function()
        except Exception as exc:
            failures.append(exc)

    def local_write():
        writer_started.set()
        try:
            tracker.process([signal()], {'股票|2330': quote(100)})
        finally:
            writer_done.set()

    tracker.db, tracker.store = db, store
    rows = [new_trade(signal(商品鍵=str(10000 + index))) for index in range(45)]
    importer = threading.Thread(target=lambda: run(lambda: tracker.merge_synced_rows(rows)), name='cloud-import')
    writer = threading.Thread(target=lambda: run(local_write))
    importer.start()
    try:
        assert entered.wait(3)
        assert tracker.records() == []  # UI reads see the last committed state without waiting for the writer.
        writer.start()
        assert writer_started.wait(3)
        writer_done.wait(.15)  # Longer than SQLite's test timeout, shorter than a live lock wait.
    finally:
        release.set()
        importer.join(5)
        if writer.ident is not None:
            writer.join(5)
    assert not importer.is_alive() and not writer.is_alive()
    assert not failures
    assert len(transactions) == 3  # History is committed in bounded groups, never an entire month.
    records = {row['商品鍵']: row for row in tracker.records()}
    assert len(records) == 46 and records['2330']['狀態'] == '模擬持倉'
    with closing(tracker.db()) as conn:
        assert conn.execute('SELECT COUNT(*) FROM trades WHERE dirty=1').fetchone()[0] == 1


def test_locked_batch_is_retried_before_later_exit_evidence(tmp_path):
    tracker = ModelTracker(tmp_path / 'external.sqlite', start=False)
    tracker.db = lambda: sqlite3.connect(tracker.path, timeout=.02)
    blocked = threading.Event()
    original_process = tracker.process

    def process(*args):
        try:
            return original_process(*args)
        except sqlite3.OperationalError:
            blocked.set()
            raise

    tracker.process = process
    lock = tracker.db()
    lock.execute('BEGIN IMMEDIATE')
    tracker.thread = threading.Thread(target=tracker.run, daemon=True)
    tracker.thread.start()
    try:
        assert tracker.submit([signal()], {'股票|2330': quote(100, 序號=1)})
        assert tracker.submit([], {'股票|2330': quote(111, 1, 序號=2)})
        assert blocked.wait(3)
        lock.rollback()
        wait_for(lambda: tracker.records() and tracker.records()[0]['狀態'] == '已平倉')
        records = tracker.records()
        assert len(records) == 1 and not records[0]['資料缺口']
        assert performance(records)[0]['有效交易數'] == 1
        wait_for(lambda: not tracker.error)
        with closing(tracker.db()) as conn:
            assert conn.execute('SELECT dirty FROM trades').fetchone()[0] == 1
    finally:
        lock.rollback()
        lock.close()
        tracker.close()


def test_bad_remote_row_cannot_partially_commit_a_chunked_import(tmp_path):
    tracker = ModelTracker(tmp_path / 'invalid.sqlite', start=False)
    tracker.process([signal()], {})
    before = tracker.records()
    rows = [new_trade(signal(商品鍵=str(10000 + index))) for index in range(45)]
    rows[-1]['交易ID'] = 'invalid'
    with pytest.raises(ValueError, match='交易ID'):
        tracker.merge_synced_rows(rows)
    assert tracker.records() == before


def test_sync_lock_keeps_dirty_and_retries_without_duplicate_cloud_trade(monkeypatch, tmp_path):
    from test_model_delta_sync import cloud, dirty
    tracker = ModelTracker(tmp_path / 'sync.sqlite', 'fake', sync_seconds=.02, start=False)
    tracker.process([signal()], {})
    remote, _ = cloud(monkeypatch, tracker)
    tracker.db = lambda: sqlite3.connect(tracker.path, timeout=.02)
    lock = tracker.db()
    lock.execute('BEGIN IMMEDIATE')
    tracker.sync_thread = threading.Thread(target=tracker.run_sync, daemon=True)
    tracker.sync_thread.start()
    try:
        wait_for(lambda: tracker.error == '本機資料庫忙碌；保留待同步並自動重試')
        assert dirty(tracker) == 1 and not tracker.synced_at
        lock.rollback()
        wait_for(lambda: tracker.synced_at and not dirty(tracker))
        assert len(remote) == len(tracker.records()) == 1
        assert not tracker.error
    finally:
        lock.rollback()
        lock.close()
        tracker.close()


def test_busy_snapshot_is_retryable_but_other_sql_errors_are_not(tmp_path):
    with closing(sqlite3.connect(tmp_path / 'snapshot.sqlite')) as reader, closing(sqlite3.connect(tmp_path / 'snapshot.sqlite')) as writer:
        reader.execute('PRAGMA journal_mode=WAL')
        reader.execute('CREATE TABLE probe (value INTEGER)')
        reader.execute('BEGIN')
        reader.execute('SELECT * FROM probe').fetchall()
        with writer:
            writer.execute('INSERT INTO probe VALUES (1)')
        with pytest.raises(sqlite3.OperationalError) as busy:
            reader.execute('INSERT INTO probe VALUES (2)')
        assert database_busy(busy.value)
        reader.rollback()
        with pytest.raises(sqlite3.OperationalError) as invalid:
            reader.execute('SELECT * FROM nonexistent_table')
        assert not database_busy(invalid.value)
