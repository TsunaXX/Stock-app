"""Recover transport/authentication without losing scopes or inventing fills."""
import ast
import threading
import weakref
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from test_core_calculations import load_app_symbols


class Session(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def recovery_context():
    ns = load_app_symbols('restore_shioaji_login', 'invalidate_shioaji_connection',
                          '_stream_state', 'clear_market_stream')
    registry, clock, logouts, logins, clears = {}, [0.], [], [], []
    registry_lock = threading.RLock()
    session = Session(sj_logged_in=True, sj_key='test-key', sj_secret='test-secret', remember_sj=False)
    ns.update(st=SimpleNamespace(session_state=session), sj=object(), weakref=weakref,
              time=SimpleNamespace(monotonic=lambda: clock[0]),
              get_market_stream_registry=lambda: (registry, registry_lock),
              get_shared_market_data_cache=lambda: ({'shioaji_usage': {}}, threading.RLock()))
    old = SimpleNamespace(logout=lambda: logouts.append('old'))
    new = SimpleNamespace(logout=lambda: logouts.append('new'))
    def login(*credentials):
        logins.append(credentials)
        return new
    login.clear = lambda: clears.append(True)
    ns['init_shioaji_connection'] = login
    session['sj_api'] = old
    return ns, session, clock, old, new, logins, clears, logouts


def test_temporary_transport_down_waits_for_sdk_without_login_or_retiring_scopes():
    ns, session, clock, old, _, logins, clears, _ = recovery_context()
    state = ns['_stream_state'](old)
    state.update(connection_down=True, connection_down_since=0, scopes={'stock': ['2330']})
    clock[0] = 119
    assert not ns['restore_shioaji_login']()
    assert session.sj_api is old and session.sj_logged_in
    assert not logins and not clears and state['scopes'] == {'stock': ['2330']}
    # An illiquid symbol's quote age alone must never trigger a relogin.
    state['connection_down'] = False
    clock[0] = 100000
    assert not ns['restore_shioaji_login']() and not logins


@pytest.mark.parametrize('failure', ['auth', 'timeout'])
def test_auth_or_long_transport_failure_recovers_existing_session_even_without_remember(failure):
    ns, session, clock, old, new, logins, clears, logouts = recovery_context()
    state = ns['_stream_state'](old)
    state.update({'auth_error': 'AuthError'} if failure == 'auth' else
                 {'connection_down': True, 'connection_down_since': 0})
    clock[0] = 120
    assert ns['restore_shioaji_login']()
    assert session.sj_api is new and session.sj_logged_in
    assert logins == [('test-key', 'test-secret')] and clears == [True] and logouts == ['old']
    assert ns['_stream_state'](old)['retired'] and not session.get('_sj_recovering')
    assert not ns['restore_shioaji_login']() and len(logins) == 1


def test_second_device_adopts_recovered_cached_api_without_clearing_it_again():
    ns, session, _, old, new, logins, clears, logouts = recovery_context()
    ns['_stream_state'](old)['auth_error'] = 'AuthError'
    assert ns['restore_shioaji_login']()
    second = Session(sj_api=old, sj_logged_in=True, sj_key='test-key', sj_secret='test-secret')
    ns['st'].session_state = second
    assert ns['restore_shioaji_login']() and second.sj_api is new
    assert session.sj_api is new and clears == [True] and logouts == ['old']
    assert len(logins) == 2  # cache_resource serves the second lookup, not a second login.


def test_recovery_retry_is_bounded_and_manual_logout_disables_it():
    ns, session, clock, old, new, logins, _, _ = recovery_context()
    ns['_stream_state'](old)['auth_error'] = 'AuthError'
    def fail(*args):
        logins.append(args)
        raise ConnectionError('private broker response')
    fail.clear = lambda: None
    ns['init_shioaji_connection'] = fail
    for delay in (30, 60, 120, 240, 300, 300):
        assert not ns['restore_shioaji_login']()
        assert session.sj_auto_login_retry_after == clock[0] + delay
        assert session.sj_connection_error == 'ConnectionError'
        count = len(logins)
        clock[0] += delay - 1
        assert not ns['restore_shioaji_login']() and len(logins) == count
        clock[0] += 1
    session.manual_logout = True
    assert not ns['restore_shioaji_login']() and len(logins) == 6
    session.manual_logout = False
    ns['init_shioaji_connection'] = lambda *args: new
    assert ns['restore_shioaji_login']() and session.sj_logged_in
    assert not session.get('sj_auto_login_retry_after') and not session.get('sj_auto_login_failures')


def test_failed_initial_login_closes_partial_connection():
    tree = ast.parse((Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8'))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'init_shioaji_connection')
    node.decorator_list = []
    ns = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'app.py', 'exec'), ns)
    closed = []
    def fail(*args):
        raise ConnectionError('login failed')
    ns['sj'] = SimpleNamespace(Shioaji=lambda **kw: SimpleNamespace(login=fail, logout=lambda: closed.append(True)))
    with pytest.raises(ConnectionError):
        ns['init_shioaji_connection']('test-key', 'test-secret')
    assert closed == [True]


def test_overlapping_device_recovery_clears_cached_login_once():
    ns, session, _, old, new, _, clears, _ = recovery_context()
    from concurrent.futures import ThreadPoolExecutor
    sessions = threading.local()
    class State:
        @property
        def session_state(self):
            return sessions.value
    ns['st'] = State()
    entered, release = threading.Event(), threading.Event()
    def logout():
        entered.set()
        assert release.wait(5)
    old.logout = logout
    ns['_stream_state'](old)['auth_error'] = 'AuthError'
    def recover_first():
        sessions.value = session
        return ns['restore_shioaji_login']()
    second = Session(sj_api=old, sj_logged_in=True, sj_key='test-key', sj_secret='test-secret')
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(recover_first)
        try:
            assert entered.wait(5)
            sessions.value = second
            assert ns['restore_shioaji_login']() and second.sj_api is new
            assert clears == [True]
        finally:
            release.set()
        assert pending.result(timeout=5) and session.sj_api is new


def test_retired_cache_is_bounded_and_late_callbacks_cannot_revive_quotes():
    ns, _, _, old, _, _, _, _ = recovery_context()
    ns.update(load_app_symbols('_stream_payload'))
    ns['_stream_payload'].__globals__.update(ns)
    ns['clear_market_stream'](old)
    ns['_stream_payload'](old, SimpleNamespace(code='2330'), 'STK')
    assert not ns['_stream_state'](old)['quotes']
    for _ in range(40):
        ns['clear_market_stream'](SimpleNamespace())
    registry, _ = ns['get_market_stream_registry']()
    assert len(registry) <= 32
    other = SimpleNamespace()
    registry[id(other)] = {'api_ref': lambda: None, 'retired': True}
    assert not ns['_stream_state'](other).get('retired')


def test_pending_history_from_old_transport_epoch_is_not_published():
    ns = load_app_symbols('get_strategy_intraday_history')
    pending, calls = Future(), []
    state = {'lock': threading.RLock(), 'model_stream_epoch': 0}
    def submit(function):
        calls.append(function)
        if len(calls) == 1:
            pending.set_running_or_notify_cancel()
            return pending
        future = Future()
        future.set_result(pd.DataFrame({'Close': [102.]}))
        return future
    state['strategy_history_pool'] = SimpleNamespace(submit=submit)
    ns.update(_stream_state=lambda api: state, _stream_quote_for_contract=lambda *a: None,
              merge_stream_quote_into_intraday=lambda data, *a: data)
    get, api, contract = ns['get_strategy_intraday_history'], object(), SimpleNamespace(code='2330')
    assert get(api, contract).empty
    state.update(connection_down=True, model_stream_epoch=1)
    pending.set_result(pd.DataFrame({'Close': [90.]}))
    assert get(api, contract).empty and len(calls) == 1
    state['connection_down'] = False
    assert get(api, contract).iloc[0]['Close'] == 102. and len(calls) == 2
    assert get(api, contract).iloc[0]['Close'] == 102. and len(calls) == 2


@pytest.mark.parametrize('room', ['stock', 'stock_independent', 'futures', 'futures_independent'])
def test_transient_disconnect_preserves_jobs_but_disabled_auto_still_removes_them(room):
    ns = load_app_symbols('background_intraday_rows', 'record_model_table')
    removed = []
    worker = SimpleNamespace(remove=removed.append)
    state = Session(sj_api=object(), sj_logged_in=True, _intraday_background=worker)
    ns.update(st=SimpleNamespace(session_state=state),
              intraday_auto_settings=lambda room: (True, 5, None, None),
              check_shioaji_connection=lambda api: False,
              get_intraday_background=lambda: worker)
    rows = pd.DataFrame({'進出場預判': ['進 100｜停 95｜目 110'], '進出場點位': ['進 100｜停 95｜目 110']})
    assert ns['background_intraday_rows'](room, rows) is rows
    ns['record_model_table'](room, rows, room.startswith('stock'), '當沖')
    assert not removed
    ns['intraday_auto_settings'] = lambda room: (False, 5, None, None)
    ns['background_intraday_rows'](room, rows)
    assert removed == [room]


def test_recovery_sidebar_fragment_restores_login_and_keeps_table_visible_in_app_test():
    from streamlit.testing.v1 import AppTest
    source = (Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8')
    names = {'restore_shioaji_login', 'render_shioaji_recovery'}
    definitions = '\n'.join(ast.unparse(node) for node in ast.parse(source).body
                            if isinstance(node, ast.FunctionDef) and node.name in names)
    setup = '''
import streamlit as st
from types import SimpleNamespace
sj = object()
time = SimpleNamespace(monotonic=lambda: st.session_state.get('clock', 0))
def init_shioaji_connection(*args):
    if st.session_state.get('clock', 0) < 30:
        raise ConnectionError('temporary')
    return 'restored'
_stream_state = lambda api: {'lock': __import__('threading').RLock()}
st.session_state.setdefault('remember_sj', True)
st.session_state.setdefault('sj_key', 'fake')
st.session_state.setdefault('sj_secret', 'fake')
'''
    app = AppTest.from_string(setup + definitions + '''
with st.sidebar:
    render_shioaji_recovery()
st.write('主表保留')
''', default_timeout=30).run()
    assert not app.exception and app.session_state['sj_auto_login_retry_after'] == 30
    assert any('待重試' in item.value for item in app.caption)
    app.session_state['clock'] = 30
    app.run()
    assert not app.exception and app.session_state['sj_api'] == 'restored'
    assert app.session_state['sj_logged_in'] and app.markdown[-1].value == '主表保留'
