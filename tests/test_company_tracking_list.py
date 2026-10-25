"""Tracking edits control visibility without deleting retained company history."""
import ast
import json
import threading
from pathlib import Path
from textwrap import dedent

from test_core_calculations import load_app_symbols


def fixtures():
    ns = load_app_symbols('company_calendar_key', 'company_display_snapshot', 'selected_company_calendar_snapshot',
                          'resolve_earnings_ticker', 'EARNINGS_TICKER_ALIASES')
    ns['load_local_stock_names'] = lambda: ({'2330':'台積電', '2408':'南亞科', '1815':'富喬'}, {'台積電':'2330'})
    snap = {'tickers':'1815, 台積電', 'calendar_companies':['2330','2408','1815'],
            'taiwan_revenue':{'events':[{'ticker':key,'revenue':{'code':key,'revenue_month':'11509'}}
                                      for key in ('2330','2408','1815')]},
            'financials':{'events':[{'ticker':'2330','date':'2026-10-22','title':'財報'}]},
            'disclosures':{'events':[{'ticker':'2408','title':'取得機器設備'}]}}
    snap['events'] = [e for section in ('taiwan_revenue','financials','disclosures') for e in snap[section]['events']]
    return ns, snap


def test_removed_company_stays_archived_but_order_and_calendar_follow_current_list():
    ns, snapshot = fixtures()
    before = json.dumps(snapshot, ensure_ascii=False)
    display = ns['company_display_snapshot'](snapshot)
    assert [e['ticker'] for e in display['taiwan_revenue']['events']] == ['1815','2330']
    assert display['disclosures']['events'] == []
    calendar = ns['selected_company_calendar_snapshot'](display)
    assert calendar['financials']['events'][0]['title'] == '財報'
    assert json.dumps(snapshot, ensure_ascii=False) == before
    snapshot['tickers'] = ''
    assert ns['company_display_snapshot'](snapshot)['events'] == []


def test_company_calendar_excludes_equipment_and_building_even_when_selected():
    ns, snapshot = fixtures()
    snapshot['tickers'] = '2408, 2330'
    snapshot['disclosures']['events'].append({'ticker':'2408','title':'廠房興建工程'})
    snapshot['dividends'] = {'events':[{'ticker':'2408','title':'除權息'}]}
    selected = ns['selected_company_calendar_snapshot'](ns['company_display_snapshot'](snapshot))
    assert selected['disclosures']['events'] == selected['dividends']['events'] == []
    assert len(selected['taiwan_revenue']['events']) == 2
    assert len(selected['financials']['events']) == 1
    assert len(snapshot['disclosures']['events']) == 2


def test_actual_input_and_order_controls_persist_removal_and_empty_list():
    from streamlit.testing.v1 import AppTest
    source = Path('app.py').read_text(encoding='utf8')
    names = {'company_calendar_key','company_display_snapshot','save_company_tracking_input','move_company_tracking_input','company_order_market_items'}
    functions = '\n'.join(ast.unparse(n) for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name in names)
    controls = dedent(source[source.index('    company_ticker_input = st.text_input('):source.index('    sync_col, status_col = st.columns(')])
    _, snapshot = fixtures()
    snapshot['tickers'] = '2330, 2408, 1815'
    setup = '''import streamlit as st
from datetime import datetime
import pytz
def resolve_earnings_ticker(value):
    return {'input':value.strip(),'display_name':value.strip(),'candidates':(value.strip(),)}
st.session_state.setdefault('calendar_preferences', {})
st.session_state.setdefault('saved_snapshots', [])
def save_company_event_snapshot(snapshot):
    st.session_state.saved_snapshots.append(snapshot)
    return True
'''
    script = setup + '\nst.session_state.setdefault("company_event_snapshot", ' + repr(snapshot) + ')\n' + functions + '\n' + controls
    app = AppTest.from_string(script).run()
    app.selectbox(key='company_order_item').select('2408').run()
    app.button(key='company_order_up').click().run()
    assert not app.exception
    assert app.session_state.company_event_snapshot['tickers'] == '2408, 2330, 1815'
    app.text_input(key='company_data_tickers').set_value('1815, 2330').run()
    assert not app.exception
    assert app.session_state.saved_snapshots[-1]['calendar_companies'] == ['2330','1815']
    assert len(app.session_state.saved_snapshots[-1]['events']) == len(snapshot['events'])
    app.text_input(key='company_data_tickers').set_value('').run()
    assert not app.exception and app.session_state.saved_snapshots[-1]['tickers'] == ''
    app.run()
    assert app.text_input(key='company_data_tickers').value == ''


def test_failed_selection_save_retries_without_restoring_removed_company():
    ns = load_app_symbols('sync_company_background_snapshot', 'empty_company_event_snapshot',
                          'normalize_company_event_snapshot', 'apply_revenue_announcement_date_overrides',
                          '_cache_payload_timestamp')
    remote = {'tickers':'2330,2408', 'calendar_companies':['2330','2408'],
              'updated_at':'2026-10-22T10:00:00+08:00',
              'taiwan_revenue':{'events':[{'ticker':'2408','date':'2026-10-10','title':'月營收'}]}}
    pending = ns['normalize_company_event_snapshot']({**remote, 'tickers':'2330', 'calendar_companies':['2330'],
                                                    'updated_at':'2026-10-22T11:00:00+08:00'})
    ns['GOOGLE_SCOPE_COMPANY'] = 'company_events'
    ns['get_data_cache_sync_lock'] = threading.RLock
    ns['_fetch_remote_scope'] = lambda *a, **kw: (remote, None)
    writes = []
    ns['_save_remote_scope'] = lambda url, scope, data, **kw: (writes.append(data) or (True, None))
    assert ns['sync_company_background_snapshot']('fake', pending)
    assert writes[0]['tickers'] == '2330' and writes[0]['calendar_companies'] == ['2330']
    assert writes[0]['taiwan_revenue']['events'] == remote['taiwan_revenue']['events']
    remote['updated_at'] = '2026-10-22T12:00:00+08:00'
    assert not ns['sync_company_background_snapshot']('fake', pending)
    assert len(writes) == 1


def test_company_order_market_items_classify_by_resolved_ticker():
    ns = load_app_symbols('company_order_market_items', 'resolve_earnings_ticker', 'EARNINGS_TICKER_ALIASES')
    ns['load_local_stock_names'] = lambda: ({'2330': '台積電', '2408': '南亞科'}, {'台積電': '2330'})
    items = ['META', '台積電', 'MU', '2408.TWO', '2330', 'TSLA']
    classify = ns['company_order_market_items']
    assert classify(items, '台股') == ['台積電', '2408.TWO', '2330']
    assert classify(items, '美股') == ['META', 'MU', 'TSLA']
    assert classify(items, '全部') == items


def test_order_buttons_swap_only_same_market_and_preserve_other_positions():
    from streamlit.testing.v1 import AppTest
    source = Path('app.py').read_text(encoding='utf8')
    names = {'company_calendar_key', 'company_display_snapshot', 'save_company_tracking_input',
             'move_company_tracking_input', 'company_order_market_items'}
    functions = '\n'.join(ast.unparse(n) for n in ast.parse(source).body
                          if isinstance(n, ast.FunctionDef) and n.name in names)
    controls = dedent(source[source.index('    company_ticker_input = st.text_input('):
                             source.index('    sync_col, status_col = st.columns(')])
    _, snapshot = fixtures()
    snapshot['tickers'] = '2330, META, 2408, MU'
    setup = """import streamlit as st
import re
from datetime import datetime
import pytz
def resolve_earnings_ticker(value):
    ticker = value.strip()
    return {'input': ticker, 'display_name': ticker,
            'candidates': (ticker + '.TW' if ticker.isdigit() else ticker,)}
st.session_state.setdefault('calendar_preferences', {})
st.session_state.setdefault('saved_snapshots', [])
def save_company_event_snapshot(snapshot):
    st.session_state.saved_snapshots.append(snapshot)
    return True
"""
    script = setup + '\nst.session_state.setdefault("company_event_snapshot", ' + repr(snapshot) + ')\n' + functions + '\n' + controls
    app = AppTest.from_string(script).run()
    app.radio(key='company_order_market').set_value('台股').run()
    assert not app.exception
    assert app.selectbox(key='company_order_item').options == ['2330', '2408']
    app.selectbox(key='company_order_item').select('2408').run()
    app.button(key='company_order_up').click().run()
    assert not app.exception
    assert app.session_state.company_event_snapshot['tickers'] == '2408, META, 2330, MU'
    app.radio(key='company_order_market').set_value('美股').run()
    assert not app.exception
    assert app.selectbox(key='company_order_item').options == ['META', 'MU']
    app.selectbox(key='company_order_item').select('MU').run()
    app.button(key='company_order_up').click().run()
    assert not app.exception
    assert app.session_state.company_event_snapshot['tickers'] == '2408, MU, 2330, META'
    app.radio(key='company_order_market').set_value('全部').run()
    assert not app.exception
    assert app.selectbox(key='company_order_item').options == ['2408', 'MU', '2330', 'META']
