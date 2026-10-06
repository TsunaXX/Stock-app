"""Official fiscal periods, GAAP values and fallback preservation."""
from pathlib import Path

from sec_financials import facts_revenue, release_revenue
from market_automation import merge_company_sections
from test_core_calculations import load_app_symbols


FIXTURE = Path(__file__).parent / 'fixtures/micron_q4_2026.html'


def test_actual_micron_release_keeps_gaap_and_rejects_future_guidance():
    data = release_revenue(FIXTURE.read_text(encoding='utf-8'))
    assert data == {'period_end': '2026-09-03', 'quarter_revenue': 54_229_000_000,
                    'previous_quarter': 41_456_000_000, 'year_ago_quarter': 11_315_000_000,
                    'eps': 32.87, 'annual_revenue': 133_188_000_000,
                    'previous_annual': 37_378_000_000, 'annual_period_end': '2026-09-03'}
    assert release_revenue('<table>Revenue $61.5 billion GAAP Outlook</table>') is None


def test_micron_requests_identify_the_app_and_follow_only_official_release_links():
    from types import SimpleNamespace
    ns = load_app_symbols('fetch_micron_revenue')
    ns['release_revenue'] = release_revenue
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        assert kwargs['headers']['User-Agent'] == 'Stock-app github.com/TsunaXX/Stock-app'
        document = ('<a href="/news/press-release/2026/Micron-Reports-Results/default.aspx">Press release</a>'
                    if '/overview/' in url else
                    '<span class="evergreen-news-date-text">September 30, 2026</span>' + FIXTURE.read_text(encoding='utf-8'))
        return SimpleNamespace(content=document.encode(), raise_for_status=lambda: None)
    ns['requests'] = SimpleNamespace(get=get)
    data = ns['fetch_micron_revenue']()
    assert data['period_end'] == '2026-09-03' and data['filed_date'] == '2026-09-30'
    assert len(calls) == 2 and calls[1][0].startswith('https://investors.micron.com/news/press-release/')


def test_saved_legacy_and_new_source_for_same_quarter_count_only_once():
    ns = load_app_symbols('normalize_company_event_snapshot', 'empty_company_event_snapshot')
    old = {'date': '2026-05-31', 'source': 'Yahoo Finance', 'title': 'MU 季營收（期末）',
           'revenue': {'ticker': 'MU', 'period_end': '2026-05-31', 'quarter_revenue': 1}}
    new = {**old, 'ticker': 'MU', 'source': 'Yahoo 備援', 'title': 'MU 季營收',
           'data_asof': '2026-05-31', 'revenue': {**old['revenue'], 'quarter_revenue': 2}}
    saved = {'us_revenue': {'events': [new, old]}, 'events': [new, old], 'calendar_companies': ['MU']}
    normalized = ns['normalize_company_event_snapshot'](saved)
    assert normalized['us_revenue']['events'] == [new]
    assert normalized['events'] == [new] and normalized['calendar_companies'] == ['MU']


def test_sec_facts_use_actual_quarters_and_derive_q4_revenue_without_deriving_eps():
    def row(start, end, value, form='10-Q', accession='quarter'):
        return {'start': start, 'end': end, 'val': value, 'form': form,
                'filed': '2026-10-01', 'accn': accession}
    revenue = [row('2026-01-01', '2026-03-31', 10), row('2026-04-01', '2026-06-30', 20),
               row('2026-07-01', '2026-09-30', 30), row('2026-01-01', '2026-09-30', 60),
               row('2026-01-01', '2026-12-31', 100, '10-K', 'annual')]
    facts = {'facts': {'us-gaap': {
        'Revenues': {'units': {'USD': revenue, 'EUR': [row('2026-10-01', '2026-12-31', 999)]}},
        'EarningsPerShareDiluted': {'units': {'USD/shares': [row('2026-01-01', '2026-12-31', 99, '10-K', 'annual')]}}}}}
    result = facts_revenue(facts)
    assert result['period_end'] == '2026-12-31'
    assert result['quarter_revenue'] == 40 and result['previous_quarter'] == 30
    assert result['eps'] is None and result['annual_revenue'] == 100
    facts['facts']['us-gaap']['EarningsPerShareDiluted']['units']['USD/shares'].append(row('2026-10-01', '2026-12-31', 3, '10-K', 'annual'))
    assert facts_revenue(facts)['eps'] == 3
    revenue.pop()
    facts['facts']['us-gaap']['EarningsPerShareDiluted']['units']['USD/shares'].append(row('2026-07-01', '2026-09-30', 0))
    result = facts_revenue(facts)
    assert result['quarter_revenue'] == 30 and result['eps'] == 0
    assert facts_revenue({'facts': {}}) is None


def test_us_source_failures_use_official_issuer_then_yahoo_without_empty_overwrite():
    import logging
    ns = load_app_symbols('fetch_us_revenue_events', '_growth_percent', '_to_number', '_format_compact_number')
    ns['logger'] = logging.getLogger(__name__)
    ns['resolve_earnings_ticker'] = lambda ticker: {'candidates': [ticker], 'display_name': ticker}
    calls = []
    def failed(ticker):
        calls.append(('SEC', ticker))
        raise ValueError('403')
    ns['fetch_sec_revenue'] = failed
    ns['fetch_micron_revenue'] = lambda: release_revenue(FIXTURE.read_text(encoding='utf-8')) | {
        'source': 'Micron 官方財報（GAAP）', 'filed_date': '2026-09-30'}
    def yahoo(ticker):
        calls.append(('Yahoo', ticker))
        return {'source': 'Yahoo', 'period_end': '2026-05-31', 'quarter_revenue': 20}
    ns['fetch_yahoo_revenue'] = yahoo
    result = ns['fetch_us_revenue_events'](('MU', 'AMD'))
    mu, amd = result['events']
    assert mu['date'] == '2026-09-30' and mu['revenue']['period_end'] == '2026-09-03'
    assert mu['revenue']['qoq'] == '+30.81%' and mu['revenue']['eps'] == 32.87
    assert ('Yahoo', 'MU') not in calls and ('Yahoo', 'AMD') in calls
    old = {'us_revenue': {'events': [dict(mu, ticker=None, source='old', data_asof='', event_id=None)]}}
    # Merge input is section-shaped, as used by background and cloud persistence.
    merged = merge_company_sections(old, {'us_revenue': result})
    assert len(merged['us_revenue']['events']) == 2 and merged['us_revenue']['events'][0]['source'] == mu['source']
    assert merge_company_sections(merged, {'us_revenue': {'events': []}})['events'] == merged['events']
    ns['fetch_micron_revenue'] = lambda: (_ for _ in ()).throw(ValueError('offline'))
    ns['fetch_yahoo_revenue'] = failed
    empty = ns['fetch_us_revenue_events'](('MU',))
    assert empty['missing'] and not empty['events']
    assert merge_company_sections(merged, {'us_revenue': empty})['events'] == merged['events']


def test_company_ui_distinguishes_filing_date_period_and_latest_company_version():
    import ast
    from streamlit.testing.v1 import AppTest
    from test_core_calculations import APP_PATH
    node = next(n for n in ast.parse(APP_PATH.read_text(encoding='utf-8')).body
                if isinstance(n, ast.FunctionDef) and n.name == 'render_company_event_snapshot')
    source = """
import streamlit as st
import pandas as pd
_revenue_metric_html = lambda *a: str(a)
_usd_currency = lambda v: str(v)
compact_table_column_config = lambda *a: {}
snapshot = {'us_revenue': {'events': [
 {'source':'old', 'revenue':{'ticker':'MU','period_end':'2026-05-31'}},
 {'source':'Micron 官方財報', 'revenue':{'ticker':'MU','company':'Micron','period_end':'2026-09-03','filed_date':'2026-09-30','eps':32.87}}]}}
""" + ast.unparse(node) + '\nrender_company_event_snapshot(snapshot)'
    app = AppTest.from_string(source).run()
    assert not app.exception
    assert sum('財報期間截至 2026-09-03' in m.value for m in app.markdown) == 1
    assert any('2026-09-30' in c.value and 'Micron 官方' in c.value for c in app.caption)
    assert all('2026-05-31' not in m.value for m in app.markdown)


def test_sec_latest_filing_parses_exhibit_and_flags_unparsed_new_release():
    ns = load_app_symbols('fetch_sec_revenue')
    ns['facts_revenue'] = facts_revenue
    ns['release_revenue'] = release_revenue
    ns['fetch_sec_tickers'] = lambda: {'0': {'ticker': 'MU', 'cik_str': 723125}}
    ns['fetch_sec_submissions'] = lambda cik: {'filings': {'recent': {
        'form':['8-K'], 'items':['2.02,9.01'], 'filingDate':['2026-09-30'],
        'accessionNumber':['0000723125-26-000018'], 'primaryDocument':['cover.htm']}}}
    ns['fetch_sec_facts'] = lambda cik, version: {'facts': {'us-gaap': {'Revenues': {'units': {'USD': [
        {'start':'2026-02-27','end':'2026-05-28','val':41_456_000_000,
         'form':'10-Q','filed':'2026-06-24','accn':'old'}]}}}}}
    cleared = []
    ns['fetch_sec_facts'].clear = lambda *a: cleared.append(a)
    paths = []
    def document(url):
        paths.append(url)
        return b'<a href="earnings99.htm">Exhibit 99.1</a>' if url.endswith('cover.htm') else FIXTURE.read_bytes()
    ns['fetch_sec_document'] = document
    result = ns['fetch_sec_revenue']('MU')
    assert result['source'].startswith('SEC EDGAR') and result['quarter_revenue'] == 54_229_000_000
    assert result['filed_date'] == '2026-09-30' and result['period_end'] == '2026-09-03'
    assert len(paths) == 2 and paths[-1].endswith('/000072312526000018/earnings99.htm')
    ns['fetch_sec_document'] = lambda url: (_ for _ in ()).throw(ValueError('unsupported/offline'))
    result = ns['fetch_sec_revenue']('MU')
    assert result['period_end'] == '2026-05-28' and result['warning']
    assert cleared == [(723125, '0000723125-26-000018')]


def test_manual_company_sync_clears_source_caches_and_keeps_prior_report_on_total_failure():
    import ast
    from contextlib import nullcontext
    from types import SimpleNamespace
    from test_core_calculations import APP_PATH
    from test_stock_supplement import State
    node = next(n for n in ast.walk(ast.parse(APP_PATH.read_text(encoding='utf-8')))
                if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == 'sync_company_data')
    ns = load_app_symbols('empty_company_event_snapshot', 'normalize_company_event_snapshot',
                          'apply_revenue_announcement_date_overrides')
    event = {'ticker':'MU','market':'美股','title':'MU 季營收','date':'2026-09-30',
             'revenue':{'ticker':'MU','period_end':'2026-09-03','quarter_revenue':54_229_000_000}}
    state = State(company_event_snapshot={'tickers':'MU','calendar_companies':['MU'],
                  'us_revenue':{'events':[event]}}, calendar_preferences={})
    cleared, saved = [], []
    ns['st'] = SimpleNamespace(session_state=state, spinner=lambda *a: nullcontext(),
                              toast=lambda *a, **kw: None, rerun=lambda: None, warning=lambda *a: None)
    for name in ('fetch_earnings_events','fetch_twse_monthly_revenue_rows','fetch_mops_company_monthly_revenue',
                 'fetch_mops_monthly_revenue_announcement','fetch_finmind_monthly_revenue_rows',
                 'fetch_taiwan_monthly_revenue_events','fetch_us_revenue_events','fetch_sec_revenue',
                 'fetch_sec_submissions','fetch_sec_facts','fetch_micron_revenue','fetch_yahoo_revenue'):
        ns[name] = SimpleNamespace(clear=lambda name=name: cleared.append(name))
    ns.update(sync_company_data=True, preview_inputs=['MU'], company_ticker_input='MU',
              COMPANY_SYNC_MAX_TICKERS=12, selected_event_types=[], CALENDAR_GROUP_OPTIONS=[], US_HIGH_IMPACT_EVENTS=[],
              fetch_company_event_sections=lambda symbols: ({'us_revenue': {'events':[], 'missing':['offline']}}, [], symbols),
              save_company_event_snapshot=lambda snapshot: saved.append(snapshot) or False,
              save_calendar_preferences=lambda *a: None, get_app_secret=lambda *a: None)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])), str(APP_PATH), 'exec'), ns)
    assert saved[0]['us_revenue']['events'] == [event]
    assert saved[0]['calendar_companies'] == ['MU']
    assert {'fetch_sec_submissions','fetch_sec_facts','fetch_micron_revenue','fetch_yahoo_revenue'} <= set(cleared)
