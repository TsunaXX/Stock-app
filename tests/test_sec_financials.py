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
    ns = load_app_symbols('fetch_us_revenue_events', '_growth_percent', '_to_number', '_format_compact_number')
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
