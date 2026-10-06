"""SEC fiscal-period facts and GAAP earnings releases; no calendar-quarter guesses."""
import math
import re
import threading
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

_HTTP_LOCK = threading.Lock()
_LAST_REQUEST = 0.0
_MONTH_DATE = r"(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}"


def sec_get(url, user_agent):
    global _LAST_REQUEST
    with _HTTP_LOCK:
        time.sleep(max(0, .12 - (time.monotonic() - _LAST_REQUEST)))
        _LAST_REQUEST = time.monotonic()
        response = requests.get(url, headers={'User-Agent': user_agent, 'Accept-Encoding': 'gzip, deflate'},
                                timeout=(4, 10))
        response.raise_for_status()
        return response


def number(value):
    try:
        value = float(str(value).replace(',', '').replace('$', '').strip().replace('(', '-').replace(')', ''))
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def facts_revenue(payload):
    """Select USD entity-wide durations; YTD revenue is never mistaken for a quarter."""
    facts = payload.get('facts', {}).get('us-gaap', {})
    concepts = ('RevenueFromContractWithCustomerExcludingAssessedTax',
                'RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenues', 'SalesRevenueNet')
    records = []
    for concept in concepts:
        for record in facts.get(concept, {}).get('units', {}).get('USD', []):
            try:
                duration = (datetime.fromisoformat(record['end']) - datetime.fromisoformat(record['start'])).days
            except (KeyError, ValueError):
                continue
            if record.get('form') not in ('10-Q', '10-K', '20-F', '40-F', '6-K', '8-K'):
                continue
            if number(record.get('val')) is not None:
                records.append({**record, 'duration': duration, 'val': number(record['val'])})
    def latest(items):
        values = {}
        for record in sorted(items, key=lambda r: r.get('filed', '')):
            values[(record['start'], record['end'])] = record
        return sorted(values.values(), key=lambda r: (r['end'], r.get('filed', '')), reverse=True)
    quarters = latest(r for r in records if 70 <= r['duration'] <= 110)
    annuals = latest(r for r in records if 330 <= r['duration'] <= 380)
    # Fourth-quarter revenue is additive; EPS is not, so only direct quarter EPS is used.
    for annual in annuals:
        nine_months = latest(r for r in records if r['start'] == annual['start']
                            and 250 <= r['duration'] <= 300 and r['end'] < annual['end'])
        if nine_months:
            prior = nine_months[0]
            days = (datetime.fromisoformat(annual['end']) - datetime.fromisoformat(prior['end'])).days
            if 70 <= days <= 110 and not any(r['end'] == annual['end'] for r in quarters):
                quarters.append({**annual, 'start': (datetime.fromisoformat(prior['end']) + timedelta(days=1)).date().isoformat(),
                                 'val': annual['val'] - prior['val']})
    quarters.sort(key=lambda r: (r['end'], r.get('filed', '')), reverse=True)
    if not quarters:
        return None
    current = quarters[0]
    end = datetime.fromisoformat(current['end'])
    year_ago = next((r for r in quarters[1:] if 320 <= (end - datetime.fromisoformat(r['end'])).days <= 410), None)
    previous = next((r for r in quarters[1:] if 70 <= (end - datetime.fromisoformat(r['end'])).days <= 110), None)
    eps = [r for r in facts.get('EarningsPerShareDiluted', {}).get('units', {}).get('USD/shares', [])
           if r.get('end') == current['end'] and r.get('start') == current['start']
           and r.get('accn') == current.get('accn') and number(r.get('val')) is not None]
    return {'period_end': current['end'], 'quarter_revenue': current['val'],
            'previous_quarter': previous['val'] if previous else None,
            'year_ago_quarter': year_ago['val'] if year_ago else None,
            'annual_revenue': annuals[0]['val'] if annuals else None,
            'annual_period_end': annuals[0]['end'] if annuals else '',
            'previous_annual': annuals[1]['val'] if len(annuals) > 1 else None,
            'eps': number(eps[-1]['val']) if eps else None,
            'filed_date': current.get('filed', ''), 'accession': current.get('accn', '')}


def release_revenue(document):
    """Accept actual GAAP tables with explicit periods/units; reject guidance and segments."""
    soup = BeautifulSoup(document, 'html.parser')
    text = ' '.join(soup.stripped_strings)
    ended = re.search(r'(?:which |that )?ended (' + _MONTH_DATE + ')', text, re.I)
    result = {}
    for table in soup.find_all('table'):
        table_text = table.get_text(' ', strip=True)
        if 'in millions' not in table_text.lower() or 'outlook' in table_text.lower():
            continue
        gaap = re.search(r'(?<!Non-)\bGAAP\b', table_text)
        if gaap and 'Non-GAAP' in table_text and table_text.index('Non-GAAP') < gaap.start():
            continue
        rows = [[c.get_text(' ', strip=True) for c in tr.find_all(['td', 'th'], recursive=False)]
                for tr in table.find_all('tr')]
        revenue = next((row for row in rows if row and row[0].lower() in ('revenue', 'total revenue', 'net sales')), [])
        values = [number(cell) for cell in revenue[1:] if number(cell) is not None]
        if not values:
            continue
        if 'Quarterly Financial Results' in table_text and ended and 'GAAP' in table_text:
            result.update(period_end=datetime.strptime(ended[1], '%B %d, %Y').date().isoformat(),
                          quarter_revenue=values[0] * 1e6,
                          previous_quarter=values[1] * 1e6 if len(values) >= 3 else None,
                          year_ago_quarter=values[2] * 1e6 if len(values) >= 3 else None)
            eps = next((row for row in rows if row and row[0].lower() == 'diluted earnings per share'), [])
            result['eps'] = next((number(cell) for cell in eps[1:] if number(cell) is not None), None)
        elif 'Annual Financial Results' in table_text and ended and 'GAAP' in table_text:
            result.update(annual_revenue=values[0] * 1e6, previous_annual=values[1] * 1e6 if len(values) > 1 else None,
                          annual_period_end=datetime.strptime(ended[1], '%B %d, %Y').date().isoformat())
        elif 'CONSOLIDATED STATEMENTS OF OPERATIONS' in table_text.upper():
            dates = next((re.findall(_MONTH_DATE, ' '.join(row)) for row in rows
                          if len(re.findall(_MONTH_DATE, ' '.join(row))) >= 3), [])
            if len(dates) == len(values) and re.search(r'Qtr\.|Three Months', table_text, re.I):
                parsed = [datetime.strptime(d, '%B %d, %Y').date().isoformat() for d in dates]
                result.update(period_end=parsed[0], quarter_revenue=values[0] * 1e6,
                              previous_quarter=values[1] * 1e6, year_ago_quarter=values[2] * 1e6)
                if len(values) == 5 and 'Year Ended' in table_text:
                    result.update(annual_revenue=values[3] * 1e6, previous_annual=values[4] * 1e6,
                                  annual_period_end=parsed[3])
    return result if result.get('period_end') and result.get('quarter_revenue') is not None else None
