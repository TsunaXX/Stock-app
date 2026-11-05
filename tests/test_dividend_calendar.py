import ast
import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from dividend_calendar import announced_date, codes, etf_events, finmind_events, merge_events, selected_events


@pytest.mark.parametrize('raw', ['1151106','115/11/06','115年11月06日','2026-11-06'])
def test_announced_dates(raw):
    assert announced_date(raw)=='2026-11-06'


def test_missing_dates_are_never_guessed():
    for raw in ['',None,'待公告','0000-00-00','2026/02/30']:
        assert announced_date(raw)==''


def test_codes_keep_etf_leading_zeroes_and_alphabetic_suffix():
    assert codes('2330.TW，0050 00400a,2330')==['2330','0050','00400A']
    with pytest.raises(ValueError):
        codes('台積電')


def etf_rows():
    return etf_events({'status':'ok','data':[
        ['0050','元大台灣50','115年11月06日','115年11月12日','115年12月01日','1.200000'],
        ['00919','群益台灣精選高息','115年11月06日','115年11月12日',None,None],
        ['0050','元大台灣50','115年12月06日','115年12月12日','116年01月01日','1.3']]},'https://www.twse.com.tw')


def test_multiple_companies_periods_and_cross_year_payment_survive():
    rows=etf_rows()
    assert len(rows)==5
    assert sum(e['date']=='2026-11-06' for e in rows)==2
    assert any(e['date']=='2027-01-01' for e in rows)
    assert len(selected_events(rows,['0050'],['股息發放日']))==2
    assert not selected_events(rows,[],['除息日'])
    assert not selected_events(rows,['0050'],[])
    assert all('None' not in e['title'] for e in rows)


def test_payment_correction_replaces_same_period_without_losing_other_periods():
    rows=etf_rows()
    updated={**rows[1],'date':'2026-12-02'}
    merged=merge_events(rows,[updated])
    assert len(merged)==5 and updated in merged
    assert merge_events(merged,[])==merged


def test_fallback_explicit_dates_and_source():
    row={'stock_id':'2330','year':'115Q1','CashExDividendTradingDate':'2026-11-06',
         'CashDividendPaymentDate':'2026-12-01','CashEarningsDistribution':5,'CashStatutorySurplus':1}
    events=finmind_events({'status':200,'data':[row]},'2330','台積電')
    assert len(events)==2 and '6元' in events[0]['title']
    assert all('備援' in e['source'] for e in events)
    assert len(finmind_events({'status':200,'data':[{**row,'CashDividendPaymentDate':''}]},'2330','台積電'))==1
    official={**events[0],'source':'上市官方除息公告','title':'官方'}
    assert selected_events([official,*events],['2330'],['除息日'])==[official]
    with pytest.raises(ValueError):
        finmind_events({'status':402,'msg':'quota'},'2330','台積電')


def test_settings_widget_save_remove_and_restore(tmp_path):
    app=Path(__file__).parents[1]/'app.py'
    tree=ast.parse(app.read_text(encoding='utf-8'))
    functions='\n'.join(ast.unparse(n) for n in tree.body if isinstance(n,ast.FunctionDef)
                        and n.name in ('save_dividend_settings','render_dividend_settings'))
    path=str(tmp_path/'dividend.json')
    Path(path).write_text(json.dumps({'codes':['0050','2330'],'kinds':['除息日']}),encoding='utf-8')
    source='''import streamlit as st
import json
from datetime import datetime
import pytz
from dividend_calendar import codes as dividend_codes, selected_events as selected_dividend_events, snapshot as normalize_dividend_snapshot
DIVIDEND_FILE = PATH
def _write_json_atomic(path, payload, indent=2):
    with open(path,'w',encoding='utf-8') as file: json.dump(payload,file)
def render_dividend_status(year): pass
def load_local_stock_names(): return {},{}
st.session_state.cal_year=2026
'''.replace('PATH',repr(path))+functions+'\nrender_dividend_settings()'
    page=AppTest.from_string(source).run()
    assert not page.exception and page.text_input[0].value=='0050,2330'
    page.text_input[0].set_value('2330').run()
    assert not page.exception and json.loads(Path(path).read_text())['codes']==['2330']
    page.multiselect[0].set_value(['股息發放日']).run()
    assert not page.exception and json.loads(Path(path).read_text())['kinds']==['股息發放日']
    fresh=AppTest.from_string(source).run()
    assert fresh.text_input[0].value=='2330' and fresh.multiselect[0].value==['股息發放日']


def test_same_day_distinct_periods_survive_within_the_preferred_source():
    rows=etf_rows()
    second={**rows[1],'event_id':rows[1]['event_id']+'-another-period'}
    fallback={**rows[1],'event_id':'fallback','source':'FinMind 股利備援'}
    chosen=selected_events([rows[1],second,fallback],['0050'],['股息發放日'])
    assert len(chosen)==2 and all(e['source']=='TWSE ETF 官方公告' for e in chosen)
