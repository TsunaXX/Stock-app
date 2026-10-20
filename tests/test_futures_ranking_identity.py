"""Delivery months, sizes and the visible table determine futures ranking identity."""
import ast
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from test_core_calculations import load_app_symbols


CONTRACT_SYMBOLS = (
    'adjust_to_next_market_day', 'futures_expiry_date',
    'FUTURES_STANDARD_SETTLEMENT_TYPES', 'FUTURES_STANDARD_SETTLEMENT_ROOTS',
    'FUTURES_SETTLEMENT_CUTOFF', '_normalize_futures_taipei_datetime',
    'is_futures_contract_settled', 'filter_active_futures_rows',
    'select_futures_ranking_contracts',
)


def contract_helpers(*extra):
    ns = load_app_symbols(*CONTRACT_SYMBOLS, *extra)
    ns['is_market_closed_func'] = lambda day: day.weekday() >= 5
    return ns


def contracts():
    return pd.DataFrame([
        {'期貨代碼': root, '契約月份': month, '契約鍵': f'{root}:{month}',
         '商品類型': kind, '契約最後交易日': expiry}
        for root, kind, expiry in [('CCF', '股票', None), ('QFF', '股票', None),
                                   ('ABC', '商品', '2026-10-19')]
        for month in ['202610', '202611', '202612']
    ]).assign(契約最後交易日=lambda df: df['契約最後交易日'].where(df['契約月份'] == '202610', None))


@pytest.mark.parametrize('clock,ccf_months,abc_months', [
    ('2026-10-19T10:00:00+08:00', ['202610'], ['202610', '202611']),
    ('2026-10-20T10:00:00+08:00', ['202610', '202611'], ['202611']),
    ('2026-10-21T13:29:59+08:00', ['202610', '202611'], ['202611']),
    ('2026-10-21T13:30:00+08:00', ['202611'], ['202611']),
    ('2026-10-16T18:00:00+08:00', ['202610'], ['202610', '202611']),
])
def test_near_month_and_settlement_rollover(clock, ccf_months, abc_months):
    ns = contract_helpers()
    rows = contracts()
    before = rows.copy(deep=True)
    selected = ns['select_futures_ranking_contracts'](pd.concat([rows, rows.iloc[:1]]), clock)
    assert selected[selected['期貨代碼'] == 'CCF']['契約月份'].tolist() == ccf_months
    assert selected[selected['期貨代碼'] == 'QFF']['契約月份'].tolist() == ccf_months
    assert selected[selected['期貨代碼'] == 'ABC']['契約月份'].tolist() == abc_months
    assert selected['契約鍵'].is_unique
    assert '202612' not in selected['契約月份'].tolist()
    pd.testing.assert_frame_equal(rows, before)


def test_next_open_skips_holidays_for_official_settlement():
    ns = contract_helpers()
    ns['is_market_closed_func'] = lambda day: day.weekday() >= 5 or day == date(2026, 10, 19)
    rows = contracts()
    rows.loc[(rows['期貨代碼'] == 'ABC') & (rows['契約月份'] == '202610'), '契約最後交易日'] = '2026-10-20'
    result = ns['select_futures_ranking_contracts'](rows, '2026-10-16T18:00:00+08:00')
    assert result[result['期貨代碼'] == 'ABC']['契約月份'].tolist() == ['202610', '202611']



@pytest.mark.parametrize('mode', ['當沖', '波段'])
def test_rank_entries_keep_contract_identity_and_deduplicate_same_contract(mode):
    ns = load_app_symbols('build_strategy_ranking_entries')
    ns['_post_close_target_date'] = lambda *a: (None, date(2026, 10, 19))
    ns['_ranking_number'] = lambda v, default=None: v
    ns['_score_futures_post_close'] = lambda row, *a: {
        'score': int(row['分數']), 'direction': '多', 'coverage': 100, 'reason': '原評分'}
    rows = pd.DataFrame([
        {'期貨代碼': 'CCF', '契約月份': '202610', '契約鍵': 'CCF:202610', '名稱': '聯電期貨', '分數': 80},
        {'期貨代碼': 'CCF', '契約月份': '202611', '契約鍵': 'CCF:202611', '名稱': '聯電期貨', '分數': 90},
        {'期貨代碼': 'CCF', '契約月份': '202610', '契約鍵': 'CCF:202610', '名稱': '聯電期貨', '分數': 80},
        {'期貨代碼': 'QFF', '契約月份': '202610', '契約鍵': 'QFF:202610', '名稱': '小型台積電期貨', '分數': 77},
    ])
    before = rows.copy(deep=True)
    result = ns['build_strategy_ranking_entries'](rows, mode, asset_type='futures')
    assert [(r['code'], r['score']) for r in result] == [('CCF:202611', 90), ('CCF:202610', 80), ('QFF:202610', 77)]
    pd.testing.assert_frame_equal(rows, before)


def test_saved_scope_expands_only_on_settlement_eve_and_keeps_removed_contracts_out():
    ns = contract_helpers('postclose_futures_seed_rows')
    select = ns['select_futures_ranking_contracts']
    ns['select_futures_ranking_contracts'] = lambda rows: select(rows, '2026-10-20T10:00:00+08:00')
    saved = {'universe': contracts().to_dict('records'),
             'strategy_ranking_snapshots': {'daytrade': {'scope': ['CCF:202610']}},
             'ignored': ['CCF:202610']}
    assert ns['postclose_futures_seed_rows'](saved)['契約鍵'].tolist() == ['CCF:202611']
    ns['select_futures_ranking_contracts'] = lambda rows: select(rows, '2026-10-19T10:00:00+08:00')
    assert ns['postclose_futures_seed_rows'](saved).empty


def test_old_device_cannot_replace_same_day_contract_snapshot():
    merge = load_app_symbols('_newer_timestamped_state')['_newer_timestamped_state']
    old = {'updated_at': '2026-10-20T08:20:00+08:00', 'target_date': '2026-10-19',
           'entries': [{'code': 'CCF', 'score': 90}, {'code': 'CCF', 'score': 80}]}
    new = {'updated_at': '2026-10-20T08:10:00+08:00', 'target_date': '2026-10-19',
           'contract_keys': True, 'entries': [{'code': 'CCF:202610', 'score': 80}]}
    assert merge(old, new) == new and merge(new, old) == new
    later_day = {**old, 'target_date': '2026-10-20', 'updated_at': '2026-10-20T21:00:00+08:00'}
    assert merge(new, later_day) == later_day


@pytest.mark.parametrize('mode', ['當沖', '波段'])
@pytest.mark.parametrize('settlement_eve', [False, True])
def test_rank_ui_only_lists_eligible_contracts_once(mode, settlement_eve):
    app = ranking_ui(mode, settlement_eve=settlement_eve)
    assert not app.exception
    cards = '\n'.join(item.value for item in app.markdown if "class='ranking-card'" in item.value)
    assert cards.count("class='ranking-result'") == (3 if settlement_eve else 2)
    assert '聯電期貨(CCF 202610)' in cards and '小型台積電期貨(QFF 202610)' in cards
    assert ('聯電期貨(CCF 202611)' in cards) is settlement_eve
    assert '(79·多)' not in cards
    assert len(app.session_state['_postclose_visible_futures']) == (3 if settlement_eve else 2)


@pytest.mark.parametrize('mode', ['當沖', '波段'])
def test_legacy_ambiguous_scores_are_retained_without_claiming_a_month(mode):
    app = ranking_ui(mode, legacy=True)
    assert not app.exception
    assert any('未記錄契約月份' in item.value for item in app.caption)
    assert not any("class='ranking-card'" in item.value for item in app.markdown)
    assert app.session_state['futures_strategy_ranking_snapshots']['daytrade']['entries'][0]['code'] == 'CCF'


def ranking_ui(mode, settlement_eve=False, legacy=False):
    from streamlit.testing.v1 import AppTest
    names = {'render_strategy_ranking', 'strategy_ranking_weights',
             'format_ranking_reason_component', 'format_ranking_entry_identity', *CONTRACT_SYMBOLS}
    tree = ast.parse((Path(__file__).parents[1] / 'app.py').read_text(encoding='utf-8'))
    definitions = '\n'.join(ast.unparse(n) for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in names) or (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in n.targets)))
    script = '''
import streamlit as st
import pandas as pd, html, pytz, calendar, re
from datetime import date, datetime, timedelta, time as dt_time
is_market_closed_func=lambda day:day.weekday() >= 5
CLOCK_PLACEHOLDER
parse_strategy_data_time=lambda value:pd.Timestamp(value) if value else None
rows=pd.DataFrame([{'期貨代碼':'CCF','契約月份':'202610','契約鍵':'CCF:202610'},
                   {'期貨代碼':'QFF','契約月份':'202610','契約鍵':'QFF:202610'}])
entries=[{'code':code,'name':name,'score':score,'direction':'多','coverage':100,'reason':'技術偏多'}
 for code,name,score in [('CCF:202611','聯電期貨',90),('CCF:202610','聯電期貨',80),
                         ('CCF:202610','聯電期貨',79),('QFF:202610','小型台積電期貨',77)]]
''' + f'''
universe=rows.copy()
universe.loc[2]=['CCF','202611','CCF:202611']
universe.loc[3]=['CCF','202612','CCF:202612']
rows['商品類型']='股票'
universe['商品類型']='股票'
# An ordinary day still excludes next month even if it is visible in the main table.
if not {settlement_eve!r}:
    rows=universe.copy()
if {legacy!r}:
    entries=[{{**entry,'code':entry['code'].split(':')[0]}} for entry in entries]
snapshot={{'updated_at':'2026-10-20T08:20:00+08:00','target_date':'2026-10-19',
           'contract_keys':{not legacy!r},'entries':entries}}
st.session_state.futures_strategy_ranking_snapshots={{'daytrade':snapshot,'swing':snapshot}}
''' + definitions + f"\nrender_strategy_ranking(rows,{mode!r},'期貨',False,ranking_universe=universe)\n"
    clock = '2026-10-20' if settlement_eve else '2026-10-19'
    script = script.replace('CLOCK_PLACEHOLDER', f"_post_close_target_date=lambda: (pd.Timestamp('{clock}T10:00:00+08:00'),date(2026,10,19))")
    return AppTest.from_string(script, default_timeout=30).run()
