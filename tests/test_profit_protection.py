"""Observed-price protection, executable costs, restart/merge and shared UI projection."""
import ast
import io
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from model_tracking import (PROTECTION_POLICY, ModelTracker, advance, new_trade, merge_trades,
                            performance, excel_report, trade_pnl)
from test_model_tracking import signal, quote
from test_core_calculations import load_app_symbols


def protected_signal(**changes):
    return signal(策略版本='V1.1-test', 保利規則=PROTECTION_POLICY, 目標價=130., **changes)


def test_stock_arms_at_one_r_trails_without_loosening_and_exits_at_observed_price():
    holding = advance(new_trade(protected_signal()), quote(100))
    assert holding['初始風險'] == 5.5 and holding['停損價'] == 95
    below = advance(holding, quote(105.9, 1))
    assert below['保利狀態'] == '未啟動' and '保利價' not in below
    armed = advance(below, quote(106, 2))
    assert armed['保利狀態'] == '保利中'
    floor = armed['保利價']
    assert floor > armed['模擬進場價']
    assert trade_pnl(armed, floor - .5)['模擬損益'] >= 0
    higher = advance(armed, quote(110, 3))
    assert higher['保利價'] == 104.5 and higher['初始風險'] == 5.5
    uncertain = advance(higher, quote(108, 4, 最高=135, 最低=100))
    assert uncertain['資料缺口'] and uncertain['狀態'] == '監控中斷'
    assert advance(higher, quote(108, 4))['保利價'] == 104.5
    closed = advance(higher, quote(103, 4))
    assert closed['出場原因'] == '保利' and closed['保利狀態'] == '保利出場'
    assert closed['模擬出場價'] == 102.5 and closed['模擬損益'] > 0
    assert closed['進場價'] == 100 and closed['停損價'] == 95 and closed['目標價'] == 130
    assert performance([closed])[0]['有效交易數'] == 1
    assert advance(closed, quote(140, 5)) == closed


def test_short_futures_moves_protection_down_and_gap_fill_can_still_lose():
    s = protected_signal(市場='期貨', 方向='偏空', 進場價=100., 停損價=110.,
                         跳動點=1, 乘數=10, 期交稅率=.00002)
    s['目標價'] = 50.
    holding = advance(new_trade(s), quote(100))
    armed = advance(holding, quote(88, 1))
    assert armed['保利價'] < holding['模擬進場價']
    assert trade_pnl(armed, armed['保利價'] + 1)['模擬損益'] >= 0
    higher = advance(armed, quote(80, 2))
    assert higher['保利價'] < armed['保利價']
    assert advance(higher, quote(83, 3))['保利價'] == higher['保利價']
    closed = advance(higher, quote(115, 3))
    assert closed['出場原因'] == '保利' and closed['模擬出場價'] == 116
    assert closed['模擬損益'] < 0  # Protection is a trigger, never a guaranteed fill.


def test_missing_stale_or_ambiguous_prices_do_not_arm_and_legacy_is_unchanged():
    holding = advance(new_trade(protected_signal()), quote(100))
    stale = advance(holding, quote(110, 1, 有效=False))
    assert '保利價' not in stale and stale['資料缺口']
    ambiguous = advance(holding, quote(110, 1, 最高=135, 最低=94))
    assert '保利價' not in ambiguous and ambiguous['資料缺口']
    high_only = advance(holding, quote(101, 1, 最高=110, 最低=100))
    assert '保利價' not in high_only
    legacy = advance(new_trade(signal(目標價=130)), quote(100))
    legacy = advance(legacy, quote(110, 1))
    legacy = advance(legacy, quote(98, 2))
    assert legacy['狀態'] == '模擬持倉' and '保利價' not in legacy


def test_armed_floor_survives_cloud_event_union_and_restart_without_retroactive_win(tmp_path):
    holding = advance(new_trade(protected_signal()), quote(100))
    armed = advance(holding, quote(106, 1))
    raised = advance(armed, quote(110, 2))
    stale_device = {**armed, '來源時間': quote(108, 3)['來源時間']}
    merged = merge_trades([raised], [stale_device])[0]
    assert merged['保利價'] == raised['保利價']
    assert len(merged['事件']) == len(raised['事件'])
    if shutil.which('node'):
        program = Path('google_apps_script.gs').read_text(encoding='utf-8')
        program += '\nconsole.log(JSON.stringify(mergeModelTrades_(' + json.dumps([raised], ensure_ascii=False) + ',' + json.dumps([stale_device], ensure_ascii=False) + ')));'
        result = subprocess.run(['node', '-e', program], capture_output=True, text=True, encoding='utf-8')
        assert result.returncode == 0, result.stderr
        # The deployed generic event union needs no new Apps Script schema.
        merged = merge_trades([], json.loads(result.stdout))[0]
        assert merged['保利價'] == raised['保利價']
    tracker = ModelTracker(tmp_path / 'restart.sqlite', start=False)
    with tracker.db() as db:
        tracker.store(db, merged)
    restarted = ModelTracker(tmp_path / 'restart.sqlite', start=False)
    row = restarted.records()[0]
    assert row['保利價'] == raised['保利價'] and row['資料缺口']
    closed = advance(row, quote(103, 4))
    assert closed['出場原因'] == '保利' and performance([closed])[0]['有效交易數'] == 0


@pytest.mark.parametrize('spread', [False, True])
@pytest.mark.parametrize('direction', ['偏多', '偏空'])
def test_options_protect_real_net_premiums_for_calls_puts_and_spreads(spread, direction):
    legs = [{'代碼': 'S', '買賣': -1}, {'代碼': 'L', '買賣': 1}] if spread else [{'代碼': 'L', '買賣': 1}]
    s = protected_signal(市場='期貨', 方向=direction, 進場價=20000., 停損價=19900. if direction == '偏多' else 20100.,
                         跳動點=1, 乘數=50, 期交稅率=.001, 選擇權腿=legs, 限定風險寬度=50 if spread else 0,
                         到期時間='2026-10-14T13:30:00+08:00')
    s['目標價'] = 20500. if direction == '偏多' else 19500.
    costs = {**s['成本設定'], 'slippage_ticks': 0}
    s['成本設定'] = costs
    def evidence(premium, second, short=40.):
        prices = {'L': {'價格': premium, '來源時間': quote(0, second)['來源時間']}}
        if spread:
            prices['S'] = {'價格': short, '來源時間': quote(0, second)['來源時間']}
        return quote(20000. + (second if direction == '偏多' else -second), second, 權利金證據=prices)
    holding = advance(new_trade(s), evidence(10., 0))
    assert holding['狀態'] == '模擬持倉'
    if spread:
        assert holding['初始風險'] == 1000.  # Width 50 less credit 30, multiplier 50.
        armed = advance(holding, evidence(10., 1, short=10.))  # 30 points gross realized credit decay.
        closed = advance(armed, evidence(10., 2, short=39.))
    else:
        assert holding['初始風險'] == 500.
        armed = advance(holding, evidence(25., 1))
        closed = advance(armed, evidence(10.4, 2))
    assert armed['保利狀態'] == '保利中' and armed['保利淨額'] >= 0
    assert '保利價' not in armed  # Never fake an underlying-index break-even for option costs.
    assert closed['出場原因'] == '保利' and closed['出場權利金']
    assert closed['交易成本'] > 0
    missing = advance(armed, quote(20002 if direction == '偏多' else 19998, 2))
    assert missing['資料缺口'] and missing['狀態'] == '資料不足'


def test_protection_queries_are_scoped_and_export_contains_new_columns(tmp_path):
    tracker = ModelTracker(tmp_path / 'views.sqlite', start=False)
    row = advance(advance(new_trade(protected_signal()), quote(100)), quote(110, 1))
    with tracker.db() as db:
        tracker.store(db, row)
        tracker.store(db, new_trade(signal(商品鍵='1815')))
    found = tracker.protection_plans('股票', ['2330', '1815'], '當沖', '2026-10-08')
    assert set(found) == {'2330'} and found['2330']['保利價'] == row['保利價']
    assert not tracker.protection_plans('股票', ['2330'], '當沖', '2026-10-09')
    assert not tracker.protection_plans('期貨', ['2330'], '當沖', '2026-10-08')
    assert not tracker.protection_plans('股票', ['2330'], '當沖', '2026-10-08', '指數操作計畫')
    closed = advance(row, quote(103, 2))
    with tracker.db() as db:
        tracker.store(db, closed)
    assert not tracker.protection_plans('股票', ['2330'], '當沖', '2026-10-09')
    book = pd.read_excel(io.BytesIO(excel_report([closed])), sheet_name='歷史明細')
    assert {'保利價', '保利狀態', '保利規則', '初始風險', '策略版本'} <= set(book.columns)
    assert book.iloc[0]['出場原因'] == '保利'


def test_shared_stock_and_futures_projection_and_compact_history_apptest():
    stock = advance(advance(new_trade(protected_signal()), quote(100)), quote(110, 1))
    tree = ast.parse(Path('app.py').read_text(encoding='utf-8'))
    names = {'model_protection_text', 'apply_model_protection', '_model_history_overview', 'render_operation_protection'}
    functions = '\n\n'.join(ast.unparse(n) for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names)
    script = '''
import streamlit as st
import pandas as pd
import json
import pytz
from datetime import datetime
from types import SimpleNamespace
fmt_price=lambda value: str(round(value, 2))
get_app_secret=lambda key: ''
get_futures_trading_date=lambda value: value
parse_trade_plan_numbers=lambda value: {'entry':100,'stop':95,'target':130}
st.session_state['sj_logged_in']=True
''' + functions + '\nrecord=' + repr(stock) + '''
get_model_tracker=lambda url: SimpleNamespace(protection_plans=lambda *args: {'2330':record})
for stock_mode in (True, False):
    frame=pd.DataFrame([{'代號':'2330','契約鍵':'2330','進出場預判':'舊摘要','進出場點位':'舊摘要'}])
    apply_model_protection(frame, stock_mode, '當沖')
    st.dataframe(frame)
history, _ = _model_history_overview(pd.DataFrame([record]))
st.dataframe(history)
record.update(名稱='單買BC', 計畫類型='單買BC', 選擇權腿=[{}], 保利淨額=123.45)
render_operation_protection('選擇權操作計畫', [{'商品鍵':'2330'}])
'''
    app = AppTest.from_string(script).run()
    assert not app.exception
    assert '保 104.5' in app.dataframe[0].value.iloc[0]['進出場預判']
    assert '保 104.5' in app.dataframe[1].value.iloc[0]['進出場點位']
    assert {'保利價', '保利狀態', '策略版本'} <= set(app.dataframe[2].value.columns)
    assert '保 淨額$123.45' in app.caption[0].value


def test_cost_space_is_required_and_wave_protection_keeps_original_tax_basis():
    small = protected_signal(成本設定={**signal()['成本設定'], 'stock_shares':1})
    row = advance(advance(new_trade(small), quote(100)), quote(106, 1))
    assert row['保利狀態'] == '成本待確認' and '保利價' not in row
    wave = protected_signal(策略='波段')
    row = advance(advance(new_trade(wave), quote(100)), quote(106, 1))
    assert row['保利狀態'] == '保利中'
    assert trade_pnl(row, row['保利價'] - .5)['模擬損益'] >= 0
    assert row['停損價'] == 95 and row['目標價'] == 130


def test_new_version_labels_keep_legacy_names_and_identify_protection():
    ns = load_app_symbols('model_version_display_labels', '_model_market_version_key', '_model_version_first_date')
    config = {'model_tracking_version_labels': {'stock': {'V1.0-old': '原版本'}}}
    import threading
    ns.update(_RUNTIME_FILE_LOCK=threading.RLock(), load_config=lambda: config, CONFIG_FILE='unused',
              _write_json_atomic=lambda *a: None, MODEL_VERSION='V1.1')
    labels = ns['model_version_display_labels']([{'策略版本':'V1.0-old','市場':'股票','建立時間':'2026-10-08'},
                                                 {'策略版本':'V1.1-new','市場':'股票','建立時間':'2026-10-19'}])
    assert labels['stock', 'V1.0-old'] == '原版本'
    assert labels['stock', 'V1.1-new'] == 'V1.1-stock-20261019-保利'
