"""Reconnect rendering and conservative executable-entry regression checks."""
import ast
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from streamlit.testing.v1 import AppTest

from test_core_calculations import load_app_symbols
from test_model_tracking import signal, quote
from model_tracking import advance, new_trade, entry_quality_reason


def test_reconnect_editor_accepts_missing_status_and_snapshot_projection():
    tree = ast.parse(Path('app.py').read_text(encoding='utf-8'))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == 'style_tab1_df')
    source = "import pandas as pd\nimport streamlit as st\nstrategy_row_limit_state = lambda row: None\n"
    source += ast.unparse(node) + "\n"
    source += """
df_display = pd.DataFrame([{'名稱': '測試', '漲跌幅': '+1%', '狀態': '命中'}])
frozen = df_display.copy()
styled = df_display[['名稱', '漲跌幅']].style.apply(style_tab1_df, axis=1, source_frame=frozen)
df_display = df_display.drop(columns='狀態')
st.data_editor(styled, key='main')
st.data_editor(df_display.style.apply(style_tab1_df, axis=1, source_frame=df_display), key='reconnect')
"""
    app = AppTest.from_string(source).run()
    assert not app.exception


def test_fixed_opening_range_preview_still_requires_breakout_and_guards():
    ns = load_app_symbols('calculate_daytrade_filter_result', 'build_trade_plan', '_as_float', '_safe_number',
                          'get_tick_size', 'get_taiwan_tick_size', 'move_tick', 'round_to_tick', 'fmt_price')
    row = {'_daytrade_close': 99.9, '_daytrade_vwap': 99, '_daytrade_or_high': 100,
           '_daytrade_or_low': 98, '_daytrade_phase': '開盤區間完成', '_daytrade_volume_ratio': 1.1,
           '_交易日漲停價': 110, '_交易日跌停價': 90}
    result = ns['calculate_daytrade_filter_result'](row, '多頭')
    assert result['eligible'] and '等待突破價' in result['rule']
    plan = ns['build_trade_plan'](row, '多頭', True, result)
    assert plan['valid'] and '進 100.5' in plan['summary']
    assert not ns['calculate_daytrade_filter_result']({**row, '_daytrade_guard_reason': '反覆穿越均價'}, '多頭')['eligible']
    assert not ns['calculate_daytrade_filter_result']({**row, '_daytrade_volume_ratio': .5}, '多頭')['eligible']
    assert not ns['calculate_daytrade_filter_result']({**row, '_daytrade_close': 99}, '多頭')['eligible']


def test_published_stock_levels_remain_visible_without_new_signal():
    ns = load_app_symbols('build_guarded_stock_trade_plan', 'parse_trade_plan_numbers', '_safe_number')
    ns['st'] = SimpleNamespace(session_state={})
    ns['fmt_price'] = lambda n: str(n)
    current = {'valid': True, 'summary': '進 101｜停 99｜目 104'}
    ns['build_trade_plan'] = lambda *args: current.copy()
    row = {'代號': '2330', '_daytrade_data_time': '2026/10/16 09:01:00', '_daytrade_close': 100}
    fn = ns['build_guarded_stock_trade_plan']
    fn(row, '多頭', True, {})
    current.update(valid=False, summary='—')
    kept = fn(row, '多頭', True, {})
    assert '進 101｜停 99｜目 104' in kept['summary'] and kept['tracking_only'] and not kept['valid']
    stopped = fn({**row, '_daytrade_close': 98}, '多頭', True, {})
    assert '失效（停損價已碰觸）' in stopped['summary']
    stale = fn({**row, '_daytrade_close': 98, '_data_stale': True}, '多頭', True, {})
    assert '行情未確認' in stale['summary'] and '已碰觸' not in stale['summary']
    assert fn({**row, '_daytrade_data_time': '2026/10/19 09:01:00'}, '多頭', True, {})['summary'] == '—'


def test_new_quality_policy_rejects_late_index_entry_and_can_resume_at_original_price():
    s = signal(市場='期貨', 方向='偏空', 進場價=48843, 停損價=48876, 目標價=48430,
               跳動點=1, 乘數=10, 期交稅率=.00002, 進場品質檢查=True)
    waiting = advance(new_trade(s), quote(48433))
    assert waiting['狀態'] == '等待進場' and '0.25R' in waiting['進場等待原因']
    assert '模擬進場價' not in waiting
    resumed = advance(waiting, quote(48843, 1))
    assert resumed['狀態'] == '模擬持倉'
    assert resumed['目標價'] == 48430


def test_cost_guard_rejects_nominal_profit_below_cost_without_rewriting_legacy():
    s = signal(市場='期貨', 進場價=100, 停損價=99, 目標價=103,
               跳動點=1, 乘數=10, 期交稅率=.00002, 進場品質檢查=True)
    waiting = advance(new_trade(s), quote(100))
    assert '成本' in waiting['進場等待原因'] and waiting['狀態'] == '等待進場'
    legacy = advance(new_trade({**s, '進場品質檢查': False}), quote(100))
    assert legacy['狀態'] == '模擬持倉'
    assert not entry_quality_reason(signal(進場品質檢查=True, 策略='波段'), 108)
