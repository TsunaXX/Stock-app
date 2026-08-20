"""Regression tests for pure calculation helpers without starting Streamlit UI."""

import ast
import calendar
import html
import io
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import pytz
from bs4 import BeautifulSoup


APP_PATH = Path(__file__).parents[1] / "app.py"


def load_app_symbols(*names):
    """Compile selected top-level helpers so importing app.py cannot render the UI."""
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    selected = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            selected.append(node)
        elif isinstance(node, ast.Assign):
            targets = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if targets.intersection(names):
                selected.append(node)
    namespace = {
        "calendar": calendar,
        "html": html,
        "io": io,
        "date": date,
        "datetime": datetime,
        "dt_time": dt_time,
        "timedelta": timedelta,
        "Decimal": Decimal,
        "ROUND_CEILING": ROUND_CEILING,
        "ROUND_FLOOR": ROUND_FLOOR,
        "ROUND_HALF_UP": ROUND_HALF_UP,
        "BeautifulSoup": BeautifulSoup,
        "json": json,
        "math": math,
        "np": np,
        "pd": pd,
        "parsedate_to_datetime": parsedate_to_datetime,
        "pytz": pytz,
        "re": re,
        "time": time,
        "ThreadPoolExecutor": ThreadPoolExecutor,
        "urljoin": urljoin,
    }
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, APP_PATH, "exec"), namespace)
    return namespace


DATE_SYMBOLS = (
    "get_holidays", "is_market_closed_func", "adjust_to_next_market_day",
    "get_near_month_futures_settlement", "futures_expiry_date",
    "get_txo_target_contract_specs",
)


def test_february_2026_expiry_moves_to_next_trading_day():
    symbols = load_app_symbols(*DATE_SYMBOLS)
    expected = date(2026, 2, 23)
    assert symbols["futures_expiry_date"]("202602") == expected
    assert symbols["get_near_month_futures_settlement"](date(2026, 2, 10)) == expected
    monthly = symbols["get_txo_target_contract_specs"]("月選", date(2026, 2, 10))[0]
    assert monthly["delivery_month"] == "202602"
    assert monthly["expiry"] == expected


def test_stock_limit_prices_are_decimal_tick_exact():
    symbols = load_app_symbols("get_tick_size", "calculate_limits")
    calculate_limits = symbols["calculate_limits"]
    assert calculate_limits(1.10) == (1.21, 0.99)
    assert calculate_limits(1.90) == (2.09, 1.71)
    assert calculate_limits(4.10) == (4.51, 3.69)
    assert calculate_limits(10.50) == (11.55, 9.45)


def test_downward_tick_uses_the_lower_price_band():
    symbols = load_app_symbols("get_tick_size", "move_tick")
    assert symbols["move_tick"](10.0, -1) == 9.99


def test_rsi_for_uninterrupted_rise_is_100():
    symbols = load_app_symbols("calculate_market_temperature")
    close = np.arange(100.0, 130.0)
    frame = pd.DataFrame({"High": close + 1, "Low": close - 1, "Close": close})
    result = symbols["calculate_market_temperature"](frame)
    assert result["rsi"] == 100.0


def test_futures_ticks_follow_product_specification():
    symbols = load_app_symbols("_safe_number", "FUTURES_FIXED_TICK_SIZES", "get_futures_tick_size")
    get_tick = symbols["get_futures_tick_size"]
    assert get_tick("TE", 2200) == 0.05
    assert get_tick("TF", 1200) == 0.2
    assert get_tick("2330", 1000, "股票") == 1.0
    assert get_tick("0050", 50, "ETF") == 0.05


def test_futures_limit_color_requires_actual_equality():
    symbols = load_app_symbols("_safe_number", "futures_limit_state")
    state = symbols["futures_limit_state"]
    assert state("101", 100, 90) == ""
    assert state("100", 100, 90) == "up"
    assert state("90", 100, 90) == "down"


def test_small_futures_detection_handles_short_stock_future_names():
    symbols = load_app_symbols("is_small_futures_product")
    is_small = symbols["is_small_futures_product"]
    assert is_small("小台積電期貨", "CDF", "股票")
    assert is_small("微型臺指期貨", "TMF", "指數")
    assert is_small("小型台指期貨", "MTX", "指數")
    # Generic commodity names are not classified by the stock/ETF shorthand.
    assert not is_small("小麥期貨", "WTF", "未知")
    assert not is_small("台積電期貨", "CDF", "股票")


def test_futures_intraday_levels_are_clamped_to_daily_limits():
    symbols = load_app_symbols(
        "_safe_number", "round_futures_price", "clamp_futures_intraday_levels",
    )
    clamp = symbols["clamp_futures_intraday_levels"]
    assert clamp(101, 80, 130, 120, 90, 1) == (101.0, 90.0, 120.0)
    assert clamp(101, 80, 130, None, None, 1) == (101, 80, 130)


def test_futures_official_snapshot_keeps_last_known_good_values_for_partial_same_day():
    symbols = load_app_symbols(
        "_safe_number", "merge_futures_official_snapshot",
    )
    merge = symbols["merge_futures_official_snapshot"]
    previous = pd.DataFrame([{
        "契約鍵": "CDF:202609", "當日成交口數": 1234,
        "收盤價": 1050, "所需保證金": 180000, "資料日期": "20260819",
    }])
    current = pd.DataFrame([{
        "契約鍵": "CDF:202609", "當日成交口數": 0,
        "收盤價": None, "所需保證金": 0, "資料日期": "20260819",
    }])
    merged, meta = merge(
        current, previous, {"updated": "20260819", "margin_date": "20260819"},
        {"updated": "20260819", "margin_date": "20260819"},
    )
    row = merged.iloc[0]
    assert row["當日成交口數"] == 1234
    assert row["收盤價"] == 1050
    assert row["所需保證金"] == 180000
    assert meta["last_known_good"] is True


def test_new_market_day_does_not_clear_last_good_margin_when_margin_feed_is_partial():
    symbols = load_app_symbols("_safe_number", "merge_futures_official_snapshot")
    previous = pd.DataFrame([{
        "契約鍵": "CDF:202609", "當日成交口數": 1234,
        "收盤價": 1050, "所需保證金": 180000, "維持保證金": 138000,
    }])
    current = pd.DataFrame([{
        "契約鍵": "CDF:202609", "當日成交口數": 1500,
        "收盤價": 1060, "所需保證金": None, "維持保證金": 0,
    }])
    merged, meta = symbols["merge_futures_official_snapshot"](
        current, previous,
        {"updated": "20260820", "margin_date": "20260820"},
        {"updated": "20260819", "margin_date": "20260819"},
    )
    row = merged.iloc[0]
    assert row["收盤價"] == 1060
    assert row["所需保證金"] == 180000
    assert row["維持保證金"] == 138000
    assert meta["margin_date"] == "20260819"


def test_stock_limit_color_requires_actual_equality_not_stale_status():
    symbols = load_app_symbols("_safe_number", "get_tick_size", "stock_limit_state")
    state = symbols["stock_limit_state"]
    assert state("100", 100.5, 90) == ""
    assert state("100.5", 100.5, 90) == "up"
    assert state("90", 100.5, 90) == "down"


def test_restored_stock_rows_use_fresh_values_and_mark_failed_rows_stale():
    symbols = load_app_symbols("_stale_stock_identity_row", "_merge_refreshed_stock_rows")
    merge = symbols["_merge_refreshed_stock_rows"]
    cached = pd.DataFrame([
        {"代號": "2330", "名稱": "台積電", "收盤價": 100, "_source": "upload", "_order": 0, "_source_rank": 1},
        {"代號": "2408", "名稱": "南亞科", "收盤價": 50, "_source": "search", "_order": 1, "_source_rank": 2},
    ])
    refreshed, count = merge(cached, [
        ("2330", {"代號": "2330", "名稱": "台積電", "收盤價": 110, "_data_as_of": "2026/08/12"}),
        ("2408", None),
    ])
    assert count == 1
    assert refreshed.loc[0, "收盤價"] == 110
    assert refreshed.loc[0, "_source"] == "upload"
    assert not bool(refreshed.loc[0, "_data_stale"])
    assert pd.isna(refreshed.loc[1, "收盤價"])
    assert refreshed.loc[1, "代號"] == "2408"
    assert bool(refreshed.loc[1, "_data_stale"])


def test_persisted_stock_refresh_fetches_every_cached_symbol():
    symbols = load_app_symbols(
        "ANALYSIS_MAX_WORKERS", "API_REQUEST_GAP_SECONDS",
        "_stale_stock_identity_row", "_merge_refreshed_stock_rows",
        "refresh_persisted_stock_rows",
    )
    fetched_codes = []

    def fake_fetch(code, name, extra, futures, notes, names, logged_in, api):
        fetched_codes.append(code)
        return {"代號": code, "名稱": name, "收盤價": 200 + len(fetched_codes)}

    symbols["fetch_stock_data_raw"] = fake_fetch
    symbols["API_REQUEST_GAP_SECONDS"] = 0
    cached = pd.DataFrame([
        {"代號": "2330", "名稱": "台積電", "收盤價": 100},
        {"代號": "2408", "名稱": "南亞科", "收盤價": 50},
    ])
    refreshed, count = symbols["refresh_persisted_stock_rows"](
        cached, {}, {}, {}, False, None,
    )
    assert sorted(fetched_codes) == ["2330", "2408"]
    assert count == 2
    assert set(refreshed["收盤價"]) == {201, 202}


def test_futures_day_and_night_sessions_display_in_the_table_cell():
    symbols = load_app_symbols(
        "TAIFEX_NIGHT_SESSION_ROOTS", "is_futures_night_session_product",
        "get_futures_session_label",
    )
    label = symbols["get_futures_session_label"]
    assert label(["一般交易時段", "盤後交易時段"]) == "日盤+夜盤"
    assert label(["一般交易時段"]) == "日盤"
    assert label(["一般交易時段"], root="QF") == "日盤+夜盤"
    assert label(["一般"], root="QFF") == "日盤+夜盤"


def test_monthly_revenue_prefers_latest_official_month_for_each_company():
    symbols = load_app_symbols("select_latest_monthly_revenue_rows")
    rows = symbols["select_latest_monthly_revenue_rows"]([
        {"公司代號": "2408", "資料年月": "11506", "營業收入-當月營收": "100"},
        {"公司代號": "2408", "資料年月": "11507", "營業收入-當月營收": "110"},
        {"公司代號": "2330", "資料年月": "11507", "營業收入-當月營收": "120"},
    ])
    assert rows["2408"]["資料年月"] == "11507"
    assert rows["2408"]["營業收入-當月營收"] == "110"


def test_finmind_monthly_revenue_fallback_converts_july_data():
    symbols = load_app_symbols(
        "_to_number", "_roc_month_text", "build_finmind_monthly_revenue_row",
    )
    row = symbols["build_finmind_monthly_revenue_row"]([
        {"date": "2025-08-01", "stock_id": "2408", "revenue": 100_000_000,
         "revenue_month": 7, "revenue_year": 2025, "create_time": "2025-08-05"},
        {"date": "2026-07-01", "stock_id": "2408", "revenue": 120_000_000,
         "revenue_month": 6, "revenue_year": 2026, "create_time": "2026-07-03"},
        {"date": "2026-08-01", "stock_id": "2408", "revenue": 150_000_000,
         "revenue_month": 7, "revenue_year": 2026, "create_time": "2026-08-05"},
    ], "2408", 2026, 7, "南亞科")
    assert row["資料年月"] == "11507"
    assert row["營業收入-當月營收"] == 150_000
    assert row["營業收入-上月比較增減(%)"] == 25
    assert row["營業收入-去年同月增減(%)"] == 50
    assert row["_report_date"] == "2026-08-05"

    fallback = symbols["build_finmind_monthly_revenue_row"]([
        {"date": "2026-07-01", "stock_id": "2408", "revenue": 120_000_000,
         "revenue_month": 6, "revenue_year": 2026, "create_time": "2026-07-03"},
    ], "2408", 2026, 7, "南亞科")
    assert fallback["資料年月"] == "11506"


def test_goodinfo_table_requires_real_turnover_rows():
    symbols = load_app_symbols("_goodinfo_normalize_text", "_clean_goodinfo_table")
    clean = symbols["_clean_goodinfo_table"]
    valid = pd.DataFrame({
        "股票代號": [f"{2300 + index}" for index in range(12)],
        "名稱": [f"股票{index}" for index in range(12)],
        "週轉率(%)": [f"{index + 1}.2%" for index in range(12)],
    })
    assert clean(valid) is not None
    invalid = valid.assign(**{"週轉率(%)": ["--"] * len(valid)})
    assert clean(invalid) is None


def test_goodinfo_parser_uses_only_verified_ranking_table_html():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text", "_clean_goodinfo_table", "_parse_goodinfo_table_html"
    )
    row_html = ''.join(
        f"<tr><td>{2300 + index}</td><td>股票{index}</td><td>{index + 1}.2%</td></tr>"
        for index in range(12)
    )
    table_html = (
        "<table><thead><tr><th>股票代號</th><th>名稱</th><th>週轉率(%)</th></tr></thead>"
        f"<tbody>{row_html}</tbody></table>"
    )
    whole_page = (
        "<html><body><table><tr><td>廣告</td></tr></table>"
        f"{table_html}</body></html>"
    )
    parsed = symbols["_parse_goodinfo_table_html"]([whole_page])
    assert parsed is not None
    assert len(parsed) == 12


def test_goodinfo_parser_promotes_td_header_after_title_row():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text", "_clean_goodinfo_table", "_parse_goodinfo_table_html"
    )
    rows = ''.join(
        f"<tr><td>{2400 + index}</td><td>股票{index}</td><td>{index + 2}.5%</td></tr>"
        for index in range(12)
    )
    markup = (
        "<table><tr><td colspan='3'>累計成交量週轉率（當日）</td></tr>"
        "<tr><td>股票代號</td><td>名稱</td><td>週轉率(%)</td></tr>"
        f"{rows}</table>"
    )
    parsed = symbols["_parse_goodinfo_table_html"]([markup])
    assert parsed is not None
    assert len(parsed) == 12
    assert "週轉率(%)" in parsed.columns


def test_goodinfo_legacy_parser_restores_original_large_table_flow():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text", "_parse_goodinfo_legacy_page",
    )
    rows = ''.join(
        f"<tr><td>{index + 1}</td><td>{2400 + index}</td><td>股票{index}</td>"
        f"<td>{100 + index}</td><td>{1000 + index}</td><td>{index + 1}.2%</td></tr>"
        for index in range(12)
    )
    markup = (
        "<html><body><div>累計成交量週轉率（當日）</div>"
        "<table><tr><th>排名</th><th>股票代號</th><th>名稱</th><th>成交價</th>"
        "<th>成交張數</th><th>週轉率</th></tr>"
        f"{rows}</table></body></html>"
    )
    parsed = symbols["_parse_goodinfo_legacy_page"](markup)
    assert parsed is not None
    assert len(parsed) == 12
    assert "股票代號" in parsed.columns


def test_goodinfo_legacy_parser_prefers_completed_rank_table_over_menu_table():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text", "_parse_goodinfo_legacy_page",
    )
    rows = ''.join(
        f"<tr><td>{index + 1}</td><td>{3000 + index}</td><td>標的{index}</td>"
        f"<td>{index + 2}.4%</td><td>{1000 + index}</td><td>上市</td></tr>"
        for index in range(12)
    )
    markup = (
        "<html><body><table><tr><td>選單</td><td>累計成交量週轉率</td></tr></table>"
        "<table><tr><th>排名</th><th>股票代號</th><th>名稱</th><th>週轉率</th>"
        "<th>成交張數</th><th>市場</th></tr>"
        f"{rows}</table></body></html>"
    )
    parsed = symbols["_parse_goodinfo_legacy_page"](markup)
    assert parsed is not None
    assert len(parsed) == 12
    assert parsed.iloc[0]["股票代號"] == 3000


def test_goodinfo_original_parser_keeps_historical_largest_table_behavior():
    parser = load_app_symbols("_parse_goodinfo_original_page")[
        "_parse_goodinfo_original_page"
    ]
    small_rows = "".join(
        f"<tr><td>{index}</td><td>選單{index}</td><td>A</td><td>B</td><td>C</td><td>D</td></tr>"
        for index in range(11)
    )
    ranking_rows = "".join(
        f"<tr><td>{index + 1}</td><td>{2400 + index}</td><td>股票{index}</td>"
        f"<td>{100 + index}</td><td>{1000 + index}</td><td>{index + 1}.2%</td>"
        f"<td>上市</td></tr>"
        for index in range(15)
    )
    markup = (
        "<html><body>"
        "<table><tr><th>甲</th><th>乙</th><th>丙</th><th>丁</th><th>戊</th><th>己</th></tr>"
        f"{small_rows}</table>"
        "<table><tr><th>排名</th><th>代號欄</th><th>名稱欄</th><th>成交價</th>"
        "<th>成交量</th><th>百分比</th><th>市場</th></tr>"
        f"{ranking_rows}</table></body></html>"
    )
    parsed = parser(markup)
    assert parsed is not None
    assert len(parsed) == 15
    assert parsed.iloc[0]["代號欄"] == 2400


def test_goodinfo_turnover_parser_restores_legacy_completed_table_check():
    parser = load_app_symbols("_parse_goodinfo_turnover_table")[
        "_parse_goodinfo_turnover_table"
    ]
    rows = "".join(
        f"<tr><td>{index + 1}</td><td>{2400 + index}</td><td>股票{index}</td>"
        f"<td>{100 + index}</td><td>{1000 + index}</td><td>{index + 1}.2%</td></tr>"
        for index in range(12)
    )
    markup = (
        "<html><body><table><tr><th>排名</th><th>股票代號</th><th>名稱</th>"
        "<th>成交價</th><th>成交量</th><th>週轉率</th></tr>"
        f"{rows}</table></body></html>"
    )
    parsed = parser(markup)
    assert parsed is not None
    assert len(parsed) == 12
    assert parser("<html><body>驗證中，請稍候</body></html>") is None


def test_goodinfo_turnover_parser_recovers_mojibake_code_and_name_columns():
    parser = load_app_symbols("_parse_goodinfo_turnover_table")[
        "_parse_goodinfo_turnover_table"
    ]
    rows = "".join(
        f"<tr><td>{index + 1}</td><td>{3000 + index}</td><td>�Ѳ�{index}</td>"
        f"<td>{50 + index}</td><td>{500 + index}</td><td>{index + 2}.1%</td></tr>"
        for index in range(12)
    )
    markup = (
        "<table><tr><th>�ƦW</th><th>�N��</th><th>�W��</th>"
        "<th>����</th><th>����q</th><th>�g��v</th></tr>"
        f"{rows}</table>"
    )
    parsed = parser(markup)
    assert parsed is not None
    assert len(parsed) == 12
    assert parsed["代號"].astype(str).iloc[0] == "3000"
    assert parsed["名稱"].eq("").all()


def test_goodinfo_fetch_keeps_legacy_cookie_and_antiblock_flow():
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_goodinfo_data"
    )
    calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)]
    called_attributes = {
        node.func.attr for node in calls if isinstance(node.func, ast.Attribute)
    }
    function_source = ast.get_source_segment(
        APP_PATH.read_text(encoding="utf-8"), function,
    )
    chrome_constructions = [
        node for node in calls
        if isinstance(node.func, ast.Attribute) and node.func.attr == "Chrome"
    ]
    assert len(chrome_constructions) == 1
    assert 'page_load_strategy = "none"' in function_source
    assert '--disable-extensions' in function_source
    assert 'profile.managed_default_content_settings.images' in function_source
    assert 'AutomationControlled' in function_source
    assert 'RETRY_TS' in function_source
    assert 'time.monotonic' in function_source
    assert 'find_elements' in called_attributes
    assert '_parse_goodinfo_turnover_table' in function_source
    assert "refresh" not in called_attributes


def test_shioaji_futures_resolver_uses_v17_lazy_root_api():
    symbols = load_app_symbols(
        "SHIOAJI_FUTURES_ROOT_ALIASES", "FUTURES_MONTH_CODE",
        "shioaji_futures_root_candidates", "expected_shioaji_futures_code",
        "_contract_delivery_month", "_is_actual_futures_contract",
        "resolve_shioaji_futures_contract",
    )

    class Contract:
        def __init__(self, code, root, month):
            self.code = code
            self.root = root
            self.delivery_month = month
            self.delivery_date = date(2026, 9, 16)

    class LazyContracts:
        def __init__(self):
            self.calls = []

        def futures(self, root, delivery_month=None):
            self.calls.append((root, delivery_month))
            if root == "MXF" and delivery_month == "202609":
                return [Contract("MXFI6", "MXF", "202609")]
            return []

    class API:
        def __init__(self):
            self.contracts = LazyContracts()

    api = API()
    contract = symbols["resolve_shioaji_futures_contract"](api, "MTX", "202609")
    assert contract.code == "MXFI6"
    assert api.contracts.calls[0] == ("MXF", "202609")
    assert symbols["expected_shioaji_futures_code"]("MTX", "202608") == "MXFH6"
    assert symbols["expected_shioaji_futures_code"]("CDF", "202609") == "CDFI6"


def test_futures_settlement_cutover_uses_taipei_1330_and_official_date_first():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "adjust_to_next_market_day",
        "futures_expiry_date", "FUTURES_STANDARD_SETTLEMENT_TYPES",
        "FUTURES_STANDARD_SETTLEMENT_ROOTS", "FUTURES_SETTLEMENT_CUTOFF",
        "_normalize_futures_taipei_datetime", "is_futures_contract_settled",
    )
    is_settled = symbols["is_futures_contract_settled"]
    tz_tw = pytz.timezone("Asia/Taipei")
    before = tz_tw.localize(datetime(2026, 8, 19, 13, 29))
    after = tz_tw.localize(datetime(2026, 8, 19, 13, 30))
    assert not is_settled("CDF", "202608", "股票", now_dt=before)
    assert is_settled("CDF", "202608", "股票", now_dt=after)
    assert not is_settled("CDF", "202609", "股票", now_dt=after)

    # 非第三週三商品不套錯誤的月契約估算；若官方提供最後交易日，則優先採用。
    assert not is_settled("ABC", "202608", "商品", now_dt=after)
    assert is_settled(
        "ABC", "202608", "商品", actual_last_trading="2026/08/19",
        now_dt=after,
    )
    # 官方只給日期或 midnight 時，結算切點仍是 13:30，不可在午夜提前刪除。
    assert not is_settled(
        "ABC", "202608", "商品", actual_last_trading="2026-08-19",
        now_dt=tz_tw.localize(datetime(2026, 8, 19, 13, 29)),
    )
    assert is_settled(
        "ABC", "202608", "商品", actual_last_trading="2026-08-19 00:00:00",
        now_dt=after,
    )
    assert is_settled(
        "ABC", "202608", "商品", actual_last_trading="115/08/19",
        now_dt=after,
    )


def test_filter_active_futures_rows_restarts_near_month_after_settlement():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "adjust_to_next_market_day",
        "futures_expiry_date", "FUTURES_STANDARD_SETTLEMENT_TYPES",
        "FUTURES_STANDARD_SETTLEMENT_ROOTS", "FUTURES_SETTLEMENT_CUTOFF",
        "_normalize_futures_taipei_datetime", "is_futures_contract_settled",
        "filter_active_futures_rows",
    )
    rows = pd.DataFrame([
        {"契約鍵": "CDF:202608", "期貨代碼": "CDF", "契約月份": "202608", "商品類型": "股票"},
        {"契約鍵": "CDF:202609", "期貨代碼": "CDF", "契約月份": "202609", "商品類型": "股票"},
        {"契約鍵": "TX:202608", "期貨代碼": "TX", "契約月份": "202608", "商品類型": "指數", "指數期貨": True},
        {"契約鍵": "TX:202609", "期貨代碼": "TX", "契約月份": "202609", "商品類型": "指數", "指數期貨": True},
    ])
    tz_tw = pytz.timezone("Asia/Taipei")
    active, removed = symbols["filter_active_futures_rows"](
        rows, tz_tw.localize(datetime(2026, 8, 19, 13, 30)),
    )
    assert set(removed) == {"CDF:202608", "TX:202608"}
    assert set(active["契約鍵"]) == {"CDF:202609", "TX:202609"}
    assert set(active["月份順位"]) == {0}
    assert not active["次月期貨"].any()


def test_prune_futures_settlement_state_removes_old_rank_and_live_keys():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "adjust_to_next_market_day",
        "futures_expiry_date", "FUTURES_STANDARD_SETTLEMENT_TYPES",
        "FUTURES_STANDARD_SETTLEMENT_ROOTS", "FUTURES_SETTLEMENT_CUTOFF",
        "_normalize_futures_taipei_datetime", "is_futures_contract_settled",
        "filter_active_futures_rows", "prune_futures_settlement_state",
    )
    state = {
        "universe": [
            {"契約鍵": "CDF:202608", "期貨代碼": "CDF", "契約月份": "202608", "商品類型": "股票"},
            {"契約鍵": "CDF:202609", "期貨代碼": "CDF", "契約月份": "202609", "商品類型": "股票"},
        ],
        "rank_cache": {"CDF:202608": {"當日成交口數": 99}, "CDF:202609": {"當日成交口數": 10}},
        "live_cache": {"CDF:202608": {"收盤價": 99}, "CDF:202609": {"收盤價": 10}},
        "manual": ["CDF:202608", "CDF:202609"],
        "ignored": ["CDF:202608"],
    }
    tz_tw = pytz.timezone("Asia/Taipei")
    pruned, removed = symbols["prune_futures_settlement_state"](
        state, tz_tw.localize(datetime(2026, 8, 19, 13, 30)),
    )
    assert removed == ["CDF:202608"]
    assert "CDF:202608" not in pruned["rank_cache"]
    assert "CDF:202608" not in pruned["live_cache"]
    assert pruned["manual"] == ["CDF:202609"]
    assert pruned["ignored"] == []


def test_only_small_stock_futures_excludes_index_and_etf_products():
    predicate = load_app_symbols("is_small_stock_futures_record")["is_small_stock_futures_record"]
    assert predicate({"商品類型": "股票", "小型期貨": True, "指數期貨": False, "ETF期貨": False})
    assert not predicate({"商品類型": "指數", "小型期貨": True, "指數期貨": True, "ETF期貨": False})
    assert not predicate({"商品類型": "ETF", "小型期貨": True, "指數期貨": False, "ETF期貨": True})
    assert not predicate({"商品類型": "股票", "小型期貨": False, "指數期貨": False, "ETF期貨": False})


def test_directional_option_quality_rejects_negative_or_unreachable_trades():
    evaluate = load_app_symbols("evaluate_txo_directional_quality")["evaluate_txo_directional_quality"]
    good = evaluate(100, 3500, -2000, 0.48, 8, "中", True)
    assert good["trade_ready"]
    assert good["payoff_ratio"] >= 1.25

    # 放寬版只讓仍有正目標情境、可達損益兩平且報酬／風險尚可的
    # 候選進入小部位觀察；不會把負損益候選標成可交易。
    small = evaluate(100, 2000, -2000, 0.35, 19, "中", True)
    assert not small["trade_ready"]
    assert small["small_position_ready"]

    poor = evaluate(100, -500, -2000, 0.32, 22, "低", False)
    assert not poor["trade_ready"]
    assert not poor["small_position_ready"]
    assert "目標情境仍未獲利" in poor["quality_notes"]
    assert "買賣價差過寬" in poor["quality_notes"]


def test_option_scenario_color_follows_actual_profit_sign():
    color = load_app_symbols("_txo_scenario_color")["_txo_scenario_color"]
    assert color(100) == "#ff4b4b"
    assert color(-100) == "#00c853"
    assert color(0) == "#cbd5e1"


def test_stock_limit_context_switches_display_reference_at_1430():
    symbols = load_app_symbols(
        "_safe_number", "get_tick_size", "calculate_limits", "stock_limit_context"
    )
    context = symbols["stock_limit_context"]
    before = context(100, 105, datetime(2026, 8, 20, 14, 29, tzinfo=pytz.timezone("Asia/Taipei")))
    after = context(100, 105, datetime(2026, 8, 20, 14, 30, tzinfo=pytz.timezone("Asia/Taipei")))
    assert (before["today_up"], before["today_down"]) == (110.0, 90.0)
    assert (before["display_up"], before["display_down"]) == (110.0, 90.0)
    assert (after["today_up"], after["today_down"]) == (110.0, 90.0)
    assert (after["display_up"], after["display_down"]) == (115.5, 94.5)


def test_stock_direction_basis_is_compact_but_keeps_key_signals():
    symbols = load_app_symbols("_as_float", "determine_stock_direction")
    result = symbols["determine_stock_direction"]({
        "收盤價": 100, "漲跌幅": 1, "_ma5": 95, "_risk_ma20": 90,
        "_risk_ma20_slope": 1, "_risk_prev_high": 105, "_risk_prev_low": 85,
        "_risk_close_position": 70,
    })
    assert result["label"] == "🔴 建議多"
    assert result["basis"] == "日K｜日 K 站上 5 日線、日 K 站上 20 日線"
    assert "多 " not in result["basis"]


def test_fibo_quick_tag_resolves_nanya_technology_code():
    symbols = load_app_symbols("normalize_fibo_quick_tag")
    normalize = symbols["normalize_fibo_quick_tag"]
    code_map = {"2408": "南亞科"}
    name_map = {"南亞科": "2408"}
    assert normalize("南亞科", code_map, name_map) == "南亞科(2408)"
    assert normalize("南亞科（2408.TW）", code_map, name_map) == "南亞科(2408)"


def test_sinopac_list_parser_reads_current_direct_pdf_layout():
    symbols = load_app_symbols("parse_sinopac_report_list")
    reports = symbols["parse_sinopac_report_list"](
        """
        <div class="research-list"><ul><li>
          <a href="/upload/sinopac/researchContent/latest.pdf">台指期籌碼快訊</a>
          <span>2026/08/11</span>
        </li></ul></div>
        """,
        "https://www.spf.com.tw/sinopacSPF/research/list.do?id=test",
    )
    assert reports == [{
        "日期": "2026-08-11",
        "title": "台指期籌碼快訊",
        "url": "https://www.spf.com.tw/upload/sinopac/researchContent/latest.pdf",
    }]


def test_sinopac_text_proxy_parser_keeps_official_pdf_link():
    symbols = load_app_symbols("parse_sinopac_report_markdown")
    reports = symbols["parse_sinopac_report_markdown"](
        "[台指期籌碼快訊](https://www.spf.com.tw/upload/sinopac/researchContent/latest.pdf) 2026/08/11"
    )
    assert reports == [{
        "日期": "2026-08-11",
        "title": "台指期籌碼快訊",
        "url": "https://www.spf.com.tw/upload/sinopac/researchContent/latest.pdf",
    }]


def test_legacy_company_events_are_restored_to_calendar_sections():
    symbols = load_app_symbols("empty_company_event_snapshot", "normalize_company_event_snapshot")
    snapshot = symbols["normalize_company_event_snapshot"]({
        "updated_at": "2026/08/11 12:00",
        "tickers": "2408",
        "events": [{
            "date": "2026-08-11", "title": "南亞科 月營收 MOM+1%／YOY+2%",
            "source": "TWSE OpenAPI（MOPS 每月營收）", "ticker": "2408",
        }],
    })
    assert len(snapshot["taiwan_revenue"]["events"]) == 1
    assert snapshot["taiwan_revenue"]["events"][0]["market"] == "台股"
    assert len(snapshot["events"]) == 1


def test_revenue_announcement_date_override_persists_in_snapshot():
    symbols = load_app_symbols(
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "apply_revenue_announcement_date_overrides",
    )
    snapshot = {
        "taiwan_revenue": {"events": [{
            "date": "2026-08-05", "title": "南亞科 7月營收",
            "ticker": "2408", "market": "台股", "source": "MOPS 單一公司月營收",
            "detail": "MOPS 單一公司資料（公告日未提供，顯示系統偵測日）；",
            "revenue": {"company": "南亞科", "revenue_month": "11507"},
        }]},
    }
    corrected = symbols["apply_revenue_announcement_date_overrides"](
        snapshot, {"2408:11507": "2026-08-04"}
    )
    event = corrected["taiwan_revenue"]["events"][0]
    assert event["date"] == "2026-08-04"
    assert event["revenue"]["report_date"] == "2026-08-04"
    assert corrected["revenue_date_overrides"] == {"2408:11507": "2026-08-04"}


def test_public_revenue_report_date_prefers_earliest_matching_release():
    symbols = load_app_symbols("_to_number", "select_cnyes_revenue_announcement_date")
    select_date = symbols["select_cnyes_revenue_announcement_date"]
    assert select_date([
        {"title": "營收速報 - 南亞科(2408)7月營收", "publishAt": 1785802800},  # 2026-08-04 18:20 Taipei
        {"title": "南亞科(2408) 7月營收續強", "publishAt": 1785859200},
        {"title": "南亞科(2408)6月營收", "publishAt": 1785802800},
        {"title": "其他公司(1234)7月營收", "publishAt": 1785802800},
    ], "2408", 2026, 7) == "2026-08-04"


def test_cnyes_current_items_data_envelope_is_parsed():
    extract = load_app_symbols("extract_cnyes_search_items")["extract_cnyes_search_items"]
    rows = [{
        "title": "DRAM漲不停！南亞科7月營收再創高",
        "content": "南亞科 (2408-TW) 今 (4) 日公告 7 月營收",
        "publishAt": 1785800804,
    }]
    assert extract({"items": {"data": rows, "total": 1}}) == rows
    assert extract({"data": {"items": rows}}) == rows


def test_google_news_revenue_date_uses_earliest_matching_company_report():
    symbols = load_app_symbols("select_google_news_revenue_announcement_date")
    rss = """<?xml version='1.0' encoding='UTF-8'?><rss><channel>
      <item><title>南亞科(2408)市場新聞</title>
        <pubDate>Mon, 03 Aug 2026 07:00:00 GMT</pubDate></item>
      <item><title>南亞科(2408)7月營收公告</title>
        <pubDate>Tue, 04 Aug 2026 07:40:03 GMT</pubDate></item>
      <item><title>南亞科(2408)7月營收後續</title>
        <pubDate>Wed, 05 Aug 2026 08:20:00 GMT</pubDate></item>
    </channel></rss>""".encode()
    assert symbols["select_google_news_revenue_announcement_date"](
        rss, "2408", "南亞科", 2026, 7
    ) == "2026-08-04"


def test_finmind_revenue_date_is_primary_and_news_sources_are_fallbacks():
    choose = load_app_symbols(
        "choose_revenue_announcement_date"
    )["choose_revenue_announcement_date"]
    assert choose("2026-08-10", "2026-08-03", "2026-08-03", 2026, 7) == (
        "2026-08-10", "FinMind 收錄日",
    )
    assert choose(None, "2026-08-04", "2026-08-03", 2026, 7) == (
        "2026-08-04", "鉅亨網直接營收報導",
    )
    assert choose(None, None, "2026-08-04", 2026, 7) == (
        "2026-08-04", "Google News 營收報導",
    )
    assert choose("2026-08-20", "2026-08-10", None, 2026, 7) == (
        "2026-08-10", "鉅亨網直接營收報導",
    )


def test_saved_manual_revenue_date_remains_authoritative_after_sync():
    symbols = load_app_symbols(
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "apply_revenue_announcement_date_overrides",
    )
    snapshot = {
        "revenue_date_overrides": {"2408:11507": "2026-08-04"},
        "taiwan_revenue": {"events": [{
            "date": "2026-08-05", "title": "南亞科 7月營收", "ticker": "2408",
            "revenue": {
                "company": "南亞科", "revenue_month": "11507",
                "date_source": "月營收值採 MOPS；公告日採 FinMind 收錄日",
            },
        }]},
    }
    corrected = symbols["apply_revenue_announcement_date_overrides"](snapshot)
    assert corrected["taiwan_revenue"]["events"][0]["date"] == "2026-08-04"
    assert corrected["revenue_date_overrides"]["2408:11507"] == "2026-08-04"


def test_finmind_revenue_date_is_unchanged_without_manual_override():
    symbols = load_app_symbols(
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "apply_revenue_announcement_date_overrides",
    )
    snapshot = {
        "taiwan_revenue": {"events": [{
            "date": "2026-08-10", "title": "台積電 7月營收", "ticker": "2330",
            "revenue": {
                "company": "台積電", "revenue_month": "11507",
                "date_source": "月營收值採 MOPS；公告日採 FinMind 收錄日",
            },
        }]},
    }
    corrected = symbols["apply_revenue_announcement_date_overrides"](snapshot)
    assert corrected["taiwan_revenue"]["events"][0]["date"] == "2026-08-10"
    assert corrected["revenue_date_overrides"] == {}


def test_explicit_user_revenue_date_correction_is_not_discarded():
    symbols = load_app_symbols(
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "apply_revenue_announcement_date_overrides",
    )
    snapshot = {
        "taiwan_revenue": {"events": [{
            "date": "2026-08-03", "title": "南亞科 7月營收", "ticker": "2408",
            "revenue": {
                "company": "南亞科", "revenue_month": "11507",
                "date_source": "多來源公開營收報導日期（採 Google News 營收報導）",
            },
        }]},
    }
    corrected = symbols["apply_revenue_announcement_date_overrides"](
        snapshot, {"2408:11507": "2026-08-04"},
    )
    assert corrected["taiwan_revenue"]["events"][0]["date"] == "2026-08-04"
    assert corrected["revenue_date_overrides"]["2408:11507"] == "2026-08-04"


def test_calendar_date_only_value_does_not_shift_to_previous_day():
    parse_date = load_app_symbols("parse_calendar_event_date")["parse_calendar_event_date"]
    assert parse_date("2026-08-04") == date(2026, 8, 4)
    assert parse_date("2026-08-03T17:00:00Z") == date(2026, 8, 4)


def test_fibonacci_initial_view_is_core_zero_to_one_range():
    view = load_app_symbols("fibonacci_initial_y_range")["fibonacci_initial_y_range"]
    assert view(100, 200) == (95, 205)
    assert view(100, 100) == (None, None)


def test_nested_google_sheet_payload_restores_fibo_tags():
    symbols = load_app_symbols(
        "_is_valid_data_cache_payload", "_decode_data_cache_payload",
        "_valid_fibo_tags", "_extract_fibo_tags",
    )
    tags = ["南亞科(2408)", "台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)"]
    payload = {
        "success": True,
        "stock_data": [],
        "data": json.dumps({"fibo_tags": tags}, ensure_ascii=False),
    }
    decoded = symbols["_decode_data_cache_payload"](payload)
    assert symbols["_extract_fibo_tags"](decoded) == tags
    legacy = symbols["_decode_data_cache_payload"]({"tags": tags})
    assert symbols["_extract_fibo_tags"](legacy) == tags
    backup = {"fibo_tags": [], "fibo_tags_backup": {"tags": tags}}
    assert symbols["_extract_fibo_tags"](backup) == tags


def test_newer_fibo_backup_wins_over_valid_but_stale_primary_tags():
    symbols = load_app_symbols("_valid_fibo_tags", "_extract_fibo_tags")
    old_tags = ["A", "B", "C", "D", "E"]
    new_tags = ["南亞科(2408)", "台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)"]
    payload = {
        "fibo_tags": old_tags,
        "fibo_tags_updated_at": "2026-08-20T09:00:00+08:00",
        "fibo_tags_backup": {
            "tags": new_tags,
            "updated_at": "2026-08-20T09:05:00+08:00",
        },
    }
    assert symbols["_extract_fibo_tags"](payload) == new_tags


def test_fibo_scope_readback_must_match_exact_saved_tags():
    symbols = load_app_symbols(
        "_valid_fibo_tags", "_extract_fibo_tags", "_json_safe",
        "_remote_scope_payload_matches",
    )
    symbols["GOOGLE_SCOPE_FIBO"] = "fibo_strategy"
    symbols["normalize_fibo_quick_tag"] = lambda value: str(value).strip()
    expected = {
        "fibo_tags": ["南亞科(2408)", "台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)"],
        "fibo_tags_updated_at": "2026-08-20T10:00:00+08:00",
    }
    same = dict(expected, _scope_updated_at="2026-08-20T10:00:00+08:00")
    stale = dict(expected, fibo_tags=["A", "B", "C", "D", "E"])
    matches = symbols["_remote_scope_payload_matches"]
    assert matches("fibo_strategy", expected, same)
    assert not matches("fibo_strategy", expected, stale)


def test_forced_fibo_cloud_import_never_falls_back_to_local_tags():
    symbols = load_app_symbols("load_fibo_tags_from_cloud")

    class FakeStreamlit:
        session_state = {}

    local_calls = []
    symbols.update({
        "st": FakeStreamlit(),
        "DEFAULT_FIBO_TAGS": ["預設1", "預設2", "預設3", "預設4", "預設5"],
        "GOOGLE_SCOPE_FIBO": "fibo_strategy",
        "get_app_secret": lambda key: "https://example.invalid/exec",
        "_fetch_remote_scope": lambda *args, **kwargs: (None, "Timeout"),
        "load_fibo_tag_cache": lambda: local_calls.append(True) or ["本機1", "本機2", "本機3", "本機4", "本機5"],
        "load_config": lambda: {"fibo_tags": ["設定1", "設定2", "設定3", "設定4", "設定5"]},
        "_valid_fibo_tags": lambda tags: list(tags[:5]) if isinstance(tags, list) and len(tags) >= 5 else [],
        "_extract_fibo_tags": lambda payload: [],
        "_fibo_tag_timestamp": lambda payload: None,
        "normalize_fibo_quick_tag": lambda value: str(value),
    })
    assert symbols["load_fibo_tags_from_cloud"](force=True) == []
    assert local_calls == []
    assert symbols["st"].session_state["_fibo_cloud_load_error"] == "Timeout"


def test_normal_fibo_startup_keeps_device_local_tags_before_cloud():
    symbols = load_app_symbols("load_fibo_tags_from_cloud")

    class FakeStreamlit:
        session_state = {}

    remote_calls = []
    local_tags = ["手機1", "手機2", "手機3", "手機4", "手機5"]
    symbols.update({
        "st": FakeStreamlit(),
        "DEFAULT_FIBO_TAGS": ["預設1", "預設2", "預設3", "預設4", "預設5"],
        "GOOGLE_SCOPE_FIBO": "fibo_strategy",
        "get_app_secret": lambda key: "https://example.invalid/exec",
        "_fetch_remote_scope": lambda *args, **kwargs: remote_calls.append(True) or ({}, None),
        "load_fibo_tag_cache": lambda: local_tags,
        "load_config": lambda: {},
        "_valid_fibo_tags": lambda tags: list(tags[:5]) if isinstance(tags, list) and len(tags) >= 5 else [],
        "_extract_fibo_tags": lambda payload: [],
        "_fibo_tag_timestamp": lambda payload: None,
        "normalize_fibo_quick_tag": lambda value: str(value),
    })
    assert symbols["load_fibo_tags_from_cloud"]() == local_tags
    assert remote_calls == []
    assert symbols["st"].session_state["_fibo_tags_source"] == "local_tag_cache"


def test_cache_merge_preserves_remote_device_sections():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state", "_cache_payload_timestamp",
        "_state_updated_at", "_newer_timestamped_state", "_extract_fibo_tags",
        "_fibo_tag_timestamp", "_newer_fibo_tag_payload",
        "_safe_number", "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
        "empty_company_event_snapshot",
        "normalize_company_event_snapshot", "_newer_company_event_snapshot",
        "_merge_data_cache_payload",
    )
    remote = {
        "stock_data": [{"代號": "2330", "收盤價": 100}],
        "ignored_stocks": [],
        "all_candidates": ["2330"],
        "saved_notes": {"2330": "remote"},
        "cached_notes": {},
        "fibo_tags": ["A", "B", "C", "D", "E"],
        "strategy_signal_log": [{"dedupe_key": "remote-1"}],
        "company_event_snapshot": {"updated_at": "2026/08/10 10:00", "events": [{"date": "2026-08-10"}]},
    }
    local = {
        "stock_data": [{"代號": "2317", "收盤價": 200}],
        "ignored_stocks": [],
        "all_candidates": ["2317"],
        "saved_notes": {"2317": "local"},
        "cached_notes": {},
        "fibo_tags": ["1", "2", "3", "4", "5"],
        "strategy_signal_log": [{"dedupe_key": "local-1"}],
        "company_event_snapshot": {"updated_at": "2026/08/11 10:00", "events": [{"date": "2026-08-11"}]},
    }
    merged = symbols["_merge_data_cache_payload"](remote, local)
    assert {row["代號"] for row in merged["stock_data"]} == {"2330", "2317"}
    assert merged["fibo_tags"] == remote["fibo_tags"]
    assert {row["dedupe_key"] for row in merged["strategy_signal_log"]} == {"remote-1", "local-1"}
    assert merged["company_event_snapshot"]["updated_at"] == "2026/08/11 10:00"


def test_cache_merge_can_replace_cloud_stock_snapshot_without_resurrecting_old_rows():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state", "_cache_payload_timestamp",
        "_state_updated_at", "_newer_timestamped_state", "_extract_fibo_tags",
        "_fibo_tag_timestamp", "_newer_fibo_tag_payload",
        "_safe_number", "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
        "empty_company_event_snapshot",
        "normalize_company_event_snapshot", "_newer_company_event_snapshot",
        "_merge_data_cache_payload",
    )
    remote = {
        "stock_data": [{"代號": "2330", "收盤價": 100}],
        "stock_data_updated_at": "2026-08-20T09:00:00+08:00",
        "fibo_tags": ["A", "B", "C", "D", "E"],
    }
    local = {
        "stock_data": [{"代號": "2408", "收盤價": 60}],
        "stock_data_updated_at": "2026-08-20T09:05:00+08:00",
        "fibo_tags": ["1", "2", "3", "4", "5"],
    }
    merged = symbols["_merge_data_cache_payload"](
        remote, local, replace_stock_data=True,
    )
    assert [row["代號"] for row in merged["stock_data"]] == ["2408"]
    assert merged["fibo_tags"] == remote["fibo_tags"]


def test_cache_merge_keeps_newer_remote_snapshot_when_local_session_is_stale():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state", "_cache_payload_timestamp",
        "_state_updated_at", "_newer_timestamped_state", "_extract_fibo_tags",
        "_fibo_tag_timestamp", "_newer_fibo_tag_payload",
        "_safe_number", "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "_newer_company_event_snapshot", "_merge_data_cache_payload",
    )
    remote = {
        "stock_data": [{"代號": "2330", "收盤價": 105}],
        "stock_data_updated_at": "2026-08-17T09:05:00+08:00",
        "fibo_tags": ["A", "B", "C", "D", "E"],
    }
    local = {
        "stock_data": [{"代號": "2330", "收盤價": 100}],
        "stock_data_updated_at": "2026-08-17T09:00:00+08:00",
    }
    merged = symbols["_merge_data_cache_payload"](
        remote, local, replace_stock_data=True,
    )
    assert merged["stock_data"][0]["收盤價"] == 105
    assert merged["stock_data_updated_at"] == remote["stock_data_updated_at"]


def test_newer_local_cache_snapshot_is_not_replaced_by_delayed_cloud_read():
    symbols = load_app_symbols(
        "_cache_payload_timestamp", "_prefer_newer_cache_payload",
    )
    remote = {
        "stock_data": [{"代號": "2330", "收盤價": 100}],
        "stock_data_updated_at": "2026-08-17T09:00:00+08:00",
    }
    local = {
        "stock_data": [{"代號": "2330", "收盤價": 105}],
        "stock_data_updated_at": "2026-08-17T09:05:00+08:00",
    }
    selected, source = symbols["_prefer_newer_cache_payload"](remote, local)
    assert source == "local_newer"
    assert selected["stock_data"][0]["收盤價"] == 105


def test_unversioned_local_stock_snapshot_cannot_replace_versioned_cloud_snapshot():
    symbols = load_app_symbols(
        "_cache_payload_timestamp", "_prefer_newer_cache_payload",
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state",
        "_state_updated_at", "_newer_timestamped_state", "_extract_fibo_tags",
        "_fibo_tag_timestamp", "_newer_fibo_tag_payload", "_safe_number",
        "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "_newer_company_event_snapshot", "_merge_data_cache_payload",
    )
    remote = {
        "stock_data": [{"代號": "2330", "收盤價": 105}],
        "stock_data_updated_at": "2026-08-20T09:05:00+08:00",
    }
    local = {"stock_data": [{"代號": "2330", "收盤價": 90}]}
    merged = symbols["_merge_data_cache_payload"](
        remote, local, replace_stock_data=True,
    )
    assert merged["stock_data"][0]["收盤價"] == 105


def test_stock_snapshot_verification_checks_all_saved_strategy_fields():
    symbols = load_app_symbols("_json_safe", "_stock_rows_signature", "_stock_snapshot_matches")
    expected = {
        "stock_data": [{"代號": "8043", "收盤價": 154, "_daytrade_vwap": 157.45}],
    }
    assert symbols["_stock_snapshot_matches"](expected, expected)
    stale = {
        "stock_data": [{"代號": "8043", "收盤價": 160.5, "_daytrade_vwap": 158}],
    }
    assert not symbols["_stock_snapshot_matches"](stale, expected)


def test_timestamped_fibo_save_survives_failed_cloud_write_and_keeps_recovery_copy():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state", "_cache_payload_timestamp",
        "_state_updated_at", "_newer_timestamped_state", "_fibo_tag_timestamp",
        "_newer_fibo_tag_payload", "_safe_number", "merge_futures_official_snapshot",
        "_parse_futures_state_time", "_prefer_futures_state_section",
        "_merge_futures_strategy_state", "compact_futures_strategy_state",
        "empty_company_event_snapshot",
        "normalize_company_event_snapshot", "_newer_company_event_snapshot",
        "_merge_data_cache_payload", "_extract_fibo_tags",
    )
    remote = {"stock_data": [], "fibo_tags": []}
    local = {
        "stock_data": [],
        "fibo_tags": ["南亞科(2408)", "台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)"],
        "fibo_tags_updated_at": "2026-08-13T10:00:00+08:00",
    }
    ordinary = symbols["_merge_data_cache_payload"](remote, local)
    assert ordinary["fibo_tags"] == local["fibo_tags"]
    assert ordinary["fibo_tags_backup"]["tags"] == local["fibo_tags"]
    explicit = symbols["_merge_data_cache_payload"](
        remote, local, explicit_fibo_tag_save=True,
    )
    assert explicit["fibo_tags"] == local["fibo_tags"]
    assert explicit["fibo_tags_backup"]["tags"] == local["fibo_tags"]
    damaged_primary = dict(explicit, fibo_tags=[])
    assert symbols["_extract_fibo_tags"](damaged_primary) == local["fibo_tags"]


def test_untimestamped_default_tags_cannot_replace_empty_cloud_section():
    symbols = load_app_symbols(
        "_valid_fibo_tags", "_extract_fibo_tags", "_fibo_tag_timestamp",
        "_newer_fibo_tag_payload",
    )
    local_defaults = {"fibo_tags": ["A", "B", "C", "D", "E"]}
    assert symbols["_newer_fibo_tag_payload"](
        {"stock_data": []}, local_defaults,
    ) == {}


def test_futures_state_uses_its_own_timestamp_during_device_merge():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "merge_strategy_signal_state", "_cache_payload_timestamp",
        "_state_updated_at", "_newer_timestamped_state", "_extract_fibo_tags",
        "_fibo_tag_timestamp", "_newer_fibo_tag_payload",
        "_safe_number", "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
        "empty_company_event_snapshot", "normalize_company_event_snapshot",
        "_newer_company_event_snapshot", "_merge_data_cache_payload",
    )
    remote = {
        "stock_data": [], "fibo_tags": ["A", "B", "C", "D", "E"],
        "futures_strategy_state": {
            "updated_at": "2026-08-20T09:05:00+08:00",
            "rank_time": "2026/08/20 09:05:00",
            "rank_cache": {"MTX": {"當日成交口數": 12345}},
            "live_time": "2026/08/20 09:00:00",
            "live_cache": {"MTX": {"收盤價": 100}},
        },
    }
    local = {
        "stock_data": [],
        "futures_strategy_state": {
            "updated_at": "2026-08-20T09:00:00+08:00",
            "rank_time": "2026/08/20 09:00:00",
            "rank_cache": {"MTX": {"當日成交口數": 100}},
            "live_time": "2026/08/20 09:06:00",
            "live_cache": {"MTX": {"收盤價": 105}},
        },
    }
    merged = symbols["_merge_data_cache_payload"](remote, local)
    assert merged["futures_strategy_state"]["rank_cache"]["MTX"]["當日成交口數"] == 12345
    assert merged["futures_strategy_state"]["live_cache"]["MTX"]["收盤價"] == 105


def test_newer_empty_futures_cache_is_a_tombstone_not_resurrected_data():
    symbols = load_app_symbols(
        "_json_safe", "_safe_number", "_state_updated_at", "_newer_timestamped_state",
        "merge_futures_official_snapshot", "_parse_futures_state_time",
        "_prefer_futures_state_section", "_merge_futures_strategy_state",
        "compact_futures_strategy_state",
    )
    remote = {
        "updated_at": "2026-08-20T09:00:00+08:00",
        "rank_updated_at": "2026-08-20T09:00:00+08:00",
        "rank_time": "2026/08/20 09:00:00",
        "rank_cache": {"MTX": {"當日成交口數": 12345}},
    }
    local = {
        "updated_at": "2026-08-20T09:05:00+08:00",
        "rank_updated_at": "2026-08-20T09:05:00+08:00",
        "rank_time": None,
        "rank_cache": {},
    }
    merged = symbols["_merge_futures_strategy_state"](remote, local)
    assert merged["rank_cache"] == {}
    assert merged["rank_time"] is None


def test_futures_cloud_snapshot_is_bounded_for_single_cell_storage():
    symbols = load_app_symbols("_json_safe", "_safe_number", "compact_futures_strategy_state")
    universe = [
        {
            "契約鍵": f"SSF:{index}", "期貨代碼": f"S{index}", "名稱": f"測試期貨{index}",
            "契約月份": "202609", "當日成交口數": 2000 - index,
            "收盤價": 100 + index, "進出場點位": "進100｜停95｜目110",
        }
        for index in range(1811)
    ]
    state = {
        "universe": universe,
        "rank_cache": {
            row["契約鍵"]: {"當日成交口數": row["當日成交口數"], "收盤價": row["收盤價"]}
            for row in universe
        },
        "live_cache": {}, "manual": ["SSF:1800"], "ignored": [],
        "updated_at": "2026-08-20T09:00:00+08:00",
    }
    compact = symbols["compact_futures_strategy_state"](state)
    assert len(json.dumps(compact, ensure_ascii=False)) <= 14000
    assert "SSF:1800" in compact["manual"]
    assert len(compact["universe"]) < len(universe)


def test_full_cloud_payload_compacts_logs_without_dropping_stock_indicators():
    symbols = load_app_symbols(
        "_json_safe", "_safe_number", "compact_futures_strategy_state",
        "_compact_cloud_data_cache_payload",
    )
    stock_rows = [{"代號": "2330", "收盤價": 100, "方向": "多頭", "信心分": 80}]
    payload = {
        "stock_data": stock_rows,
        "strategy_signal_log": [
            {"dedupe_key": f"signal-{index}", "detail": "測試訊號" * 20}
            for index in range(2000)
        ],
        "futures_strategy_state": {},
        "company_event_snapshot": {"events": []},
    }
    compact = symbols["_compact_cloud_data_cache_payload"](payload)
    assert compact["stock_data"] == stock_rows
    assert len(compact["strategy_signal_log"]) < 2000
    assert len(json.dumps(compact, ensure_ascii=False, separators=(",", ":"))) < 45100


def test_deleted_strategy_signal_wins_over_stale_cloud_copy():
    symbols = load_app_symbols(
        "_json_safe", "_merge_unique_records", "_merge_unique_values",
        "merge_strategy_signal_state",
    )
    remote = [
        {"dedupe_key": "deleted-1", "名稱": "舊訊號"},
        {"dedupe_key": "remote-2", "名稱": "遠端保留"},
    ]
    local = [{"dedupe_key": "local-3", "名稱": "本機新增"}]
    records, deleted = symbols["merge_strategy_signal_state"](
        remote, local, ["deleted-1"], [],
    )
    assert {row["dedupe_key"] for row in records} == {"remote-2", "local-3"}
    assert deleted == ["deleted-1"]


def test_daytrade_plan_rejects_limit_up_chase_with_low_reward_risk():
    symbols = load_app_symbols(
        "get_taiwan_tick_size", "round_to_tick", "get_tick_size", "move_tick",
        "_safe_number", "_as_float", "fmt_price", "build_trade_plan",
    )
    plan = symbols["build_trade_plan"](
        {
            "_daytrade_vwap": 158,
            "_daytrade_or_high": 160,
            "_daytrade_or_low": 157,
            "_daytrade_close": 160.5,
            "_daytrade_phase": "開盤區間完成",
            "當日漲停價": 162,
        },
        "多頭", True, {"eligible": True, "rule": "觸發"},
    )
    assert plan["valid"] is False
    assert plan["reward_risk"] < 1.3
    assert "不追價" in plan["summary"]
    assert "可接受進場 ≤159.5" in plan["summary"]


def test_daytrade_plan_keeps_normal_breakout_when_space_is_sufficient():
    symbols = load_app_symbols(
        "get_taiwan_tick_size", "round_to_tick", "get_tick_size", "move_tick",
        "_safe_number", "_as_float", "fmt_price", "build_trade_plan",
    )
    plan = symbols["build_trade_plan"](
        {
            "_daytrade_vwap": 158,
            "_daytrade_or_high": 160,
            "_daytrade_or_low": 157,
            "_daytrade_close": 160.5,
            "_daytrade_phase": "開盤區間完成",
            "當日漲停價": 180,
        },
        "多頭", True, {"eligible": True, "rule": "觸發"},
    )
    assert plan["valid"] is True
    assert plan["reward_risk"] >= 1.3
    assert "RR" in plan["summary"]


def test_daytrade_breakout_at_daily_limit_is_rejected_instead_of_moved_backwards():
    symbols = load_app_symbols(
        "get_taiwan_tick_size", "round_to_tick", "get_tick_size", "move_tick",
        "_safe_number", "_as_float", "fmt_price", "build_trade_plan",
    )
    common = {
        "_daytrade_vwap": 100, "_daytrade_or_high": 110,
        "_daytrade_or_low": 90, "_daytrade_close": 110,
        "_daytrade_phase": "開盤區間完成",
        "_交易日漲停價": 110, "_交易日跌停價": 90,
    }
    long_plan = symbols["build_trade_plan"](
        common, "多頭", True, {"eligible": True, "rule": "觸發"},
    )
    short_plan = symbols["build_trade_plan"](
        dict(common, _daytrade_close=90), "空頭", True,
        {"eligible": True, "rule": "觸發"},
    )
    assert long_plan["valid"] is False and long_plan["blocking_reason"] == "已到漲停"
    assert short_plan["valid"] is False and short_plan["blocking_reason"] == "已到跌停"
