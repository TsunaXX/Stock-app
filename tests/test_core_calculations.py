"""Regression tests for pure calculation helpers without starting Streamlit UI."""

import ast
import calendar
import html
import io
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
APPS_SCRIPT_PATH = Path(__file__).parents[1] / "google_apps_script.gs"


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
        "threading": threading,
        "time": time,
        "ThreadPoolExecutor": ThreadPoolExecutor,
        "as_completed": as_completed,
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


def test_goodinfo_manual_csv_backup_promotes_cp950_embedded_header():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text",
        "_promote_uploaded_stock_header",
        "_read_uploaded_stock_csv",
    )
    content = (
        "Goodinfo累計成交量週轉率排行,,\r\n"
        "資料時間,2026/08/21,\r\n"
        "代號,名稱,週轉率\r\n"
        '=\"2330\",台積電,1.25%\r\n'
        '=\"2408\",南亞科,9.8%\r\n'
    ).encode("cp950")
    parsed = symbols["_read_uploaded_stock_csv"](io.BytesIO(content))
    assert list(parsed.columns) == ["代號", "名稱", "週轉率"]
    assert parsed["代號"].tolist() == ['="2330"', '="2408"']
    assert parsed["名稱"].tolist() == ["台積電", "南亞科"]


def test_goodinfo_manual_csv_backup_accepts_utf8_bom_standard_header():
    symbols = load_app_symbols(
        "_goodinfo_normalize_text",
        "_promote_uploaded_stock_header",
        "_read_uploaded_stock_csv",
    )
    content = "代號,名稱,週轉率\n2330,台積電,1.25%\n".encode("utf-8-sig")
    parsed = symbols["_read_uploaded_stock_csv"](io.BytesIO(content))
    assert parsed.iloc[0].to_dict() == {
        "代號": "2330", "名稱": "台積電", "週轉率": "1.25%",
    }


def test_goodinfo_cloudflare_block_is_detected_without_waiting_for_table():
    detector = load_app_symbols("_is_goodinfo_block_page")["_is_goodinfo_block_page"]
    assert detector("<title>Just a moment...</title>") is True
    assert detector("<div id='cf-chl-widget'>Checking your browser</div>") is True
    assert detector("", status_code=403) is True
    assert detector("", status_code=429) is True
    assert detector("", status_code=430) is True
    assert detector("Too many requests") is True
    assert detector("<table><th>代號</th><th>週轉率</th></table>", 200) is False


def test_official_turnover_ranking_merges_twse_tpex_and_etf_units():
    symbols = load_app_symbols(
        "_parse_roc_trade_date", "_official_number", "_is_turnover_security_code",
        "_build_official_turnover_ranking",
    )
    twse_payload = {"tables": [{
        "fields": ["證券代號", "證券名稱", "成交股數"],
        "data": [["2330", "台積電", "100,000"], ["0050", "元大台灣50", "200,000"]],
    }]}
    companies = [{
        "公司代號": "2330", "已發行普通股數或TDR原股發行股數": "1000000",
    }]
    funds = [{"基金代號": "0050", "發行單位數/轉換數": "1000000"}]
    tpex = [{
        "Date": "1150824", "SecuritiesCompanyCode": "6488",
        "CompanyName": "環球晶", "TradingShares": "300000", "Capitals": "1000000",
    }, {
        "Date": "1150824", "SecuritiesCompanyCode": "708516",
        "CompanyName": "權證應排除", "TradingShares": "900000", "Capitals": "1000000",
    }]
    ranking, source_date = symbols["_build_official_turnover_ranking"](
        tpex, twse_payload, companies, funds,
    )
    assert source_date == date(2026, 8, 24)
    assert ranking["代號"].tolist() == ["6488", "0050", "2330"]
    assert ranking["週轉率(%)"].round(2).tolist() == [30.0, 20.0, 10.0]
    assert ranking["市場"].tolist() == ["上櫃", "上市", "上市"]
    assert ranking["排名"].tolist() == [1, 2, 3]


def test_official_turnover_ranking_rejects_missing_same_date_rows():
    symbols = load_app_symbols(
        "_parse_roc_trade_date", "_official_number", "_is_turnover_security_code",
        "_build_official_turnover_ranking",
    )
    try:
        symbols["_build_official_turnover_ranking"]([], {"tables": []}, [], [])
    except ValueError as exc:
        assert "TPEx" in str(exc)
    else:
        raise AssertionError("missing TPEx date must not produce an empty ranking")


def test_tpex_ssl_compatibility_keeps_verification_and_is_host_scoped():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    definitions = {
        node.name: ast.get_source_segment(source, node)
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef))
    }
    adapter_source = definitions["_TpexRelaxedStrictSSLAdapter"]
    session_source = definitions["_tpex_verified_session"]
    fetch_source = definitions["fetch_official_turnover_ranking"]
    ranking_fetch_source = definitions["fetch_post_close_stock_ranking_context"]
    assert 'context.load_verify_locations(certifi.where())' in adapter_source
    assert 'context.verify_flags &= ~strict_flag' in adapter_source
    assert 'context.check_hostname = False' not in adapter_source
    assert 'verify=False' not in adapter_source
    assert 'session.mount(_TPEX_ORIGIN, _TpexRelaxedStrictSSLAdapter())' in session_source
    assert 'with _tpex_verified_session() as session' in fetch_source
    assert 'with _tpex_verified_session() as session' in ranking_fetch_source


def test_official_turnover_auto_analysis_cannot_fall_back_to_stale_url():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "st.session_state['_stock_analysis_source_once'] = 'official'" in source
    assert "forced_analysis_source = st.session_state.pop(" in source
    forced_branch = source.index("if forced_analysis_source == 'official':")
    upload_branch = source.index("elif uploaded_file:", forced_branch)
    cloud_branch = source.index("elif st.session_state.cloud_url_input:", upload_branch)
    assert forced_branch < upload_branch < cloud_branch
    assert "本次官方週轉率排行不存在，已停止分析以避免載入舊資料" in source


def test_apps_script_rejects_only_older_stock_strategy_snapshots():
    source = APPS_SCRIPT_PATH.read_text(encoding="utf-8")
    assert "existingRow && scope === 'stock_strategy'" in source
    assert "incomingTime < existingTime" in source
    assert "incomingTime <= existingTime" not in source
    assert "incomingUpdatedAt" in source


def test_apps_script_splits_large_scope_data_across_columns_and_rows():
    source = APPS_SCRIPT_PATH.read_text(encoding="utf-8")
    assert "['scope', 'part', 'data', 'updated_at']" in source
    assert "function splitJsonChunks_" in source
    assert "splitJsonChunks_(serialized)" in source
    assert "'market_risk_data', 'display_settings'" in source
    assert "sheet.getRange('A1').getDisplayValue()" in source


def test_stock_display_settings_are_normalized_for_reboot_restore():
    symbols = load_app_symbols("normalize_stock_display_settings")
    normalize = symbols["normalize_stock_display_settings"]
    assert normalize({
        "limit_rows": "8", "hide_non_stock": False,
        "allow_warrant_search": True, "show_3d_hilo": True,
    }) == {
        "limit_rows": 8, "hide_non_stock": False,
        "allow_warrant_search": True, "show_3d_hilo": True,
    }
    assert normalize({"limit_rows": 0})["limit_rows"] == 1
    assert normalize({"limit_rows": "invalid"})["limit_rows"] == 5


def test_post_21_strategy_ranking_uses_visible_rows_and_selected_mode():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "_as_float", "_ranking_number",
        "_ranking_clamp", "_ranking_component_from_items", "_ranking_average",
        "_post_close_target_date", "_ranking_fundamental_component",
        "_ranking_chip_component", "_stock_ranking_technical_component",
        "_futures_ranking_technical_component", "_futures_contract_chip_component",
        "_combine_ranking_components", "strategy_ranking_weights", "_score_stock_post_close",
        "_score_futures_post_close", "build_strategy_ranking_entries",
    )
    rows = pd.DataFrame([
        {"代號": "2330", "名稱": "台積電", "信心分": 1, "收盤價": 110,
         "漲跌幅": 3.5, "_ma5": 102, "_risk_atr14": 3, "_risk_ma20_slope": 1,
         "_risk_close_position": 85, "_daytrade_vwap": 104},
        {"代號": "2408", "名稱": "南亞科", "信心分": 99, "收盤價": 92,
         "漲跌幅": -2.5, "_ma5": 100, "_risk_atr14": 4, "_risk_ma20_slope": -1,
         "_risk_close_position": 20, "_daytrade_vwap": 97},
    ])
    original_codes = rows["代號"].tolist()
    context = {
        "stocks": {
            "2330": {"institutional_net": 800000, "margin_delta": -200,
                     "short_delta": 20, "pe": 18, "pb": 3, "yield": 2.5},
            "2408": {"institutional_net": -700000, "margin_delta": 180,
                     "short_delta": -15, "pe": 55, "pb": 6.5, "yield": 0.5},
        },
        "scales": {"institutional": 800000, "margin": 200, "short": 20},
    }
    before_21 = symbols["build_strategy_ranking_entries"](
        rows, "當沖", now_value="2026-08-24 20:59:00", market_context=context,
    )
    intraday = symbols["build_strategy_ranking_entries"](
        rows, "當沖", now_value="2026-08-24 21:00:00", market_context=context,
    )
    swing = symbols["build_strategy_ranking_entries"](
        rows, "隔日／波段", now_value="2026-08-24 21:00:00", market_context=context,
    )
    assert {item["code"] for item in before_21} == {"2330", "2408"}
    assert {item["code"] for item in intraday} == {"2330", "2408"}
    assert {item["code"] for item in swing} == {"2330", "2408"}
    assert all(item["direction"] in {"多", "空"} for item in intraday + swing)
    assert all("技術偏" in item["reason"] and "籌碼偏" in item["reason"] for item in intraday)
    assert all("基本偏" in item["reason"] for item in swing)
    assert rows["代號"].tolist() == original_codes
    assert intraday[0]["score"] != rows.loc[rows["代號"] == intraday[0]["code"], "信心分"].iloc[0]


def test_post_close_ranking_date_uses_previous_session_before_21():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "_post_close_target_date",
    )
    _, before_21 = symbols["_post_close_target_date"]("2026-08-25 20:59:00")
    _, at_21 = symbols["_post_close_target_date"]("2026-08-25 21:00:00")
    _, monday_before_21 = symbols["_post_close_target_date"]("2026-08-24 10:00:00")
    _, holiday_before_21 = symbols["_post_close_target_date"]("2026-09-28 10:00:00")
    assert before_21 == date(2026, 8, 24)
    assert at_21 == date(2026, 8, 25)
    assert monday_before_21 == date(2026, 8, 21)
    assert holiday_before_21 == date(2026, 9, 24)


def test_ranking_reason_uses_consistent_category_and_direction_colors():
    formatter = load_app_symbols("format_ranking_reason_component")[
        "format_ranking_reason_component"
    ]
    technical = formatter("技術偏多 80（站上5日線）")
    chips = formatter("量倉／籌碼偏空 70（外資淨部位-1,000口）")
    fundamental = formatter("標的基本偏多 65（本益比 12）")
    unavailable = formatter("基本面不適用或未取得，不以零分處理")
    assert "#4fc3f7" in technical and "#ff6b6b" in technical
    assert "#ffb74d" in chips and "#35d07f" in chips
    assert "#c4b5fd" in fundamental and "#ff6b6b" in fundamental
    assert "#94a3b8" in unavailable


def test_strategy_ranking_weights_match_stock_and_futures_modes():
    weights = load_app_symbols("strategy_ranking_weights")["strategy_ranking_weights"]
    assert weights("stock", "當沖") == {
        "technical": 0.55, "chips": 0.35, "fundamental": 0.10,
    }
    assert weights("stock", "隔日／波段") == {
        "technical": 0.40, "chips": 0.30, "fundamental": 0.30,
    }
    assert weights("futures", "當沖") == {
        "technical": 0.50, "chips": 0.45, "fundamental": 0.05,
    }
    assert weights("futures", "隔日／波段") == {
        "technical": 0.35, "chips": 0.50, "fundamental": 0.15,
    }


def test_fundamental_ranking_uses_revenue_eps_and_reweights_missing_groups():
    symbols = load_app_symbols(
        "_as_float", "_ranking_number", "_ranking_clamp",
        "_ranking_component_from_items", "_ranking_average",
        "_ranking_fundamental_component",
    )
    complete = symbols["_ranking_fundamental_component"]({
        "revenue_yoy": 25, "revenue_mom": 8, "revenue_cumulative_yoy": 18,
        "eps": 5, "operating_margin": 20, "pe": 15, "pb": 2, "yield": 3,
    }, False)
    valuation_only = symbols["_ranking_fundamental_component"]({
        "pe": 15, "pb": 2, "yield": 3,
    }, False)
    assert complete is not None and valuation_only is not None
    assert "營收YoY" in complete["text"] and "EPS" in complete["text"]
    assert valuation_only["coverage"] < complete["coverage"]
    assert valuation_only["quality"] > 0


def test_ranking_explanation_identity_includes_code_and_direction_color():
    formatter = load_app_symbols("format_ranking_entry_identity")[
        "format_ranking_entry_identity"
    ]

    long_entry = formatter(1, "台積電", "2330", "多")
    short_entry = formatter(2, "南亞科", "2408", "空")

    assert "1.台積電(2330)" in long_entry
    assert "#ff6b6b" in long_entry
    assert "2.南亞科(2408)" in short_entry
    assert "#35d07f" in short_entry


def test_futures_post_close_ranking_uses_taifex_position_direction():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "_as_float", "_ranking_number",
        "_ranking_clamp", "_ranking_component_from_items", "_ranking_average",
        "_post_close_target_date", "_ranking_fundamental_component",
        "_ranking_chip_component", "_stock_ranking_technical_component",
        "_futures_ranking_technical_component", "_futures_contract_chip_component",
        "_combine_ranking_components", "strategy_ranking_weights", "_score_stock_post_close",
        "_score_futures_post_close", "build_strategy_ranking_entries",
    )
    rows = pd.DataFrame([
        {"期貨代碼": "TX", "名稱": "臺股期貨", "商品類型": "指數", "收盤價": 22000,
         "開盤價": 22000, "漲跌幅": 0, "當日成交口數": 90000, "未平倉量": 70000},
        {"期貨代碼": "MTX", "名稱": "小型臺指期貨", "商品類型": "指數", "收盤價": 22000,
         "開盤價": 22000, "漲跌幅": 0, "當日成交口數": 80000, "未平倉量": 60000},
    ])
    context = {
        "stocks": {}, "scales": {},
        "futures_products": {
            "臺股期貨": {"signal": 0.9, "quality": 90, "text": "外資淨部位+10,000口"},
            "小型臺指期貨": {"signal": -0.9, "quality": 90, "text": "外資淨部位-10,000口"},
        },
    }
    entries = symbols["build_strategy_ranking_entries"](
        rows, "當沖", now_value="2026-08-24 21:00:00",
        market_context=context, asset_type="futures",
    )
    assert {item["code"]: item["direction"] for item in entries} == {"TX": "多", "MTX": "空"}
    assert all("籌碼偏" in item["reason"] for item in entries)


def test_stock_search_options_are_shared_without_leaking_local_scope():
    symbols = load_app_symbols("build_stock_search_options")
    symbols["load_local_stock_names"] = lambda: (
        {"2330": "台積電", "123456": "測試權證"}, {}
    )
    symbols["is_warrant"] = lambda code: len(str(code)) > 4
    symbols["st"] = type("FakeStreamlit", (), {"session_state": {}})()
    assert symbols["build_stock_search_options"]() == ["2330 台積電"]
    assert symbols["build_stock_search_options"](allow_warrants=True) == [
        "123456 測試權證", "2330 台積電",
    ]
    source = APP_PATH.read_text(encoding="utf-8")
    independent_start = source.index('key="indep_search_multiselect"')
    assert "options=build_stock_search_options()" in source[independent_start - 180:independent_start]


def test_futures_ranking_derives_curve_oi_range_and_spot_basis_without_fake_values():
    symbols = load_app_symbols(
        "_safe_number", "_as_float", "_ranking_number", "_ranking_clamp",
        "_ranking_market_date", "_ranking_component_from_items", "_ranking_average",
        "_ranking_fundamental_component", "_ranking_chip_component",
        "_futures_ranking_technical_component", "_futures_contract_chip_component",
        "_combine_ranking_components", "strategy_ranking_weights",
        "_score_futures_post_close", "enrich_futures_ranking_fields",
    )
    current = pd.DataFrame([
        {"契約鍵": "CDF:202609", "期貨代碼": "CDF", "契約月份": "202609",
         "月份順位": 0, "商品類型": "股票", "標的代號": "2330", "名稱": "台積電期貨",
         "收盤價": 100, "開盤價": 98, "當日高": 105, "當日低": 95,
         "漲跌幅": 2, "當日成交口數": 5000, "未平倉量": 1000},
        {"契約鍵": "CDF:202610", "期貨代碼": "CDF", "契約月份": "202610",
         "月份順位": 1, "商品類型": "股票", "標的代號": "2330", "名稱": "台積電期貨",
         "收盤價": 99, "開盤價": 98, "當日高": 102, "當日低": 96,
         "漲跌幅": 1, "當日成交口數": 800, "未平倉量": 300},
    ])
    previous = pd.DataFrame([
        {"契約鍵": "CDF:202609", "未平倉量": 800},
        {"契約鍵": "CDF:202610", "未平倉量": 350},
    ])
    enriched = symbols["enrich_futures_ranking_fields"](
        current, previous, {"updated": "20260827"}, {"updated": "20260826"},
    )
    assert enriched.loc[0, "近遠月價差"] == 1
    assert enriched.loc[0, "未平倉增減"] == 200
    same_day = symbols["enrich_futures_ranking_fields"](
        current, previous, {"updated": "20260827"}, {"updated": "20260827"},
    )
    assert "未平倉增減" not in same_day.columns

    context = {
        "stocks": {"2330": {
            "close": 98, "foreign_net": 100000, "trust_net": 20000,
            "dealer_net": 5000, "revenue_yoy": 10, "eps": 5, "pe": 18,
        }},
        "scales": {"foreign": 100000, "trust": 20000, "dealer": 5000,
                   "margin": 100, "short": 20},
        "futures_products": {"股票期貨": {
            "signal": 0.5, "quality": 70, "text": "外資淨部位+1,000口",
        }},
    }
    row_scales = {"volume": 5000, "open_interest": 1000}
    baseline = symbols["_score_futures_post_close"](
        current.iloc[0], "當沖", context, row_scales,
    )
    improved = symbols["_score_futures_post_close"](
        enriched.iloc[0], "當沖", context, row_scales,
    )
    assert improved["coverage"] > baseline["coverage"]
    assert "日內位置" in improved["reason"]
    assert "價格×OI" in improved["reason"]
    assert "基差" in improved["reason"] or "近遠月" in improved["reason"]


def test_limit_note_uses_the_candles_own_previous_close():
    symbols = load_app_symbols(
        "_safe_number", "get_tick_size", "calculate_limits",
        "detect_stock_candle_limit_touch",
    )
    touch = symbols["detect_stock_candle_limit_touch"](21.2, 23.3, 21.0)
    assert touch["limit_up"] == 23.3
    assert touch["limit_down"] == 19.1
    assert touch["touched_up"] is True
    assert touch["touched_down"] is False


def test_realtime_stock_snapshots_merge_one_batch_without_network_calls():
    symbols = load_app_symbols(
        "_safe_number", "snapshot_change_rate", "price_change_amount",
        "merge_realtime_stock_snapshots",
    )

    class Snapshot:
        close = 110
        change_rate = 10
        buy_price = 109.5
        sell_price = 110

    source = pd.DataFrame([{"代號": "2330", "收盤價": 100, "漲跌幅": 0}])
    merged, count = symbols["merge_realtime_stock_snapshots"](
        source, {"2330": Snapshot()}, quote_time="2026/08/21 10:00:00",
    )
    assert count == 1
    assert merged.at[0, "收盤價"] == 110
    assert merged.at[0, "漲跌幅"] == 10
    assert merged.at[0, "_quote_time"] == "2026/08/21 10:00:00"


def test_partial_market_risk_refresh_keeps_last_complete_lists():
    merge = load_app_symbols("merge_market_risk_refresh")["merge_market_risk_refresh"]
    previous = {
        "attention": {"2330": 2}, "disposition": ["2408"],
        "disposition_tomorrow": [], "updated": "2026/08/21 09:00:00",
        "errors": [],
    }
    result = merge(
        previous, {}, [], [], ["上市注意: timeout"],
        attempted_at="2026/08/21 10:00:00",
    )
    assert result["attention"] == {"2330": 2}
    assert result["disposition"] == ["2408"]
    assert result["updated"] == "2026/08/21 09:00:00"
    assert result["last_attempt"] == "2026/08/21 10:00:00"
    assert result["using_last_success"] is True


def test_disposition_preview_uses_next_market_day_not_calendar_tomorrow():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "adjust_to_next_market_day(" in source
    assert "'🔶 下個開盤日處置'" in source
    assert "'🔶 明天處置'" not in source


def test_disposition_period_status_skips_weekend_to_next_open_day():
    symbols = load_app_symbols(
        "get_holidays", "is_market_closed_func", "adjust_to_next_market_day",
        "disposition_period_status",
    )
    status = symbols["disposition_period_status"]
    assert status("1150824~1150828", date(2026, 8, 21)) == "next_open"
    assert status("115/08/24～115/08/28", date(2026, 8, 24)) == "today"


def test_company_sync_is_deduplicated_capped_and_section_bounded():
    symbols = load_app_symbols(
        "COMPANY_SYNC_MAX_TICKERS", "fetch_company_event_sections",
    )
    calls = []

    def fake_fetcher(values):
        calls.append(tuple(values))
        return {"events": [{"ticker": values[0]}]}

    symbols["fetch_earnings_events"] = fake_fetcher
    symbols["fetch_taiwan_monthly_revenue_events"] = fake_fetcher
    symbols["fetch_us_revenue_events"] = fake_fetcher
    inputs = [f"{index:04d}" for index in range(20)] + ["0001"]
    results, errors, used = symbols["fetch_company_event_sections"](inputs)
    assert set(results) == {"earnings", "taiwan_revenue", "us_revenue"}
    assert errors == []
    assert len(used) == 12
    assert len(calls) == 3
    assert all(call == used for call in calls)


def test_calendar_sources_are_bounded_and_keep_partial_results():
    symbols = load_app_symbols(
        "fetch_calendar_base_sources", "fetch_selected_calendar_sources",
    )
    symbols["fetch_twse_holiday_events"] = lambda year: [{"date": f"{year}-01-01"}]
    symbols["fetch_twse_temporary_closure_events"] = lambda: [{"date": "2026-08-21"}]
    base, base_errors = symbols["fetch_calendar_base_sources"](2026)
    assert base_errors == []
    assert base["holidays"] == [{"date": "2026-01-01"}]
    assert base["temporary"] == [{"date": "2026-08-21"}]

    symbols["fetch_fomc_events"] = lambda year: [{"title": "FOMC", "year": year}]

    def failed_cpi(_year):
        raise TimeoutError("test")

    symbols["fetch_bls_cpi_events"] = failed_cpi
    selected, selected_errors = symbols["fetch_selected_calendar_sources"](
        2026, {"FOMC 利率決議", "美國 CPI"}, set(),
    )
    assert selected["FOMC"] == [{"title": "FOMC", "year": 2026}]
    assert selected["CPI"] == []
    assert selected_errors == ["CPI: TimeoutError"]


def test_calendar_source_failure_keeps_each_last_success_feed():
    merge = load_app_symbols("merge_calendar_last_success")["merge_calendar_last_success"]
    previous = {
        "FOMC": [{"date": "2026-09-17"}],
        "CPI": [{"date": "2026-09-11"}],
    }
    merged, retained = merge(
        {"FOMC": [], "CPI": [{"date": "2026-10-14"}]}, previous,
    )
    assert merged["FOMC"] == previous["FOMC"]
    assert merged["CPI"] == [{"date": "2026-10-14"}]
    assert retained == ["FOMC"]


def test_taiex_history_fetches_months_independently_and_in_parallel():
    symbols = load_app_symbols("fetch_twse_taiex_daily_history")
    calls = []

    def fake_month(month_text, refresh_bucket=None):
        calls.append((month_text, refresh_bucket))
        year, month = int(month_text[:4]), int(month_text[4:6])
        return [{
            "ts": pd.Timestamp(year=year, month=month, day=15),
            "Open": 100.0, "High": 102.0, "Low": 99.0,
            "Close": 101.0, "Volume": np.nan,
        }]

    symbols["fetch_twse_taiex_month"] = fake_month
    result = symbols["fetch_twse_taiex_daily_history"](lookback_days=60)
    assert not result.empty
    assert {"Open", "High", "Low", "Close", "Volume"}.issubset(result.columns)
    assert 2 <= len(calls) <= 4
    assert sum(bucket is not None for _month, bucket in calls) == 1


def test_index_history_empty_refresh_keeps_stale_nonempty_data():
    symbols = load_app_symbols("get_cached_market_temperature_data")

    class FakeStreamlit:
        session_state = {
            "sj_logged_in": False,
            "sj_api": None,
            "_market_temperature_history_cache": {
                ("^TWII", 180, False, None): {
                    "saved_at": 0.0,
                    "df": pd.DataFrame(
                        [{"Open": 100, "High": 102, "Low": 99, "Close": 101}],
                        index=[pd.Timestamp("2026-08-20")],
                    ),
                    "attrs": {},
                    "source": "證交所官方歷史日K",
                }
            },
        }

    symbols["st"] = FakeStreamlit()
    symbols["fetch_market_temperature_data"] = lambda *_args, **_kwargs: (
        pd.DataFrame(), "",
    )
    # This case verifies the expired-cache fallback.  Do not rely on the
    # runner's uptime: a freshly started CI runner can have monotonic < 180.
    data, source = symbols["get_cached_market_temperature_data"](
        "^TWII", max_age_seconds=0,
    )
    assert not data.empty
    assert "暫時沿用" in source


def test_heavy_hidden_sources_require_an_active_keyed_tab():
    source = APP_PATH.read_text(encoding="utf-8")
    assert 'key="main_workspace_active_tab", on_change="rerun"' in source
    assert 'key="strategy_database_active_tab", on_change="rerun"' in source
    assert 'key="strategy_room_active_tab", on_change="rerun"' in source
    assert 'if tab1.open and futures_strategy_tab.open' in source
    assert 'key="profit_room_active_tab", on_change="rerun"' in source
    assert 'options_profit_active = bool(tab2.open and tab2_3.open)' in source
    assert 'if tab_fibo.open and (' in source
    assert 'database_institutional_active = bool(tab_db.open and sub_tab1.open)' in source
    assert 'reports_active = bool(tab_db.open and sub_tab2.open)' in source
    assert 'calendar_network_active = bool(tab3.open)' in source
    assert 'if tab1.open and stock_strategy_tab.open:' in source
    assert 'schedule_calendar_source_preload(' in source
    assert "calendar_preload_task['event'].wait(timeout=12)" in source


def test_independent_tables_use_the_same_post_21_ranking():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "independent_rows, strategy_mode, '期貨獨立計算'" in source
    assert "df_indep, indep_strategy_mode, '股票獨立計算'" in source
    main_futures_table = source.index("edited = st.data_editor(", source.index("def futures_column_config"))
    main_futures_rank = source.index("render_strategy_ranking(display_rows, strategy_mode, '期貨')")
    main_futures_detail = source.index("'查看期貨策略信心明細'")
    assert main_futures_table < main_futures_rank < main_futures_detail
    main_stock_table = source.index("edited_df = st.data_editor(", source.index("stock_editor_key ="))
    main_stock_rank = source.index("render_strategy_ranking(df_display, strategy_mode, '股票')")
    main_stock_detail = source.index('"查看策略信心明細"', main_stock_table)
    assert main_stock_table < main_stock_rank < main_stock_detail


def test_streamlit_magic_ast_rewrite_is_disabled_for_large_app():
    config = (APP_PATH.parent / ".streamlit" / "config.toml").read_text(encoding="utf-8")
    assert "magicEnabled = false" in config
    assert 'fileWatcherType = "none"' in config


def test_calendar_http_has_a_short_single_attempt_deadline():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    calendar_get = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_calendar_get"
    )
    calendar_source = ast.get_source_segment(source, calendar_get)
    assert "timeout=(3, 8)" in calendar_source
    assert "range(3)" not in calendar_source
    tradingview = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "fetch_tradingview_us_calendar"
    )
    tradingview_source = ast.get_source_segment(source, tradingview)
    assert "max_workers=4" in tradingview_source
    assert "timeout=(3, 9)" in tradingview_source


def test_daily_risk_refresh_skips_redundant_live_quote_fetches():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    raw_fetch = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_stock_data_raw"
    )
    risk_refresh = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "refresh_risk_metrics_for_codes"
    )
    raw_source = ast.get_source_segment(source, raw_fetch)
    risk_source = ast.get_source_segment(source, risk_refresh)
    assert "include_live_quote=True" in raw_source
    assert "include_live_quote and re.fullmatch" in raw_source
    assert "include_live_quote=False" in risk_source


def test_goodinfo_fetch_uses_legacy_user_agent_without_browser_or_crawler_retries():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    wrapper = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_goodinfo_data"
    )
    wrapper_source = ast.get_source_segment(source, wrapper)
    assert 'requests.Session()' in wrapper_source
    assert "_GOODINFO_LEGACY_USER_AGENT" in wrapper_source
    assert "total_budget = 12.0" in wrapper_source
    assert '_parse_goodinfo_original_page' in wrapper_source
    assert "rate_limited" in wrapper_source
    assert "session.get(" in wrapper_source
    assert "session.post(" in wrapper_source
    assert "webdriver" not in wrapper_source
    assert "while " not in wrapper_source
    assert "crawl4ai" not in wrapper_source.lower()
    assert 'scrapling' not in wrapper_source.lower()


def test_verified_2026_bls_schedules_skip_blocked_network_retries():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for function_name, release_type in (
        ("fetch_bls_cpi_events", "cpi"),
        ("fetch_bls_employment_events", "employment"),
    ):
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        )
        function_source = ast.get_source_segment(source, function)
        verified_position = function_source.index(
            f'_verified_bls_2026_events(year, "{release_type}")'
        )
        calendar_get_position = function_source.index('_calendar_get(')
        assert verified_position < calendar_get_position
        assert 'if verified_events:' in function_source
        assert 'return verified_events' in function_source


def test_current_bea_schedule_uses_full_official_page_before_fallback():
    source = APP_PATH.read_text(encoding="utf-8")
    assert 'BEA_RELEASE_SCHEDULE_URL = "https://www.bea.gov/news/schedule/full"' in source
    tree = ast.parse(source)
    for function_name in ("fetch_bea_gdp_events", "fetch_bea_core_pce_events"):
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        )
        function_source = ast.get_source_segment(source, function)
        assert function_source.index("_parse_bea_release_schedule") < function_source.index(
            "_tradingview_macro_events"
        )
        assert "if official_events and year >= current_year" in function_source


def test_opening_yahoo_batch_does_not_request_nonexistent_twf_symbol():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "fetch_opening_overseas_signals"
    )
    function_source = ast.get_source_segment(source, function)
    assert "['TWF=F']" not in function_source
    assert "_extract_yfinance_close(downloaded, 'TWF=F')" not in function_source
    assert "[item[1] for item in intraday_symbols.values()]" in function_source


def test_goodinfo_http_path_has_no_browser_dependency():
    requirements = (APP_PATH.parent / "requirements.txt").read_text(encoding="utf-8")
    packages = (APP_PATH.parent / "packages.txt").read_text(encoding="utf-8")
    assert "selenium" not in requirements.lower()
    assert "crawl4ai" not in requirements.lower()
    assert "scrapling" not in requirements.lower()
    assert "chromium" not in packages.lower()


def test_fibo_tag_slots_keep_positions_and_restore_codes():
    symbols = load_app_symbols(
        "DEFAULT_FIBO_TAGS", "normalize_fibo_quick_tag", "normalize_fibo_tag_slots",
    )
    symbols["load_local_stock_names"] = lambda: (
        {"2330": "台積電", "2408": "南亞科", "2454": "聯發科", "6215": "和椿", "3535": "晶彩科"},
        {"台積電": "2330", "南亞科": "2408", "聯發科": "2454", "和椿": "6215", "晶彩科": "3535"},
    )
    values = ["台積電", "", "南亞科", "和椿", "晶彩科"]
    fallback = ["台積電(2330)", "南亞科(2408)", "聯發科(2454)", "和椿(6215)", "晶彩科(3535)"]
    assert symbols["normalize_fibo_tag_slots"](values, fallback) == [
        "台積電(2330)", "南亞科(2408)", "南亞科(2408)", "和椿(6215)", "晶彩科(3535)",
    ]


def test_shioaji_credentials_are_never_mixed_across_sources():
    symbols = load_app_symbols("resolve_shioaji_credentials")
    runtime = {"sj_key": "render-key", "sj_secret": ""}
    symbols["get_app_secret"] = lambda key, default='': runtime.get(key, default)
    symbols["logger"] = type("Logger", (), {"warning": staticmethod(lambda *_args: None)})()
    assert symbols["resolve_shioaji_credentials"]({
        "sj_key": "saved-key", "sj_secret": "saved-secret", "remember_sj": True,
    })[:2] == ("saved-key", "saved-secret")
    runtime["sj_secret"] = "render-secret"
    assert symbols["resolve_shioaji_credentials"]({})[:2] == ("render-key", "render-secret")


def test_render_uses_persistent_shioaji_env_and_cached_startup_snapshots():
    source = APP_PATH.read_text(encoding="utf-8")
    render_yaml = (APP_PATH.parent / "render.yaml").read_text(encoding="utf-8")
    assert "SHIOAJI_API_KEY" in render_yaml
    assert "SHIOAJI_SECRET_KEY" in render_yaml
    assert "st.session_state._stock_cache_refresh_pending = False" in source
    assert "def _fetch_remote_scope_cached" in source
    assert "save_fibo_config(sync_cloud=False)" in source
    assert "def fetch_fibonacci_yahoo_history" in source


def test_shioaji_futures_resolver_uses_v17_lazy_root_api():
    symbols = load_app_symbols(
        "SHIOAJI_FUTURES_ROOT_ALIASES", "FUTURES_MONTH_CODE",
        "shioaji_futures_root_candidates", "expected_shioaji_futures_code",
        "_is_shioaji_auth_error", "_contract_delivery_month", "_is_actual_futures_contract",
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


def test_expired_shioaji_session_is_invalidated_without_crashing_futures_room():
    symbols = load_app_symbols(
        "SHIOAJI_FUTURES_ROOT_ALIASES", "FUTURES_MONTH_CODE",
        "shioaji_futures_root_candidates", "expected_shioaji_futures_code",
        "_is_shioaji_auth_error", "_contract_delivery_month", "_is_actual_futures_contract",
        "_list_shioaji_futures_contracts", "resolve_shioaji_futures_contract",
    )

    class AuthError(Exception):
        pass

    class ExpiredContracts:
        def futures(self, *_args, **_kwargs):
            raise AuthError("Not authenticated")

    class API:
        contracts = ExpiredContracts()

    invalidated = []
    symbols["invalidate_shioaji_connection"] = lambda exc: invalidated.append(str(exc))
    assert symbols["resolve_shioaji_futures_contract"](API(), "TMF", "202609") is None
    assert invalidated == ["Not authenticated"]
    assert symbols["_is_shioaji_auth_error"](TimeoutError("temporary timeout")) is False


def test_mobile_layout_prevents_metrics_and_table_cells_from_overlapping():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "@media (max-width: 480px)" in source
    assert "flex:1 1 100% !important" in source
    assert "text-overflow:ellipsis" in source
    assert "overflow-wrap:anywhere" in source
    assert "def render_compact_metric_card_grid" in source
    assert "compact-metric-grid index-level-metric-grid" in source
    assert "compact-metric-grid fibo-suggestion-metric-grid" in source
    assert "compact-metric-grid index-entry-risk-grid" in source
    assert "compact-metric-grid index-short-risk-grid" in source


def test_phone_charts_keep_payoff_levels_outside_the_plot_canvas():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {
        node.name: ast.get_source_segment(source, node)
        for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    payoff_source = functions["build_txo_payoff_chart"]
    level_source = functions["get_txo_payoff_level_items"]
    fibo_source = functions["plot_fibonacci_chart"]
    assert "'目前'" in level_source
    assert "'損益兩平'" in level_source
    assert "fig.add_annotation" not in payoff_source
    assert "fig.add_vline" not in payoff_source
    assert "showlegend=False" in payoff_source
    assert "title=dict(text='到期損益曲線" not in payoff_source
    assert 'st.markdown("#### 到期損益曲線（每口／每組）")' in source
    assert "compact-metric-grid txo-payoff-level-grid" in source
    assert "padding_ratio=0.10" in fibo_source
    assert "displayModeBar': False" in fibo_source
    assert "orientation='h'" in fibo_source


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


def test_only_night_futures_filter_excludes_day_contracts_and_uses_night_volume_order():
    symbols = load_app_symbols(
        "TAIFEX_NIGHT_SESSION_ROOTS", "is_futures_night_session_product",
        "is_small_stock_futures_record", "filter_futures_strategy_display_rows",
    )
    filter_rows = symbols["filter_futures_strategy_display_rows"]
    rows = pd.DataFrame([
        {
            "契約鍵": "TX:202609", "期貨代碼": "TX", "交易時段": "日盤+夜盤",
            "商品類型": "指數", "指數期貨": True, "ETF期貨": False, "小型期貨": False,
            "次月期貨": False, "月份順位": 0, "當日成交口數": 900, "夜盤成交口數": 45,
        },
        {
            "契約鍵": "QFF:202609", "期貨代碼": "QFF", "交易時段": "日盤+夜盤",
            "商品類型": "股票", "指數期貨": False, "ETF期貨": False, "小型期貨": False,
            "次月期貨": False, "月份順位": 0, "當日成交口數": 300, "夜盤成交口數": 80,
        },
        {
            "契約鍵": "DAY:202609", "期貨代碼": "DAY", "交易時段": "日盤",
            "商品類型": "股票", "指數期貨": False, "ETF期貨": False, "小型期貨": False,
            "次月期貨": False, "月份順位": 0, "當日成交口數": 1200,
        },
    ])

    visible = filter_rows(rows, only_night=True, hide_index=False, minimum_volume=1)
    assert visible["契約鍵"].tolist() == ["QFF:202609", "TX:202609"]
    assert "DAY:202609" not in set(visible["契約鍵"])

    without_index = filter_rows(rows, only_night=True, hide_index=True, minimum_volume=1)
    assert without_index["契約鍵"].tolist() == ["QFF:202609"]


def test_requested_futures_controls_and_company_tab_order_are_present():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "🚨 官方上市處置公告" in source
    assert "🚨 官方上櫃處置公告" in source
    assert "taiwan_tab, us_tab, earnings_tab = st.tabs" in source
    assert "只顯示夜盤期貨" in source
    assert source.index("只顯示夜盤期貨") > source.index("只顯示小型股票期貨")
    assert source.index("只顯示夜盤期貨") < source.index("隱藏小型期貨")
    compact_start = source.index("if compact_futures_table:")
    compact_end = source.index("else:\n            futures_display_columns", compact_start)
    compact_columns = source[compact_start:compact_end]
    assert compact_columns.index("'收盤價'") < compact_columns.index("'方向'")
    assert compact_columns.index("'進出場點位'") < compact_columns.index("'支撐壓力'")


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
