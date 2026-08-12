"""Regression tests for pure calculation helpers without starting Streamlit UI."""

import ast
import calendar
import html
import io
import json
import math
import re
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


def test_stock_limit_color_requires_actual_equality_not_stale_status():
    symbols = load_app_symbols("_safe_number", "get_tick_size", "stock_limit_state")
    state = symbols["stock_limit_state"]
    assert state("100", 100.5, 90) == ""
    assert state("100.5", 100.5, 90) == "up"
    assert state("90", 100.5, 90) == "down"


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
    symbols = load_app_symbols("_clean_goodinfo_table")
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
    symbols = load_app_symbols("_clean_goodinfo_table", "_parse_goodinfo_table_html")
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


def test_cache_merge_preserves_remote_device_sections():
    symbols = load_app_symbols(
        "_json_safe", "_valid_fibo_tags", "_merge_unique_records",
        "_merge_unique_values", "empty_company_event_snapshot",
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
