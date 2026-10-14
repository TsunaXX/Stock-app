"""PPI calendar release dates, source selection and result attachment regression tests."""

from datetime import datetime
from types import SimpleNamespace

import pytz

from market_automation import attach_macro_results, macro_results
from test_core_calculations import load_app_symbols


def test_ppi_2026_official_schedule_has_all_13_releases_without_network():
    symbols = load_app_symbols(
        "_taiwan_time_from_eastern", "_verified_bls_2026_events",
        "fetch_bls_ppi_events",
    )
    symbols["_calendar_get"] = lambda _: (_ for _ in ()).throw(
        AssertionError("2026 BLS verified schedule must not make network calls")
    )
    events = symbols["fetch_bls_ppi_events"](2026)
    assert len(events) == 13  # Jan includes November and December 2025 releases.
    assert [e["date"] for e in events if e["date"].startswith("2026-01")] == [
        "2026-01-14", "2026-01-30",
    ]
    dates_and_times = {e["date"]: e["title"] for e in events}
    assert "21:30" in dates_and_times["2026-01-14"]
    assert "20:30" in dates_and_times["2026-10-15"]
    assert "21:30" in dates_and_times["2026-11-13"]
    assert "21:30" in dates_and_times["2026-12-15"]
    assert all(e["impact"] == "high" and "PPI" in e["title"] for e in events)


def test_ppi_official_ics_parses_2027_timezone_and_deduplicates():
    symbols = load_app_symbols("_taiwan_time_from_eastern", "_parse_bls_release_ics")
    ics = (
        "BEGIN:VCALENDAR\n"
        "BEGIN:VEVENT\nSUMMARY:Producer Price Index\nDTSTART:20270114T083000\nEND:VEVENT\n"
        "BEGIN:VEVENT\nSUMMARY:Producer Price Index\nDTSTART:20270114T083000\nEND:VEVENT\n"
        "BEGIN:VEVENT\nSUMMARY:Consumer Price Index\nDTSTART:20270115T083000\nEND:VEVENT\n"
        "END:VCALENDAR"
    )
    events = symbols["_parse_bls_release_ics"](
        SimpleNamespace(text=ics), 2027, "Producer Price Index", "美國 PPI 公布"
    )
    assert len(events) == 1
    assert events[0]["date"] == "2027-01-14"
    assert events[0]["title"] == "美國 PPI 公布（21:30）"


def test_calendar_ppi_filter_only_fetches_ppi_when_selected():
    symbols = load_app_symbols("fetch_selected_calendar_sources")
    calls = []
    symbols["fetch_bls_ppi_events"] = lambda year: (
        calls.append(year) or [{"date": "2026-10-15", "title": "美國 PPI 公布（20:30）"}]
    )
    results, errors = symbols["fetch_selected_calendar_sources"](
        2026, {"美國 PPI"}, set()
    )
    assert calls == [2026]
    assert list(results) == ["PPI"]
    assert errors == []
    assert results["PPI"][0]["date"] == "2026-10-15"
    without_ppi, _ = symbols["fetch_selected_calendar_sources"](2026, set(), set())
    assert without_ppi == {}
    assert calls == [2026]


def test_ppi_release_result_attaches_to_official_date():
    rows = [
        {"ticker": "ECONOMICS:USPPIMM", "date": "2026-10-15T12:30:00Z",
         "title": "PPI MoM", "actual": 0.4, "forecast": 0.2,
         "previous": 0.1, "unit": "%"},
        {"ticker": "ECONOMICS:USPPIYY", "date": "2026-10-15T12:30:00Z",
         "title": "PPI YoY", "actual": 3.0, "forecast": 2.8,
         "previous": 2.9, "unit": "%"},
    ]
    results = macro_results(rows)
    assert len(results[("PPI", "2026-10-15")]) == 2
    events = {"PPI": [{"date": "2026-10-15", "title": "美國 PPI 公布（20:30）",
                       "detail": "BLS 官方公布排程。"}]}
    now = pytz.timezone("Asia/Taipei").localize(datetime(2026, 10, 15, 21, 0))
    merged = attach_macro_results(events, results, now)
    assert len(merged["PPI"][0]["release_results"]) == 2
    assert "PPI MoM：已公布" in merged["PPI"][0]["detail"]
    assert "PPI YoY：已公布" in merged["PPI"][0]["detail"]
