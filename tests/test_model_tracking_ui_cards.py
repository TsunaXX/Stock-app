from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import ast
import html
import json

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _app_helpers():
    source = (ROOT / "app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = {
        "_model_notice_amount_text",
        "_model_metric_card_html",
        "_model_display_value",
        "_model_number_text",
        "_model_display_frame",
    }
    definitions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {
        "Decimal": Decimal,
        "ROUND_HALF_UP": ROUND_HALF_UP,
        "html": html,
        "json": json,
        "np": np,
        "pd": pd,
    }
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(ROOT / "app.py"), "exec"), namespace)
    return namespace


def test_trade_notice_amount_trims_only_unneeded_decimal_zeros():
    helpers = _app_helpers()
    format_amount = helpers["_model_notice_amount_text"]
    assert format_amount("-329.00") == "-329"
    assert format_amount("+1,200.50") == "+1,200.5"
    assert format_amount("1,000") == "1,000"
    assert format_amount("-0.00") == "0"


def test_performance_metric_cards_color_pnl_and_plan_table():
    helpers = _app_helpers()
    render_card = helpers["_model_metric_card_html"]
    profit = render_card("累積模擬損益", 1234.5)
    loss = render_card("累積模擬損益", -329.0)
    neutral = render_card("有效交易數", 3)

    assert "1,234.5" in profit and "color:#ff7373" in profit
    assert "-329" in loss and "color:#55d6a8" in loss
    assert "color:#93c5fd" in neutral and "border:1px solid" in neutral

    frame = helpers["_model_display_frame"](
        pd.DataFrame({"計畫累積損益": [100.0, -100.0]})
    )
    frame._compute()
    assert ("color", "#ff7373") in frame.ctx[(0, 0)]
    assert ("color", "#55d6a8") in frame.ctx[(1, 0)]
