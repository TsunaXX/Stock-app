"""Compare exact trade/strategy outputs and CPU against a Git revision, without network or app data.
Run: python tests/cpu_refresh_benchmark.py --baseline-ref c5f1aad
"""
import argparse
import ast
import json
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from types import MethodType

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))
from model_tracking import ModelTracker, PROTECTION_POLICY, advance, new_trade
from test_core_calculations import load_app_symbols
from test_model_tracking import signal


def measure(call, repeats):
    elapsed, cpu = [], []
    for _ in range(repeats):
        started, cpu_started = time.perf_counter(), time.process_time()
        result = call()
        elapsed.append((time.perf_counter() - started) * 1000)
        cpu.append((time.process_time() - cpu_started) * 1000)
    return {'median_ms': round(statistics.median(elapsed), 2),
            'cpu_ms_per_round': round(statistics.mean(cpu), 2)}, result


def baseline_function(ref, path, name, namespace):
    source = subprocess.run(['git', 'show', f'{ref}:{path}'], check=True,
                            capture_output=True, encoding='utf-8').stdout
    node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = dict(namespace)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), path, 'exec'), namespace)
    return namespace[name]


def main(ref):
    import model_tracking
    old_process = baseline_function(ref, 'model_tracking.py', 'process', vars(model_tracking))
    base = pd.Timestamp('2026-10-08T09:30:10+08:00')
    originals, quotes = [], {}
    for i in range(6):
        item = signal(**{'商品鍵': str(i), '代碼': str(i), '保利規則': PROTECTION_POLICY})
        row = advance(new_trade(item), {'價格': 100., '來源時間': base.isoformat(),
            '交易日': item['交易日'], '有效': True, '序號': 4096, '串流識別': 'desktop:1'})
        originals.append(row)
        ticks = [{'價格': 100., '來源時間': (base + pd.Timedelta(milliseconds=i - 4096)).isoformat(),
                  '序號': i, '串流識別': 'desktop:1'} for i in range(51, 4147)]
        quotes['股票|' + item['商品鍵']] = {**ticks[-1], '有效': True, '交易日': item['交易日'], '成交證據': ticks}
    output, trade_results = {}, []
    with tempfile.TemporaryDirectory(prefix='stock-cpu-benchmark-') as directory:
        for label in ('before', 'after'):
            tracker = ModelTracker(Path(directory) / f'{label}.sqlite', start=False)
            if label == 'before':
                tracker.process = MethodType(old_process, tracker)
            # Reset outside the timed call, so both versions receive identical executions.
            elapsed, cpu = [], []
            for _ in range(5):
                with closing(tracker.db()) as db, db:
                    for row in originals:
                        tracker.store(db, row)
                measurements, _ = measure(lambda: tracker.process([], quotes), 1)
                elapsed.append(measurements['median_ms'])
                cpu.append(measurements['cpu_ms_per_round'])
            output['model_' + label] = {'median_ms': round(statistics.median(elapsed), 2),
                                        'cpu_ms_per_round': round(statistics.mean(cpu), 2)}
            trade_results.append(tracker.records())
            tracker.close()
    assert trade_results[0] == trade_results[1]
    assert all(row['最後序號'] == 4146 and not row['資料缺口'] for row in trade_results[1])

    ns = load_app_symbols('calculate_futures_strategy_levels', 'get_futures_trading_date',
        'get_futures_tick_size', 'FUTURES_FIXED_TICK_SIZES', 'round_futures_price',
        'clamp_futures_intraday_levels', '_safe_number', 'fmt_price')
    # Calendar classification is deterministic; this benchmark never fetches holidays.
    ns['is_market_closed_func'] = lambda d: d.weekday() >= 5
    old_futures = baseline_function(ref, 'app.py', 'calculate_futures_strategy_levels', ns)
    index = pd.date_range(end='2026-10-20 16:00', periods=86400, freq='min')
    minutes = index.hour * 60 + index.minute
    index = index[(index.weekday < 5) & ((index.hour >= 15) | (index.hour < 5) | ((minutes >= 525) & (minutes <= 825)))]
    price = 20000 + np.sin(np.arange(len(index)) / 30) * 3
    bars = pd.DataFrame({'Open': price - .1, 'High': price + .5, 'Low': price - .5,
                         'Close': price, 'Volume': 100.}, index=index)
    row = {'期貨代碼': 'TX', '商品類型': '指數', '收盤價': price[-1], '開盤價': 20000.,
           '當日高': 20005., '當日低': 19995., '當日漲停價': 22000., '當日跌停價': 18000.}
    for mode in ('當沖', '波段'):
        results = []
        for label, calculate in (('before', old_futures), ('after', ns['calculate_futures_strategy_levels'])):
            measurements, result = measure(lambda: [calculate(row, mode, '自動', bars) for _ in range(5)], 8)
            output[f'futures_{mode}_{label}'] = measurements
            results.append(result)
        assert results[0] == results[1]
    output.update(synthetic=True, same_complete_trade_records=True, same_futures_plans=True,
                  instruments={'model': 6, 'futures': 5}, retained_ticks=4096, new_ticks=50,
                  minute_bars=len(bars), extra_broker_requests=0)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', required=True)
    main(parser.parse_args().baseline_ref)
