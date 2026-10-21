"""Full-module before/after model CPU and memory comparison; synthetic data, no network."""
import json
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from types import ModuleType

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))
import model_tracking as current
from test_model_tracking import signal


def main(ref='f63eea5', history=1000):
    source = subprocess.run(['git', 'show', f'{ref}:model_tracking.py'], check=True,
                            capture_output=True, encoding='utf-8').stdout
    old = ModuleType('baseline_model')
    exec(compile(source, 'baseline_model.py', 'exec'), vars(old))
    results, comparisons = {}, []
    base = pd.Timestamp('2026-10-08T09:30:10+08:00')
    with tempfile.TemporaryDirectory(prefix='model-v12-benchmark-') as folder:
        for label, module, version in [('before', old, 'V1.1-test'), ('after', current, 'V1.2-test')]:
            tracker = module.ModelTracker(Path(folder)/f'{label}.sqlite', start=False)
            originals, quotes = [], {}
            for i in range(11):
                market = '股票' if i < 6 else '期貨'
                spec = signal(商品鍵=str(i), 代碼=str(i), 市場=market, 策略版本=version,
                              保利規則=module.PROTECTION_POLICY, 期交稅率=.00002, 乘數=50.)
                initial = {'價格':100., '來源時間':base.isoformat(), '交易日':spec['交易日'],
                           '有效':True, '序號':4096, '串流識別':'desktop:1'}
                originals.append(module.advance(module.new_trade(spec), initial))
                ticks = [{**initial, '來源時間':(base+pd.Timedelta(milliseconds=j-4096)).isoformat(), '序號':j}
                         for j in range(51, 4147)]
                quotes[market+'|'+str(i)] = {**ticks[-1], '成交證據':ticks, '策略':'當沖', '策略版本':version,
                                           '計畫來源':'股期戰略室', '計畫類型':market+'當沖'}
            with tracker.write_db() as db:
                for i in range(history):
                    extra = module.new_trade(signal(商品鍵=f'old{i}', 代碼=f'old{i}', 策略版本='V1.1-test'))
                    extra['事件'] *= 50
                    tracker.store(db, extra)
            elapsed, cpu, memory = [], [], []
            for _ in range(4):
                with tracker.write_db() as db:
                    for row in originals:
                        tracker.store(db, row)
                start, process = time.perf_counter(), time.process_time()
                tracker.process([], quotes)
                elapsed.append((time.perf_counter()-start)*1000)
                cpu.append((time.process_time()-process)*1000)
            with tracker.write_db() as db:
                for row in originals:
                    tracker.store(db, row)
            tracemalloc.start()
            tracker.process([], quotes)
            memory.append(tracemalloc.get_traced_memory()[1]/1024/1024)
            tracemalloc.stop()
            keys = ('商品鍵','狀態','資料缺口','模擬進場價','模擬出場價','模擬損益','結果(R)','保利價','保利淨額')
            ids = {row['交易ID'] for row in originals}
            comparisons.append(sorted([{k:row.get(k) for k in keys} for row in tracker.records()
                                       if row['交易ID'] in ids], key=lambda row:row['商品鍵']))
            results[label] = {'wall_median_ms':round(statistics.median(elapsed), 2),
                              'cpu_mean_ms':round(statistics.mean(cpu), 2),
                              'peak_memory_MiB':round(max(memory), 2)}
    assert comparisons[0] == comparisons[1], 'Financial/strategy results changed'
    print(json.dumps({'baseline':ref, 'results':results, 'financial_results_equal':True,
                      'stocks':6, 'futures':5, 'unrelated_active_rows':history,
                      'buffer_ticks':4096, 'new_ticks_per_product':50, 'extra_broker_calls':0}))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'f63eea5', int(sys.argv[2]) if len(sys.argv) > 2 else 1000)
