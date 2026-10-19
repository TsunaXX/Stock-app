"""Repeatable cache/CPU benchmark with synthetic OHLCV and real calculation functions.
No broker, Streamlit server, Sheets, model database or personal data is used.
"""
import json
import statistics
import sys
import threading
import time
import tracemalloc
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))
from market_automation import IntradayBackground
from test_core_calculations import load_app_symbols

ns = load_app_symbols('calculate_operation_snapshot','get_stable_index_trade_plan',
    'calculate_market_temperature','calculate_index_trade_plan','get_futures_intraday_state',
    'evaluate_trade_entry_state','calculate_short_wave_plan','resolve_short_wave_direction')
api = object()
now = pd.Timestamp.now(tz='Asia/Taipei').tz_localize(None)
index = pd.bdate_range(end=now.normalize()-pd.Timedelta(days=1), periods=180)
close = np.linspace(19000,21000,180)+np.sin(np.arange(180))*50
frame = pd.DataFrame({'Open':close-20,'High':close+100,'Low':close-100,'Close':close,'Volume':10000.},index=index)
minutes = pd.date_range(end=now.floor('min'), periods=9000, freq='min')
price = 21000+np.sin(np.arange(9000)/30)*40
bars = pd.DataFrame({'Open':price-2,'High':price+10,'Low':price-10,'Close':price,'Volume':100.},index=minutes)
def resample(api,interval):
    return bars.resample(interval,origin='start_day').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
ns.update(merge_market_temperature_snapshot=lambda data,*a,**kw:data,
    _post_close_target_date=lambda:(now,index[-1].date()),get_cached_index_minutes=resample,
    get_cached_futures_intraday_state=lambda api,direction,**kw:ns['get_futures_intraday_state'](api,direction,logged_in=True),
    get_cached_short_wave_plan=lambda api,direction,**kw:ns['calculate_short_wave_plan'](api,direction),
    get_live_futures_snapshot=lambda *a,**kw:{'price':float(price[-1]),'change':10.,'contract_code':'TMF-test','updated':now})
seed = [('加權','^TWII',frame,'synthetic',ns['calculate_market_temperature'](frame)),
        ('期貨','TWF=F',frame+10,'synthetic',ns['calculate_market_temperature'](frame+10))]
state = {}
def calculate():
    return ns['calculate_operation_snapshot'](seed,api,'積極（提早確認）',state,background=True)
start = time.perf_counter(); snapshot = calculate(); cold = time.perf_counter()-start
assert snapshot['plan'] and snapshot['short_wave']
def measurements(call,n=200):
    values=[]; cpu=time.process_time()
    for _ in range(n):
        start=time.perf_counter();call();values.append((time.perf_counter()-start)*1000)
    return {'median_ms':round(statistics.median(values),3),'p95_ms':round(sorted(values)[int(n*.95)],3),
            'cpu_seconds':round(time.process_time()-cpu,3)}
foreground=measurements(calculate)
worker=IntradayBackground(lambda:True)
worker.configure('index_plan','benchmark',snapshot,5,lambda previous:(calculate(),1),lambda:True,lambda:None,low_priority=True)
try:
    deadline=time.monotonic()+5
    while not worker.result('index_plan') and time.monotonic()<deadline: time.sleep(.01)
    assert worker.result('index_plan')
    cached=measurements(lambda:worker.result('index_plan'))
    tracemalloc.start()
    for _ in range(200): worker.result('index_plan')
    before=tracemalloc.get_traced_memory()[0]
    for _ in range(3000): worker.result('index_plan')
    retained,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    result={'synthetic':True,'cold_calculation_ms':round(cold*1000,3),
        'foreground_equivalent_calculation':foreground,'cached_return':cached,
        'retained_bytes_after_3000_reads':retained-before,'peak_traced_bytes':peak,
        'background_jobs':len(worker.jobs),'extra_quote_requests':0,'extra_logins':0}
    print(json.dumps(result,indent=2))
finally:
    worker.close();worker.thread.join(2)
