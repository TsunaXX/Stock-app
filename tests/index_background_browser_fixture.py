"""Local-only Streamlit lifecycle fixture; synthetic quotes, no credentials/Sheet/broker I/O.
Run: python -m streamlit run tests/index_background_browser_fixture.py --server.port 8502
"""
import sys
import threading
import time
import weakref
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))
from market_automation import IntradayBackground, BackgroundJobs
from test_core_calculations import load_app_symbols

st.set_page_config(page_title='Index background lifecycle test', layout='wide')
@st.cache_resource
def runtime_fixture():
    return {'api':object(), 'stream':{'lock':threading.RLock()}, 'jobs':BackgroundJobs(),
            'subscriptions':{}, 'updates':{}, 'started':time.monotonic()}
f = runtime_fixture()
ns = load_app_symbols('index_operation_snapshot', 'get_intraday_background',
                      'get_stable_index_trade_plan', 'calculate_operation_snapshot',
                      'index_operation_model_specs', '_stream_datetime', 'parse_strategy_data_time')
frame = pd.DataFrame({'Close':[20000.,20100.]}, index=pd.to_datetime(['2026-10-15','2026-10-16']))
seed = [('加權','^TWII',frame,'synthetic',{'score':50.}), ('期貨','TWF=F',frame,'synthetic',{'score':50.})]
st.session_state['sj_api'], st.session_state['sj_logged_in'] = f['api'], True

def live(*args, **kw):
    return {'price':20100 + int(time.monotonic()-f['started']), 'change':100.,
            'contract_code':'TMFJ6', 'updated':pd.Timestamp.now(tz='Asia/Taipei')}
def record(*args, **kw):
    key = id(st.session_state) if threading.current_thread() is threading.main_thread() else threading.get_ident()
    f['updates'][key] = f['updates'].get(key,0)+1
ns.update(st=st, weakref=weakref, IntradayBackground=IntradayBackground,
    get_public_maintenance_jobs=lambda:f['jobs'], refresh_index_background_history=lambda api,seed:seed,
    _stream_state=lambda api:f['stream'], get_model_tracker=lambda url:object(), get_app_secret=lambda k:'',
    load_model_tracking_costs=lambda:{}, get_operation_model_version=lambda source:'fixture',
    get_near_futures_contract=lambda api, product:SimpleNamespace(code=product),
    get_taiex_contract=lambda api:SimpleNamespace(code='IX0001'),
    sync_strategy_stream_scope=lambda api, contracts, scope:f['subscriptions'].__setitem__(scope,[c.code for c in contracts]),
    model_market_open=lambda row:True, _post_close_target_date=lambda:(None,pd.Timestamp('2026-10-16').date()),
    merge_market_temperature_snapshot=lambda data,*a,**kw:data,
    calculate_market_temperature=lambda data:{'score':50.},
    calculate_index_trade_plan=lambda *a:{'direction':'偏多','latest':20100.,'atr':100.,'trigger':'fixture'},
    get_live_futures_snapshot=live,
    get_cached_futures_intraday_state=lambda *a,**kw:{'vwap':20000.},
    evaluate_trade_entry_state=lambda plan,price,*a:{'can_enter':True,'entry_level':20000.,'invalidation':19900.,'target':20500.},
    resolve_short_wave_direction=lambda *a:('偏多','fixture'),get_cached_short_wave_plan=lambda *a,**kw:None,
    operation_model_spec=lambda *a,**kw:None,record_operation_models=record)

st.title('指數操作計畫背景工作驗證')
st.caption('模擬行情；沿用正式 session worker 與 snapshot adapter，沒有外部 API 或交易紀錄寫入。')
index, other = st.tabs(['指數操作計畫','股期戰略室'], key='fixture_page', on_change='rerun')
worker = st.session_state.get('_intraday_background')
if index.open:
    with index:
        seconds = st.number_input('更新秒數',2,300,value=5)
        @st.fragment(run_every=seconds)
        def view():
            snap = ns['index_operation_snapshot'](seed,seconds)
            st.metric('最新點位',snap.get('live_price'))
            st.caption('快照來源時間：'+str(snap.get('live_snapshot',{}).get('updated','waiting')))
            st.caption('工作數：'+str(len(st.session_state['_intraday_background'].jobs)))
        view()
else:
    with other:
        st.info('目前顯示股期頁；沒有隱藏 fragment 或畫面計時器。')
        st.write('此刻背景已完成輪數',sum(f['updates'].values()))
st.write('有效訂閱範圍',len([v for v in f['subscriptions'].values() if v]))
st.write('背景執行緒',len([w for w in threading.enumerate() if w.name=='intraday-background']))
