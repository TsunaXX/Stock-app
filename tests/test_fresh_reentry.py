"""Stock/futures reentry revalidates a fresh plan without changing a closed fill."""
import ast
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from model_tracking import ModelTracker, PROTECTION_POLICY, entry_economics, merge_trades
from test_model_tracking import signal, quote, real_stock_adapter, stock_rows_with_evidence


def seed(market='股票',mode='當沖',short=False,**changes):
    result=signal(市場=market,策略=mode,策略版本='V1.2-exec2-test',計畫來源='股期戰略室',計畫類型=market+mode,
                  方向='空頭' if short else '多頭',進場價=100.,停損價=105. if short else 95.,
                  目標價=90. if short else 110.,乘數=50 if market=='期貨' else 1,期交稅率=.00002,
                  跳動點=.5,進場品質檢查=True,保利規則=PROTECTION_POLICY)
    if mode=='當沖':
        result['失效重觸規則']='executable-reset-v1'
    result.update(changes)
    return result


def sample(candidate,price,second,**changes):
    tick=quote(price,second,序號=second+1,串流識別='desktop:0')
    result={**tick,'成交證據':[tick],'策略':candidate['策略'],'策略版本':candidate['策略版本'],
            '計畫來源':'股期戰略室','計畫類型':candidate['計畫類型'],'進場品質許可':True,
            '進場品質方向':candidate['方向'],'進場品質時間':tick['來源時間'],
            '候選已重估':True,'再入規則':'fresh-plan-v1',
            '候選計畫':{**candidate,'來源時間':tick['來源時間']}}
    result.update(changes)
    return result


@pytest.mark.parametrize('market',['股票','期貨'])
@pytest.mark.parametrize('mode',['當沖','波段'])
@pytest.mark.parametrize('short',[False,True])
@pytest.mark.parametrize('loss',[False,True])
def test_exit_then_new_levels_costs_and_protection_without_old_price_reset(tmp_path,market,mode,short,loss):
    first=seed(market,mode,short)
    key=market+'|2330'
    tracker=ModelTracker(tmp_path/'fresh.sqlite',start=False)
    tracker.process([first],{key:sample(first,100,0)})
    exit_price=(106 if loss else 89) if short else (94 if loss else 111)
    tracker.process([],{key:sample(first,exit_price,1)})
    old=copy.deepcopy(tracker.records()[0])
    assert old['狀態']=='已平倉' and old['出場原因']==('停損' if loss else '停利')
    entry=exit_price
    current=seed(market,mode,short,進場價=entry,停損價=entry+5 if short else entry-5,
                 目標價=entry-12 if short else entry+12,
                 成本設定={**first['成本設定'],'futures_fee':18},指標快照={'VWAP':entry-1})
    latest=sample(current,entry,2)
    tracker.process([current],{key:latest})
    rows=sorted(tracker.records(),key=lambda r:r['進場次數'])
    assert len(rows)==2
    before,new=rows
    for field,value in old.items():
        assert before[field]==value
    assert new['狀態']=='模擬持倉' and new['進場次數']==2
    assert new['前次執行ID']==old['交易ID'] and new['原始計畫ID']==old['原始計畫ID']
    assert new['進場價']==entry and new['停損價']==current['停損價'] and new['目標價']==current['目標價']
    assert new['成本設定']['futures_fee']==18 and new['指標快照']=={'VWAP':entry-1}
    assert new['保利狀態']=='未啟動' and new.get('保利價') is None and new['保利最大浮盈']==0
    snapshot=json.dumps(rows,sort_keys=True)
    tracker.process([current],{key:latest})
    assert json.dumps(sorted(tracker.records(),key=lambda r:r['進場次數']),sort_keys=True)==snapshot
    second=ModelTracker(tmp_path/'phone.sqlite',start=False)
    with second.write_db() as db:
        second.store(db,old)
    second.process([current],{key:latest})
    assert {r['交易ID'] for r in second.records()}=={r['交易ID'] for r in rows}
    assert len(merge_trades(rows,second.records()))==2


@pytest.mark.parametrize('blocked',['not_recalculated','not_allowed','old_permission','no_candidate','gap','reconnect','stale','cost','version','day'])
def test_new_entry_keeps_strategy_freshness_and_evidence_checks(tmp_path,blocked):
    first=seed(); tracker=ModelTracker(tmp_path/'blocked.sqlite',start=False)
    tracker.process([first],{'股票|2330':sample(first,100,0)})
    tracker.process([],{'股票|2330':sample(first,111,1)})
    current=seed(進場價=112.,停損價=107.,目標價=124.)
    latest=sample(current,112,2)
    if blocked=='not_recalculated': latest['候選已重估']=False
    elif blocked=='not_allowed': latest['進場品質許可']=False
    elif blocked=='old_permission': latest['進場品質時間']=quote(0,1)['來源時間']
    elif blocked=='no_candidate': latest.pop('候選計畫')
    elif blocked=='gap': latest['成交證據'][0]['序號']=20
    elif blocked=='reconnect': latest['成交證據'][0]['串流識別']='desktop:1'
    elif blocked=='stale': latest['有效']=False
    elif blocked=='cost': latest['候選計畫'].update(停損價=111.9,目標價=112.2)
    elif blocked=='version': latest['候選計畫']['策略版本']='V1.2-exec2-other'
    elif blocked=='day': latest['交易日']='2026-10-09'
    tracker.process([],{'股票|2330':latest})
    assert len(tracker.records())==1


def test_adapter_new_version_rechecks_old_ui_text_and_publishes_candidate():
    ns=real_stock_adapter(); rows,config=stock_rows_with_evidence(ns,1)
    config['rules_version']='V1.2-exec2-test'
    config['costs']['slippage_ticks']=0
    rows['進出場預判']='多·停利｜進 90｜停 85｜目 100'
    signals,quotes=ns['model_tracking_samples'](rows,True,'當沖','多',config,5,object())
    assert signals and signals[0]['進場價']!=90
    assert quotes['股票|2330']['候選已重估'] and quotes['股票|2330']['再入規則']=='fresh-plan-v1'
    assert quotes['股票|2330']['候選計畫']==signals[0]


def test_closed_projection_releases_both_tables_for_new_plan_apptest():
    tree=ast.parse(Path('app.py').read_text(encoding='utf-8'))
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='apply_model_protection')
    script="""import streamlit as st
import pandas as pd, pytz
from datetime import datetime
from types import SimpleNamespace
st.session_state['sj_logged_in']=True
get_app_secret=lambda key:''
get_futures_trading_date=lambda value:value
get_model_tracker=lambda url:SimpleNamespace(protection_plans=lambda *args:{'2330':{'模擬進場價':100,'狀態':'已平倉'}})
parse_trade_plan_numbers=lambda text:{'entry':112,'stop':107,'target':124}
"""+ast.unparse(function)+"""
for stock in (True,False):
    frame=pd.DataFrame([{'代號':'2330','契約鍵':'2330','進出場預判':'多｜進 112｜停 107｜目 124','進出場點位':'多｜進 112｜停 107｜目 124'}])
    apply_model_protection(frame,stock,'當沖')
    st.dataframe(frame)
"""
    app=AppTest.from_string(script).run()
    assert not app.exception
    assert '進 112' in app.dataframe[0].value.iloc[0]['進出場預判'] and '保 —' in app.dataframe[0].value.iloc[0]['進出場預判']
    assert '進 112' in app.dataframe[1].value.iloc[0]['進出場點位']


def test_third_entry_and_apps_script_accepts_new_levels_on_same_execution_chain(tmp_path):
    import shutil,subprocess
    tracker=ModelTracker(tmp_path/'cycles.sqlite',start=False)
    first=seed()
    for second,price,candidate in ((0,100,first),(1,111,first),
            (2,111,seed(進場價=111.,停損價=106.,目標價=123.)),
            (3,124,seed(進場價=111.,停損價=106.,目標價=123.)),
            (4,124,seed(進場價=124.,停損價=119.,目標價=136.))):
        tracker.process([candidate],{'股票|2330':sample(candidate,price,second)})
    records=sorted(tracker.records(),key=lambda r:r['進場次數'])
    assert len(records)==3 and [r['進場價'] for r in records]==[100,111,124]
    assert records[-1]['狀態']=='模擬持倉' and records[-1]['進場次數']==3
    node=shutil.which('node')
    if not node: pytest.skip('Node required for Apps Script validation')
    js="""const crypto=require('node:crypto');
global.Utilities={DigestAlgorithm:{SHA_256:'sha256'},Charset:{UTF_8:'utf8'},computeDigest:(a,t)=>Array.from(crypto.createHash(a).update(t).digest())};
"""+Path('google_apps_script.gs').read_text(encoding='utf-8')+'\nconst rows='+json.dumps(records,ensure_ascii=False)+';validateModelExecutions_(rows,rows);'
    result=subprocess.run([node],input=js,text=True,encoding='utf-8',capture_output=True)
    assert result.returncode==0,result.stderr
