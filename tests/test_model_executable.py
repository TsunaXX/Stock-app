"""Executable plan admission and independent reset plans retain evidence and old results."""
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from model_tracking import (ModelTracker, new_trade, advance, entry_economics, entry_quality_reason,
                            executable_entry_band, merge_trades, performance, trade_id)
from test_model_tracking import signal, quote, real_stock_adapter, stock_rows_with_evidence


def plan(**changes):
    row = signal(策略版本='V1.2-exec1-test', 跳動點=.1, 進場品質檢查=True,
                 失效重觸規則='executable-reset-v1', **changes)
    return {**row, **entry_economics(row,row['進場價'])}


def sample(price, second, candidate=None, allowed=True, **changes):
    tick = quote(price,second,序號=second+1,串流識別='desktop:0')
    value = {**tick, '成交證據':[tick], '策略':'當沖', '策略版本':'V1.2-exec1-test',
             '計畫來源':'股期戰略室', '計畫類型':'股票當沖', '進場品質許可':allowed,
             '進場品質方向':'多頭', '進場品質時間':tick['來源時間'], '候選已重估':True,
             '候選診斷':{'名稱':'台積電','商品鍵':'2330','方向':'多頭','候選':True,
                       '未建立原因':'' if allowed else '策略條件未放行', '計畫可執行性':'通過'}}
    if candidate:
        value['候選計畫'] = {**candidate,'來源時間':tick['來源時間']}
    return {**value, **changes}


@pytest.mark.parametrize('entry,stop,target,tick,direction',[
    (217,220.5,212,.5,'空頭'), (45.75,46.15,45.15,.05,'空頭'),
    (55.2,55.8,54.3,.1,'空頭'), (79.7,78.7,81.2,.1,'多頭'),
    (77.8,79,76,.1,'空頭'), (224,219,231.5,.5,'多頭')])
def test_oct27_six_plan_prices_fail_the_same_net_cost_gate(entry,stop,target,tick,direction):
    row=signal(進場價=entry,停損價=stop,目標價=target,跳動點=tick,方向=direction,進場品質檢查=True)
    quality=entry_economics(row,entry)
    assert quality['原始RR']>=1.3 and 0<quality['淨RR']<1
    assert quality['計畫可執行性']=='待調整' and '成本' in entry_quality_reason(row,entry)
    assert executable_entry_band(row) is None


def test_net_cost_band_and_observed_fill_have_identical_decisions():
    row=plan()
    band=executable_entry_band(row)
    assert band and band[0]==100 and 100<=band[1]<=101.25
    for price in (100,band[1]):
        assert entry_economics(row,price)['淨RR']>=1
        assert not entry_quality_reason(row,price)
    assert '0.25R' in entry_quality_reason(row,102)
    future=plan(市場='期貨',乘數=50,期交稅率=.00002)
    assert entry_economics(future,100)['淨RR']>=1
    assert entry_economics({**future,'期交稅率':None},100)['未建立原因']


@pytest.mark.parametrize('invalid_price',[94,112])
def test_invalid_then_reset_new_plan_is_independent_and_replay_safe(tmp_path,invalid_price):
    t=ModelTracker(tmp_path/'replan.sqlite',start=False)
    seed=plan()
    t.process([seed],{'股票|2330':sample(99,0,seed)})
    t.process([],{'股票|2330':sample(invalid_price,1,seed)})
    original=t.records()[0]
    assert original['狀態']=='訊號失效' and original.get('模擬進場價') is None
    t.process([seed],{'股票|2330':sample(100,2,seed)})
    assert len(t.records())==1
    t.process([],{'股票|2330':sample(99,3,seed)})
    t.process([],{'股票|2330':sample(100,4,seed)})
    rows=t.records(); new=next(r for r in rows if r.get('重新觸發來源ID'))
    old=next(r for r in rows if not r.get('重新觸發來源ID'))
    assert len(rows)==2 and new['狀態']=='模擬持倉' and not new['資料缺口']
    assert new['原始計畫ID']!=old['原始計畫ID'] and new['交易ID']!=old['交易ID']
    assert new['進場次數']==1 and new['重新觸發來源ID']==old['交易ID']
    for field in ('交易ID','訊號失效原因','訊號失效時間','成本設定','進場價','停損價','目標價','事件'):
        assert old[field]==original[field]
    before=json.dumps(t.records(),sort_keys=True)
    t.process([seed],{'股票|2330':sample(100,4,seed)})
    assert json.dumps(t.records(),sort_keys=True)==before
    t.process([],{'股票|2330':sample(111,5,seed)})
    assert performance(t.records())[0]['有效交易數']==1
    # The existing closed-trade reentry stays inside this new original plan.
    t.process([],{'股票|2330':sample(99,6,seed)})
    t.process([],{'股票|2330':sample(100,7,seed)})
    assert len(t.records())==3
    latest=next(r for r in t.records() if r['進場次數']==2)
    assert latest['原始計畫ID']==new['原始計畫ID'] and latest['前次執行ID']==new['交易ID']


@pytest.mark.parametrize('block',['gap','reconnect','stale','not_recalculated','day','cost'])
def test_replan_requires_fresh_unbroken_reset_and_current_strategy(tmp_path,block):
    t=ModelTracker(tmp_path/(block+'.sqlite'),start=False)
    seed=plan()
    for p,i in ((99,0),(94,1),(99,2)):
        t.process([seed] if i==0 else [],{'股票|2330':sample(p,i,seed)})
    current=sample(100,3,seed)
    if block=='gap':
        current['成交證據'][0]['序號']=9
    elif block=='reconnect':
        current['成交證據'][0]['串流識別']='desktop:1'
    elif block=='stale':
        current['有效']=False
    elif block=='not_recalculated':
        current['候選已重估']=False
    elif block=='day':
        current['交易日']='2026-10-09'
    else:
        current['候選計畫']=plan(目標價=100.2)
    t.process([],{'股票|2330':current})
    assert len(t.records())==1


def test_cross_device_new_plan_identity_and_script_proof_validation(tmp_path):
    generated=[]
    for name in ('desktop','phone'):
        t=ModelTracker(tmp_path/(name+'.sqlite'),start=False)
        seed=plan()
        for i,p in enumerate((99,94,99,100,111,111,99,100)):
            q=sample(p,i,seed)
            q['串流識別']=q['成交證據'][0]['串流識別']=name+(':1' if i>=6 else ':0')
            t.process([seed] if i==0 else [],{'股票|2330':q})
        generated.append(t.records())
    assert {r['交易ID'] for r in generated[0]}=={r['交易ID'] for r in generated[1]}
    assert len(merge_trades(*generated))==3
    node=shutil.which('node')
    if not node:
        pytest.skip('Node required for shared storage verification')
    script="""const assert=require('node:assert/strict'),crypto=require('node:crypto');
global.Utilities={DigestAlgorithm:{SHA_256:'sha256'},Charset:{UTF_8:'utf8'},computeDigest:(a,t)=>Array.from(crypto.createHash(a).update(t).digest())};
"""+Path('google_apps_script.gs').read_text(encoding='utf8')+'\nconst rows='+json.dumps(generated[0],ensure_ascii=False)+""";
validateModelExecutions_(rows,rows);
const next=rows.find(r=>r['重新觸發來源ID']&&!r['前次執行ID']), old=rows.find(r=>!r['重新觸發來源ID']);
assert.throws(()=>validateModelExecutions_([next],[next]));
assert.throws(()=>validateModelExecutions_(rows,[{...next,'重新觸發解除證據':{...next['重新觸發解除證據'],'來源時間':old['訊號失效時間']}}]));
assert.throws(()=>validateModelExecutions_(rows,[{...next,'成交證據識別':{...next['成交證據識別'],'串流識別':'other'}}]));
assert.throws(()=>validateModelExecutions_(rows,[{...next,'淨RR':.9}]));
"""
    result=subprocess.run([node],input=script,text=True,encoding='utf8',capture_output=True)
    assert result.returncode==0,result.stderr


def test_state_diagnostics_do_not_write_on_each_tick_and_restore_after_restart(tmp_path):
    t=ModelTracker(tmp_path/'diagnosis.sqlite',start=False)
    seed=plan()
    t.process([],{'股票|2330':sample(99,0,seed,allowed=False)})
    before=t.diagnostics('2026-10-08','2026-10-08')
    t.process([],{'股票|2330':sample(99,1,seed,allowed=False)})
    assert t.diagnostics('2026-10-08','2026-10-08')==before
    assert len(t.diagnostic_events('2026-10-08','2026-10-08',[before[0]['診斷範圍']]))==1
    t.process([],{'股票|2330':sample(100,2,seed)})
    assert len(t.diagnostics('2026-10-08','2026-10-08')[0]['狀態變更次數'])==2
    assert len(t.diagnostic_events('2026-10-08','2026-10-08',[before[0]['診斷範圍']]))==2
    restored=ModelTracker(t.path,start=False)
    assert restored.diagnostics('2026-10-08','2026-10-08')==t.diagnostics('2026-10-08','2026-10-08')


def test_background_recalculates_despite_old_displayed_plan_and_cost_gate():
    ns=real_stock_adapter()
    rows,config=stock_rows_with_evidence(ns,1)
    config['rules_version']='V1.2-exec1-test'
    rows['進出場預判']='進 100.5｜停 99｜目 110'
    rows['_daytrade_volume_ratio']=.01
    published,_=ns['model_tracking_samples'](rows,True,'當沖','多',config,5,object())
    assert published
    current,quotes=ns['model_tracking_samples'](rows,True,'當沖','多',config,5,object(),recalculate=True)
    assert not current and '策略條件未放行' in quotes['股票|2330']['候選診斷']['未建立原因']
    rows['進出場預判']='進 100.5｜停 100.3｜目 100.8'
    current,quotes=ns['model_tracking_samples'](rows,True,'當沖','多',config,5,object())
    assert not current and '成本' in quotes['股票|2330']['候選診斷']['未建立原因']


def test_legacy_identity_and_quality_do_not_change():
    for version in ('V1.0','V1.1','V1.2-old'):
        row=signal(策略版本=version)
        old_id=trade_id(row)
        assert new_trade(row)['交易ID']==old_id
        assert not new_trade(row).get('失效重觸規則')


def test_ui_stock_publication_and_custom_futures_fees_use_shared_gate():
    from test_core_calculations import load_app_symbols
    ns=load_app_symbols('build_trade_plan','get_taiwan_tick_size','round_to_tick','get_tick_size','move_tick',
                        '_safe_number','_as_float','fmt_price','get_futures_tick_size','FUTURES_FIXED_TICK_SIZES')
    row={'收盤價':79.6,'_daytrade_close':79.6,'_daytrade_vwap':78.7,
         '_daytrade_or_high':79.6,'_daytrade_or_low':78.5,'當日漲停價':85.,'當日跌停價':70.}
    old=ns['build_trade_plan'](row,'多頭',True,{'eligible':True})
    checked=ns['build_trade_plan'](row,'多頭',True,{'eligible':True},signal()['成本設定'])
    assert old['valid'] and not checked['valid'] and '計畫待調整' in checked['summary']
    assert checked['execution']['原始RR']>=1.3 and checked['execution']['淨RR']<1
    terms=ns['strategy_execution_terms']({'期貨代碼':'TMF','商品類型':'指數','收盤價':20000},False,'偏多',
                                       {'futures_tmf_fee':35,'futures_fee':10})
    assert terms['成本設定']['futures_fee']==35 and terms['乘數']==10
    wave=ns['build_trade_plan']({**row,'_risk_atr14':3,'_risk_prev_high':80,'_risk_prev_low':77},
                              '多頭',False,{'eligible':True},signal()['成本設定'])
    assert wave['valid'] and not wave['execution']


@pytest.mark.parametrize('market,direction',[('股票','空頭'),('期貨','多頭')])
def test_replan_supports_short_and_actual_futures_contract_costs(tmp_path,market,direction):
    t=ModelTracker(tmp_path/(market+'.sqlite'),start=False)
    short=direction=='空頭'
    seed=plan(市場=market,方向=direction,停損價=105 if short else 95,目標價=90 if short else 110,
              乘數=50 if market=='期貨' else 1,期交稅率=.00002 if market=='期貨' else None)
    for second,price in enumerate((101,106,101,100) if short else (99,94,99,100)):
        q=sample(price,second,seed,進場品質方向=direction,計畫類型=market+'當沖')
        t.process([seed] if second==0 else [],{market+'|2330':q})
    rows=t.records()
    assert len(rows)==2 and any(r.get('模擬進場價') is not None and not r['資料缺口'] for r in rows)


def test_dirty_replan_parent_is_sent_before_child_even_when_cursor_has_advanced(tmp_path):
    t=ModelTracker(tmp_path/'sync-parent.sqlite','fake',start=False)
    seed=plan()
    for i,p in enumerate((99,94,99,100)):
        t.process([seed] if i==0 else [],{'股票|2330':sample(p,i,seed)})
    all_rows=t.records(); parent=next(r for r in all_rows if not r.get('重新觸發來源ID'))
    child=next(r for r in all_rows if r.get('重新觸發來源ID'))
    t.hydrated=True
    t.sync_positions['202610']=1
    catalog={'model_schema':1,'model_sync':2,'model_reentry':1,'model_replan':1,
             'model_months':[],'model_versions':{}}
    writes=[]
    def save(scope,data,delta=False):
        writes.append(data['model_trades'])
        assert data['model_trades'][0]['交易ID']==parent['交易ID']
        return {'model_trades':[{**r,'同步確認':True} for r in data['model_trades']],
                '_previous_version':'','_committed_version':'one'}
    t.remote_save=save
    t.sync_delta(catalog,['202610'])
    assert len(writes)==1 and len({r['交易ID'] for r in writes[0]})==2
    assert {r['交易ID'] for r in writes[0]}=={parent['交易ID'],child['交易ID']}
    assert all(r['同步確認'] for r in t.records())
