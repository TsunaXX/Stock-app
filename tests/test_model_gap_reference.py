"""Incomplete monitoring stays inspectable without becoming verified performance."""
import copy
import io
import threading

import pytest
from openpyxl import load_workbook

from model_tracking import (ModelTracker, advance, excel_report, gap_reference_trade, merge_trades,
                            new_trade, performance, reference_performance_rows, trade_display)
from test_core_calculations import load_app_symbols
from test_model_tracking import signal, quote


def completed(code='4977', pnl=-6871, gap=True):
    row=new_trade(signal(商品鍵=code,代碼=code,名稱=code,策略版本='V1.2-reference'))
    row.update({'狀態':'已平倉','模擬進場價':167.5,'模擬出場價':161.,
                '進場時間':'2026-10-08T09:31:00+08:00','結案時間':'2026-10-08T10:00:00+08:00',
                '毛損益':pnl+371,'交易成本':371,'模擬損益':pnl,'出場原因':'停損' if pnl<0 else '停利',
                '資料缺口':gap,'資料缺口原因':['監控中斷'] if gap else [],'同步確認':True,
                '未進場分類':'未觸價','最後進場等待原因':'尚未觸及原進場價'})
    return row


def test_verified_gap_and_combined_results_remain_distinct_and_do_not_mutate_history():
    rows=[completed(),completed('TMF',404),completed('2330',100,False)]
    before=copy.deepcopy(rows)
    metrics,_,frame=performance(rows)
    assert metrics['有效交易數']==1 and metrics['累積模擬損益']==100
    assert metrics['缺口參考交易數']==2 and metrics['缺口參考勝率(%)']==50
    assert metrics['缺口參考損益']==-6467
    assert metrics['合併參考交易數']==3 and metrics['合併參考勝率(%)']==pytest.approx(200/3)
    assert metrics['合併參考損益']==-6367
    assert [r['績效範圍'] for r in reference_performance_rows(metrics)]==['完整驗證','缺口參考','合併參考']
    assert frame['績效分類'].tolist()==['缺口參考','缺口參考','完整驗證']
    assert frame['未進場分類'].tolist()==['','','']
    assert rows==before and '缺口參考績效' in trade_display(rows[0])['狀態說明']
    assert rows[0]['最後進場等待原因']=='尚未觸及原進場價'


@pytest.mark.parametrize('changes',[
    {'狀態':'模擬持倉'}, {'模擬進場價':None}, {'模擬出場價':None}, {'模擬損益':float('nan')},
    {'毛損益':None}, {'毛損益':1e10,'模擬損益':1e10-372}, {'交易成本':-1}, {'模擬損益':-6000}, {'進場時間':''}, {'結案時間':''},
    {'結案時間':'2026-10-08T09:00:00+08:00'}, {'到期未確認':True},
    {'異常原因':'跨裝置模擬成交證據不一致，排除有效績效'},
])
def test_incomplete_or_conflicting_records_stay_saved_without_inflating_reference(changes):
    row={**completed(),**changes}
    assert not gap_reference_trade(row)
    metrics,_,frame=performance([row])
    assert len(frame)==1 and metrics['缺口參考交易數']==0 and metrics['合併參考交易數']==0
    assert metrics['缺口參考勝率(%)'] is None


def test_reference_handles_pending_sync_zero_profit_and_credit_option_prices():
    row=completed('OPTION',0)
    row.update({'模擬進場價':-100.,'模擬出場價':-101.,'同步確認':False})
    metrics,_,_=performance([row])
    assert metrics['有效交易數']==0 and metrics['缺口參考交易數']==1
    assert metrics['缺口參考勝率(%)']==0 and metrics['缺口參考獲勝']==0 and metrics['缺口參考虧損']==0
    assert performance([])[0]['合併參考勝率(%)'] is None


def test_cloud_merge_and_incremental_reclassification_do_not_double_count():
    row=completed()
    merged=merge_trades([row],[copy.deepcopy(row)])
    state={}
    metrics,_,_=performance(merged,state)
    assert metrics['缺口參考交易數']==1
    records=[*merged,completed('TMF',404)]
    assert performance(records,state)[0]['缺口參考交易數']==2 and state['added']==0
    records[0]={**row,'資料缺口':False}
    metrics,_,_=performance(records,state)
    assert metrics['有效交易數']==1 and metrics['缺口參考交易數']==1
    assert metrics['合併參考交易數']==2 and metrics['合併參考損益']==-6467


def test_excel_splits_reference_rows_and_preserves_all_original_trades_and_events():
    rows=[completed(),completed('TMF',404),completed('2330',100,False)]
    before=copy.deepcopy(rows)
    book=load_workbook(io.BytesIO(excel_report(rows)),data_only=True)
    values=list(book['參考績效'].values)
    summary=[dict(zip(values[0],row)) for row in values[1:]]
    assert summary[1]['交易數']==2 and summary[1]['勝率(%)']==50
    assert summary[2]['累積模擬損益']==-6367
    assert book['缺口參考交易'].max_row==3 and book['歷史明細'].max_row==4
    assert book['事件'].max_row==4
    headers={c.value:c.column for c in book['缺口參考交易'][1]}
    assert book['缺口參考交易'].cell(2,headers['模擬損益']).data_type=='n'
    assert rows==before


def tick(sequence,microsecond,price=100.):
    return quote(price,來源時間=f'2026-10-08T09:30:01.{microsecond:06d}+08:00',
                 序號=sequence,串流識別='api:0:2')


def test_observed_late_tick_advances_cursor_without_inventing_queue_loss_or_backdating_fill():
    row=advance(new_trade(signal()),tick(1,100000))
    late=advance(row,tick(2,50000,111.))
    assert late['最後序號']==2 and late['模擬進場價']==row['模擬進場價']
    assert late['來源時間']==row['來源時間'] and late['狀態']!='已平倉'
    result=advance(late,tick(3,200000))
    assert result['資料缺口'] and not any('佇列缺漏' in e['原因'] for e in result['事件'])
    diagnostic=next(e for e in result['事件'] if e['原因'].startswith('成交時間倒序'))
    assert diagnostic['缺漏證據']['原始來源時間']==tick(2,50000)['來源時間']
    replay=advance(result,tick(2,50000,111.))
    assert replay==result
    real_gap=advance(row,tick(4,50000))
    assert any('佇列缺漏' in e['原因'] for e in real_gap['事件'])


def test_preplan_late_evidence_is_acknowledged_without_marking_a_new_monitor_gap():
    row=advance(new_trade(signal()),tick(1,100000))
    row=advance(row,{**tick(2,0),'來源時間':'2026-10-08T09:29:59+08:00'})
    row=advance(row,tick(3,200000))
    assert row['最後序號']==3 and not row['資料缺口']


def test_sqlite_consumes_out_of_order_evidence_and_replay_keeps_one_record(tmp_path):
    tracker=ModelTracker(tmp_path/'late.sqlite',start=False)
    evidence=[tick(1,100000),tick(2,50000),tick(3,200000),tick(4,300000,111.)]
    payload={**evidence[-1],'成交證據':evidence}
    tracker.process([signal()],{'股票|2330':payload})
    rows=tracker.records()
    assert len(rows)==1 and rows[0]['狀態']=='已平倉' and rows[0]['最後序號']==4
    assert tracker.health['時間倒序筆數']==1
    assert not any('佇列缺漏' in e['原因'] for e in rows[0]['事件'])
    assert performance(rows)[0]['缺口參考交易數']==1
    before=copy.deepcopy(rows)
    tracker.process([],{'股票|2330':payload})
    assert tracker.records()==before
    tracker.close()


def test_buffer_reset_and_transport_reconnect_have_distinct_reasons():
    row=advance(new_trade(signal()),tick(1,100000))
    rebuilt=advance(row,{**tick(1,200000),'串流識別':'api:0:3'})
    reconnected=advance(row,{**tick(2,200000),'串流識別':'api:1:2'})
    assert '成交緩衝區重建；中斷區間未確認' in rebuilt['資料缺口原因']
    assert '串流重新連線；中斷區間未確認' in reconnected['資料缺口原因']


@pytest.mark.parametrize('owned',[False,True])
def test_live_ordinary_subscription_retains_buffer_but_last_auto_owner_releases(owned):
    ns=load_app_symbols('sync_strategy_stream_scope')
    state={'lock':threading.RLock(),'strategy_stream_scopes':{'stock':{'4977'}},
           'strategy_owned_subscriptions':{'stock':{'4977':{}} if owned else {}},
           'subscriptions':{'4977':{}},'model_tick_buffers':{'4977':['existing evidence']},
           'model_tick_sequences':{'4977':99},'strategy_histories':{}}
    ns['_stream_state']=lambda api:state
    ns['_unsubscribe_market_stream']=lambda api,cache,code,metadata:cache['subscriptions'].pop(code)
    ns['sync_strategy_stream_scope'](object(),[],'stock')
    assert ('4977' in state['model_tick_buffers']) is (not owned)
    assert ('4977' in state['subscriptions']) is (not owned)
    if not owned: assert state['model_tick_sequences']['4977']==99
