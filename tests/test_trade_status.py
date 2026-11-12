"""Explicit entry/exit states and invalidation reasons, without rewriting evidence."""
import copy
import ast
import io
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from openpyxl import load_workbook

from model_tracking import (ModelTracker, PROTECTION_POLICY, advance, event_message, excel_report, new_trade,
                            performance, plan_performance, trade_display)
from test_core_calculations import load_app_symbols
from test_model_tracking import quote, signal


def record(**changes):
    return new_trade(signal(交易日='2026-11-05', 來源時間='2026-11-05T09:02:00+08:00',
                            策略版本='V1.2-test', **changes))


@pytest.mark.parametrize('direction,entry,stop,target,price', [
    ('多頭',46.,45.8,46.4,45.8), ('多頭',46.1,45.8,46.6,45.8),
    ('空頭',65.7,66.3,64.8,66.3), ('空頭',45.05,45.8,43.55,45.95),
])
def test_preentry_stop_touch_is_risk_invalidation_not_end_of_session(direction,entry,stop,target,price):
    row = record(方向=direction,進場價=entry,停損價=stop,目標價=target)
    result = advance(row, quote(price,交易日='2026-11-05',來源時間='2026-11-05T09:02:01+08:00'))
    assert result['狀態'] == '訊號失效' and result.get('模擬進場價') is None
    assert result['未進場分類'] == '進場前風險失效'
    assert '尚未成交' in result['訊號失效原因'] and '停損' in result['訊號失效原因']
    assert '交易日已結束' not in result['訊號失效原因']


def test_session_end_and_expiry_remain_separate_from_stop_risk():
    original = record()
    ended = advance(original, quote(90,交易日='2026-11-06',來源時間='2026-11-06T09:02:01+08:00'))
    assert ended['未進場分類'] == '交易時段結束' and ended.get('模擬進場價') is None
    expired = advance({**original,'到期時間':'2026-11-05T09:02:01+08:00'},
                      quote(90,交易日='2026-11-05',來源時間='2026-11-05T09:02:01+08:00'))
    assert expired['未進場分類'] == '契約到期' and expired.get('模擬進場價') is None


def test_legacy_reason_is_explained_without_changing_trade_or_event_identity():
    row = {**record(), '狀態':'訊號失效','最新價':94.,
           '訊號失效原因':'未進場前已越過停損或交易日已結束','未進場分類':'超過有效時間',
           '事件':[{'事件ID':'saved-original','狀態':'訊號失效','原因':'未進場前已越過停損或交易日已結束'}]}
    before = copy.deepcopy(row)
    view = trade_display(row)
    assert view['未進場分類'] == '進場前風險失效' and view['交易狀態'] == '取消進場'
    assert '價格已觸及原停損' in event_message(row,row['事件'][0])
    assert view['交易ID'] == row['交易ID'] and view['事件'] == row['事件']
    assert row == before


def test_unfilled_gap_cannot_be_shown_as_a_position_and_closed_gap_cannot_be_a_verified_win():
    waiting = {**record(),'狀態':'資料不足','資料缺口':True}
    holding = {**waiting,'模擬進場價':100.}
    closed = {**holding,'狀態':'已平倉','模擬出場價':110.,'出場原因':'停利','模擬損益':300.}
    assert trade_display(waiting)['交易狀態'] == '待進場⚠'
    assert trade_display(holding)['交易狀態'] == '已進場⚠'
    shown = trade_display(closed)
    assert shown['交易狀態'] == '已出場（停利）⚠' and '不納入勝率與累積損益' in shown['狀態說明']
    assert performance([closed])[0]['有效交易數'] == 0
    assert '停利出場' in event_message(closed,{'狀態':'已平倉','原因':''})


def test_record_counts_plan_counts_and_verified_first_reentries_have_distinct_meanings():
    rows = []
    for index,pnl in enumerate((-120,100,200,5)):
        row = {**record(),'交易ID':f'first-{index}','原始計畫ID':f'plan-{index}',
               '狀態':'已平倉','模擬進場價':100.,'模擬出場價':101.,'結案時間':'2026-11-05T09:22:00+08:00',
               '模擬損益':pnl,'出場原因':'停損' if pnl<0 else '停利','同步確認':True}
        rows.append(row)
    rows += [{**record(),'交易ID':f'invalid-{i}','原始計畫ID':f'invalid-plan-{i}',
              '狀態':'訊號失效','最新價':95.,'未進場分類':'超過有效時間',
              '訊號失效原因':'未進場前已越過停損或交易日已結束'} for i in range(4)]
    rows.append({**rows[0],'交易ID':'second-0','進場次數':2,'前次執行ID':'first-0',
                 '資料缺口':True,'模擬損益':300.,'出場原因':'停利'})
    before = copy.deepcopy(rows)
    metrics,_,frame = performance(rows)
    plans,grouped = plan_performance(rows)
    assert metrics['追蹤執行紀錄數'] == 9 and metrics['含資料缺口的執行紀錄數'] == 1
    assert metrics['有效交易數'] == 4 and metrics['勝率(%)'] == 75 and metrics['累積模擬損益'] == 185
    assert plans['原始計畫數'] == 8 and plans['已觸發原始計畫數'] == 4
    assert plans['首次有效交易數'] == 4 and plans['二次以上有效交易數'] == 0
    assert plans['二次以上勝率(%)'] is None
    first = grouped[grouped['原始計畫ID']=='plan-0'].iloc[0]
    assert first['執行鏈完整'] and not first['行情證據完整']
    assert (frame['未進場分類']=='進場前風險失效').sum() == 4 and rows == before


@pytest.mark.parametrize('stock,room', [(True,'stock'),(True,'stock_independent'),
                                      (False,'futures'),(False,'futures_independent')])
def test_table_shows_position_exit_and_fresh_waiting_plan_without_stale_exit_override(stock,room):
    ns = load_app_symbols('apply_model_protection','parse_trade_plan_numbers','fmt_price')
    closed = {**record(),'模擬進場價':100.,'狀態':'已平倉','出場原因':'停損',
              '模擬出場價':94.,'模擬損益':-100.,'結案時間':'2026-11-05T09:22:57+08:00','同步確認':True}
    plans = {'B':{**record(),'狀態':'模擬持倉','模擬進場價':100.,'進場時間':'2026-11-05T09:10:00+08:00'},
             'C':{**closed,'交易ID':'closed-C'},'D':{**closed,'交易ID':'closed-D','出場原因':'停利'},
             'E':{**record(),'模擬進場價':100.,'狀態':'資料不足','進場次數':2,'資料缺口':True},
             'F':{**closed,'交易ID':'closed-F','出場原因':'停利','資料缺口':True}}
    before = copy.deepcopy(plans)
    notes,toasts = [],[]
    ns.update(trade_display=trade_display,
              st=SimpleNamespace(session_state={'sj_logged_in':True},caption=lambda text, **kw: notes.append(text),
                                 toast=lambda text,**kw:toasts.append(text)),
              get_app_secret=lambda key:'',get_model_tracker=lambda url:SimpleNamespace(protection_plans=lambda *a:plans),
              get_futures_trading_date=lambda day:day)
    column = '進出場預判' if stock else '進出場點位'
    frame = pd.DataFrame([{'代號':key,'契約鍵':key,'名稱':'商品'+key,'方向':'偏多','_系統方向':'多頭',
                           column:'進 112｜停 107｜目 124' if key in ('A','D') else '等待'} for key in 'ABCDEF'])
    show = ns['apply_model_protection']
    show(frame,stock,'當沖',room)
    values = frame[column].tolist()
    assert values[0].startswith('多·待進場｜') and values[1].startswith('多·已進場｜')
    assert values[2].startswith('多·已出場(損)｜')
    assert values[3].startswith('多·待進場｜進 112') and '停 107' in values[3]
    assert values[4].startswith('多·已進場⚠2次｜') and values[5].startswith('多·已出場(利)⚠｜')
    assert plans == before and not toasts and any('商品D（D）' in value for value in notes)
    assert 'color:#00c853' in notes[-1] and '>-100</strong>' in notes[-1] and '-100.00' not in notes[-1]
    show(frame,stock,'當沖',room)
    assert frame[column].tolist() == values and not toasts
    plans['B'] = {**closed,'交易ID':plans['B']['交易ID']}
    show(frame,stock,'當沖',room)
    assert frame.loc[1,column].startswith('多·已出場(損)｜')
    assert len(toasts) == 1 and '商品B（B）' in toasts[0] and '已出場' in toasts[0]
    show(frame,stock,'當沖',room)
    assert len(toasts) == 1


def test_export_adds_readable_statuses_but_keeps_original_events_ids_and_numeric_prices():
    row = {**record(),'狀態':'訊號失效','最新價':94.,'停損價':95.00000000001,
           '訊號失效原因':'未進場前已越過停損或交易日已結束',
           '事件':[{'事件ID':'original-event','時間':'2026-11-05T01:02:01.483062+00:00','狀態':'訊號失效',
                    '原因':'未進場前已越過停損或交易日已結束'}]}
    before = copy.deepcopy(row)
    book = load_workbook(io.BytesIO(excel_report([row])))
    values = list(book['歷史明細'].values)
    result = dict(zip(values[0],values[1]))
    assert result['交易ID'] == row['交易ID'] and result['交易狀態'] == '取消進場'
    assert result['未進場分類'] == '進場前風險失效' and result['停損價'] == row['停損價']
    assert book['歷史明細'].cell(2,values[0].index('停損價')+1).number_format == '#,##0.########'
    values = list(book['事件'].values)
    item = dict(zip(values[0],values[1]))
    assert item['事件ID'] == 'original-event' and item['原因'] == row['事件'][0]['原因']
    assert item['時間'] == '2026/11/05 09:02:01' and '尚未成交' in item['顯示訊息']
    assert row == before


def test_main_table_colors_identify_pending_holding_and_closed_without_losing_prices():
    ns = load_app_symbols('trade_plan_text_style','has_trade_prediction_signal','parse_trade_plan_numbers','_safe_number')
    ns['strategy_row_limit_state'] = lambda *a,**kw: ''
    tree = ast.parse((Path(__file__).parents[1]/'app.py').read_text(encoding='utf-8'))
    for name,column in [('style_tab1_df','進出場預判'),('style_futures_row','進出場點位')]:
        node = next(node for node in ast.walk(tree) if isinstance(node,ast.FunctionDef) and node.name==name)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'app.py','exec'),ns)
        texts = ['多·待進場','多·已進場2次','多·已出場(損)','多·已進場⚠2次']
        frame = pd.DataFrame([{'名稱':'測試商品',column:text+'｜進 100｜停 95｜目 110'} for text in texts])
        styled = frame.style.apply(ns[name],axis=1,source_frame=frame)
        styled._compute()
        assert ('color','#fbbf24') in styled.ctx[(0,1)]
        assert ('color','#67e8f9') in styled.ctx[(1,1)]
        assert ('color','#cbd5e1') in styled.ctx[(2,1)]
        assert ('color','#fbbf24') in styled.ctx[(3,1)]
        for text in frame[column]:
            assert ns['parse_trade_plan_numbers'](text) == {'entry':100.,'stop':95.,'target':110.}
        assert ns['has_trade_prediction_signal'](frame.iloc[0][column])
        assert not ns['has_trade_prediction_signal'](frame.iloc[2][column])


def test_event_times_are_taipei_seconds_and_no_samples_are_not_zero_percent():
    ns = load_app_symbols('_model_time_text','_model_display_value')
    assert ns['_model_display_value']('2026-11-05T01:02:01.483062+00:00','時間') == '2026/11/05 09:02:01'
    assert ns['_model_display_value'](None,'二次以上勝率(%)') == '—（無有效樣本）'
    assert ns['_model_display_value'](0.,'勝率(%)') == 0.


@pytest.mark.parametrize('cause',['停利','停損','保利'])
def test_same_update_close_and_reentry_reports_both_saved_transitions(tmp_path,cause):
    tracker = ModelTracker(tmp_path/'linked.sqlite',start=False)
    first = {**record(保利規則=PROTECTION_POLICY),'模擬進場價':100.,'狀態':'模擬持倉'}
    first_id = first['交易ID']
    with tracker.write_db() as db:
        tracker.store(db,first)
    ns = load_app_symbols('apply_model_protection','parse_trade_plan_numbers','fmt_price')
    class Clock(ns['datetime']):
        @classmethod
        def now(cls,tz=None):
            value = pd.Timestamp('2026-11-05T10:00:00+08:00').to_pydatetime()
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    ns['datetime'] = Clock
    messages = []
    ns.update(trade_display=trade_display, st=SimpleNamespace(
        session_state={'sj_logged_in':True},caption=lambda text, **kw:None,toast=lambda text,**kw:messages.append(text)),
        get_app_secret=lambda key:'',get_model_tracker=lambda url:tracker,get_futures_trading_date=lambda day:day)
    frame = pd.DataFrame([{'代號':'2330','名稱':'測試商品','_系統方向':'多頭','進出場預判':'進 100｜停 95｜目 110'}])
    ns['apply_model_protection'](frame,True,'當沖')
    assert not messages
    first.update({'狀態':'已平倉','出場原因':cause,'模擬損益':20.,'模擬出場價':101.,
                  '結案時間':'2026-11-05T09:22:57+08:00','同步確認':True})
    second = {**first,'交易ID':'new-execution','狀態':'模擬持倉','出場原因':'','模擬出場價':None,
              '進場次數':2,'前次執行ID':first_id,'進場時間':'2026-11-05T09:22:58+08:00'}
    second.pop('結案時間')
    with tracker.write_db() as db:
        tracker.store(db,first)
        tracker.store(db,second)
    ns['apply_model_protection'](frame,True,'當沖')
    assert len(messages)==1 and '已出場（'+cause+'）' in messages[0] and '已進場2次' in messages[0]
    assert messages[0].index('已出場') < messages[0].index('已進場')
    assert frame.iloc[0]['進出場預判'].startswith('多·已進場2次｜')
    ns['apply_model_protection'](frame,True,'當沖')
    assert len(messages)==1


def test_cloud_restore_order_cannot_hide_the_latest_exit(tmp_path):
    tracker = ModelTracker(tmp_path/'restore-order.sqlite',start=False)
    latest = {**record(保利規則=PROTECTION_POLICY),'交易ID':'newest-exit','狀態':'已平倉',
              '模擬進場價':100.,'出場原因':'停利','結案時間':'2026-11-05T13:13:47+08:00'}
    older = {**latest,'交易ID':'older-invalid','狀態':'訊號失效','模擬進場價':None,
             '結案時間':None,'訊號失效時間':'2026-11-05T09:22:57+08:00'}
    with tracker.write_db() as db:
        tracker.store(db,latest)
        tracker.store(db,older)  # Read-back insertion order is not execution time.
    shown = tracker.protection_plans('股票',['2330'],'當沖','2026-11-05')['2330']
    assert shown['交易ID']=='newest-exit' and shown['狀態']=='已平倉'
    holding = {**latest,'交易ID':'still-holding','狀態':'模擬持倉','結案時間':None,
               '進場時間':'2026-11-05T09:00:00+08:00'}
    with tracker.write_db() as db:
        tracker.store(db,holding)
    assert tracker.protection_plans('股票',['2330'],'當沖','2026-11-05')['2330']['交易ID']=='still-holding'


@pytest.mark.parametrize('closed_time',['2026-11-05T13:13:47+08:00','2026-11-04T16:30:00+00:00'])
def test_wave_closed_today_is_visible_even_when_it_entered_on_an_earlier_day(tmp_path,closed_time):
    tracker = ModelTracker(tmp_path/'wave-exit.sqlite',start=False)
    row = {**record(策略='波段',保利規則=PROTECTION_POLICY),'交易日':'2026-11-04',
           '狀態':'已平倉','模擬進場價':100.,'結案時間':closed_time,'出場原因':'停利'}
    with tracker.write_db() as db:
        tracker.store(db,row)
    assert tracker.protection_plans('股票',['2330'],'波段','2026-11-05')['2330']['交易ID']==row['交易ID']


def test_futures_wave_exit_uses_display_day_without_changing_trading_day(tmp_path):
    tracker = ModelTracker(tmp_path/'night-wave-exit.sqlite',start=False)
    row = {**record(策略='波段',保利規則=PROTECTION_POLICY),'市場':'期貨','商品鍵':'TX:202611',
           '交易日':'2026-11-04','狀態':'已平倉','模擬進場價':100.,
           '結案時間':'2026-11-06T16:00:00+08:00','出場原因':'保利'}
    with tracker.write_db() as db:
        tracker.store(db,row)
    shown=tracker.protection_plans('期貨',['TX:202611'],'波段','2026-11-09',None,'2026-11-06')
    assert shown['TX:202611']['交易ID']==row['交易ID']
    assert tracker.records()[0]['交易日']=='2026-11-04'


def test_old_target_touch_and_stale_quotes_are_not_fresh_reentry_candidates():
    ns=load_app_symbols('apply_model_protection','parse_trade_plan_numbers','fmt_price')
    row={**record(),'狀態':'已平倉','模擬進場價':100.,'出場原因':'停利',
         '結案時間':'2026-11-05T09:22:57+08:00','同步確認':True}
    ns.update(trade_display=trade_display,st=SimpleNamespace(session_state={'sj_logged_in':True},
              caption=lambda text, **kw:None,toast=lambda *a,**kw:None),get_app_secret=lambda key:'',
              get_model_tracker=lambda url:SimpleNamespace(protection_plans=lambda *a:{'2330':row}))
    for text,health in [('目標觸價｜進 100｜停 95｜目 110',''),('進 112｜停 107｜目 124','🔴 報價過期')]:
        frame=pd.DataFrame([{'代號':'2330','名稱':'測試商品','_系統方向':'多頭','進出場預判':text,'資料狀態':health}])
        ns['apply_model_protection'](frame,True,'當沖')
        assert frame.iloc[0]['進出場預判'].startswith('多·已出場(利)｜')
