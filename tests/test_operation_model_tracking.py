"""Plan-source simulations reuse subscribed evidence, never theoretical option premiums."""
from collections import deque
from datetime import date, datetime, timedelta
from types import SimpleNamespace
import io
import threading

import pandas as pd
import pytest
import pytz

from test_core_calculations import load_app_symbols
from model_tracking import DEFAULT_COSTS, ModelTracker, advance, new_trade, performance, excel_report, option_fills


NOW = pytz.timezone('Asia/Taipei').localize(datetime(2026, 10, 12, 10))


def adapter():
    ns = load_app_symbols('operation_model_spec', 'operation_model_samples', 'record_operation_models',
                          '_safe_number', 'parse_strategy_data_time')
    state = {'lock': threading.RLock(), 'strategy_stream_scopes': {'index': {'TMFJ6'}, 'options': {'C1', 'P1', 'P2'}},
             'model_tick_buffers': {}, 'aliases': {}}
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)
    ns.update(datetime=Clock, deque=deque, normalize_costs=lambda c: dict(DEFAULT_COSTS),
              load_config=lambda: {}, get_operation_model_version=lambda s:'test-'+s,
              get_futures_trading_date=lambda now:now, model_market_open=lambda *a:True,
              _stream_state=lambda api:state)
    return ns, state


def spec(ns, option=None, spread=False, direction='偏多', kind='日線／15分K', allowed=True):
    source = '選擇權操作計畫' if option else '指數操作計畫'
    return ns['operation_model_spec'](source, kind, {'contract_code':'TMFJ6'}, direction,
                                     20000., 19950. if direction == '偏多' else 20050.,
                                     20200. if direction == '偏多' else 19800., allowed, 1, 'default',
                                     {'VWAP':19990.}, '原計畫條件', option_quote=option, spread=spread)


def option(spread=False, put=False):
    result = {'name':'價差單' if spread else ('單買BP' if put else '單買BC'), 'expiry':date(2026,10,14),
              'contract':SimpleNamespace(code='P1' if put else 'C1'), 'model_probability':.99,
              'target_option_price':999999.}
    if spread:
        result.update(short_contract=SimpleNamespace(code='P1'), long_contract=SimpleNamespace(code='P2'),
                      short_strike=20000., long_strike=19900.)
    return result


def tick(state, code, price, second, sequence=None, epoch='test:0'):
    buf = state['model_tick_buffers'].setdefault(code, deque(maxlen=4096))
    buf.append({'價格':price, '來源時間':(NOW+timedelta(seconds=second)).isoformat(),
                '序號':sequence if sequence is not None else (buf[-1]['序號']+1 if buf else 1), '串流識別':epoch, '有效':True})


def sample(ns, specs, second):
    return ns['operation_model_samples'](specs, object(), 5, NOW+timedelta(seconds=second))


def test_index_plan_permission_direction_freeze_replay_and_costs(tmp_path):
    ns, state = adapter()
    waiting = spec(ns, allowed=False)
    tracker = ModelTracker(tmp_path/'index.sqlite', start=False)
    tick(state, 'TMFJ6', 20000., 0)
    tracker.process(*sample(ns, [waiting], 0))
    assert tracker.records()[0]['狀態'] == '等待進場'
    active = {**waiting, '進場許可':True, '目標價':20300.}
    tick(state, 'TMFJ6', 20001., 1)
    tracker.process(*sample(ns, [active], 1))
    holding = tracker.records()[0]
    assert holding['狀態'] == '模擬持倉' and holding['目標價'] == 20200.
    active = {**waiting, '進場許可':True}
    tick(state, 'TMFJ6', 20210., 2)
    payload = sample(ns, [active], 2)
    tracker.process(*payload)
    tracker.process(*payload)
    row = tracker.records()[0]
    assert len(tracker.records()) == 1 and row['狀態'] == '已平倉'
    assert row['毛損益'] == (20209.-20002.)*10
    assert 0 < row['模擬損益'] < row['毛損益']
    assert row['計畫來源'] == '指數操作計畫'
    assert performance(tracker.records())[0]['有效交易數'] == 1
    # Opposite live permission cannot enter an old, waiting long plan.
    row = new_trade(sample(ns, [waiting], 2)[0][0])
    quote = {**payload[1]['期貨|TMFJ6|日線／15分K'], '價格':20001., '進場許可':True, '許可方向':'偏空'}
    assert advance(row, quote)['狀態'] == '等待進場'


@pytest.mark.parametrize('put', [False, True])
def test_single_bc_bp_use_observed_premium_and_option_tax(tmp_path, put):
    ns, state = adapter()
    plan = spec(ns, option(put=put), direction='偏空' if put else '偏多')
    tracker = ModelTracker(tmp_path/'option.sqlite', start=False)
    for second, underlying, premium in ((0,19999. if not put else 20001.,30.),
                                         (1,20000.,32.), (2,20200. if not put else 19800.,47.)):
        tick(state, 'P1' if put else 'C1', premium, second)
        tick(state, 'TMFJ6', underlying, second)
        tracker.process(*sample(ns, [plan], second))
    row = tracker.records()[0]
    assert row['狀態'] == '已平倉' and row['毛損益'] == (46.5-32.5)*50
    code = 'P1' if put else 'C1'
    assert row['進場權利金'] == {code:32.5} and row['出場權利金'] == {code:46.5}
    assert row['交易成本'] == 104 and row['模擬損益'] == 596
    assert row['乘數'] == 50 and row['計畫來源'] == '選擇權操作計畫'
    assert 'target_option_price' not in row  # An exaggerated model estimate never becomes evidence.


def test_credit_spread_preserves_both_legs_and_costs(tmp_path):
    ns, state = adapter()
    plan = spec(ns, option(True), spread=True)
    tracker = ModelTracker(tmp_path/'spread.sqlite', start=False)
    for second, underlying, short, long in ((0,19999.,31.,13.), (1,20000.,30.,12.), (2,20200.,10.,4.)):
        tick(state, 'P1', short, second)
        tick(state, 'P2', long, second)
        tick(state, 'TMFJ6', underlying, second)
        tracker.process(*sample(ns, [plan], second))
    row = tracker.records()[0]
    assert row['進場權利金'] == {'P1':29.5, 'P2':12.5}
    assert row['出場權利金'] == {'P1':10.5, 'P2':3.9}
    assert row['毛損益'] == 520 and row['交易成本'] == 203 and row['模擬損益'] == 317
    assert len(row['選擇權腿']) == 2 and performance(tracker.records())[0]['有效交易數'] == 1


def test_missing_future_stale_leg_and_reconnect_never_create_verified_win(tmp_path):
    ns, state = adapter()
    plan = spec(ns, option(True), spread=True)
    tick(state, 'P1', 30., 0)
    tick(state, 'P2', 12., 1)  # Future price cannot be used at second zero.
    tick(state, 'TMFJ6', 20000., 0)
    signals, quotes = sample(ns, [plan], 0)
    assert len(signals) == 1 and not next(iter(quotes.values()))['有效']
    tick(state, 'TMFJ6', 20000., 3)  # P1 is now too old to establish a paired fill.
    assert not next(iter(sample(ns, [plan], 3)[1].values()))['有效']
    for code, price in [('P1',30.), ('P2',12.), ('TMFJ6',20000.)]:
        tick(state, code, price, 4)
    tracker = ModelTracker(tmp_path/'gap.sqlite', start=False)
    tracker.process(*sample(ns, [plan], 4))
    assert tracker.records()[0]['狀態'] == '模擬持倉'
    tick(state, 'TMFJ6', 20200., 7)
    tracker.process(*sample(ns, [plan], 7))
    assert tracker.records()[0]['資料缺口'] and performance(tracker.records())[0]['有效交易數'] == 0
    for code, price in [('P1',10.), ('P2',4.), ('TMFJ6',20200.)]:
        tick(state, code, price, 8, epoch='test:1')
    tracker.process(*sample(ns, [plan], 8))
    assert tracker.records()[0]['狀態'] == '已平倉'
    assert performance(tracker.records())[0]['有效交易數'] == 0
    state['connection_down'] = True
    assert not sample(ns, [plan], 8)[0]
    assert not sample(ns, [plan], 40)[0]


def test_premium_tick_boundaries_expiry_and_impossible_spread():
    ns, _ = adapter()
    row = new_trade({**spec(ns, option()), '交易日':NOW.date().isoformat(), '來源時間':NOW.isoformat()})
    for price, expected in ((9.9,10.), (49.5,50.), (499.,500.), (995.,1000.), (1000.,1010.)):
        q = {'來源時間':NOW.isoformat(), '權利金證據':{'C1':{'價格':price, '來源時間':NOW.isoformat()}}}
        assert option_fills(row,q,True) == {'C1':expected}
    expired = {**row, '到期時間':NOW.isoformat()}
    assert advance(expired, {'有效':True,'價格':20000.,'來源時間':NOW.isoformat(),'交易日':NOW.date().isoformat()})['狀態'] == '訊號失效'
    spread = new_trade({**spec(ns,option(True),spread=True), '交易日':NOW.date().isoformat(),'來源時間':NOW.isoformat()})
    q = {'來源時間':NOW.isoformat(), '權利金證據':{code:{'價格':p,'來源時間':NOW.isoformat()} for code,p in [('P1',200.),('P2',1.)]}}
    assert option_fills(spread,q,True) is None
    q['權利金證據']['P1']['價格'] = 101.
    assert option_fills(spread,q,False) == {'P1':102., 'P2':.9}  # Maximum-loss spread still includes adverse slippage.
    with pytest.raises(ValueError):
        new_trade({**spread, '選擇權腿':[{'代碼':'P1','買賣':-1}]})
    no_slip = {**row, '成本設定':{**DEFAULT_COSTS,'slippage_ticks':0}}
    q = {'有效':True,'價格':20000.,'來源時間':NOW.isoformat(),'交易日':NOW.date().isoformat(),
         '權利金證據':{'C1':{'價格':50.,'來源時間':NOW.isoformat()}}}
    holding = advance(no_slip,q)
    q.update(價格=20200.,來源時間=(NOW+timedelta(seconds=1)).isoformat())
    q['權利金證據']['C1']['來源時間'] = q['來源時間']
    assert advance(holding,q)['交易成本'] == 106  # 2.5 NTD tax per side rounds half up to 3.
    expired_holding = advance({**holding,'到期時間':NOW.isoformat()}, {**q,'有效':False})
    assert expired_holding['狀態'] == '資料不足' and expired_holding['到期未確認']
    assert advance(expired_holding,{**q,'來源時間':(NOW+timedelta(seconds=2)).isoformat()}) == expired_holding


def test_no_extra_broker_query_subscription_or_timer_and_gate_timestamp():
    ns, state = adapter()
    plan = spec(ns)
    assert sample(ns, [plan], 0) == ([], {'期貨|TMFJ6|日線／15分K': {
        '有效':False,'成交證據':[], '交易日':'2026-10-12','更新秒數':5,
        '來源時間':NOW.isoformat(), '休市':False, '進場許可':True,'許可方向':'偏多','進場許可時間':None,'收盤確認':False,'暖機等待':False}})
    assert set(state['model_tick_buffers']) == {'TMFJ6'}
    tick(state,'TMFJ6',20000.,0)
    old_signal = sample(ns,[plan],0)[0][0]
    tick(state,'TMFJ6',20001.,1)
    payload = sample(ns,[plan],1)[1]['期貨|TMFJ6|日線／15分K']
    old_tick = payload['成交證據'][0]
    assert advance(new_trade(old_signal), {**payload, **old_tick})['狀態'] == '等待進場'
    assert advance(new_trade(old_signal), payload)['狀態'] == '模擬持倉'


def test_active_option_continues_exit_when_recommendation_waits_and_storage_is_shared(tmp_path):
    ns, state = adapter()
    plan = spec(ns,option())
    for code,p in [('C1',32.),('TMFJ6',20000.)]:
        tick(state,code,p,0)
    desktop = ModelTracker(tmp_path/'desktop.sqlite',start=False)
    desktop.process(*sample(ns,[plan],0))
    desktop.process(*sample(ns,[plan],0))
    assert len(desktop.records()) == 1
    phone = ModelTracker(tmp_path/'phone.sqlite',start=False)
    with phone.db() as db:
        phone.store(db, desktop.records()[0])
    assert phone.active_plans('選擇權操作計畫','2026-10-12') == desktop.active_plans('選擇權操作計畫','2026-10-12')
    for code,p in [('C1',47.),('TMFJ6',20200.)]:
        tick(state,code,p,1)
    calls=[]
    ns['st'] = SimpleNamespace(session_state={'sj_logged_in':True})
    ns['get_model_tracker'] = lambda url:phone
    ns['get_app_secret'] = lambda key:''
    phone.submit=lambda signals,quotes: (calls.append(signals),phone.process(signals,quotes))
    ns['record_operation_models']('選擇權操作計畫', [], object(), 5)
    assert calls == [[]] and phone.records()[0]['狀態'] == '已平倉'
    assert phone.active_plans('選擇權操作計畫','2026-10-12') == []
    workbook = pd.read_excel(io.BytesIO(excel_report(phone.records())),sheet_name='歷史明細')
    assert workbook.iloc[0]['計畫來源'] == '選擇權操作計畫'
    assert 'C1' in workbook.iloc[0]['選擇權腿']


def test_incremental_cursor_keeps_transient_entry_and_exit_without_backfilling_permission(tmp_path):
    ns, state = adapter()
    tracker = ModelTracker(tmp_path/'transient.sqlite',start=False)
    plans = [spec(ns)]
    tick(state,'TMFJ6',19999.,0)
    initial = sample(ns,plans,0)
    tracker.process(*initial)
    cursor = {'TMFJ6|日線／15分K':{'stream':'test:0','sequence':1,'time':NOW.isoformat(),
                                  'allowed':True,'direction':'偏多'}}
    tick(state,'TMFJ6',20000.,1)
    tick(state,'TMFJ6',20200.,2)
    tick(state,'TMFJ6',20020.,3)
    payload = ns['operation_model_samples']([{**plans[0],'進場許可':False}],object(),5,NOW+timedelta(seconds=3),after=cursor)
    tracker.process(*payload)
    row = tracker.records()[0]
    assert row['狀態'] == '已平倉' and row['進場時間'] == (NOW+timedelta(seconds=1)).isoformat()
    assert row['結案時間'] == (NOW+timedelta(seconds=2)).isoformat()
    assert performance(tracker.records())[0]['有效交易數'] == 1
    # If permission had been off, later permission does not backfill the intermediate crossing.
    cursor['TMFJ6|日線／15分K']['allowed'] = False
    quotes = ns['operation_model_samples'](plans,object(),5,NOW+timedelta(seconds=3),after=cursor)[1]
    early = quotes['期貨|TMFJ6|日線／15分K']['成交證據'][1]
    assert not early['進場許可']
    # Replay is only an overlap plus new ticks, and a transport epoch restores the whole buffer.
    cursor['TMFJ6|日線／15分K']['sequence'] = 3
    assert len(ns['operation_model_samples'](plans,object(),5,NOW+timedelta(seconds=3),after=cursor)[1]['期貨|TMFJ6|日線／15分K']['成交證據']) == 2
    tick(state,'TMFJ6',20020.,4,epoch='test:1')
    assert len(ns['operation_model_samples'](plans,object(),5,NOW+timedelta(seconds=4),after=cursor)[1]['期貨|TMFJ6|日線／15分K']['成交證據']) == 5


def test_initial_live_plan_is_listed_before_ticks_and_first_verified_entry_can_count(tmp_path):
    ns,state = adapter()
    plan = {**spec(ns, option(True), spread=True), '觀察時間':NOW.isoformat(), '觀察價格':19999.}
    tracker = ModelTracker(tmp_path/'warming.sqlite',start=False)
    tracker.process(*sample(ns,[plan],0))
    row = tracker.records()[0]
    assert row['狀態'] == '資料不足' and not row['資料缺口'] and row.get('模擬進場價') is None
    assert performance([row])[0]['有效交易數'] == 0 and performance([row])[0]['資料完整率(%)'] == 0
    for second,price,p1,p2 in [(1,20000.,30.,12.),(2,20200.,10.,4.)]:
        for code,p in [('P1',p1),('P2',p2),('TMFJ6',price)]: tick(state,code,p,second)
        tracker.process(*sample(ns,[plan],second))
    assert len(tracker.records()) == 1 and performance(tracker.records())[0]['有效交易數'] == 1
