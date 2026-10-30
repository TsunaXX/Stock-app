import ast
import numpy as np
import pandas as pd
from test_core_calculations import APP_PATH, load_app_symbols


def test_offline_mixed_rows_missing_points():
    source = APP_PATH.read_text(encoding='utf-8')
    tree = ast.parse(source)
    loop = next(n for n in ast.walk(tree) if isinstance(n, ast.For) and
                ast.unparse(n.target) == '(i, row)' and 'ma5_val' in ast.unparse(n))
    frame = pd.DataFrame([{'收盤價':100, '漲跌幅':0, '_points':[{'tag':'多','val':98}]},
                          {'收盤價':90, '漲跌幅':0, '_ma5':np.nan},
                          {'收盤價':80, '漲跌幅':0, '_points':None}])
    symbols=load_app_symbols('price_change_amount', '_safe_number')
    symbols['df_display']=frame
    exec(compile(ast.Module(body=[loop],type_ignores=[]),str(APP_PATH),'exec'),symbols)
    assert frame.at[0,'5日線價差']==2
    assert pd.isna(frame.at[1,'5日線價差'])
    filter_points=load_app_symbols('filter_strategy_note_points','calculate_limits','get_tick_size')['filter_strategy_note_points']
    for points in (None,np.nan,[],[None]):
        assert filter_points(points,100)==[]


def test_prediction_name_highlight():
    check=load_app_symbols('has_trade_prediction_signal','parse_trade_plan_numbers')['has_trade_prediction_signal']
    for text in ('多｜進 100｜停 98｜目 104','觸價｜空｜進 100｜停 102｜目 96',
                 '多·已進場｜進 100｜停 98｜保 101｜目 104','多·停損｜進 100｜停 98｜目 104'):
        assert check(text)
    for text in (None,np.nan,'命中','昨高 100','等待｜進 100｜停 98｜目 104',
                 '行情待確認｜進 100｜停 98｜目 104','計畫待調整｜扣成本風報比不足'):
        assert not check(text)
    source=APP_PATH.read_text(encoding='utf-8')
    assert "has_trade_prediction_signal(source_row.get('進出場預判'))" in source
    assert "has_trade_prediction_signal(source_row.get('進出場點位'))" in source



def test_stock_and_futures_styler_signal_and_limit_priority():
    tree=ast.parse(APP_PATH.read_text(encoding='utf-8'))
    symbols=load_app_symbols('has_trade_prediction_signal','parse_trade_plan_numbers','_safe_number')
    symbols['strategy_row_limit_state']=lambda row,stock=True: row.get('_limit','')
    for name,column in [('style_tab1_df','進出場預判'),('style_futures_row','進出場點位')]:
        function=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name==name)
        exec(compile(ast.Module(body=[function],type_ignores=[]),str(APP_PATH),'exec'),symbols)
        frame=pd.DataFrame([{'名稱':'備援股票','狀態':'命中',column:'等待'},
                            {'名稱':'訊號股票',column:'多·已進場｜進 100｜停 98｜目 104'},
                            {'名稱':'漲停股票',column:'多｜進 100｜停 98｜目 104','_limit':'up'}]).fillna('')
        display=frame[['名稱']]
        styled=display.style.apply(symbols[name],axis=1,source_frame=frame)
        styled._compute()
        assert ('background-color','#ffeb3b') not in styled.ctx.get((0,0),[])
        assert ('background-color','#ffeb3b') in styled.ctx[(1,0)]
        assert ('background-color','#ff4b4b') in styled.ctx[(2,0)]
