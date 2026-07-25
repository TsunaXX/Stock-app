import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import time
import os
import itertools
import json
import re
from datetime import datetime, time as dt_time, timedelta, date
import pytz
from decimal import Decimal, ROUND_HALF_UP
import io
import twstock
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import gc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import streamlit.components.v1 as components
import urllib3
import pdfplumber
import fitz  # PyMuPDF 用於將 PDF 轉為圖片
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# 關閉 SSL 驗證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 引入 yahoo_fin 與 shioaji
try:
    import yahoo_fin.stock_info as si
except ImportError:
    si = None

try:
    import shioaji as sj
except ImportError:
    sj = None
# ==========================================
# 新增: 全域行事曆與權證判斷函數
# ==========================================
def get_holidays(year):
    h = {}
    if year == 2025:
         h.update({
             (1, 1): "元旦", (1, 27): "春節", (1, 28): "春節", (1, 29): "春節", (1, 30): "春節", (1, 31): "春節",
             (2, 3): "春節", (2, 28): "228紀念日", (4, 3): "兒童節", (4, 4): "清明節",
             (5, 1): "勞動節", (5, 30): "端午節", (10, 6): "中秋節", (10, 10): "國慶日"
         })
    if year == 2026:
        h.update({
            (1, 1): "元旦", (2, 11): "封關日", (2, 12): "市場無交易", (2, 13): "市場無交易",
            (2, 14): "春節", (2, 15): "春節", (2, 16): "春節", (2, 17): "春節",
            (2, 18): "春節", (2, 19): "春節", (2, 20): "春節", (2, 21): "春節", (2, 22): "春節",
            (2, 27): "和平紀念日(補)", (2, 28): "和平紀念日",
            (4, 3): "兒童節(補)", (4, 4): "兒童節", (4, 5): "清明節", (4, 6): "清明節(補)",
            (5, 1): "勞動節", (6, 19): "端午節", (9, 25): "中秋節", (9, 28): "教師節",
            (10, 9): "國慶日(補)", (10, 10): "國慶日", (10, 25): "光復節", (10, 26): "光復節(補)",
            (12, 25): "行憲紀念日"
        })
    return h

def is_market_closed_func(d_date):
    if d_date.weekday() >= 5: return True
    h_dict = get_holidays(d_date.year)
    name = h_dict.get((d_date.month, d_date.day), "")
    if name and name != "封關日": return True
    return False

def get_futures_trading_date(now_dt):
    """Map Taiwan local time to the active futures trading date."""
    trading_date = pd.Timestamp(now_dt.date())
    if now_dt.time() >= dt_time(15, 0):
        trading_date += pd.Timedelta(days=1)
    while is_market_closed_func(trading_date.date()):
        trading_date += pd.Timedelta(days=1)
    return trading_date.normalize()

def is_warrant(code):
    c = str(code)
    if c.startswith('00'): return False
    return len(c) > 4

# ==========================================
# 永豐 API (Shioaji) 擷取核心
# ==========================================
def fetch_shioaji_data(api, code, interval='1d', lookback_days=10):
    try:
        # ==========================================
        # 1. 暴力破解版：強迫取得「近月合約」物件
        # ==========================================
        contract = None
        is_future = False
        is_index = False
        
        if code in ["^TWII", "加權指數", "TSE", "加權指數(^TWII)"]:
            contract = api.Contracts.Indices.TSE.TSE01
            is_index = True
        elif code in ["TWF=F", "台指期貨", "TXF", "台指期貨(TWF=F)", "台指(全)", "台指期(全)", "台指期貨(全)"]:
            is_future = True
            try:
                contract = min(
                    [c for c in api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code],
                    key=lambda c: c.delivery_date
                )
            except (ValueError, AttributeError):
                contract = api.Contracts.Futures.TXF.TXFR1

        elif code in ["TMF=F", "微型台指期貨", "TMF", "微型台指", "微型台指期貨(TMF=F)", "微台(全)", "微台期(全)", "微型台指(全)", "微型台指期貨(全)"]:
            is_future = True
            # TMF 是微型台指；MXF 是小型台指。使用 R1 可讓歷史 K 棒依結算日
            # 自動換月，避免用「今天的近月實體合約」回查舊日期而混入不同月份資料。
            contract = api.Contracts.Futures.TMF.TMFR1
        else:
            try:
                contract = api.Contracts.Stocks[code]
            except:
                pass

        if not contract:
            return pd.DataFrame()

        # ==========================================
        # 2. 終極測試：把天數壓到最小極限值
        # ==========================================
        tz_tw = pytz.timezone('Asia/Taipei')
        now = datetime.now(tz_tw)
        end_date = now.strftime("%Y-%m-%d")
        
        if is_future or is_index:
            if interval in ['1d', '1wk', '1mo']:
                actual_lookback = min(lookback_days, 90)
            elif interval == '60m':
                actual_lookback = min(lookback_days, 12)
            elif interval == '15m':
                actual_lookback = min(lookback_days, 5)
            elif interval == '5m':
                actual_lookback = min(lookback_days, 3)
            else:
                actual_lookback = 1
        else:
            actual_lookback = min(lookback_days, 150) if interval in ['1d', '1wk', '1mo'] else 5
            
        start_date = (now - timedelta(days=actual_lookback)).strftime("%Y-%m-%d")

        # 3. 呼叫官方 api.kbars
        kbars_dict = None
        
        # Shioaji 單次 K 棒查詢有日期區間上限；日 K 也必須分段，否則 60 根
        # 費波樣本可能不完整。分 K 使用較短區間以控制單次資料量。
        max_chunk_days = 15 if interval in ['1m', '5m', '15m', '60m'] else 30
        if actual_lookback > max_chunk_days:
            all_ts, all_open, all_high, all_low, all_close, all_vol = [], [], [], [], [], []
            curr_end = now
            chunks = []
            
            while curr_end > now - timedelta(days=actual_lookback):
                curr_start = curr_end - timedelta(days=max_chunk_days)
                if curr_start < now - timedelta(days=actual_lookback):
                    curr_start = now - timedelta(days=actual_lookback)
                chunks.append((curr_start, curr_end))
                curr_end = curr_start - timedelta(days=1)
            
            for c_start, c_end in reversed(chunks):
                s_str = c_start.strftime("%Y-%m-%d")
                e_str = c_end.strftime("%Y-%m-%d")
                for attempt in range(2):
                    try:
                        k = api.kbars(contract=contract, start=s_str, end=e_str)
                        if k and hasattr(k, 'ts') and len(k.ts) > 0:
                            all_ts.extend(k.ts)
                            all_open.extend(k.Open)
                            all_high.extend(k.High)
                            all_low.extend(k.Low)
                            all_close.extend(k.Close)
                            all_vol.extend(k.Volume)
                            break
                        time.sleep(0.3)
                    except:
                        time.sleep(0.3)
            
            if all_ts:
                kbars_dict = {
                    'ts': all_ts, 'Open': all_open, 'High': all_high,
                    'Low': all_low, 'Close': all_close, 'Volume': all_vol
                }
        else:
            # 關鍵修正：分K (短天數) 直接單次抓取，不經過複雜的迴圈切割，速度最快且最穩
            for attempt in range(3):
                try:
                    kbars = api.kbars(contract=contract, start=start_date, end=end_date)
                    if kbars and hasattr(kbars, 'ts') and len(kbars.ts) > 0:
                        kbars_dict = {**kbars}
                        break
                    time.sleep(0.5)
                except Exception:
                    time.sleep(1.0)
        
        # 4. 依照官方文件轉換成 DataFrame 格式
        if not kbars_dict:
            st.session_state['sj_last_error'] = f"contract={getattr(contract, 'code', contract)} 期間內查無K棒（API回傳0筆，非例外錯誤）"
            return pd.DataFrame()
            
        df = pd.DataFrame(kbars_dict)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # 將時間設為 Index 並確保移除時區資訊以利畫圖
        if df['ts'].dt.tz is not None:
            df['ts'] = df['ts'].dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
        
        df.set_index('ts', inplace=True)
        
        # 確保擁有官方的開高低收量欄位
        agg_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}

        # 5. K棒週期重取樣
        if interval == '1m':
            pass
        elif interval in ['1d', '1wk', '1mo']:
            if is_future:
                # 1. 取得真實時間的浮點數小時 (例如 08:45 為 8.75) 以判斷是否為日盤
                original_hours = df.index.hour + df.index.minute / 60.0
                
                # 2. 處理期貨夜盤日K對齊 (將 T-1 15:00 ~ T 13:45 平移至 T 日)
                df.index = df.index + pd.Timedelta(hours=9)
                
                # 3. 動態判定開市日：加入「有實際成交量」才算開市 (過濾休市但 API 回傳 0 量假資料的情形)
                is_day_session = (original_hours >= 8.75) & (original_hours <= 13.75) & (df['Volume'] > 0)
                date_series = df.index.normalize()
                dates_with_day_session = date_series[is_day_session].unique()
                
                # 4. 計算現實世界中「當前」的期貨 T 盤日期，作為兜底目標
                now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
                current_t_date = get_futures_trading_date(now_tw)

                # 5. 建立日期映射：將沒有日盤的孤立夜盤往後併入有效的 T 盤
                all_dates = pd.Series(date_series.unique()).sort_values()
                date_mapping = {}
                
                for d in all_dates:
                    if d in dates_with_day_session:
                        date_mapping[d] = d
                    else:
                        future_valid = [fd for fd in all_dates if fd > d and fd in dates_with_day_session]
                        if future_valid:
                            date_mapping[d] = min(future_valid)
                        else:
                            # 找不到未來的日盤 (如目前的最新夜盤)，強制併入當前的「現實 T 盤日期」
                            target_date = current_t_date
                            if d > target_date:  # 防呆機制
                                target_date = d
                                while target_date.dayofweek >= 5:
                                    target_date += pd.Timedelta(days=1)
                            date_mapping[d] = target_date
                
                # 6. 套用動態修正
                df.index = date_series.map(date_mapping) + (df.index - date_series)

            if interval == '1d': df = df.resample('D').agg(agg_dict).dropna()
            elif interval == '1wk': df = df.resample('W-MON').agg(agg_dict).dropna()
            else: df = df.resample('M').agg(agg_dict).dropna()
            
            if is_future:
                df.index = df.index.normalize()
        else:
            resample_map = {'5m': '5min', '15m': '15min', '60m': '60min'}
            if interval in resample_map:
                df = df.copy()
                if is_future:
                    # 期貨：1分K為「結束時間」，需用 closed='right' 避免整點K棒吸收到上一小時的高低點
                    if interval == '60m':
                        day_mask = (df.index.time > dt_time(8, 45)) & (df.index.time <= dt_time(13, 45))
                        df_day = df[day_mask].resample('60min', closed='right', label='left', offset='45min').agg(agg_dict).dropna()
                        df_night = df[~day_mask].resample('60min', closed='right', label='left').agg(agg_dict).dropna()
                        df = pd.concat([df_day, df_night]).sort_index()
                    else:
                        df = df.resample(resample_map[interval], closed='right', label='left').agg(agg_dict).dropna()
                else:
                    # 個股：開盤第一筆為 09:00:00，若用 closed='right' 會被誤分到上一根空K棒，必須維持 closed='left'
                    df = df.resample(resample_map[interval], closed='left', label='left').agg(agg_dict).dropna()

        # 保留資料實際來源，讓圖表可以驗證商品與連續契約是否正確。
        df.attrs['shioaji_contract_code'] = getattr(contract, 'code', '')
        df.attrs['shioaji_target_code'] = getattr(contract, 'target_code', '')
        return df
    except Exception as e:
        print(f"Shioaji fetch error for {code}: {e}")
        st.session_state['sj_last_error'] = f"{type(e).__name__}: {e}"
        return pd.DataFrame()

# ==========================================
# 費波計算核心函數
# ==========================================
def get_taiwan_tick_size(price):
    if price < 10: return 0.01
    elif price < 50: return 0.05
    elif price < 100: return 0.1
    elif price < 500: return 0.5
    elif price < 1000: return 1
    else: return 5

def round_to_tick(price):
    tick = get_taiwan_tick_size(price)
    p_dec = Decimal(str(price))
    t_dec = Decimal(str(tick))
    rounded = (p_dec / t_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * t_dec
    return float(rounded)

def plot_fibonacci_chart(symbol, interval, lookback=60, font_size=15, ma_flags=None, ma_width=1.5, show_vol=True):
    if ma_flags is None:
        ma_flags = {'5': True, '10': True, '20': True, '60': True}

    code_map_fibo, name_map_fibo = load_local_stock_names()
    
    # 處理輸入(支援名稱或代號)
    raw_input = symbol.strip()
    ticker_code = raw_input
    display_name = raw_input

    # 針對大盤與期貨的特例處理 (支援含有"全"字的自訂輸入)
    if raw_input in ["^TWII", "加權指數", "加權指數(^TWII)", "加權股價指數(TAIEX)"]:
        ticker_code = "^TWII"
        display_name = "加權股價指數(TAIEX)"
    elif raw_input in ["TWF=F", "台指期貨", "臺股期貨", "台指", "小型台指", "台指期貨(TWF=F)", "臺股期貨(TX)", "台指(全)", "台指期(全)", "台指期貨(全)"]:
        ticker_code = "TWF=F"
        display_name = "臺股期貨(TX)"
    elif raw_input in ["TMF=F", "微型台指期貨", "微型臺指期貨", "微台", "微型台指", "微型台指期貨(TMF=F)", "微型臺指期貨(TMF)", "微台(全)", "微台期(全)", "微型台指(全)", "微型台指期貨(全)"]:
        ticker_code = "TMF=F"
        display_name = "微型臺指期貨(TMF)"
    else:
        if "(" in raw_input and raw_input.endswith(")"):
            name_part, code_part = raw_input.rsplit("(", 1)
            ticker_code = code_part[:-1]
            display_name = raw_input
        elif " " in raw_input:
            parts = raw_input.split(" ", 1)
            if parts[0].isdigit() or parts[0].endswith(".TW") or parts[0].endswith(".TWO"):
                ticker_code = parts[0]
                display_name = f"{parts[1]}({parts[0]})"
            elif parts[1].isdigit() or parts[1].endswith(".TW") or parts[1].endswith(".TWO"):
                ticker_code = parts[1]
                display_name = f"{parts[0]}({parts[1]})"
        else:
            if raw_input.isdigit():
                ticker_code = raw_input
                name = code_map_fibo.get(ticker_code, "")
                display_name = f"{name}({ticker_code})" if name else ticker_code
            else:
                if raw_input in name_map_fibo:
                    ticker_code = name_map_fibo[raw_input]
                    display_name = f"{raw_input}({ticker_code})"
                else:
                    ticker_code = raw_input

    ticker = ticker_code if (ticker_code.endswith(".TW") or ticker_code.endswith(".TWO") or ticker_code.startswith("^") or "=" in ticker_code) else f"{ticker_code}.TW"
    period_map = {"1m": "7d", "5m": "30d", "15m": "60d", "60m": "730d", "1d": "2y", "1wk": "2y", "1mo": "5y"}
    is_index = ticker.startswith('^') or 'TWF' in ticker or 'TMF' in ticker
    
    try:
        df = pd.DataFrame()
        raw_code = ticker_code.split('.')[0]
        sj_kbars_used = False
        sj_snap_used = False
        
        twstock_used = False
        explicit_ref_prev_close = None
        
        # 1. 優先使用永豐 API 獲取盤中即時 K 線
        if st.session_state.get('sj_logged_in', False):
            days_needed = {"1m": 3, "5m": 3, "15m": 5, "60m": 12, "1d": 150, "1wk": 730, "1mo": 1825}
            if interval in days_needed:
                req_days = days_needed[interval]
                sj_df = fetch_shioaji_data(st.session_state.sj_api, raw_code, interval=interval, lookback_days=req_days)
                if not sj_df.empty:
                    df = sj_df
                    sj_kbars_used = True

        # 若永豐未登入、沒抓到，直接退回使用 yfinance (已移除 cnyes 判斷)
        if not sj_kbars_used:
            import time
            for attempt in range(3):
                try:
                    stock_data = yf.Ticker(ticker)
                    df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    if not df.empty:
                        break
                    time.sleep(1) # 若遇 Rate limit 導致空資料，稍待後重試
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1)
                        continue
                    else:
                        break

            # 自動處理上櫃代號
            if (df.empty or 'High' not in df.columns) and ticker.endswith(".TW"):
                ticker_two = ticker.replace(".TW", ".TWO")
                for attempt in range(3):
                    try:
                        stock_data = yf.Ticker(ticker_two)
                        df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.droplevel(1)
                        if not df.empty:
                            ticker = ticker_two 
                            break
                        time.sleep(1)
                    except Exception as e:
                        if attempt < 2:
                            time.sleep(1)
                            continue
                        else:
                            break

           # 期貨異常保護 (移除自動替換加權指數邏輯)
            if (df.empty or 'High' not in df.columns) and (ticker == "TWF=F" or ticker == "TMF=F"):
                sj_status = "已登入" if st.session_state.get('sj_logged_in', False) else "未登入"
                st.warning(f"⚠️ 無法獲取 {display_name} 的資料。診斷：永豐API={sj_status}（{'有取得資料' if sj_kbars_used else '沒取得資料'}）；永豐詳細錯誤：{st.session_state.get('sj_last_error', '無')}。請確保網路連線正常或稍後再試。")
                return
                
            # 將 YF 的個股成交量 (股) 統一轉換為 (張)
            if not df.empty and not is_index and 'Volume' in df.columns:
                df['Volume'] = df['Volume'] / 1000

            # --- 新增：動態過濾歷史 K 線中的未開市日 (如颱風天) ---
            if not df.empty:
                if df.index.tzinfo is not None:
                    df.index = df.index.tz_convert('Asia/Taipei').tz_localize(None)
                
                # 自動判斷未開盤：若日K以上的交易量為 0，視為未開市自動剔除 (不依賴行事曆)
                if interval in ["1d", "1wk", "1mo"]:
                    drop_indices = df[df['Volume'] == 0].index
                    if not drop_indices.empty:
                        df.drop(index=drop_indices, inplace=True)
                
        # 透過 snapshots 即時快照更新圖表最後一筆資料
        if st.session_state.get('sj_logged_in', False) and not df.empty:
            try:
                contract_snap = None
                if ticker.startswith("^TWII"):
                    contract_snap = st.session_state.sj_api.Contracts.Indices.TSE.TSE01
                elif ticker == "TWF=F":
                    try:
                        contract_snap = min(
                            [c for c in st.session_state.sj_api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code],
                            key=lambda c: c.delivery_date
                        )
                    except (ValueError, AttributeError):
                        contract_snap = st.session_state.sj_api.Contracts.Futures.TXF.TXFR1
                elif ticker == "TMF=F":
                    contract_snap = st.session_state.sj_api.Contracts.Futures.TMF.TMFR1
                else:
                    try: 
                        contract_snap = st.session_state.sj_api.Contracts.Stocks[raw_code]
                    except:
                        try: contract_snap = getattr(st.session_state.sj_api.Contracts.Stocks, raw_code, None)
                        except: pass
                
                if contract_snap:
                    snap = st.session_state.sj_api.snapshots([contract_snap])
                    if snap and len(snap) > 0:
                        s = snap[0]
                        rt_price = s.close
                        rt_open = s.open
                        rt_high = s.high
                        rt_low = s.low
                        rt_vol = s.total_volume 
                        
                        # 擷取永豐快照的正確昨日參考價，避免計算漲跌幅異常
                        try:
                            # 修正：利用快照的 change_price 反推官方真實昨收(日盤基準價)，解決期貨夜盤基準落差
                            change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                            if change_val is not None and rt_price > 0:
                                explicit_ref_prev_close = rt_price - float(change_val)
                            elif hasattr(contract_snap, 'reference') and contract_snap.reference > 0:
                                explicit_ref_prev_close = float(contract_snap.reference)
                        except:
                            pass
                        
                        if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                        
                        tz_tw = pytz.timezone('Asia/Taipei')
                        now_tw_aware = datetime.now(tz_tw)
                        now_dt = now_tw_aware.replace(tzinfo=None)
                        if interval == "1d" and ticker in ["TWF=F", "TMF=F"]:
                            # 15:00 起的夜盤屬於下一個交易日；該未完成日 K
                            # 會在隔日日盤持續累加，不會另開一根日 K。
                            now_dt = get_futures_trading_date(now_tw_aware).to_pydatetime()
                        elif interval in ["1d", "1wk", "1mo"]:
                            now_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                            
                        if rt_price > 0:
                            is_before_open = False
                            current_time = datetime.now(tz_tw).time()
                            if ticker in ["TWF=F", "TMF=F"]:
                                if (dt_time(5, 0) <= current_time < dt_time(8, 45)) or (dt_time(13, 45) <= current_time < dt_time(15, 0)):
                                    is_before_open = True
                            else:
                                if current_time < dt_time(9, 0): is_before_open = True

                            # 新增：臨時狀況休市（如颱風天）攔截，若已過開盤時間且成交量為 0，視為休市不產生新 K 棒
                            is_temporary_closed = False
                            if ticker in ["TWF=F", "TMF=F"]:
                                if (dt_time(8, 50) <= current_time < dt_time(13, 45)) and rt_vol == 0:
                                    is_temporary_closed = True
                                elif (current_time >= dt_time(15, 5) or current_time < dt_time(5, 0)) and rt_vol == 0:
                                    is_temporary_closed = True
                            else:
                                if current_time >= dt_time(9, 5) and rt_vol == 0:
                                    is_temporary_closed = True

                            bucket_minutes_map = {'1m': 1, '5m': 5, '15m': 15, '60m': 60}
                            bucket_minutes = bucket_minutes_map.get(interval)
                            if bucket_minutes is not None:
                                is_new_bucket = now_dt >= (df.index[-1] + pd.Timedelta(minutes=bucket_minutes))
                            else:
                                is_new_bucket = df.index[-1] < now_dt  

                            if is_before_open or is_market_closed_func(now_dt.date()) or is_temporary_closed:
                                is_morning_premarket = (current_time < dt_time(9, 0)) if ticker not in ["TWF=F", "TMF=F"] else (dt_time(5, 0) <= current_time < dt_time(8, 45))
                                
                                # 補齊條件：非早晨盤前且「非臨時狀況休市」
                                if df.index[-1].date() < now_dt.date() and not is_market_closed_func(now_dt.date()) and not is_morning_premarket and not is_temporary_closed:
                                    now_dt_naive = now_dt if interval in ["1d", "1wk", "1mo"] else datetime.now(tz_tw).replace(tzinfo=None)
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt_naive])
                                    df = pd.concat([df, new_row])
                                else:
                                    # 未開盤/空窗/假日/臨時休市：只同步收盤價，不可更新高低 (避免扁平快照污染)
                                    ref_high = rt_high if interval in ["1d", "1wk", "1mo"] else rt_price
                                    ref_low = rt_low if interval in ["1d", "1wk", "1mo"] else rt_price
                                    df.at[df.index[-1], 'Close'] = rt_price
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), ref_high)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), ref_low)
                                    if interval in ["1d", "1wk", "1mo"]:
                                        df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                            elif is_new_bucket:
                                # 真正跨入新的一根K棒，才建立新row
                                if interval == "1d":
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                                elif interval in ["1m", "5m", "15m", "60m"]:
                                    same_day_mask = df.index.date == df.index[-1].date()
                                    today_known_vol = float(df.loc[same_day_mask, 'Volume'].sum())
                                    bar_vol = max(0.0, rt_vol - today_known_vol)
                                    new_row = pd.DataFrame([{'Open': rt_price, 'High': rt_price, 'Low': rt_price, 'Close': rt_price, 'Volume': bar_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                                else:
                                    new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt])
                                    df = pd.concat([df, new_row])
                            else:
                                # 仍在同一根K棒區間內：保留原本開盤價，只更新累積高低收
                                df.at[df.index[-1], 'Close'] = rt_price
                                if interval in ["1d", "1wk", "1mo"]:
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                                    df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                                else:
                                    # 分K不可套用快照「全盤累計」高低，只能用目前報價當作這根K棒目前已知的高低邊界
                                    df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_price)
                                    df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_price)
                            
                        sj_snap_used = True
            except Exception:
                pass
                
       # twstock 盤後修補方案 (修正: 支援盤中所有週期取得最新即時價格，以修復盤中漲跌幅度與標題顏色)
        if not sj_kbars_used and not sj_snap_used and not df.empty and not is_index:
            try:
                tz_tw = pytz.timezone('Asia/Taipei')
                now_tw = datetime.now(tz_tw)
                today_date = pd.Timestamp(now_tw.date())
                is_post_market = now_tw.time() >= dt_time(15, 0)
                
                rt_price = None
                rt_open = None
                rt_high = None
                rt_low = None
                rt_vol = 0.0

                rt_data = twstock.realtime.get(raw_code)
                if rt_data and rt_data.get('success') and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                    rt_price = float(rt_data['realtime']['latest_trade_price'])
                    rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                    rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                    rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                    rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                
                if is_post_market and (rt_price is None or rt_price == 0):
                    stock = twstock.Stock(raw_code)
                    if len(stock.date) > 0 and stock.date[-1].date() == today_date.date():
                        rt_price = float(stock.price[-1])
                        rt_open = float(stock.open[-1])
                        rt_high = float(stock.high[-1])
                        rt_low = float(stock.low[-1])
                        rt_vol = float(stock.capacity[-1]) / 1000 

                if rt_price not in [None, 0]:
                    if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                    last_hist_date = pd.Timestamp(df.index[-1].date())
                    
                    # 新增：未開盤時間攔截，防止重覆產生K線
                    current_time = datetime.now(tz_tw).time()
                    is_before_open = current_time < dt_time(9, 0)
                    is_temporary_closed = current_time >= dt_time(9, 5) and rt_vol == 0
                    
                    if interval == "1d":
                        if last_hist_date < today_date and not is_market_closed_func(now_tw.date()) and not is_before_open and not is_temporary_closed:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                            df = pd.concat([df, new_row])
                        else:
                            df.at[df.index[-1], 'Close'] = rt_price
                            df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                            df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                            df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                    elif interval in ["1m", "5m", "15m", "60m"]:
                        now_dt_naive = now_tw.replace(tzinfo=None)
                        # 判斷最後一根K棒時間，若落後即時時間則追加一根最新的，否則直接更新最後一根
                        if df.index[-1] < now_dt_naive and rt_price > 0 and not is_market_closed_func(now_tw.date()) and not is_temporary_closed:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt_naive])
                            df = pd.concat([df, new_row])
                        else:
                            df.at[df.index[-1], 'Close'] = rt_price
                            df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                            df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                    else:
                        df.at[df.index[-1], 'Close'] = rt_price
                        df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                        df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                        df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                        if df['Open'].iloc[-1] == 0: df.at[df.index[-1], 'Open'] = rt_open
                    twstock_used = True
            except Exception: pass

    except Exception as e:
        st.error(f"⚠️ 獲取數據失敗: {e}")
        return

    if df.empty or 'High' not in df.columns or 'Low' not in df.columns:
        st.warning(f"無法獲取有效的交易數據 ({ticker}, {interval})，可能是該區間無資料或代號錯誤。")
        return
    
    # 計算均線
    if ma_flags['5']: df['MA5'] = df['Close'].rolling(window=5).mean()
    if ma_flags['10']: df['MA10'] = df['Close'].rolling(window=10).mean()
    if ma_flags['20']: df['MA20'] = df['Close'].rolling(window=20).mean()
    if ma_flags['60']: df['MA60'] = df['Close'].rolling(window=60).mean()

    # 裁切近期 K 棒，加入 60 -> 45 -> 30 的回落機制
    lookbacks_to_try = [60, 45, 30] if lookback == 60 else [lookback]
    
    df_subset = pd.DataFrame()
    for lb in lookbacks_to_try:
        temp_subset = df.tail(lb).copy()
        temp_subset = temp_subset.dropna(subset=['High', 'Low'])
        if not temp_subset.empty:
            h = float(temp_subset['High'].max())
            l = float(temp_subset['Low'].min())
            if h != l:
                df_subset = temp_subset
                high_60 = h
                low_60 = l
                break

    if df_subset.empty:
        temp_subset = df.tail(lookback).copy().dropna(subset=['High', 'Low'])
        if temp_subset.empty:
            st.error(f"該股票 ({ticker}, {interval}) 的近期 K 線資料不完整或為空。")
            return
        else:
            st.warning(f"該股票 ({ticker}, {interval}) 近期高低點相同，無法畫出波段比例。")
            return

    diff = high_60 - low_60
    ratios = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
    
    # 費波顏色映射表
    color_map = {
        1.0: "#ff4b4b",
        0.786: "#ff9d00",
        0.618: "#7fff00",
        0.5: "#00ffff",
        0.382: "#1e90ff",
        0.236: "#9370db",
        0.0: "#ffffff"
    }
    
    fmt = '%Y-%m-%d %H:%M:%S'
    x_strings = df_subset.index.strftime(fmt).tolist()
    if interval in ["1d", "1wk", "1mo"]: x_display = df_subset.index.strftime('%Y-%m-%d').tolist()
    else: x_display = df_subset.index.strftime('%m-%d %H:%M').tolist()

    if show_vol and 'Volume' in df_subset.columns:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    else:
        fig = go.Figure()

    # Plotly 的 Candlestick 只支援漲跌兩色，收盤=開盤會被強制當作漲(紅)；
    # 改用三條trace疊加(各自只保留該類別資料、其餘設NaN)，正確呈現紅漲/綠跌/白平盤
    up_mask = df_subset['Close'] > df_subset['Open']
    down_mask = df_subset['Close'] < df_subset['Open']
    flat_mask = df_subset['Close'] == df_subset['Open']

    def _masked(col, mask):
        return df_subset[col].where(mask)

    for mask, color, show_legend in [(up_mask, '#ff4b4b', True), (down_mask, '#00e676', False), (flat_mask, '#ffffff', False)]:
        kline_trace = go.Candlestick(
            x=x_strings, open=_masked('Open', mask), high=_masked('High', mask),
            low=_masked('Low', mask), close=_masked('Close', mask), name="K線",
            increasing=dict(line=dict(color=color), fillcolor=color),
            decreasing=dict(line=dict(color=color), fillcolor=color),
            showlegend=show_legend
        )
        if show_vol and 'Volume' in df_subset.columns: fig.add_trace(kline_trace, row=1, col=1)
        else: fig.add_trace(kline_trace)

    # 繪製均線
    ma_settings = {
        'MA5': ('orange', ma_flags['5']),
        'MA10': ('lightblue', ma_flags['10']),
        'MA20': ('green', ma_flags['20']),
        'MA60': ('yellow', ma_flags['60'])
    }
    for ma_name, (color, is_show) in ma_settings.items():
        if is_show and ma_name in df_subset.columns:
            ma_trace = go.Scatter(x=x_strings, y=df_subset[ma_name], mode='lines', name=ma_name, line=dict(color=color, width=ma_width))
            if show_vol and 'Volume' in df_subset.columns: fig.add_trace(ma_trace, row=1, col=1)
            else: fig.add_trace(ma_trace)

    # 繪製成交量
    if show_vol and 'Volume' in df_subset.columns:
        colors = ['#ff4b4b' if close > open else ('#00e676' if close < open else '#ffffff') for close, open in zip(df_subset['Close'], df_subset['Open'])]
        vol_trace = go.Bar(x=x_strings, y=df_subset['Volume'], name="成交量", marker_color=colors)
        fig.add_trace(vol_trace, row=2, col=1)

    high_idx_str = df_subset['High'].idxmax().strftime(fmt)
    low_idx_str = df_subset['Low'].idxmin().strftime(fmt)
    disp_high = round_to_tick(high_60)
    disp_low = round_to_tick(low_60)
    
    target_row = 1 if (show_vol and 'Volume' in df_subset.columns) else None
    target_col = 1 if (show_vol and 'Volume' in df_subset.columns) else None

    fig.add_annotation(x=high_idx_str, y=high_60, text=f"最高:{disp_high:g}", showarrow=True, arrowhead=1, yshift=10, font=dict(color="red", size=font_size), row=target_row, col=target_col)
    fig.add_annotation(x=low_idx_str, y=low_60, text=f"最低:{disp_low:g}", showarrow=True, arrowhead=1, ay=40, font=dict(color="green", size=font_size), row=target_row, col=target_col)

    last_date_str = x_strings[-1]
    first_date_str = x_strings[0]
    
    for r in ratios:
        price = low_60 + r * diff
        rounded_price = round_to_tick(price)
        line_col = color_map.get(r, "rgba(150, 150, 150, 0.5)")
        
        fig.add_shape(type="line", x0=first_date_str, y0=price, x1=last_date_str, y1=price,
            line=dict(color=line_col, width=1, dash="dash" if r not in [0, 1] else "solid"), row=target_row, col=target_col)
            
        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
        fig.add_annotation(x=last_date_str, y=price, text=f"{r_label} ({rounded_price:g})",
            showarrow=False, xanchor="left", xshift=10, font=dict(size=font_size, color=line_col), row=target_row, col=target_col)

    y_min_view = low_60 - diff * 1.05
    y_max_view = high_60 + diff * 0.05
    if pd.isna(y_min_view) or pd.isna(y_max_view): y_min_view, y_max_view = None, None

    interval_display_map = {"1m": "1分K", "5m": "5分K", "15m": "15分K", "60m": "60分K", "1d": "日K", "1wk": "週K", "1mo": "月K"}
    interval_name = interval_display_map.get(interval, interval)
    ticker_suffix = ".TW" if ticker.endswith(".TW") else (".TWO" if ticker.endswith(".TWO") else "")
    
    try:
        last_date_obj = df_subset.index[-1]
        if interval in ["1d", "1wk", "1mo"]:
            date_str = last_date_obj.strftime('%Y/%m/%d')
        else:
            date_str = last_date_obj.strftime('%Y/%m/%d %H:%M')

        op = float(df_subset['Open'].iloc[-1])
        hi = float(df_subset['High'].iloc[-1])
        lo = float(df_subset['Low'].iloc[-1])
        cl = float(df_subset['Close'].iloc[-1])
        vol = float(df_subset['Volume'].iloc[-1]) if 'Volume' in df_subset.columns else 0.0

       # 修正：總漲跌幅必須與「日線級別的昨日收盤價 (日盤)」對比，不能與「上一根分K」對比
        if explicit_ref_prev_close is not None and explicit_ref_prev_close > 0:
            daily_ref_close = explicit_ref_prev_close
        else:
            if interval in ["1d", "1wk", "1mo"]:
                daily_ref_close = float(df['Close'].iloc[-2]) if len(df) > 1 else cl
            else:
                last_date = df.index[-1].date()
                past_df = df[df.index.date < last_date]
                if not past_df.empty:
                    daily_ref_close = float(past_df['Close'].iloc[-1])
                else:
                    daily_ref_close = float(df['Close'].iloc[-2]) if len(df) > 1 else cl

        chg = cl - daily_ref_close
        pct_chg = (chg / daily_ref_close * 100) if daily_ref_close > 0 else 0.0

        chg = round(chg, 2)
        pct_chg = round(pct_chg, 2)
        color = "#ff4b4b" if chg > 0 else ("#00e676" if chg < 0 else "white")
        sign = "+" if chg > 0 else ""

        # K棒內部變色邏輯維持與「上一根K棒收盤價」對比 (解決連續區段跳空落差)
        prev_k_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else op

        def get_ohlc_color(val, ref):
            if val > ref: return "#ff4b4b"
            elif val < ref: return "#00e676"
            return "white"

        # 標題上的開高低收文字，使用上一根K棒收盤價(prev_k_close)變色
        color_op = get_ohlc_color(op, prev_k_close)
        color_hi = get_ohlc_color(hi, prev_k_close)
        color_lo = get_ohlc_color(lo, prev_k_close)
        color_cl = get_ohlc_color(cl, prev_k_close)
        
        # 主名稱與總漲跌幅的顏色，則使用正確的日盤昨收 (daily_ref_close)
        color_main = get_ohlc_color(cl, daily_ref_close)

        if is_index:
            if ticker == '^TWII':
                if vol == 0 or pd.isna(vol):
                    vol_num = "無資料(缺漏)"
                    vol_unit = ""
                else:
                    vol_num = f"{vol/100000000:.2f}" if vol > 100000000 else f"{vol:,.2f}"
                    vol_unit = " 億"
            else:
                vol_num = f"{vol:,.0f}"
                vol_unit = " 單位(口)"
            price_unit = " 點"
        else:
            vol_num = f"{vol:,.0f}"
            vol_unit = " 張"
            price_unit = " 元"

        disp_title = display_name.replace('(^TWII)', '(TSE)') if ticker == '^TWII' else display_name
        
        # 標題改為各自獨立套用顏色
        title_html = (
            f"<span style='color:{color_main};'>{disp_title}{ticker_suffix}</span> - {interval_name} {date_str} "
            f"開 <span style='color:{color_op};'>{op:.2f}</span> "
            f"高 <span style='color:{color_hi};'>{hi:.2f}</span> "
            f"低 <span style='color:{color_lo};'>{lo:.2f}</span> "
            f"收 <span style='color:{color_cl};'>{cl:.2f}</span>{price_unit} "
            f"量 {vol_num}{vol_unit} "
            f"<span style='color:{color_main};'>{sign}{chg:.2f}({sign}{pct_chg:.2f}%)</span>"
        )
    except Exception:
        title_html = f"{display_name}{ticker_suffix} - {interval_name}"

    layout_update = dict(
        title=dict(text=title_html, font=dict(size=16)),
        template="plotly_dark",
        height=800 if show_vol else 700,
        showlegend=True,
    )

    if show_vol and 'Volume' in df_subset.columns:
        fig.update_yaxes(title_text="點數", range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False, row=1, col=1)
        fig.update_yaxes(title_text="成交量", fixedrange=False, row=2, col=1)
        fig.update_xaxes(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False, rangeslider_visible=False, row=2, col=1)
        fig.update_xaxes(type='category', showgrid=False, rangeslider_visible=False, showticklabels=False, row=1, col=1) 
    else:
        layout_update.update(
            yaxis_title="點數",
            yaxis=dict(range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False),
            xaxis=dict(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False),
            xaxis_rangeslider_visible=False
        )

    fig.update_layout(**layout_update)
    st.plotly_chart(fig, width='stretch')
    
    fetch_time_str = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')
    
    if sj_kbars_used:
        contract_code = df.attrs.get('shioaji_contract_code', '')
        target_code = df.attrs.get('shioaji_target_code', '')
        resolved_contract = f" → {target_code}" if target_code else ""
        data_source_text = f"永豐 Shioaji API (K線合約：{contract_code}{resolved_contract})"
    elif sj_snap_used:
        data_source_text = "YF歷史 + 永豐即時快照"
    elif twstock_used:
        data_source_text = "YF歷史 + Twstock即時"
    else:
        data_source_text = "YF 歷史數據"
        
    session_note = "；日K口徑：夜盤 15:00 起＋次日日盤，依查詢時間動態更新" if (sj_kbars_used and interval == '1d' and ticker in ['TWF=F', 'TMF=F']) else ""
    st.caption(f"📊 數據最後更新時間: {fetch_time_str} ({data_source_text}{session_note})")


# ==========================================
# 網路爬蟲加入快取，避免切換分頁時卡死
# ==========================================
@st.cache_data(ttl=3600, max_entries=2, show_spinner=False)
def fetch_fubon_html(url):
    """解決富邦 DJ 拒絕 iframe 連線的問題、處理亂碼與排版"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10, verify=False)
        r.encoding = 'cp950' 
        html = r.text
        
        # 強制轉換 meta charset 避免瀏覽器以預設編碼解析造成亂碼
        html = re.sub(r'charset=["\']?(big5|utf-8|cp950)["\']?', 'charset=utf-8', html, flags=re.IGNORECASE)
        
        # 注入 base 標籤與 CSS：消除右方空白但保留預設字體大小
        injection = '''
        <base href="https://fubon-ebrokerdj.fbs.com.tw/">
        <meta charset="utf-8">
        <style>
            body { margin: 0 !important; padding: 0 !important; text-align: left; background-color: white;}
            center { text-align: left !important; margin: 0 !important; }
            table { margin: 0 !important; width: 100% !important; max-width: 100% !important; }
            .dj-container, .wrapper { margin: 0 !important; padding: 0 !important; width: 100% !important; }
            a { text-decoration: none; color: #333; }
        </style>
        '''
        html = re.sub(r'<head>', f'<head>{injection}', html, flags=re.IGNORECASE)
        return html
    except Exception as e:
        return f"<html><body><h3>無法載入資料: {e}</h3></body></html>"

@st.cache_data(ttl=600, max_entries=1, show_spinner=False)
def get_report_list():
    """爬取永豐期貨盤後快訊列表，嚴格過濾台指期籌碼快訊"""
    url = "https://www.spf.com.tw/sinopacSPF/research/list.do?id=1709f20d3ff00000d8e2039e8984ed51"
    base_url = "https://www.spf.com.tw"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        reports = []
        items = soup.select('div.list_news ul li')
        
        if items:
            for item in items:
                link_tag = item.find('a')
                date_tag = item.find('span', class_='date')
                if link_tag:
                    title = link_tag.get_text(strip=True)
                    # 嚴格過濾: 只有標題含有 "台指期籌碼快訊" 才會抓取
                    if "台指期籌碼快訊" not in title:
                        continue
                        
                    href = link_tag['href']
                    pdf_url = f"{base_url}{href}" if href.startswith('/') else f"{base_url}/sinopacSPF/research/{href}"
                    
                    date_str = ""
                    date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                    if date_match:
                        date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                    elif date_tag:
                        date_str = date_tag.get_text(strip=True).replace("/", "-")
                    else:
                        date_str = "近期發布"
                        
                    reports.append({"日期": date_str, "title": title, "url": pdf_url})
        else:
            # 舊的 DOM 結構 fallback
            for a_tag in soup.find_all('a', href=re.compile(r'\.pdf')):
                title = a_tag.get_text(strip=True) or a_tag.get('title', '')
                if "台指期籌碼快訊" not in title:
                    continue
                    
                href = a_tag['href']
                pdf_url = f"{base_url}{href}" if href.startswith('/') else href
                
                date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                if date_match:
                    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                else:
                    parent = a_tag.find_parent(['tr', 'li'])
                    dt_span = parent.find(text=re.compile(r'202\d[/-]\d{2}[/-]\d{2}')) if parent else None
                    date_str = re.search(r'202\d[/-]\d{2}[/-]\d{2}', dt_span).group() if dt_span else "近期發布"

                reports.append({"日期": date_str, "title": title, "url": pdf_url})
                
        # 過濾重複連結並依照日期降冪排列 (確保第一筆一定是最新的)
        unique_reports = []
        seen = set()
        for r in reports:
            if r['url'] not in seen:
                unique_reports.append(r)
                seen.add(r['url'])
                
        unique_reports.sort(key=lambda x: x['日期'], reverse=True)
        return unique_reports
    except Exception as e:
        return []

@st.cache_data(ttl=600, max_entries=1, show_spinner=False)
def fetch_and_parse_pdf(pdf_url):
    """下載、解析數值並將 PDF 轉為圖片供直接預覽"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        if not pdf_url.lower().endswith('.pdf'):
            r_inner = requests.get(pdf_url, headers=headers, timeout=10, verify=False)
            soup_inner = BeautifulSoup(r_inner.text, 'html.parser')
            for tag in soup_inner.find_all(['a', 'iframe']):
                link = tag.get('href') or tag.get('src')
                if link and link.lower().endswith('.pdf'):
                    pdf_url = link
                    if not pdf_url.startswith('http'):
                        pdf_url = "https://www.spf.com.tw" + pdf_url
                    break

        response = requests.get(pdf_url, headers=headers, timeout=15, verify=False)
        pdf_bytes = response.content
        
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
            
        ratio_match = re.search(r"散戶小台多空比[:：]\s*([-+]?[\d\.]+)%", text)
        ratio = ratio_match.group(1) if ratio_match else "N/A"
        
        images = []
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            # 記憶體優化：最多只轉譯前 2 頁，並將解析度降到 72 dpi，防止大檔撐爆記憶體
            for page_idx in range(min(len(doc), 2)):
                page = doc[page_idx]
                pix = page.get_pixmap(dpi=100)
                img_bytes = io.BytesIO(pix.tobytes("png"))
                img_obj = Image.open(img_bytes)
                img_obj.load()
                buf = io.BytesIO()
                img_obj.save(buf, format='PNG')
                images.append(buf.getvalue())
                del img_obj
            doc.close()  
        except Exception as e:
            pass
            
        return {
            "ratio": ratio,
            "images": images
        }
    except Exception as e:
        return {"ratio": "解析錯誤", "images": []}

@st.cache_data(ttl=1800, max_entries=2, show_spinner=False)
def get_major_institutional_data(date_str):
    """從證交所 API 抓取三大法人買賣金額統計 (套用正確 API 結構)"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={date_str}&response=json"
    try:
        response = requests.get(url, timeout=5, verify=False)
        data = response.json()
        
        if data.get("stat") != "OK":
            return None
        
        # 轉換為 DataFrame
        df = pd.DataFrame(data["data"], columns=data["fields"])
        
        # 清理數據：移除千分號並轉為數字
        cols_to_fix = ['買進金額', '賣出金額', '買賣差額']
        for col in cols_to_fix:
            df[col] = df[col].astype(str).str.replace(',', '').astype(float)
            
        return df
    except Exception as e:
            # 拒絕回傳 None，透過拋出異常來阻止 Streamlit 將失敗狀態寫入快取
            raise RuntimeError(f"連線異常不寫入快取: {e}")

def color_negative_positive(val):
    """定義表格文字顏色：正數紅、負數綠"""
    if isinstance(val, (int, float)):
        color = '#ff4b4b' if val > 0 else '#00e676' if val < 0 else 'white'
        return f'color: {color}'
    return ''

@st.cache_data(ttl=3600, max_entries=2, show_spinner=False)
def get_tw_stocker_data(direction):
    url = f"https://voidful.github.io/tw-institutional-stocker/data/top_three_inst_change_20_{direction}.json"
    try:
        r = requests.get(url, timeout=3, verify=False)
        if r.status_code == 200:
            data = r.json()
            if data:
                df = pd.DataFrame(data).head(20)
                if 'code' in df.columns:
                    df = df.rename(columns={
                        'code': '代號',
                        'name': '名稱',
                        'change': '持股變化(%)',
                        'three_inst_ratio': '三大法人持股(%)'
                    })
                    return df[['代號', '名稱', '持股變化(%)', '三大法人持股(%)']]
    except:
        pass
    return pd.DataFrame()

def fetch_goodinfo_data():
    url = "https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%28%E7%95%B6%E6%97%A5%29%40%40%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%40%40%E7%95%B6%E6%97%A5"
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    # --- 新增：極致記憶體瘦身設定 ---
    chrome_options.add_argument("--single-process") 
    chrome_options.add_argument("--no-zygote")      
    chrome_options.add_argument("--disable-software-rasterizer")
    # -------------------------------
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

    chrome_options.binary_location = "/usr/bin/chromium"

    try:
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
            """
        })
        
        driver.get(url)
        time.sleep(15) # 等待動態表格載入
        
        html = driver.page_source
        
        tables = pd.read_html(io.StringIO(html))
        
        target_df = None
        for df in tables:
            if df.shape[0] > 10 and df.shape[1] > 5:
                if target_df is None or (df.shape[0] * df.shape[1] > target_df.shape[0] * target_df.shape[1]):
                    target_df = df
                    
        if target_df is not None:
            if isinstance(target_df.columns, pd.MultiIndex):
                new_columns = []
                for col in target_df.columns:
                    cleaned_parts = []
                    for item in col:
                        # 加上 .replace(' ', '') 移除所有空白字元
                        item_str = str(item).replace(' ', '').strip()
                        if item_str and not item_str.startswith('Unnamed'):
                            if not cleaned_parts or cleaned_parts[-1] != item_str:
                                cleaned_parts.append(item_str)
                    new_columns.append('_'.join(cleaned_parts))
                target_df.columns = new_columns
            else:
                # 預防若表格非多層欄位，也一併移除空白
                target_df.columns = [str(c).replace(' ', '').strip() for c in target_df.columns]
            return target_df
    except Exception as e:
        st.error(f"系統發生異常: {e}")
        return None
    finally:
        if 'driver' in locals():
            driver.quit()
    return None

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="台股全盤戰略室", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    [data-testid="stSidebar"] button { white-space: nowrap !important; text-overflow: clip !important; padding-left: 5px !important; padding-right: 5px !important; }
    div.stButton > button { min-height: 45px; font-size: 20px; }
    .stButton { margin-top: 5px; }
    .calendar-header { font-size: 2.5em; font-weight: 900; text-align: center; color: #ff9800; margin-bottom: 10px; line-height: 1.5; font-family: 'Arial', sans-serif; }
    .cal-box { text-align: center; padding: 5px; border-radius: 4px; margin: 2px; min-height: 90px; border: 1px solid #555; font-size: 0.9em; display: flex; flex-direction: column; justify-content: space-between; }
    .cal-open { background-color: #000000 !important; color: #ffffff !important; }
    .cal-closed { background-color: #d32f2f !important; color: #ffffff !important; font-weight: bold; }
    .cal-week { background-color: #f0f0f0; color: #333; font-weight: bold; display: flex; align-items: center; justify-content: center; font-size: 0.8em; }
    .settle-m { color: #ffff00; font-weight: bold; font-size: 0.85em; margin-top: 2px; line-height: 1.2; } 
    .settle-w { color: #00e676; font-size: 0.8em; margin-top: 2px; } 
    .settle-f { color: #29b6f6; font-size: 0.8em; margin-top: 2px; } 
    .holiday-tag { font-size: 0.85em; margin-bottom: 2px; color: #ffeb3b; background-color: rgba(0,0,0,0.5); border-radius: 3px; padding: 1px;}
    .today-border { border: 3px solid #ffff00 !important; }
    div[data-testid="column"] { text-align: center; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ 台股全盤戰略室 ⚡")

CONFIG_FILE = "config.json"
DATA_CACHE_FILE = "data_cache.json"
URL_CACHE_FILE = "url_cache.json"
SEARCH_CACHE_FILE = "search_cache.json"
DEFAULT_FIBO_TAGS = ["台積電(2330)", "鴻海(2317)", "聯發科(2454)", "和椿(6215)", "晶彩科(3535)"]
ANALYSIS_MAX_WORKERS = 2
API_REQUEST_GAP_SECONDS = 0.1

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except Exception: return {}
    return {}

def save_config(font_size, limit_rows, auto_update, delay_sec, sj_key="", sj_secret="", remember_sj=False):
    try:
        config = load_config()
        config.update({
            "font_size": font_size, 
            "limit_rows": limit_rows, 
            "auto_update": auto_update, 
            "delay_sec": delay_sec,
            "sj_key": sj_key if remember_sj else "",
            "sj_secret": sj_secret if remember_sj else "",
            "remember_sj": remember_sj
        })
        with open(CONFIG_FILE, "w", encoding="utf-8") as f: json.dump(config, f)
        return True
    except Exception: return False

def save_fibo_config():
    config = load_config()
    fibo_tags = [
        st.session_state.get('custom_tag_1', "台積電(2330)"), 
        st.session_state.get('custom_tag_2', "鴻海(2317)"), 
        st.session_state.get('custom_tag_3', "聯發科(2454)"), 
        st.session_state.get('custom_tag_4', "和椿(6215)"), 
        st.session_state.get('custom_tag_5', "晶彩科(3535)")
    ]
    config['fibo_tags'] = fibo_tags
    st.session_state.fibo_tags = fibo_tags
    if 'ma_w' in st.session_state:
        config['ma_width'] = st.session_state.ma_w
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f: json.dump(config, f)
    except Exception: pass
    # 同步寫入 Google Sheets
    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes, fibo_tags)

def save_data_cache(df, ignored_set, candidates=None, saved_notes=None, fibo_tags=None):
    if candidates is None:
        candidates = []
    if saved_notes is None:
        saved_notes = {}
    if fibo_tags is None:
        fibo_tags = st.session_state.get('fibo_tags', list(DEFAULT_FIBO_TAGS))
    try:
        # 只在主執行緒做最輕量的複製，避免鎖死 UI
        df_save = df.fillna("").copy()
        ignored_list = list(ignored_set)
        # 快取完整備註供重整後還原（須在 drop 內部欄位前執行）
        empty_col = pd.Series("", index=df_save.index, dtype=str)
        codes = df_save.get('代號', empty_col).astype(str).str.strip()
        notes = df_save.get('戰略備註', empty_col).astype(str).str.strip()
        auto_notes = df_save.get('_auto_note', empty_col).astype(str).str.strip()
        cached_notes = {
            code: {'note': note, 'auto': auto}
            for code, note, auto in zip(codes, notes, auto_notes)
            if code
        }

        # 移除只供執行期間使用的欄位，避免重新載入時污染資料。
        df_save.drop(columns=['_auto_note'], errors='ignore', inplace=True)
        
        # 本地存檔維持在主執行緒 (若不想寫入本地也可將此段一併移入背景)
        data_to_save_local = {"stock_data": df_save.to_dict(orient='records'), "ignored_stocks": ignored_list, "all_candidates": candidates, "saved_notes": saved_notes, "fibo_tags": fibo_tags, "cached_notes": cached_notes}
        with open(DATA_CACHE_FILE, "w", encoding='utf-8') as f: 
            json.dump(data_to_save_local, f, ensure_ascii=False, indent=4)
        
        # 記憶體與速度終極優化：將「轉換 Dict」與「轉 JSON 字串」等高耗 RAM 動作全部移入背景執行緒
        if "gsheet_api_url" in st.secrets:
            def bg_save(bg_df, bg_ignored, bg_cands, bg_notes, bg_tags, bg_cn):
                try:
                    data_to_save = {
                        "stock_data": bg_df.to_dict(orient='records'), 
                        "ignored_stocks": bg_ignored, 
                        "all_candidates": bg_cands, 
                        "saved_notes": bg_notes, 
                        "fibo_tags": bg_tags,
                        "cached_notes": bg_cn
                    }
                    json_str = json.dumps(data_to_save, ensure_ascii=False)
                    requests.post(st.secrets["gsheet_api_url"], json={"action": "save", "data": json_str}, timeout=5)
                except Exception: pass
                finally:
                    # 強制回收背景執行緒產生的巨大 JSON 與 Dict 記憶體
                    gc.collect()

            import threading
            threading.Thread(target=bg_save, args=(df_save, ignored_list, candidates, saved_notes, fibo_tags, cached_notes), daemon=True).start()
    except Exception: pass

def load_data_cache():
    if "gsheet_api_url" in st.secrets:
        try:
            r = requests.get(st.secrets["gsheet_api_url"], timeout=5)
            if r.status_code == 200 and r.text.strip():
                data = json.loads(r.text)
                df = pd.DataFrame(data.get('stock_data', []))
                ignored = set(data.get('ignored_stocks', []))
                candidates = data.get('all_candidates', [])
                saved_notes = data.get('saved_notes', {}) 
                fibo_tags = data.get('fibo_tags', [])
                return df, ignored, candidates, saved_notes, fibo_tags, data.get('cached_notes', {})
        except Exception: pass

    if os.path.exists(DATA_CACHE_FILE):
        try:
            with open(DATA_CACHE_FILE, "r", encoding='utf-8') as f: data = json.load(f)
            df = pd.DataFrame(data.get('stock_data', []))
            ignored = set(data.get('ignored_stocks', []))
            candidates = data.get('all_candidates', [])
            saved_notes = data.get('saved_notes', {}) 
            fibo_tags = data.get('fibo_tags', [])
            return df, ignored, candidates, saved_notes, fibo_tags, data.get('cached_notes', {})
        except Exception: return pd.DataFrame(), set(), [], {}, [], {}
    return pd.DataFrame(), set(), [], {}, [], {}

def load_url_history():
    if os.path.exists(URL_CACHE_FILE):
        try:
            with open(URL_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                if "url" in data and isinstance(data["url"], str) and data["url"]: return [data["url"]]
                return data.get("urls", [])
        except Exception: return []
    return []

def save_url_history(urls):
    try:
        unique_urls = []
        seen = set()
        for u in urls:
            u_clean = u.strip()
            if u_clean and u_clean not in seen:
                unique_urls.append(u_clean)
                seen.add(u_clean)
        with open(URL_CACHE_FILE, "w", encoding='utf-8') as f: json.dump({"urls": unique_urls}, f)
        return True
    except Exception: return False

def load_search_cache():
    if os.path.exists(SEARCH_CACHE_FILE):
        try:
            with open(SEARCH_CACHE_FILE, "r", encoding='utf-8') as f: data = json.load(f)
            return data.get("selected", [])
        except Exception: return []
    return []

def save_search_cache(selected_items):
    try:
        with open(SEARCH_CACHE_FILE, "w", encoding='utf-8') as f: json.dump({"selected": selected_items}, f, ensure_ascii=False)
    except Exception: pass

if 'stock_data' not in st.session_state:
    cached_df, cached_ignored, cached_candidates, cached_saved_notes, cached_fibo_tags, cached_note_dict = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored
    st.session_state.all_candidates = cached_candidates
    st.session_state.saved_notes = cached_saved_notes
    st.session_state.fibo_tags = cached_fibo_tags if cached_fibo_tags else list(DEFAULT_FIBO_TAGS)
    st.session_state.cached_notes = cached_note_dict

if 'ignored_stocks' not in st.session_state: st.session_state.ignored_stocks = set()
if 'all_candidates' not in st.session_state: st.session_state.all_candidates = []
if 'calc_base_price' not in st.session_state: st.session_state.calc_base_price = 100.0
if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = 100.0
if 'url_history' not in st.session_state: st.session_state.url_history = load_url_history()
if 'cloud_url_input' not in st.session_state: st.session_state.cloud_url_input = st.session_state.url_history[0] if st.session_state.url_history else ""
if 'search_multiselect' not in st.session_state: st.session_state.search_multiselect = load_search_cache()
if 'saved_notes' not in st.session_state: st.session_state.saved_notes = {}
if 'futures_list' not in st.session_state: st.session_state.futures_list = {}
if 'ignored_data_cache' not in st.session_state: st.session_state.ignored_data_cache = {} # 新增這行：忽略資料快取
if 'prefetch_cache' not in st.session_state: st.session_state.prefetch_cache = {} # 🚀 新增：預載快取
if 'cached_notes' not in st.session_state: st.session_state.cached_notes = {}    


# Fibo 標籤與狀態初始化
saved_config = load_config()
fibo_tags = saved_config.get('fibo_tags', list(DEFAULT_FIBO_TAGS))

# 優先採用從 Google Sheets 載入回來的雲端快取標籤紀錄
fibo_tags_source = st.session_state.get('fibo_tags', fibo_tags)
if not fibo_tags_source or len(fibo_tags_source) < 5:
    fibo_tags_source = fibo_tags

if 'fibo_search_input' not in st.session_state: st.session_state.fibo_search_input = ""
if 'fibo_trigger_search' not in st.session_state: st.session_state.fibo_trigger_search = False

if 'custom_tag_1' not in st.session_state: st.session_state.custom_tag_1 = fibo_tags_source[0] if len(fibo_tags_source)>0 else "台積電(2330)"
if 'custom_tag_2' not in st.session_state: st.session_state.custom_tag_2 = fibo_tags_source[1] if len(fibo_tags_source)>1 else "鴻海(2317)"
if 'custom_tag_3' not in st.session_state: st.session_state.custom_tag_3 = fibo_tags_source[2] if len(fibo_tags_source)>2 else "聯發科(2454)"
if 'custom_tag_4' not in st.session_state: st.session_state.custom_tag_4 = fibo_tags_source[3] if len(fibo_tags_source)>3 else "和椿(6215)"
if 'custom_tag_5' not in st.session_state: st.session_state.custom_tag_5 = fibo_tags_source[4] if len(fibo_tags_source)>4 else "晶彩科(3535)"

if 'ma_w' not in st.session_state: st.session_state.ma_w = saved_config.get('ma_width', 1.5)

# 控制圖表預設時間週期
if 'fibo_interval' not in st.session_state: st.session_state.fibo_interval = "1d" 
if 'fibo_font_size' not in st.session_state: st.session_state.fibo_font_size = 15

tz_tw = pytz.timezone('Asia/Taipei')
now_tw = datetime.now(tz_tw)
if 'cal_year' not in st.session_state: st.session_state.cal_year = now_tw.year
if 'cal_month' not in st.session_state: st.session_state.cal_month = now_tw.month

@st.cache_resource
def init_shioaji_connection(api_key, secret_key):
    api = sj.Shioaji(simulation=False)
    api.login(api_key, secret_key)
    return api

if 'font_size' not in st.session_state: st.session_state.font_size = saved_config.get('font_size', 15)
if 'limit_rows' not in st.session_state: st.session_state.limit_rows = saved_config.get('limit_rows', 5)
if 'auto_update_last_row' not in st.session_state: st.session_state.auto_update_last_row = saved_config.get('auto_update', True)
if 'update_delay_sec' not in st.session_state: st.session_state.update_delay_sec = saved_config.get('delay_sec', 1.0) 

# 永豐API帳密：
if 'sj_key' not in st.session_state: 
    st.session_state.sj_key = st.secrets.get('sj_key', saved_config.get('sj_key', ''))
if 'sj_secret' not in st.session_state: 
    st.session_state.sj_secret = st.secrets.get('sj_secret', saved_config.get('sj_secret', ''))
if 'remember_sj' not in st.session_state: 
    st.session_state.remember_sj = True if 'sj_key' in st.secrets else saved_config.get('remember_sj', False)

if sj and st.session_state.remember_sj and st.session_state.sj_key and not st.session_state.get('manual_logout', False):
    try:
        # 取得快取的連線物件
        api_obj = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
        # 測試連線是否存活 (防呆：解決 AuthError: Not authenticated)
        try:
            api_obj.usage()
            st.session_state.sj_api = api_obj
            st.session_state.sj_logged_in = True
        except Exception:
            # 若快取的連線已失效，則清除快取並強制重新登入
            init_shioaji_connection.clear()
            st.session_state.sj_api = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
            st.session_state.sj_logged_in = True
    except Exception:
        st.session_state.sj_logged_in = False

@st.cache_data(max_entries=1)
def load_local_stock_names():
    if os.path.exists("stock_names.csv"):
        try:
            df = pd.read_csv("stock_names.csv", header=None, names=["code", "name"], dtype=str)
            codes = df['code'].astype(str).str.strip()
            names = df['name'].astype(str).str.strip()
            return dict(zip(codes, names)), dict(zip(names, codes))
        except Exception:
            pass
    return {}, {}

@st.cache_data(ttl=86400)
def get_stock_name_online(code):
    code = str(code).strip()
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    query = query.strip()
    if query.isdigit(): return query
    _, name_map = load_local_stock_names()
    if query in name_map: return name_map[query]
    return None

with st.sidebar:
    st.header("🔑 永豐證券 API 登入")
    if sj is None:
        st.error("⚠️ 未偵測到 shioaji 套件\n\n請先在終端機執行：\n`pip install shioaji`")
    else:
        if st.session_state.get('sj_logged_in', False):
            st.success("✅ 永豐 API 已登入")
            
            try:
                usage = st.session_state.sj_api.usage()
                rem_mb = usage.remaining_bytes / (1024 * 1024)
                st.caption(f"📊 API 今日剩餘流量: {rem_mb:.2f} MB")
            except Exception:
                st.caption("📊 API 今日剩餘流量: 暫時無法獲取 (連線讀取中)")

            col_logout, col_relogin = st.columns(2)
            with col_logout:
                if st.button("登出", key="btn_logout_sj", use_container_width=True):
                    st.session_state.sj_logged_in = False
                    st.session_state.manual_logout = True # 標記為手動登出，阻擋重整時自動登入
                    
                    try: 
                        st.session_state.sj_api.logout()
                        init_shioaji_connection.clear() 
                    except: pass
                    
                    # 保持儲存現有的 Key 與記住狀態
                    save_config(
                        st.session_state.font_size, 
                        st.session_state.limit_rows, 
                        st.session_state.auto_update_last_row, 
                        st.session_state.update_delay_sec, 
                        st.session_state.sj_key, 
                        st.session_state.sj_secret, 
                        st.session_state.remember_sj
                    )
                    st.rerun()
            with col_relogin:
                relogin_clicked = st.button("快速重新登入", key="btn_relogin_sj", use_container_width=True)
                
            msg_placeholder = st.empty()
            
            if relogin_clicked:
                try:
                    st.session_state.sj_api.logout()
                    init_shioaji_connection.clear() # 必須清除快取，否則只會拿到舊的連線物件
                    time.sleep(0.5)
                    
                    st.session_state.sj_api = init_shioaji_connection(st.session_state.sj_key, st.session_state.sj_secret)
                    st.session_state.sj_logged_in = True
                    msg_placeholder.success("✅ 重新登入成功！")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    msg_placeholder.error(f"❌ 重新登入失敗: {e}")
                    st.session_state.sj_logged_in = False
                    st.rerun()
        else:
            sj_api_key = st.text_input("API Key", type="password", value=st.session_state.sj_key, key="input_sj_key")
            sj_secret = st.text_input("Secret Key", type="password", value=st.session_state.sj_secret, key="input_sj_secret")
            remember_sj = st.checkbox("記住 API 資訊", value=st.session_state.remember_sj, key="input_remember_sj")
            
            if st.button("登入 Shioaji"):
                try:
                    st.session_state.manual_logout = False # 解除手動登出標記
                    st.session_state.sj_api = init_shioaji_connection(sj_api_key, sj_secret)
                    st.session_state.sj_logged_in = True
                    
                    st.session_state.sj_key = sj_api_key
                    st.session_state.sj_secret = sj_secret
                    st.session_state.remember_sj = remember_sj
                    save_config(
                        st.session_state.font_size, 
                        st.session_state.limit_rows, 
                        st.session_state.auto_update_last_row, 
                        st.session_state.update_delay_sec, 
                        sj_api_key, 
                        sj_secret, 
                        remember_sj
                    )
                    st.success("✅ 永豐 API 登入成功！")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 登入失敗: {e}")
    st.markdown("---")

    st.header("⚙️ 設定")
    current_font_size = st.slider("字體大小 (表格)", min_value=12, max_value=72, value=st.session_state.font_size, key='font_size_slider')
    st.session_state.font_size = current_font_size
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True)
    st.checkbox("查詢權證 (含圖表快速標籤)", value=False, key="allow_warrant_search") # 新增權證過濾開關
    show_3d_hilo = st.checkbox("近3日高低點 (戰略備註)", value=False, help="勾選後，將於戰略備註中加入前天、昨天、今天的最高與最低價 (僅顯示數值)")
    st.markdown("---")
    current_limit_rows = st.number_input("顯示筆數 (檔案/雲端)", min_value=1, value=st.session_state.limit_rows, key='limit_rows_input')
    st.session_state.limit_rows = current_limit_rows
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows, st.session_state.auto_update_last_row, st.session_state.update_delay_sec, st.session_state.sj_key, st.session_state.sj_secret, st.session_state.remember_sj):
            st.toast("設定已儲存！", icon="✅")
            
    st.markdown("### 資料管理")
    if st.session_state.ignored_stocks:
        st.write(f"🚫 忽略名單 (取消勾選以復原):")
        ignored_list = sorted(list(st.session_state.ignored_stocks))
        options_map = {f"{c} {get_stock_name_online(c)}": c for c in ignored_list}
        options_display = list(options_map.keys())
        selected_ignored_display = st.multiselect("管理忽略股票", options=options_display, default=options_display, label_visibility="collapsed")
        current_selected_codes = set(options_map[opt] for opt in selected_ignored_display)
        if len(current_selected_codes) != len(st.session_state.ignored_stocks):
            unignored_codes = st.session_state.ignored_stocks - current_selected_codes
            st.session_state.ignored_stocks = current_selected_codes
            # --- 新增：將待加回的股票存入 pending 狀態 ---
            if unignored_codes:
                st.session_state.pending_unignore = unignored_codes
            # ---------------------------------------------
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已更新忽略名單。", icon="🔄")
            st.rerun()
    else:
        st.write("🚫 目前無忽略股票")
    
    col_restore, col_clear = st.columns([1, 1], gap="small")
    with col_restore:
        if st.button("♻️ 全部復原", width='stretch'):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已重置忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 全部清空", type="primary", width='stretch'):
            st.session_state.stock_data = pd.DataFrame()
            st.session_state.ignored_stocks = set()
            st.session_state.all_candidates = []
            st.session_state.search_multiselect = []
            st.session_state.saved_notes = {} 
            save_search_cache([])
            if os.path.exists(DATA_CACHE_FILE): os.remove(DATA_CACHE_FILE)
            st.toast("資料已全部清空", icon="🗑️")
            st.rerun()
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選「刪除」框，資料將會立即移除並**自動遞補下一檔**。")
    st.markdown("---")
    st.markdown("### 🔗 外部資源")
    
    def perform_goodinfo_fetch():
        with st.spinner("正在抓取最新資料，請稍候約 15 秒..."):
            df_goodinfo = fetch_goodinfo_data()
            if df_goodinfo is not None and not df_goodinfo.empty:
                # 🟢 變更：同時將 DataFrame 存入 session_state 供戰略室直接讀取
                st.session_state['goodinfo_df'] = df_goodinfo.astype(str)
                st.session_state['goodinfo_fetch_failed'] = False
                st.success("💡 抓取成功並已載入暫存！\n\n若要分析此數據，請確保主畫面「未上傳檔案」且「未輸入雲端連結」，直接點擊主畫面的『🚀 執行分析』即可。")
            else:
                st.session_state['goodinfo_fetch_failed'] = True
                st.error("抓取失敗或查無資料，請稍後再試。")

    # 觸發爬蟲的按鈕
    if st.button("📥 抓取 Goodinfo 週轉率排行", help="需等待網頁載入約15秒", width='stretch'):
        perform_goodinfo_fetch()
        
    if st.session_state.get('goodinfo_fetch_failed', False):
        col_retry, col_link = st.columns(2)
        with col_retry:
            if st.button("🔄 重新抓取", key="retry_goodinfo_btn"):
                perform_goodinfo_fetch()
                st.rerun()
        with col_link:
            goodinfo_url = "https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%28%E7%95%B6%E6%97%A5%29%40%40%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%40%40%E7%95%B6%E6%97%A5"
            st.link_button("🌐 直接連結", goodinfo_url)
                    
    # 保留原有的下載按鈕
    if 'goodinfo_df' in st.session_state:
        st.download_button(
            label="💾 下載 Report.csv",
            data=st.session_state['goodinfo_df'].to_csv(index=False).encode('utf-8-sig'),
            file_name="Report.csv",
            mime="text/csv",
            width='stretch'
        )
    st.link_button("🚨 上市處置有價證券公告", "https://www.twse.com.tw/zh/announcement/punish.html", width='stretch')
    st.link_button("🚨 上櫃處置有價證券公告", "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html", width='stretch')

@st.cache_data(ttl=86400)
def fetch_futures_list():
    try:
        url = "https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining"
        r = requests.get(url, headers={'accept': 'application/json'}, timeout=5, verify=False)
        if r.status_code == 200:
            data = r.json()
            stock_contracts = {}
            for item in data:
                code = str(item.get("UnderlyingSecurityCode", "")).strip()
                contract = str(item.get("Contract", "")).strip()
                if code and contract:
                    if code not in stock_contracts:
                        stock_contracts[code] = set()
                    stock_contracts[code].add(contract)
            
            futures_dict = {}
            for code, contracts in stock_contracts.items():
                futures_dict[code] = "✅(有小型)" if len(contracts) > 1 else "✅"
            if futures_dict:
                return futures_dict
    except: pass

    try:
        url = "https://www.taifex.com.tw/cht/2/stockLists"
        dfs = pd.read_html(url)
        futures_dict = {}
        if dfs:
            for df in dfs:
                df_cols = [str(c).replace('\n', '').replace(' ', '') for c in df.columns]
                df.columns = df_cols
                
                code_col = next((c for c in df.columns if '證券代號' in c or 'StockCode' in c), None)
                if code_col:
                    small_col = next((c for c in df.columns if '小' in c or 'Small' in c), None)
                    for _, row in df.iterrows():
                        code = str(row[code_col]).strip()
                        if pd.isna(code) or not code: continue
                        is_small = False
                        if small_col and pd.notna(row[small_col]):
                            val = str(row[small_col]).strip()
                            if val and val not in ['-', 'nan', 'NaN', '']:
                                is_small = True
                        
                        if is_small:
                            futures_dict[code] = "✅(有小型)"
                        elif code not in futures_dict:
                            futures_dict[code] = "✅"
            return futures_dict
    except: pass
    return {}

@st.cache_data(ttl=900, max_entries=1, show_spinner=False)
def fetch_twse_market_risk_lists():
    """取得上市注意／處置名單；僅在使用者手動更新時呼叫。"""
    attention_counts = {}
    disposition_codes = set()
    errors = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    def parse_rows(payload, target, is_attention=False):
        fields = payload.get('fields', [])
        for values in payload.get('data', []):
            record = dict(zip(fields, values))
            raw_code = str(record.get('證券代號', record.get('有價證券代號', '')))
            code_match = re.search(r'\d{4,6}', raw_code)
            if not code_match:
                continue
            code = code_match.group(0)
            if is_attention:
                raw_count = str(record.get('累計次數', record.get('累計', '1')))
                count_match = re.search(r'\d+', raw_count)
                target[code] = max(target.get(code, 0), int(count_match.group()) if count_match else 1)
            else:
                target.add(code)

    for url, target, is_attention in [
        ('https://www.twse.com.tw/announcement/notice?response=json', attention_counts, True),
        ('https://www.twse.com.tw/announcement/punish?response=json', disposition_codes, False),
    ]:
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            parse_rows(response.json(), target, is_attention)
        except Exception as exc:
            errors.append(str(exc))

    return attention_counts, sorted(disposition_codes), errors

def _as_float(value, default=None):
    try:
        number = float(value)
        return default if math.isnan(number) else number
    except (TypeError, ValueError):
        return default

def calculate_risk_filter_result(row, direction, max_extension_atr, attention_counts=None, disposition_codes=None, twse_lists_updated=False, block_attention=True):
    """建立風險篩選預覽資料；不會改寫原本的選股資料或下單行為。"""
    attention_counts = attention_counts or {}
    disposition_codes = set(disposition_codes or [])
    code = str(row.get('代號', '')).strip()
    close = _as_float(row.get('收盤價'))
    ma5 = _as_float(row.get('_ma5'))
    ma20 = _as_float(row.get('_risk_ma20'))
    ma20_slope = _as_float(row.get('_risk_ma20_slope'))
    atr14 = _as_float(row.get('_risk_atr14'))
    close_position = _as_float(row.get('_risk_close_position'))
    previous_high = _as_float(row.get('_risk_prev_high'))
    previous_low = _as_float(row.get('_risk_prev_low'))
    is_long = direction == '多頭'

    if close is None or ma5 is None or ma20 is None or atr14 is None or atr14 <= 0:
        return {
            'score': 0, 'risk': '⚪ 資料不足', 'extension': None,
            'rule': '資料不足，維持原判斷', 'eligible': False,
            'detail': '缺少計算 20 日趨勢或 ATR 所需資料。'
        }

    extension = (close - ma20) / atr14 if is_long else (ma20 - close) / atr14
    trend_score = 0
    if (is_long and close > ma5) or (not is_long and close < ma5):
        trend_score += 10
    if (is_long and close > ma20) or (not is_long and close < ma20):
        trend_score += 10
    if ma20_slope is not None and ((is_long and ma20_slope > 0) or (not is_long and ma20_slope < 0)):
        trend_score += 10

    if extension <= 1:
        extension_score = 20
    elif extension <= 1.5:
        extension_score = 15
    elif extension <= 2:
        extension_score = 8
    else:
        extension_score = 0

    candle_score = 5
    if close_position is not None:
        if (is_long and close_position >= 65) or (not is_long and close_position <= 35):
            candle_score = 15
        elif (is_long and close_position >= 50) or (not is_long and close_position <= 50):
            candle_score = 10

    attention_count = attention_counts.get(code, 0)
    is_disposed = code in disposition_codes
    try:
        market = getattr(twstock.codes.get(code), 'market', '')
    except Exception:
        market = ''
    if is_disposed:
        risk_label, risk_score = '🚫 處置中', 0
    elif attention_count >= 2:
        risk_label, risk_score = f'🔴 注意 {attention_count}', 0
    elif attention_count == 1:
        risk_label, risk_score = '🟡 注意 1', 10
    elif twse_lists_updated and market == '上市':
        risk_label, risk_score = '🟢 上市名單未列示', 20
    elif twse_lists_updated:
        risk_label, risk_score = '⚪ 上櫃未查核', 12
    else:
        risk_label, risk_score = '⚪ 未查核', 12

    has_breakout = previous_high is not None and close > previous_high if is_long else previous_low is not None and close < previous_low
    confirmation_score = 15 if has_breakout else 5
    score = trend_score + extension_score + candle_score + risk_score + confirmation_score
    too_extended = extension > max_extension_atr
    eligible = not is_disposed and (not block_attention or attention_count < 2) and not too_extended
    if is_long:
        rule = '突破昨高後站穩' if has_breakout else '回踩 5／10 日線止穩'
    else:
        rule = '跌破昨低後確認' if has_breakout else '反彈不過 5／10 日線'
    if is_disposed:
        rule = '排除：處置中'
    elif attention_count >= 2 and block_attention:
        rule = '排除：注意累計偏高'
    elif attention_count >= 2:
        rule = '高風險：僅等待確認'
    elif too_extended:
        rule = f'排除：乖離超過 {max_extension_atr:.1f} ATR'

    return {
        'score': score, 'risk': risk_label, 'extension': extension,
        'rule': rule, 'eligible': eligible,
        'detail': f'趨勢 {trend_score}/30｜乖離 {extension_score}/20｜K棒 {candle_score}/15｜風險 {risk_score}/20｜確認 {confirmation_score}/15'
    }

def get_tick_size(price):
    try: price = float(price)
    except: return 0.01
    if pd.isna(price) or price <= 0: return 0.01
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def apply_tick_rules(price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded = (Decimal(str(p)) / Decimal(str(tick))).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
        return float(rounded)
    except:
        try: return float(price)
        except: return 0.0

def calculate_stop_loss_price(base_price, stop_loss_percent, is_long=True):
    """依交易方向及台股跳動單位，計算可下單的停損價格。"""
    try:
        base = float(base_price)
        percent = float(stop_loss_percent)
        if math.isnan(base) or math.isnan(percent) or base <= 0 or percent < 0:
            return 0.0
        raw_price = base * (1 - percent / 100) if is_long else base * (1 + percent / 100)
        return apply_tick_rules(raw_price)
    except (TypeError, ValueError):
        return 0.0

def calculate_limits(price):
    try:
        p = float(price)
        if math.isnan(p) or p <= 0: return 0, 0
        raw_up = p * 1.10
        tick_up = get_tick_size(raw_up) 
        limit_up = math.floor(raw_up / tick_up) * tick_up
        raw_down = p * 0.90
        tick_down = get_tick_size(raw_down) 
        limit_down = math.ceil(raw_down / tick_down) * tick_down
        return float(f"{limit_up:.2f}"), float(f"{limit_down:.2f}")
    except: return 0, 0

def move_tick(price, steps):
    try:
        curr = float(price)
        if steps > 0:
            for _ in range(steps):
                tick = get_tick_size(curr)
                curr = round(curr + tick, 2)
        elif steps < 0:
            for _ in range(abs(steps)):
                tick = get_tick_size(curr - 0.0001)
                curr = round(curr - tick, 2)
        return curr
    except: return price

def apply_sr_rules(price, base_price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        d_val = Decimal(str(p))
        d_tick = Decimal(str(tick))
        if p < base_price: return float(math.ceil(d_val / d_tick) * d_tick)
        elif p > base_price: return float(math.floor(d_val / d_tick) * d_tick)
        else: return apply_tick_rules(p)
    except: return price

def fmt_price(v):
    try:
        if pd.isna(v) or v == "": return ""
        return f"{float(v):.2f}".rstrip('0').rstrip('.')
    except: return str(v)

def calculate_note_width(series, font_size):
    def get_width(s):
        w = 0
        for c in str(s): w += 2.0 if ord(c) > 127 else 1.0
        return w
    if series.empty: return 50
    max_w = series.apply(get_width).max()
    if pd.isna(max_w): max_w = 0
    pixel_width = int(max_w * (font_size * 0.44))
    return max(50, pixel_width)

def recalculate_row(row, points_map):
    custom_price = row.get('自訂價(可修)')
    code = row.get('代號')
    status = ""
    if pd.isna(custom_price) or str(custom_price).strip() == "": return status
    try:
        price = float(custom_price)
        limit_up = row.get('當日漲停價')
        limit_down = row.get('當日跌停價')
        l_up = float(limit_up) if limit_up and str(limit_up).replace('.','').isdigit() else None
        l_down = float(limit_down) if limit_down and str(limit_down).replace('.','').isdigit() else None
        
        strat_values = []
        points = points_map.get(code, [])
        if isinstance(points, list):
            for p in points: strat_values.append(p['val'])
            
        note_text = str(row.get('戰略備註', ''))
        found_prices = re.findall(r'\d+\.?\d*', note_text)
        for fp in found_prices:
            try: strat_values.append(float(fp))
            except: pass
            
        if l_up is not None and abs(price - l_up) < 0.01: status = "漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: status = "跌停"
        elif strat_values:
            max_val = max(strat_values)
            min_val = min(strat_values)
            if price > max_val: status = "強"
            elif price < min_val: status = "弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "命中"
        return status
    except: return status

def generate_note_from_points(points, manual_note, show_3d):
    # 修正：加入安全判斷，防止重整或合併時產生 NaN 導致的 TypeError
    if not isinstance(points, list):
        points = []
        
    display_candidates = []
    target_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        t = p.get('tag', '')
        if t in target_tags and not show_3d: continue
        if p['val'] <= 0: continue
        display_candidates.append(p)
        
    display_candidates.sort(key=lambda x: x['val'])
    note_parts = []
    seen_vals = set() 
    
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        if val in seen_vals: continue
        seen_vals.add(val)
        g_list = list(group)
        tags = [x['tag'] for x in g_list if x['tag']]
        
        final_tag = ""
        if "漲停高" in tags: final_tag = "漲停高"
        elif "跌停低" in tags: final_tag = "跌停低" 
        elif "漲停" in tags: final_tag = "漲停"
        elif "跌停" in tags: final_tag = "跌停"
        elif "多" in tags: final_tag = "多"
        elif "空" in tags: final_tag = "空"
        elif "平" in tags: final_tag = "平"
        elif "高" in tags: final_tag = "高"
        elif "低" in tags: final_tag = "低"
        elif "今高" in tags: final_tag = "今高"
        elif "今低" in tags: final_tag = "今低"
        elif "昨高" in tags: final_tag = "昨高"
        elif "昨低" in tags: final_tag = "昨低"
        elif "前高" in tags: final_tag = "前高"
        elif "前低" in tags: final_tag = "前低"
        
        v_str = fmt_price(val)
        suffix_tags = ["多", "空", "平"]
        prefix_tags = ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]
        numeric_only_tags = ["前高", "前低", "昨高", "昨低", "今高", "今低"]
        
        if final_tag in suffix_tags: 
            if final_tag == "多":
                item = f"🔴{v_str}{final_tag}"
            elif final_tag == "空":
                item = f"🟢{v_str}{final_tag}"
            else:
                item = f"{v_str}{final_tag}"
        elif final_tag in prefix_tags: item = f"{final_tag}{v_str}"
        elif final_tag in numeric_only_tags: item = v_str 
        elif final_tag: item = f"{v_str}{final_tag}" 
        else: item = v_str
        note_parts.append(item)
        
    auto_note = "-".join(note_parts)
    if manual_note:
        if manual_note.startswith("[M]"): return manual_note[3:], auto_note
        if auto_note and manual_note.strip().startswith(auto_note.strip()): return manual_note, auto_note
        return f"{auto_note}{manual_note}", auto_note
    return auto_note, auto_note

def fetch_stock_data_raw(code, name_hint="", extra_data=None, futures_set=None, saved_notes_dict=None, name_map_dict=None, sj_logged_in=False, sj_api=None):
    code = str(code).strip()
    hist = pd.DataFrame()
    source_used = "none"
    
    # 優先使用永豐 API 擷取昨日/歷史日 K 線資料
    if sj_logged_in and sj_api is not None:
        sj_df = fetch_shioaji_data(sj_api, code, interval='1d', lookback_days=40)
        if not sj_df.empty:
            hist = sj_df
            source_used = "shioaji"

    # 若永豐未登入或沒抓到，退回使用 twstock 擷取
    if hist.empty:
        try:
            stock = twstock.Stock(code)
            tw_data = stock.fetch_31()
            if tw_data and len(tw_data) > 0:
                df_tw = pd.DataFrame(tw_data)
                df_tw['Date'] = pd.to_datetime(df_tw['date'])
                df_tw = df_tw.set_index('Date')
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}
                df_tw = df_tw.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in cols: df_tw[c] = pd.to_numeric(df_tw[c], errors='coerce')
                if not df_tw.empty:
                    hist = df_tw[cols]
                    source_used = "twstock"
        except: pass

    if hist.empty and si is not None:
        try:
            try: df_yf = si.get_data(f"{code}.TW", start_date=(datetime.now() - timedelta(days=40)))
            except:
                try: df_yf = si.get_data(f"{code}.TWO", start_date=(datetime.now() - timedelta(days=40)))
                except: df_yf = pd.DataFrame()
            
            if not df_yf.empty:
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
                df_yf = df_yf.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(c in df_yf.columns for c in cols):
                    hist = df_yf[cols]
                    source_used = "yahoo_fin"
        except: pass

    if hist.empty:
        try:
            ticker_obj = yf.Ticker(f"{code}.TW")
            hist_yf = ticker_obj.history(period="3mo")
            if hist_yf.empty:
                ticker_obj = yf.Ticker(f"{code}.TWO")
                hist_yf = ticker_obj.history(period="3mo")
            if not hist_yf.empty:
                hist = hist_yf
                source_used = "yfinance"
        except Exception: 
            pass

    # 僅當未使用永豐 API，且需獲取即時資訊時，才透過 twstock.realtime 補足今日最新
    if source_used != "shioaji":
        try:
            rt_data = twstock.realtime.get(code)
            if rt_data['success'] and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                rt_price = float(rt_data['realtime']['latest_trade_price'])
                rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                
                rt_time_str = rt_data['info']['time']
                rt_dt = datetime.strptime(rt_time_str, "%Y-%m-%d %H:%M:%S")
                today_date = pd.Timestamp(datetime.now(tz_tw).date())

                if hist.empty:
                    hist = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                else:
                    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
                    last_hist_date = hist.index[-1]
                    if last_hist_date < today_date:
                        if datetime.now(tz_tw).weekday() < 5:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                            hist = pd.concat([hist, new_row])
                            hist.sort_index(inplace=True)
                    elif last_hist_date == today_date:
                        hist.at[last_hist_date, 'Close'] = rt_price
                        hist.at[last_hist_date, 'High'] = max(hist.at[last_hist_date, 'High'], rt_high)
                        hist.at[last_hist_date, 'Low'] = min(hist.at[last_hist_date, 'Low'], rt_low)
                        hist.at[last_hist_date, 'Volume'] = rt_vol
                        if hist.at[last_hist_date, 'Open'] == 0: hist.at[last_hist_date, 'Open'] = rt_open
        except: pass 

    if hist.empty: return None
    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

   # 包含收盤價、漲跌幅，14:30 前排除今日資料，套用昨日指標 (配合主要擷取昨日盤後資料需求)
    tz_tw_calc = pytz.timezone('Asia/Taipei')
    now_tw_calc = datetime.now(tz_tw_calc)
    switch_time = dt_time(14, 30)
    
    if now_tw_calc.time() < switch_time:
        if not hist.empty and hist.index[-1].date() == now_tw_calc.date():
            if len(hist) > 1:
                hist = hist.iloc[:-1]

    if hist.empty: return None

    # 修正夜盤基準：若是期貨，透過快照直接擷取官方基準價 (日盤 13:45 收盤價)
    if sj_logged_in and sj_api is not None and code in ["TWF=F", "TMF=F"]:
        try:
            contract = None
            if code == "TWF=F":
                contract = min([c for c in sj_api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code], key=lambda c: getattr(c, 'delivery_date', '999999'))
            else:
                contract = sj_api.Contracts.Futures.TMF.TMFR1
            if contract:
                snap = sj_api.snapshots([contract])
                if snap and len(snap) > 0:
                    s = snap[0]
                    rt_p = s.close if s.close > 0 else s.open
                    change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                    if change_val is not None and rt_p > 0:
                        official_ref = rt_p - float(change_val)
                        if len(hist) >= 2:
                            hist.iloc[-2, hist.columns.get_loc('Close')] = official_ref
        except:
            pass

    live_base_price = hist.iloc[-1]['Close']
    if len(hist) >= 2: live_prev_price = hist.iloc[-2]['Close']
    else: live_prev_price = live_base_price
    if live_prev_price > 0: live_pct_change = ((live_base_price - live_prev_price) / live_prev_price) * 100
    else: live_pct_change = 0.0

    hist_strat = hist.copy()

    # 風險篩選預覽所需指標：只沿用已取得的日 K，不增加任何資料請求。
    risk_atr14 = risk_ma20 = risk_ma20_slope = risk_close_position = None
    risk_prev_high = risk_prev_low = None
    if len(hist_strat) >= 2:
        prev_close_series = hist_strat['Close'].shift(1)
        true_range = pd.concat([
            hist_strat['High'] - hist_strat['Low'],
            (hist_strat['High'] - prev_close_series).abs(),
            (hist_strat['Low'] - prev_close_series).abs()
        ], axis=1).max(axis=1)
        risk_atr14 = float(true_range.tail(min(14, len(true_range))).mean())
        risk_prev_high = float(hist_strat['High'].iloc[-2])
        risk_prev_low = float(hist_strat['Low'].iloc[-2])
        latest_range = float(hist_strat['High'].iloc[-1] - hist_strat['Low'].iloc[-1])
        if latest_range > 0:
            risk_close_position = float((hist_strat['Close'].iloc[-1] - hist_strat['Low'].iloc[-1]) / latest_range * 100)

    if len(hist_strat) >= 20:
        ma20_series = hist_strat['Close'].rolling(20).mean()
        risk_ma20 = float(ma20_series.iloc[-1])
        if len(ma20_series.dropna()) >= 6:
            risk_ma20_slope = float(ma20_series.iloc[-1] - ma20_series.iloc[-6])

    strategy_base_price = hist_strat.iloc[-1]['Close']
    if len(hist_strat) >= 2: prev_of_base = hist_strat.iloc[-2]['Close']
    else: prev_of_base = strategy_base_price 

    base_price_for_limit = strategy_base_price
    limit_up_show, limit_down_show = calculate_limits(base_price_for_limit)

    limit_up_T = None
    limit_down_T = None
    if len(hist_strat) >= 2:
        prev_close_T = hist_strat.iloc[-2]['Close']
        limit_up_T, limit_down_T = calculate_limits(prev_close_T)

    target_price = apply_sr_rules(strategy_base_price * 1.03, strategy_base_price)
    stop_price = apply_sr_rules(strategy_base_price * 0.97, strategy_base_price)
    
    points = []
    recent_records = hist_strat.tail(3).to_dict('records')
    recent_records.reverse()
    days_map = {0: "今", 1: "昨", 2: "前"}
    
    for idx, row in enumerate(recent_records):
        if idx in days_map:
            prefix = days_map[idx]
            h_val = apply_tick_rules(row['High'])
            l_val = apply_tick_rules(row['Low'])
            if h_val > 0 and limit_down_show <= h_val <= limit_up_show: points.append({"val": h_val, "tag": f"{prefix}高"})
            if l_val > 0 and limit_down_show <= l_val <= limit_up_show: points.append({"val": l_val, "tag": f"{prefix}低"})

    if len(hist_strat) >= 5:
        last_5_closes = hist_strat['Close'].tail(5).values
        avg_val = sum(Decimal(str(x)) for x in last_5_closes) / Decimal("5")
        ma5_raw = float(avg_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1]
        p_open = apply_tick_rules(last_candle['Open'])
        if limit_down_show <= p_open <= limit_up_show: points.append({"val": p_open, "tag": ""})

        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        if limit_down_show <= p_low <= limit_up_show: 
             tag_low = "跌停" if limit_down_T and abs(p_low - limit_down_T) < 0.01 else ""
             points.append({"val": p_low, "tag": tag_low})

    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2]
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    show_plus_3 = False
    show_minus_3 = False
    
    if not hist_strat.empty:
        high_90_raw = hist_strat['High'].max()
        low_vals = hist_strat['Low'][hist_strat['Low'] > 0]
        low_90_raw = low_vals.min() if not low_vals.empty else hist_strat['Low'].min()
            
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})
        
        if len(hist_strat) >= 2:
             today_high = hist_strat.iloc[-1]['High']
             if limit_up_T and abs(today_high - limit_up_T) < 0.01:
                 tag_label = "漲停高" if (abs(limit_up_T - high_90_raw) < 0.05) else "漲停"
                 if limit_down_show <= limit_up_T <= limit_up_show: points.append({"val": limit_up_T, "tag": tag_label})

        if len(hist_strat) >= 2:
            high_T = hist_strat.iloc[-1]['High']
            low_T = hist_strat.iloc[-1]['Low']
            close_T = hist_strat.iloc[-1]['Close']
            if (limit_up_T and high_T >= limit_up_T - 0.01) and (limit_up_T and close_T >= limit_up_T * 0.97): show_plus_3 = True
            if (limit_down_T and low_T <= limit_down_T + 0.01) and (limit_down_T and close_T <= limit_down_T * 1.03): show_minus_3 = True

    if show_plus_3: points.append({"val": target_price, "tag": ""})
    if show_minus_3: points.append({"val": stop_price, "tag": ""})
        
    full_calc_points = []
    threed_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        v = float(f"{p['val']:.2f}")
        if p.get('force', False) or p.get('tag') in threed_tags or (limit_down_show <= v <= limit_up_show): full_calc_points.append(p) 
    
    manual_note = saved_notes_dict.get(code, "") if saved_notes_dict else ""
    strategy_note, auto_note = generate_note_from_points(full_calc_points, manual_note, show_3d=False)
    
    if name_hint: final_name = name_hint
    elif name_map_dict and code in name_map_dict: final_name = name_map_dict[code]
    else: final_name = code

    final_name_display = final_name
    # 兼容字典與舊版集合的判定
    has_futures = futures_set.get(code, "") if isinstance(futures_set, dict) else ("✅" if futures_set and code in futures_set else "")
    
    return {
        "代號": code, "名稱": final_name_display, "收盤價": round(live_base_price, 2), "漲跌幅": live_pct_change, "期貨": has_futures, 
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show, "自訂價(可修)": None,   
        "戰略備註": strategy_note, "_points": full_calc_points, "狀態": "", "_auto_note": auto_note, "_ma5": ma5 if 'ma5' in locals() else None,
        "_risk_atr14": risk_atr14, "_risk_ma20": risk_ma20, "_risk_ma20_slope": risk_ma20_slope,
        "_risk_close_position": risk_close_position, "_risk_prev_high": risk_prev_high, "_risk_prev_low": risk_prev_low
    }

# ==========================================
# 處理待加回的忽略股票 (防止 NameError & 提速)
# ==========================================
if 'pending_unignore' in st.session_state and st.session_state.pending_unignore:
    unignored_codes = st.session_state.pending_unignore
    st.session_state.pending_unignore = set()
    
    if 'futures_list' not in st.session_state or not st.session_state.futures_list:
        st.session_state.futures_list = fetch_futures_list()
    futures_copy = dict(st.session_state.futures_list)
    notes_copy = dict(st.session_state.get('saved_notes', {}))
    code_map_copy, _ = load_local_stock_names()
    sj_logged = st.session_state.get('sj_logged_in', False)
    sj_api_obj = st.session_state.get('sj_api', None)
    
    with st.spinner("正在將股票加回分析區..."):
        cached_rows = []
        fetch_tasks = []
        for c_code in unignored_codes:
            cand_info = next((c for c in st.session_state.all_candidates if c[0] == c_code), None)
            c_name = cand_info[1] if cand_info else get_stock_name_online(c_code)
            c_source = cand_info[2] if cand_info else 'search'
            c_order = cand_info[3] if cand_info else 999
            c_rank = 1 if c_source == 'upload' else 2
            
            # 速度優化：如果有暫存的分析資料，直接取用，省去等待
            if c_code in st.session_state.get('ignored_data_cache', {}):
                cached_row = st.session_state.ignored_data_cache.pop(c_code)
                cached_row.update({'_source': c_source, '_order': c_order, '_source_rank': c_rank})
                cached_rows.append(cached_row)
            else:
                fetch_tasks.append((c_code, c_name, c_source, c_order, c_rank))

        if cached_rows:
            st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(cached_rows)], ignore_index=True)
            
        if fetch_tasks:
            def _fetch_worker(task):
                time.sleep(API_REQUEST_GAP_SECONDS)  # 保留既有請求間隔，避免 API 封鎖
                t_code, t_name, t_src, t_ord, t_rnk = task
                res = fetch_stock_data_raw(t_code, t_name, None, futures_copy, notes_copy, code_map_copy, sj_logged, sj_api_obj)
                if res: res.update({'_source': t_src, '_order': t_ord, '_source_rank': t_rnk})
                return res
                
            with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                results = list(executor.map(_fetch_worker, fetch_tasks))
                valid_results = [r for r in results if r]
                if valid_results:
                    st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(valid_results)], ignore_index=True)

        if not st.session_state.stock_data.empty and '_source_rank' in st.session_state.stock_data.columns:
            st.session_state.stock_data = st.session_state.stock_data.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
            
    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
    st.rerun()


# 強制釋放不再使用的記憶體與執行緒資源
gc.collect()
# ==========================================
# 主介面 (Tabs)
# ==========================================
tab1, tab2, tab_fibo, tab_db, tab3 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 交易損益室 💰", "📈 費波計算", "📚 戰略資料庫", "📅 股市行事曆"])

with tab1:
    col_search, col_file = st.columns([2, 1])
    with col_search:
        code_map, name_map = load_local_stock_names()
        stock_options = []
        for code, name in sorted(code_map.items()):
            if not st.session_state.get('allow_warrant_search', False) and is_warrant(code):
                continue
            stock_options.append(f"{code} {name}")
        
        src_tab1, src_tab2 = st.tabs(["📂 本機", "☁️ 雲端"])
        with src_tab1:
            uploaded_file = st.file_uploader("上傳檔案 (CSV/XLS/HTML)", type=['xlsx', 'csv', 'html', 'xls'], label_visibility="collapsed")
            selected_sheet = 0
            if uploaded_file:
                try:
                    if not uploaded_file.name.endswith('.csv'):
                        xl_file = pd.ExcelFile(uploaded_file)
                        sheet_options = xl_file.sheet_names
                        default_idx = sheet_options.index("週轉率") if "週轉率" in sheet_options else 0
                        selected_sheet = st.selectbox("選擇工作表", sheet_options, index=default_idx)
                except: pass

        with src_tab2:
            def on_history_change(): st.session_state.cloud_url_input = st.session_state.history_selected
            history_opts = st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"]
            c_sel, c_del = st.columns([8, 1], gap="small")
            with c_sel:
                selected = st.selectbox("📜 歷史紀錄 (選取自動填入)", options=history_opts, key="history_selected", index=None, placeholder="請選擇...", on_change=on_history_change, label_visibility="collapsed")
            with c_del:
                if st.button("🗑️", help="刪除選取的歷史紀錄"):
                    if st.session_state.history_selected and st.session_state.history_selected in st.session_state.url_history:
                        st.session_state.url_history.remove(st.session_state.history_selected)
                        save_url_history(st.session_state.url_history)
                        st.toast("已刪除。", icon="🗑️")
                        st.rerun()
            st.text_input("輸入連結 (CSV/Excel/Google Sheet)", key="cloud_url_input", placeholder="https://...")
        
        def update_search_cache(): save_search_cache(st.session_state.search_multiselect)
        search_selection = st.multiselect("🔍 快速查詢 (中文/代號)", options=stock_options, key="search_multiselect", on_change=update_search_cache, placeholder="輸入 2330 或 台積電...")

    c_run, c_space = st.columns([1.5, 5])
    with c_run: btn_run = st.button("🚀 執行分析", width='stretch')

    if btn_run:
        save_search_cache(st.session_state.search_multiselect)
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []
        df_up = pd.DataFrame()
        current_url = st.session_state.cloud_url_input.strip()
        if current_url:
            if current_url not in st.session_state.url_history:
                st.session_state.url_history.insert(0, current_url) 
                save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0)
                fname = uploaded_file.name.lower()
                if fname.endswith('.csv'):
                    try: df_up = pd.read_csv(uploaded_file, dtype=str, encoding='cp950')
                    except: 
                        uploaded_file.seek(0)
                        df_up = pd.read_csv(uploaded_file, dtype=str)
                elif fname.endswith('.html') or fname.endswith('.htm') or fname.endswith('.xls'):
                    try: dfs = pd.read_html(uploaded_file, encoding='cp950')
                    except:
                        uploaded_file.seek(0)
                        dfs = pd.read_html(uploaded_file, encoding='utf-8')
                    for df in dfs:
                        if df.apply(lambda r: r.astype(str).str.contains('代號').any(), axis=1).any():
                             df_up = df
                             for i, row in df.iterrows():
                                 if "代號" in row.values:
                                     df_up.columns = row
                                     df_up = df_up.iloc[i+1:]
                                     break
                             break
                    if df_up.empty and dfs: df_up = dfs[0]
                elif fname.endswith('.xlsx'):
                    df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)

            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url and "/spreadsheets/" in url and "/edit" in url:
                    url = url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(url, dtype=str)
                except:
                    try: df_up = pd.read_excel(url, dtype=str)
                    except: st.error("❌ 無法讀取雲端檔案。")
            
            # 🟢 新增：若無上傳與雲端輸入，且檢測到有 Goodinfo 暫存資料時直接讀取
            elif 'goodinfo_df' in st.session_state:
                df_up = st.session_state['goodinfo_df'].copy()
                st.toast("已直接載入 Goodinfo 週轉率排行暫存資料進行分析！", icon="🔄")
                
        except Exception as e: st.error(f"讀取失敗: {e}")
        
        # 先處理上傳/資料帶入的股票 (優先權高)
        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            if c_col:
                limit_rows = st.session_state.limit_rows
                count = 0
                for _, row in df_up.iterrows():
                    c_raw = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if not c_raw or c_raw.lower() == 'nan': continue
                    is_valid = False
                    if c_raw.isdigit() and len(c_raw) <= 4: is_valid = True
                    elif len(c_raw) > 0 and (c_raw[0].isdigit() or c_raw[0] in ['0','00']): is_valid = True
                    if not is_valid: continue
                    if c_raw in st.session_state.ignored_stocks: continue
                    if hide_non_stock:
                        is_etf = c_raw.startswith('00')
                        is_warrant_flag = (len(c_raw) > 4) and c_raw.isdigit()
                        if is_etf or is_warrant_flag: continue
                    n = str(row[n_col]) if n_col else ""
                    if n.lower() == 'nan': n = ""
                    targets.append((c_raw, n, 'upload', count))
                    count += 1

        # 再處理快速查詢的股票，確保其排序在後面
        if search_selection:
            for i, item in enumerate(search_selection):
                parts = item.split(' ', 1)
                targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', i))

        st.session_state.all_candidates = targets

        st.session_state.all_candidates = targets
        seen = set()
        status_text = st.empty()
        bar = st.progress(0)
        
        upload_limit = st.session_state.limit_rows
        upload_current = 0
        existing_data = {}
        st.session_state.stock_data = pd.DataFrame() 
        
        futures_copy = dict(st.session_state.futures_list)
        notes_copy = dict(st.session_state.saved_notes)
        code_map_copy, _ = load_local_stock_names()

        def process_stock_task(t_code, t_name, t_source, t_extra, f_set, n_dict, c_map, sj_logged, sj_api_obj):
            # 將原本 0.5~1.5 秒的長時間延遲，改為 0.1 秒的微小緩衝以防 API 封鎖
            time.sleep(API_REQUEST_GAP_SECONDS)
            try: return (t_code, t_source, t_extra, fetch_stock_data_raw(t_code, t_name, t_extra, f_set, n_dict, c_map, sj_logged, sj_api_obj))
            except Exception: return (t_code, t_source, t_extra, None)

        tasks_to_run = []
        for i, (code, name, source, extra) in enumerate(targets):
            if source == 'upload' and upload_current >= upload_limit: continue
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            tasks_to_run.append((code, name, source, extra))
            if source == 'upload': upload_current += 1
            seen.add((code, source))

        sj_logged_in_flag = st.session_state.get('sj_logged_in', False)
        sj_api_obj = st.session_state.get('sj_api', None)

        with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
            future_to_task = {executor.submit(process_stock_task, t[0], t[1], t[2], t[3], futures_copy, notes_copy, code_map_copy, sj_logged_in_flag, sj_api_obj): t for t in tasks_to_run}
            completed_count = 0
            total_tasks = len(tasks_to_run) if len(tasks_to_run) > 0 else 1
            
            for future in as_completed(future_to_task):
                t_code, t_source, t_extra, data = future.result()
                completed_count += 1
                bar.progress(min(completed_count / total_tasks, 1.0))
                status_text.text(f"正在分析 ({completed_count}/{total_tasks}): {t_code} ...")
                if data:
                    data['_source'] = t_source
                    data['_order'] = t_extra
                    data['_source_rank'] = 1 if t_source == 'upload' else 2
                    
                    # 檢查是否已存在資料，若存在則依據來源優先權判斷是否覆蓋
                    if t_code in existing_data:
                        if existing_data[t_code]['_source'] == 'upload' and t_source == 'search':
                            pass  # 已有在顯示筆數內的檔案資料，忽略查詢資料的覆蓋，保留檔案排序
                        elif existing_data[t_code]['_source'] == 'search' and t_source == 'upload':
                            existing_data[t_code] = data  # 檔案資料優先權高，覆蓋掉原先寫入的查詢資料
                        else:
                            existing_data[t_code] = data
                    else:
                        existing_data[t_code] = data
                # 每個單一股票任務結束後即時主動回收
                del data
        
        bar.empty()
        status_text.empty()
        
        if existing_data:
            df_temp = pd.DataFrame(list(existing_data.values()))
            # 確保寫入 stock_data 時，嚴格按照來源優先權與順序排序
            if '_source_rank' in df_temp.columns:
                df_temp = df_temp.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
            st.session_state.stock_data = df_temp
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            
        # 強制清除大型臨時變數並回收記憶體
        del tasks_to_run
        del existing_data
        gc.collect()

    if not st.session_state.stock_data.empty:
        # 新增：當限制筆數減少時，自動隱藏多餘的檔案上傳資料
        df_check = st.session_state.stock_data
        if '_source' in df_check.columns:
            upload_mask = df_check['_source'] == 'upload'
            if upload_mask.sum() > st.session_state.limit_rows:
                keep_upload = df_check[upload_mask].head(st.session_state.limit_rows)
                keep_other = df_check[~upload_mask]
                st.session_state.stock_data = pd.concat([keep_upload, keep_other]).sort_values(by=['_source_rank', '_order'] if '_source_rank' in df_check.columns else None)
                save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)

        df_all = st.session_state.stock_data.copy()
        if '_source' not in df_all.columns: df_all['_source'] = 'upload'
        df_all = df_all.rename(columns={"漲停價": "當日漲停價", "跌停價": "當日跌停價", "獲利目標": "+3%", "防守停損": "-3%"})
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        
        if hide_non_stock:
             mask_etf = df_all['代號'].str.startswith('00')
             mask_warrant = (df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()
             df_all = df_all[~(mask_etf | mask_warrant)]
        
        if '_source_rank' in df_all.columns: df_all = df_all.sort_values(by=['_source_rank', '_order'])
        df_display = df_all.reset_index(drop=True)
        
        for i, row in df_display.iterrows():
            code = row['代號']
            points = row.get('_points', [])
            manual = st.session_state.saved_notes.get(code, "")
        
            if not points:
                cached = st.session_state.get('cached_notes', {}).get(code, {})
                if cached and cached.get('note'):
                    new_full_note = cached['note']
                    new_auto_note = cached.get('auto', '')
                else:
                    new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
            else:
                new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
        
            df_display.at[i, "戰略備註"] = new_full_note
            df_display.at[i, "_auto_note"] = new_auto_note
            clean_name = row['名稱'].replace('🔴 ', '').replace('🟢 ', '').replace('⚪ ', '')
            df_display.at[i, "名稱"] = clean_name

        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        points_map = df_display.set_index('代號')['_points'].to_dict() if '_points' in df_display.columns else {}
        auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict() if '_auto_note' in df_display.columns else {}

        # 進行自訂價價差與5日線價差計算
        if "自訂價價差" not in df_display.columns: df_display["自訂價價差"] = None
        if "5日線價差" not in df_display.columns: df_display["5日線價差"] = None
        
        for i, row in df_display.iterrows():
            ma5_val = row.get('_ma5')
            if pd.isna(ma5_val):
                for p in row.get('_points', []):
                    if p.get('tag') in ['多', '空', '平']:
                        ma5_val = p.get('val')
                        break
            
            if pd.notna(ma5_val):
                c_price = row.get('自訂價(可修)')
                if pd.notna(c_price) and str(c_price).strip() != "":
                    try: df_display.at[i, '自訂價價差'] = round(float(c_price) - float(ma5_val), 2)
                    except: pass
                    
                close_p = row.get('收盤價')
                if pd.notna(close_p) and str(close_p).strip() != "":
                    try: df_display.at[i, '5日線價差'] = round(float(close_p) - float(ma5_val), 2)
                    except: pass

        # 預設關閉，關閉後維持既有選股表格與資料流程，不會篩掉任何候選。
        risk_preview_enabled = st.checkbox(
            "🛡️ 啟用風險篩選預覽（可隨時關閉回到原表）",
            value=False,
            key="risk_filter_preview_enabled",
            help="僅提供選股風險評分與預覽，不會自動下單或改寫原始選股資料。"
        )
        risk_details = {}
        risk_show_only_eligible = False

        if risk_preview_enabled:
            if 'risk_filter_market_data' not in st.session_state:
                st.session_state.risk_filter_market_data = {
                    'attention': {}, 'disposition': [], 'updated': None, 'errors': []
                }

            with st.expander("🛡️ 選股風險篩選設定", expanded=True):
                risk_col1, risk_col2, risk_col3 = st.columns(3)
                with risk_col1:
                    risk_direction = st.radio("判斷方向", ["多頭", "空頭"], horizontal=True, key="risk_filter_direction")
                    risk_min_score = st.slider("最低評分", min_value=60, max_value=90, value=75, key="risk_filter_min_score")
                with risk_col2:
                    risk_max_extension = st.slider("最大乖離（ATR）", min_value=1.0, max_value=3.0, value=2.0, step=0.1, key="risk_filter_max_extension")
                    risk_block_attention = st.checkbox("封鎖注意累計 ≥ 2", value=True, key="risk_filter_block_attention")
                with risk_col3:
                    risk_show_only_eligible = st.checkbox("只顯示可操作候選", value=False, key="risk_filter_show_eligible")
                    if st.button("🔄 更新上市注意／處置名單", key="refresh_risk_filter_market_data"):
                        fetch_twse_market_risk_lists.clear()
                        with st.spinner("正在更新上市注意／處置名單..."):
                            attention, disposition, errors = fetch_twse_market_risk_lists()
                        st.session_state.risk_filter_market_data = {
                            'attention': attention,
                            'disposition': disposition,
                            'updated': datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M:%S'),
                            'errors': errors
                        }

                market_risk_data = st.session_state.risk_filter_market_data
                if market_risk_data.get('updated') and not market_risk_data.get('errors'):
                    st.caption(f"上市注意／處置名單更新：{market_risk_data['updated']}；上櫃標的請仍以『戰略資料庫 → 處置股』為準。")
                elif market_risk_data.get('errors'):
                    st.warning("上市注意／處置名單暫時無法完整更新；本次僅保留技術面風險評分。")
                else:
                    st.info("尚未更新上市注意／處置名單；資料未查核時不會被誤判為安全。")

            market_risk_data = st.session_state.risk_filter_market_data
            attention_counts = market_risk_data.get('attention', {})
            disposition_codes = market_risk_data.get('disposition', [])
            twse_lists_updated = bool(market_risk_data.get('updated')) and not market_risk_data.get('errors')

            for i, row in df_display.iterrows():
                result = calculate_risk_filter_result(
                    row, risk_direction, risk_max_extension, attention_counts, disposition_codes,
                    twse_lists_updated, risk_block_attention
                )
                code = str(row.get('代號', ''))
                risk_details[code] = result
                df_display.at[i, '風險'] = result['risk']
                df_display.at[i, '評分'] = result['score']
                df_display.at[i, '乖離'] = f"{result['extension']:+.1f} ATR" if result['extension'] is not None else "—"
                df_display.at[i, '隔日規則'] = result['rule']
                df_display.at[i, '_risk_eligible'] = result['eligible'] and result['score'] >= risk_min_score

            if risk_show_only_eligible:
                df_display = df_display[df_display['_risk_eligible']].reset_index(drop=True)
                if df_display.empty:
                    st.warning("目前沒有符合門檻的候選；可降低最低評分、放寬最大乖離，或切換回原表。")

            input_cols = ["移除", "代號", "名稱", "風險", "評分", "乖離", "隔日規則", "戰略備註", "自訂價(可修)", "狀態", "自訂價價差", "5日線價差", "當日漲停價", "當日跌停價", "收盤價", "漲跌幅", "期貨"]
        else:
            input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "自訂價價差", "5日線價差", "當日漲停價", "當日跌停價", "收盤價", "漲跌幅", "期貨"]
        for col in input_cols:
            if col not in df_display.columns: df_display[col] = None

        cols_to_fmt = ["當日漲停價", "當日跌停價", "自訂價(可修)", "自訂價價差", "5日線價差"]
        for c in cols_to_fmt:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        if "收盤價" in df_display.columns: df_display["收盤價"] = df_display["收盤價"].astype(object)
        if "漲跌幅" in df_display.columns: df_display["漲跌幅"] = df_display["漲跌幅"].astype(object)

        if "收盤價" in df_display.columns and "漲跌幅" in df_display.columns:
            for i in range(len(df_display)):
                try:
                    p = float(df_display.at[i, "收盤價"])
                    chg = float(df_display.at[i, "漲跌幅"])
                    df_display.at[i, "收盤價"] = fmt_price(p)
                    df_display.at[i, "漲跌幅"] = f"{chg:+.2f}%"
                except:
                    df_display.at[i, "收盤價"] = fmt_price(df_display.at[i, "收盤價"])
                    try: df_display.at[i, "漲跌幅"] = f"{float(df_display.at[i, '漲跌幅']):.2f}%"
                    except: pass

        df_display = df_display.reset_index(drop=True)
        for col in input_cols:
             if col not in ["移除", "評分"]: df_display[col] = df_display[col].astype(str)

        # 定義上色邏輯
        def style_tab1_df(row):
            styles = [''] * len(row)
            note = str(row.get('戰略備註', ''))
            st_val = str(row.get('狀態', ''))
            risk_val = str(row.get('風險', ''))
            
            if st_val == "漲停":
                name_c = 'background-color: #ff4b4b; color: #ffffff; font-weight: bold;'
            elif st_val == "跌停":
                name_c = 'background-color: #00e676; color: #ffffff; font-weight: bold;'
            elif st_val == "命中":
                name_c = 'background-color: #ffeb3b; color: #000000; font-weight: bold;'
            else:
                name_c = 'color: #ff4b4b;' if "多" in note else ('color: #00e676;' if "空" in note else '')
            
            price_c = ''
            try:
                c_val = float(str(row.get('漲跌幅', '0')).replace('%', '').replace('+', ''))
                price_c = 'color: #ff4b4b;' if c_val > 0 else ('color: #00e676;' if c_val < 0 else '')
            except: pass
            
            st_val = str(row.get('狀態', ''))
            status_c = 'color: #ff4b4b;' if st_val in ["漲停", "強"] else ('color: #00e676;' if st_val in ["跌停", "弱"] else ('color: #ffeb3b;' if st_val == "命中" else ''))
            
            for idx, col in enumerate(row.index):
                if col == "名稱": styles[idx] = name_c
                elif col in ["收盤價", "漲跌幅"]: styles[idx] = price_c
                elif col == "狀態": styles[idx] = status_c
                elif col in ["自訂價價差", "5日線價差"]:
                    val = row[col]
                    try:
                        f_val = float(val)
                        if f_val > 0: styles[idx] = 'color: #ff4b4b;'
                        elif f_val < 0: styles[idx] = 'color: #00e676;'
                        else: styles[idx] = 'color: white;'
                    except: pass
                elif col == "風險":
                    if risk_val.startswith('🚫') or risk_val.startswith('🔴'):
                        styles[idx] = 'color: #ff4b4b; font-weight: bold;'
                    elif risk_val.startswith('🟡'):
                        styles[idx] = 'color: #ffeb3b; font-weight: bold;'
                    elif risk_val.startswith('🟢'):
                        styles[idx] = 'color: #00e676;'
            return styles
            
        styled_df = df_display[input_cols].style.apply(style_tab1_df, axis=1)

        risk_column_config = {}
        if risk_preview_enabled:
            risk_column_config = {
                "風險": st.column_config.TextColumn(width=115, disabled=True),
                "評分": st.column_config.ProgressColumn("評分", min_value=0, max_value=100, format="%d", width=90),
                "乖離": st.column_config.TextColumn(width=85, disabled=True),
                "隔日規則": st.column_config.TextColumn(width=150, disabled=True),
            }

        edited_df = st.data_editor(
            styled_df,
            column_config={
                **risk_column_config,
                "移除": st.column_config.CheckboxColumn("刪除", width=40, help="勾選後刪除並自動遞補"),
                "代號": st.column_config.TextColumn(disabled=True, width=50), 
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.TextColumn(disabled=True, width="small"),
                "期貨": st.column_config.TextColumn(width=80), 
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60), 
                "當日漲停價": st.column_config.TextColumn(width="small", disabled=True),
                "當日跌停價": st.column_config.TextColumn(width="small", disabled=True),
                "自訂價價差": st.column_config.TextColumn(width=70, disabled=True),
                "5日線價差": st.column_config.TextColumn(width=70, disabled=True),
                "狀態": None, # 設定為 None 即可在資料編輯器中隱藏該欄位
                "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px, disabled=False),
            },
            hide_index=True, width='stretch' if risk_preview_enabled else 'content', num_rows="fixed", key="main_editor"
        )

        if risk_preview_enabled and risk_details:
            detail_options = {
                f"{row['代號']} {row['名稱']}": str(row['代號'])
                for _, row in df_display.iterrows()
            }
            if detail_options:
                selected_risk_label = st.selectbox("查看風險評分明細", list(detail_options), key="risk_filter_detail_code")
                selected_risk = risk_details[detail_options[selected_risk_label]]
                st.caption(f"{selected_risk['detail']}｜隔日規則：{selected_risk['rule']}。僅供策略篩選與回測，不構成買賣建議。")
        
        if not edited_df.empty:
            trigger_rerun = False
            if "移除" in edited_df.columns:
                to_remove = edited_df[edited_df["移除"] == True]
                if not to_remove.empty:
                    remove_codes = to_remove["代號"].unique()
                    
                    # 速度優化：將刪除的股票資料快取起來
                    for c in remove_codes: 
                        st.session_state.ignored_stocks.add(str(c))
                        row_data = st.session_state.stock_data[st.session_state.stock_data["代號"] == c]
                        if not row_data.empty:
                            st.session_state.ignored_data_cache[c] = row_data.iloc[0].to_dict()
                            
                    # --- 新增：資源優化，忽略快取超過 5 檔就刪除最舊的 ---
                    while len(st.session_state.ignored_data_cache) > 5:
                        oldest_key = next(iter(st.session_state.ignored_data_cache))
                        del st.session_state.ignored_data_cache[oldest_key]
                    # ----------------------------------------------------
                    
                    # 從表格資料中剃除
                    st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(remove_codes)]
                    
                    # 🚀 關鍵修改：立刻儲存並 Rerun，讓畫面「瞬間」移除該行，
                    # 把耗時的遞補抓取動作交給最下方的區塊去處理，避免畫面卡死！
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.rerun()

            if not trigger_rerun and st.session_state.auto_update_last_row:
                editor_state = st.session_state.get('main_editor', {})
                edited_rows = editor_state.get('edited_rows', {})
                last_visible_idx = len(edited_df) - 1
                
                # 嚴格限制：只有當最後一列有發生編輯行為時，才觸發更新，避免中斷前面的連續輸入
                last_row_edited = str(last_visible_idx) in str(edited_rows.keys())
                
                if last_row_edited:
                    update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
                    
                    if st.session_state.update_delay_sec > 0: time.sleep(st.session_state.update_delay_sec)
                    
                    for j, r in st.session_state.stock_data.iterrows():
                        c_code = r['代號']
                        if c_code in update_map:
                            np, nn = update_map[c_code]['自訂價(可修)'], update_map[c_code]['戰略備註']
                            st.session_state.stock_data.at[j, '自訂價(可修)'] = np
                            if str(r['戰略備註']) != str(nn):
                                b_auto = str(auto_notes_dict.get(c_code, "")).strip()
                                n_note = str(nn).strip()
                                st.session_state.stock_data.at[j, '戰略備註'] = nn
                                st.session_state.saved_notes[c_code] = n_note[len(b_auto):] if b_auto and n_note.startswith(b_auto) else f"[M]{n_note}"
                        
                        st.session_state.stock_data.at[j, '狀態'] = recalculate_row(st.session_state.stock_data.loc[j], points_map)
                        
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    trigger_rerun = True

            if trigger_rerun: st.rerun()

        df_curr = st.session_state.stock_data
        if not df_curr.empty:
            upload_count = len(df_curr) if '_source' not in df_curr.columns else len(df_curr[df_curr['_source'] == 'upload'])
            limit = st.session_state.limit_rows
            
            # 若筆數不足且有候補名單，則進行自動遞補
            if upload_count < limit and st.session_state.all_candidates:
                needed = limit - upload_count
                existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                futures_copy = dict(st.session_state.futures_list)
                notes_copy = dict(st.session_state.saved_notes)
                code_map_copy, _ = load_local_stock_names()
                
                cand_to_fetch = []
                for cand in st.session_state.all_candidates:
                     c_code, c_name, c_source, c_extra = str(cand[0]), cand[1], cand[2], cand[3]
                     if c_source != 'upload' or c_code in st.session_state.ignored_stocks or c_code in existing_codes: continue
                     cand_to_fetch.append((c_code, c_name, c_source, c_extra))
                     if len(cand_to_fetch) >= needed: break
                     
                if cand_to_fetch:
                    with st.spinner(f"正在遞補 {len(cand_to_fetch)} 檔股票..."):
                        # 🚀 1. 優先從「背景預載快取」提取 (瞬間完成)
                        ready_results = []
                        remaining_to_fetch = []
                        
                        for cand in cand_to_fetch:
                            t_code = cand[0]
                            if t_code in st.session_state.get('prefetch_cache', {}):
                                ready_results.append(st.session_state.prefetch_cache.pop(t_code))
                            else:
                                remaining_to_fetch.append(cand)
                                
                        if ready_results:
                            st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(ready_results)], ignore_index=True)
                            
                        # 🚀 2. 若快取不足(例如剛開啟網頁還沒預載完)，才即時抓取剩下的
                        if remaining_to_fetch:
                            def _replenish_worker(cand):
                                time.sleep(API_REQUEST_GAP_SECONDS)
                                t_code, t_name, t_src, t_extra = cand
                                res = fetch_stock_data_raw(t_code, t_name, t_extra, futures_copy, notes_copy, code_map_copy, st.session_state.get('sj_logged_in', False), st.session_state.get('sj_api', None))
                                if res: res.update({'_source': t_src, '_order': t_extra, '_source_rank': 1})
                                return res

                            with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                                results = list(executor.map(_replenish_worker, remaining_to_fetch))
                                valid_results = [r for r in results if r]
                                if valid_results:
                                    st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame(valid_results)], ignore_index=True)
                        
                        # 修正：強制依據來源優先權進行排序，讓自動遞補的新股票排在查詢的股票之前
                        if '_source_rank' in st.session_state.stock_data.columns:
                            st.session_state.stock_data = st.session_state.stock_data.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)

                        save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                        st.rerun() # 遞補完成，立刻更新畫面

        
        st.markdown("---")
        # 重新配置欄位比例與順序
        col_rt_update, col_btn, col_clear, col_clear_price = st.columns([2, 2.5, 2, 2])
        with col_rt_update: btn_rt_update = st.button("⏱️ 即時更新報價", width='stretch', type="primary")
        with col_btn: btn_update = st.button("⚡ 執行更新&儲存手動備註", width='stretch')
        with col_clear: btn_clear_notes = st.button("🧹 清除手動備註", width='stretch', help="清除所有記憶的戰略備註內容")
        with col_clear_price: btn_clear_price = st.button("🗑️ 清除自訂價", width='stretch', help="清除所有輸入的自訂價")
        
        if btn_clear_price:
            for idx, row in st.session_state.stock_data.iterrows():
                st.session_state.stock_data.at[idx, '自訂價(可修)'] = ""
                st.session_state.stock_data.at[idx, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[idx], points_map)
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("自訂價已清除", icon="🗑️")
            st.rerun()

        if btn_rt_update:
            if st.session_state.get('sj_logged_in', False) and st.session_state.get('sj_api'):
                sj_api = st.session_state.sj_api
                updated = False
                with st.spinner("正在透過永豐API更新報價..."):
                    for i, row in st.session_state.stock_data.iterrows():
                        code = str(row['代號'])
                        try:
                            contract = None
                            if code in ["TWF=F", "TMF=F"]:
                                if code == "TWF=F":
                                    contract = min([c for c in sj_api.Contracts.Futures.TXF if c.code[-2:] not in ["R1", "R2"] and '/' not in c.code], key=lambda c: getattr(c, 'delivery_date', '999999'))
                                else:
                                    contract = sj_api.Contracts.Futures.TMF.TMFR1
                            else:
                                try: contract = sj_api.Contracts.Stocks[code]
                                except: pass
                            
                            if contract:
                                snap = sj_api.snapshots([contract])
                                if snap and len(snap) > 0:
                                    rt_price = snap[0].close
                                    if rt_price > 0:
                                        st.session_state.stock_data.at[i, '自訂價(可修)'] = fmt_price(rt_price)
                                        st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
                                        updated = True
                        except Exception:
                            pass
                if updated:
                    tz_tw = pytz.timezone('Asia/Taipei')
                    st.session_state.last_rt_update_time = datetime.now(tz_tw).strftime("%Y/%m/%d %H:%M:%S")
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.rerun()
            else:
                st.warning("⚠️ 請先登入永豐 API 才能使用即時更新報價功能。")

        if btn_clear_notes:
            st.session_state.saved_notes = {}
            st.toast("手動備註已清除", icon="🧹")
            if not st.session_state.stock_data.empty:
                 for idx, row in st.session_state.stock_data.iterrows():
                     clean_note, _ = generate_note_from_points(row.get('_points', []), "", show_3d_hilo)
                     st.session_state.stock_data.at[idx, '戰略備註'] = clean_note
                     if '_auto_note' in st.session_state.stock_data.columns: st.session_state.stock_data.at[idx, '_auto_note'] = clean_note
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.rerun()
        
        auto_update = st.checkbox("☑️ 啟用最後一列自動更新", value=st.session_state.auto_update_last_row, key="toggle_auto_update")
        st.session_state.auto_update_last_row = auto_update
        if auto_update:
            col_delay, _ = st.columns([2, 8])
            with col_delay: st.session_state.update_delay_sec = st.number_input("⏳ 緩衝秒數", min_value=0.0, max_value=5.0, step=0.1, value=st.session_state.update_delay_sec)

        if 'last_rt_update_time' in st.session_state:
            st.markdown(f"<div style='text-align: left; color: #888; font-size: 14px; margin-top: 5px; margin-bottom: 10px;'>透過永豐API即時更新報價(更新時間:{st.session_state.last_rt_update_time})</div>", unsafe_allow_html=True)

        if btn_update:
             update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
             for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    new_note = update_map[code]['戰略備註']
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = update_map[code]['自訂價(可修)']
                    if str(row['戰略備註']) != str(new_note):
                        b_auto = str(auto_notes_dict.get(code, "")).strip()
                        n_note = str(new_note).strip()
                        st.session_state.saved_notes[code] = n_note[len(b_auto):] if b_auto and n_note.startswith(b_auto) else f"[M]{n_note}"
                    st.session_state.stock_data.at[i, '戰略備註'] = new_note
                st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
             save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
             st.rerun()

        st.markdown("### ⚡獨立計算")
        col_q1, col_q2 = st.columns([5, 1.5])
        with col_q1:
            indep_selection = st.multiselect(
                "🔍 快速查詢 (中文/代號)", 
                options=stock_options, 
                key="indep_search_multiselect", 
                placeholder="輸入 2330 或 台積電..."
            )
        with col_q2:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            btn_indep_run = st.button("🚀 執行分析", key="btn_indep_run", use_container_width=True)
            
        if btn_indep_run and indep_selection:
            # --- 確保期貨清單有被載入 ---
            if not st.session_state.futures_list: 
                st.session_state.futures_list = fetch_futures_list()
                
            indep_data = []
            c_map_q, n_map_q = load_local_stock_names()
            f_set = st.session_state.futures_list if 'futures_list' in st.session_state else {}
            sj_logged = st.session_state.get('sj_logged_in', False)
            sj_api_obj = st.session_state.get('sj_api', None)
            
            # 🚀 修正關鍵：在進入多執行緒前，先將 session_state 內容提取為區域變數
            notes_copy = dict(st.session_state.get('saved_notes', {}))
            
            with st.spinner("正在獨立分析..."):
                def _indep_worker(item):
                    time.sleep(API_REQUEST_GAP_SECONDS)  # 保留既有請求間隔
                    parts = item.split(' ', 1)
                    q_code = parts[0]
                    q_name = parts[1] if len(parts) > 1 else ""
                    return fetch_stock_data_raw(
                        q_code, q_name, None, f_set, notes_copy, # <--- 這裡改用提取出來的 notes_copy
                        c_map_q, sj_logged, sj_api_obj
                    )
                
                with ThreadPoolExecutor(max_workers=ANALYSIS_MAX_WORKERS) as executor:
                    results = list(executor.map(_indep_worker, indep_selection))
                    indep_data = [res for res in results if res]
                        
            if indep_data:
                df_indep = pd.DataFrame(indep_data)
                
                # 重新套用戰略備註與價差邏輯
                for i, row in df_indep.iterrows():
                    pts = row.get('_points', [])
                    manual = st.session_state.saved_notes.get(row['代號'], "")
                    n_full, n_auto = generate_note_from_points(pts, manual, show_3d_hilo)
                    df_indep.at[i, "戰略備註"] = n_full
                    df_indep.at[i, "名稱"] = row['名稱'].replace('🔴 ', '').replace('🟢 ', '').replace('⚪ ', '')
                    
                    ma5_val = row.get('_ma5')
                    if pd.isna(ma5_val):
                        for p in pts:
                            if p.get('tag') in ['多', '空', '平']:
                                ma5_val = p.get('val')
                                break
                    if pd.notna(ma5_val):
                        close_p = row.get('收盤價')
                        if pd.notna(close_p) and str(close_p).strip() != "":
                            try: df_indep.at[i, '5日線價差'] = round(float(close_p) - float(ma5_val), 2)
                            except: pass

                input_cols = ["代號", "名稱", "戰略備註", "5日線價差", "當日漲停價", "當日跌停價", "收盤價", "漲跌幅", "期貨"]
                for col in input_cols:
                    if col not in df_indep.columns: df_indep[col] = None
                    
                cols_to_fmt = ["當日漲停價", "當日跌停價", "5日線價差"]
                for c in cols_to_fmt:
                    if c in df_indep.columns: df_indep[c] = df_indep[c].apply(fmt_price)

                # 強制轉換為 object 型別，以防寫入字串時發生 TypeError
                if "收盤價" in df_indep.columns: df_indep["收盤價"] = df_indep["收盤價"].astype(object)
                if "漲跌幅" in df_indep.columns: df_indep["漲跌幅"] = df_indep["漲跌幅"].astype(object)

                if "收盤價" in df_indep.columns and "漲跌幅" in df_indep.columns:
                    for i in range(len(df_indep)):
                        try:
                            p = float(df_indep.at[i, "收盤價"])
                            chg = float(df_indep.at[i, "漲跌幅"])
                            df_indep.at[i, "收盤價"] = fmt_price(p)
                            df_indep.at[i, "漲跌幅"] = f"{chg:+.2f}%"
                        except:
                            df_indep.at[i, "收盤價"] = fmt_price(df_indep.at[i, "收盤價"])
                            try: df_indep.at[i, "漲跌幅"] = f"{float(df_indep.at[i, '漲跌幅']):.2f}%"
                            except: pass

                for col in input_cols:
                    df_indep[col] = df_indep[col].astype(str)

                # 套用與主表格完全一致的顏色邏輯
                styled_indep = df_indep[input_cols].style.apply(style_tab1_df, axis=1)

                st.dataframe(
                    styled_indep,
                    column_config={
                        "代號": st.column_config.TextColumn(width=50), 
                        "名稱": st.column_config.TextColumn(width="small"),
                        "收盤價": st.column_config.TextColumn(width="small"),
                        "漲跌幅": st.column_config.TextColumn(width="small"),
                        "期貨": st.column_config.TextColumn(width=80), 
                        "自訂價(可修)": st.column_config.TextColumn("自訂價", width=60), 
                        "當日漲停價": st.column_config.TextColumn(width="small"),
                        "當日跌停價": st.column_config.TextColumn(width="small"),
                        "自訂價價差": st.column_config.TextColumn(width=70),
                        "5日線價差": st.column_config.TextColumn(width=70),
                        "狀態": None, # 設定為 None 隱藏獨立計算結果的狀態欄位
                        "戰略備註": st.column_config.TextColumn("戰略備註", width=note_width_px)
                    },
                    hide_index=True, width='content', key="indep_table_output"
                )

with tab2:
    tab2_1, tab2_2, tab2_3 = st.tabs(["當沖損益室", "波段信用室", "期權交易室"])
    
    with tab2_1:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            calc_price = st.number_input("基準價格", value=float(st.session_state.calc_base_price), step=0.01, format="%.2f", key="input_base_price")
            if calc_price != st.session_state.calc_base_price:
                st.session_state.calc_base_price = calc_price
                st.session_state.calc_view_price = apply_tick_rules(calc_price)
        with c2: shares = st.number_input("股數", value=1000, step=1000)
        with c3: discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0)
        with c4: min_fee = st.number_input("最低手續費 (元)", value=20, step=1)
        with c5: tick_count = st.number_input("顯示檔數 (檔)", value=10, min_value=1, max_value=50, step=1)
        direction = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)

        stop_col1, stop_col2, _ = st.columns([1, 1, 3])
        with stop_col1:
            day_stop_loss_percent = st.number_input(
                "停損幅度 (%)", value=5.0, min_value=0.0, max_value=100.0,
                step=0.5, format="%.1f", key="day_stop_loss_percent"
            )
        with stop_col2:
            day_is_long = "多" in direction
            day_stop_price = calculate_stop_loss_price(calc_price, day_stop_loss_percent, day_is_long)
            stop_direction = "下跌" if day_is_long else "上漲"
            st.metric("停損價", fmt_price(day_stop_price), f"{stop_direction} {day_stop_loss_percent:g}%")
        
        # --- 新增：目標價快速試算區 ---
        st.markdown("##### 🎯 目標價快速試算")
        col_t1, col_t2 = st.columns([1, 4])
        with col_t1:
            # 將 value 設為 None 並加入 placeholder，實現預設為空值
            target_p = st.number_input("輸入目標價", value=None, step=0.5, format="%.2f", key="input_target_price", placeholder="請輸入...")
        with col_t2:
            fee_rate = 0.001425; tax_rate = 0.0015
            base_p = st.session_state.calc_base_price
            is_long = "多" in direction
            
            # 加入防呆判斷：只有當使用者輸入數字時才進行計算
            if target_p is not None:
                if is_long:
                    t_buy = base_p; t_sell = target_p
                    t_buy_fee = max(min_fee, math.floor(t_buy * shares * fee_rate * (discount/10)))
                    t_sell_fee = max(min_fee, math.floor(t_sell * shares * fee_rate * (discount/10)))
                    t_tax = math.floor(t_sell * shares * tax_rate)
                    t_cost = (t_buy * shares) + t_buy_fee
                    t_income = (t_sell * shares) - t_sell_fee - t_tax
                    t_profit = t_income - t_cost
                    t_total_fee = t_buy_fee + t_sell_fee
                else:
                    t_sell = base_p; t_buy = target_p
                    t_sell_fee = max(min_fee, math.floor(t_sell * shares * fee_rate * (discount/10)))
                    t_buy_fee = max(min_fee, math.floor(t_buy * shares * fee_rate * (discount/10)))
                    t_tax = math.floor(t_sell * shares * tax_rate)
                    t_income = (t_sell * shares) - t_sell_fee - t_tax
                    t_cost = (t_buy * shares) + t_buy_fee
                    t_profit = t_income - t_cost
                    t_total_fee = t_buy_fee + t_sell_fee
                
                t_roi = (t_profit / (base_p * shares) * 100) if (base_p * shares) != 0 else 0
                
                t_color = "#ff4b4b" if t_profit > 0 else ("#00e676" if t_profit < 0 else "white")
                diff_val = target_p - base_p
                diff_color = "#ff4b4b" if diff_val > 0 else ("#00e676" if diff_val < 0 else "white")
                
                html_str = f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: {t_color}; font-weight: bold;">{int(t_profit):,} ({t_roi:+.2f}%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">{int(t_total_fee):,}</span></div>
                    <div>交易稅: <span style="color: #cccccc;">{int(t_tax):,}</span></div>
                    <div>價差: <span style="color: {diff_color}; font-weight: bold;">{diff_val:+.2f}</span></div>
                </div>
                """
            else:
                # 空值時的預設顯示狀態
                html_str = f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: white; font-weight: bold;">0 (+0.00%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">0</span></div>
                    <div>交易稅: <span style="color: #cccccc;">0</span></div>
                    <div>價差: <span style="color: white; font-weight: bold;">+0.00</span></div>
                </div>
                """
                
            st.markdown(html_str, unsafe_allow_html=True)
        st.markdown("---")
        
        limit_up, limit_down = calculate_limits(st.session_state.calc_base_price)
        b1, b2, _ = st.columns([1, 1, 6])
        with b1:
            if st.button("🔽 向下", width='stretch'):
                if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
                st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, -tick_count)
                st.rerun()
        with b2:
            if st.button("🔼 向上", width='stretch'):
                if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
                st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, tick_count)
                st.rerun()
        
        ticks_range = range(tick_count, -(tick_count + 1), -1)
        calc_data = []
        base_p = st.session_state.calc_base_price
        if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = base_p
        view_p = st.session_state.calc_view_price
        is_long = "多" in direction
        fee_rate = 0.001425; tax_rate = 0.0015 
        
        for i in ticks_range:
            p = move_tick(view_p, i)
            
            if is_long:
                buy_price = base_p; sell_price = p
                buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
                sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
                tax = math.floor(sell_price * shares * tax_rate)
                cost = (buy_price * shares) + buy_fee
                income = (sell_price * shares) - sell_fee - tax
                profit = income - cost
                total_fee = buy_fee + sell_fee
            else: 
                sell_price = base_p; buy_price = p
                sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
                buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
                tax = math.floor(sell_price * shares * tax_rate)
                income = (sell_price * shares) - sell_fee - tax
                cost = (buy_price * shares) + buy_fee
                profit = income - cost
                total_fee = buy_fee + sell_fee
            roi = 0
            if (base_p * shares) != 0: roi = (profit / (base_p * shares)) * 100
            diff = p - base_p
            diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
            if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
            
            note_type = ""
            if abs(p - limit_up) < 0.001: note_type = "up"
            elif abs(p - limit_down) < 0.001: note_type = "down"
            is_base = (abs(p - base_p) < 0.001)
            
            calc_data.append({
                "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%",
                "手續費": int(total_fee), "交易稅": int(tax), "_profit": profit, "_note_type": note_type, "_is_base": is_base
            })
            
        df_calc = pd.DataFrame(calc_data)
        
        def style_calc_row(row):
            is_base = row['_is_base']
            prof = row['_profit']
            
            if is_base: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
            if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
            if prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
            return ['color: gray'] * len(row)

        if not df_calc.empty:
            table_height = (len(df_calc) + 1) * 35
            st.dataframe(
                df_calc.style.apply(style_calc_row, axis=1), 
                width=425, 
                hide_index=True, 
                height=table_height,
                column_config={"_profit": None, "_note_type": None, "_is_base": None}
            )

    with tab2_2:
        st.markdown("##### 📊 波段信用室")
        c2_1, c2_2, c2_3, c2_4, c2_5 = st.columns(5)
        with c2_1:
            swing_calc_price = st.number_input("基準價格", value=None, step=0.5, format="%.2f", key="input_swing_base_price", placeholder="請輸入...")
        with c2_2: swing_shares = st.number_input("股數", value=1000, step=1000, key="swing_shares")
        with c2_3: swing_discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0, key="swing_discount")
        with c2_4: swing_min_fee = st.number_input("最低手續費 (元)", value=20, step=1, key="swing_min_fee")
        with c2_5: swing_tick_count = st.number_input("顯示檔數 (檔)", value=10, min_value=1, max_value=50, step=1, key="swing_tick_count")
        
        c2_type, c2_margin, c2_rate, c2_days, c2_fee_rate = st.columns(5)
        with c2_type:
            swing_type = st.selectbox("交易選項", ["個股", "融資(多)", "融券(空)"], key="swing_type")
        with c2_margin:
            margin_ratio = st.number_input("融資/券成數(%)", value=60.0 if swing_type == "融資(多)" else (90.0 if swing_type == "融券(空)" else 0.0), step=10.0, key="margin_ratio")
        with c2_rate:
            annual_rate = st.number_input("年利率(%)", value=6.25 if swing_type == "融資(多)" else (0.2 if swing_type == "融券(空)" else 0.0), step=0.1, key="annual_rate")
        with c2_days:
            swing_date_range = st.date_input("選擇區間", value=(datetime.now(tz_tw).date(), datetime.now(tz_tw).date() + timedelta(days=1)), key="swing_date_range")
            if isinstance(swing_date_range, tuple) and len(swing_date_range) == 2:
                swing_days = (swing_date_range[1] - swing_date_range[0]).days
                if swing_days < 1: swing_days = 1
            else:
                swing_days = 1
            st.caption(f"總天數: {swing_days} 天")
        with c2_fee_rate:
            short_fee_rate = st.number_input("借券費率(‱)", value=8.0 if swing_type == "融券(空)" else 0.0, step=1.0, key="short_fee_rate")

        swing_stop_col1, swing_stop_col2, _ = st.columns([1, 1, 3])
        with swing_stop_col1:
            swing_stop_loss_percent = st.number_input(
                "停損幅度 (%)", value=5.0, min_value=0.0, max_value=100.0,
                step=0.5, format="%.1f", key="swing_stop_loss_percent"
            )
        with swing_stop_col2:
            swing_is_long = swing_type != "融券(空)"
            swing_stop_price = (
                calculate_stop_loss_price(swing_calc_price, swing_stop_loss_percent, swing_is_long)
                if swing_calc_price is not None else 0.0
            )
            swing_stop_direction = "下跌" if swing_is_long else "上漲"
            st.metric(
                "停損價", fmt_price(swing_stop_price) if swing_calc_price is not None else "—",
                f"{swing_stop_direction} {swing_stop_loss_percent:g}%"
            )

        # --- 目標價快速試算區 ---
        st.markdown("##### 🎯 目標價快速試算")
        col_st1, col_st2 = st.columns([1, 4])
        with col_st1:
            swing_target_p = st.number_input("輸入目標價", value=None, step=0.5, format="%.2f", key="swing_target_price", placeholder="請輸入...")
        with col_st2:
            if swing_calc_price is not None and swing_target_p is not None:
                s_base_p = swing_calc_price
                s_fee_rate = 0.001425; s_tax_rate = 0.003
                
                t_buy_price = s_base_p if swing_type in ["個股", "融資(多)"] else swing_target_p
                t_sell_price = swing_target_p if swing_type in ["個股", "融資(多)"] else s_base_p
                
                t_buy_fee = max(swing_min_fee, math.floor(t_buy_price * swing_shares * s_fee_rate * (swing_discount/10)))
                t_sell_fee = max(swing_min_fee, math.floor(t_sell_price * swing_shares * s_fee_rate * (swing_discount/10)))
                t_tax = math.floor(t_sell_price * swing_shares * s_tax_rate)
                t_total_fee = t_buy_fee + t_sell_fee
                
                t_stock_value_buy = t_buy_price * swing_shares
                t_stock_value_sell = t_sell_price * swing_shares
                
                t_profit = 0
                t_interest = 0
                t_borrow_fee = 0
                t_roi = 0
                
                if swing_type == "個股":
                    t_cost = t_stock_value_buy + t_buy_fee
                    t_income = t_stock_value_sell - t_sell_fee - t_tax
                    t_profit = t_income - t_cost
                    t_roi = (t_profit / t_cost * 100) if t_cost > 0 else 0
                elif swing_type == "融資(多)":
                    t_margin_loan = math.floor(t_stock_value_buy * (margin_ratio/100) / 1000) * 1000
                    t_self_prepare = t_stock_value_buy - t_margin_loan + t_buy_fee
                    t_interest = round(t_margin_loan * (annual_rate/100) * (swing_days/365))
                    t_net_sell = t_stock_value_sell - t_sell_fee - t_tax - t_margin_loan - t_interest
                    t_profit = t_net_sell - t_self_prepare
                    t_roi = (t_profit / t_self_prepare * 100) if t_self_prepare > 0 else 0
                elif swing_type == "融券(空)":
                    t_margin_deposit = math.ceil(t_stock_value_sell * (margin_ratio/100) / 100) * 100
                    t_borrow_fee = math.floor(t_stock_value_sell * (short_fee_rate/10000))
                    t_sell_guaranty = t_stock_value_sell - t_sell_fee - t_tax - t_borrow_fee
                    t_interest = round((t_margin_deposit + t_sell_guaranty) * (annual_rate/100) * (swing_days/365))
                    t_buy_cost = t_stock_value_buy + t_buy_fee
                    t_refund = t_margin_deposit + t_sell_guaranty + t_interest - t_buy_cost
                    t_profit = t_refund - t_margin_deposit
                    t_roi = (t_profit / t_margin_deposit * 100) if t_margin_deposit > 0 else 0

                t_color = "#ff4b4b" if t_profit > 0 else ("#00e676" if t_profit < 0 else "white")
                t_diff_val = swing_target_p - s_base_p
                t_diff_color = "#ff4b4b" if t_diff_val > 0 else ("#00e676" if t_diff_val < 0 else "white")
                
                swing_html_str = (
                    f"<div style='font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;'>"
                    f"<div>預估損益: <span style='color: {t_color}; font-weight: bold;'>{int(t_profit):,} ({t_roi:+.2f}%)</span></div>"
                    f"<div>手續費總和: <span style='color: #cccccc;'>{int(t_total_fee):,}</span></div>"
                    f"<div>交易稅: <span style='color: #cccccc;'>{int(t_tax):,}</span></div>"
                )
                if swing_type in ["融資(多)", "融券(空)"]:
                    swing_html_str += f"<div>利息: <span style='color: #cccccc;'>{int(t_interest):,}</span></div>"
                    if swing_type == "融券(空)":
                        swing_html_str += f"<div>借券費: <span style='color: #cccccc;'>{int(t_borrow_fee):,}</span></div>"
                swing_html_str += f"<div>價差: <span style='color: {t_diff_color}; font-weight: bold;'>{t_diff_val:+.2f}</span></div></div>"
                st.markdown(swing_html_str, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="font-size: 16px; display: flex; flex-wrap: wrap; gap: 15px; padding: 10px; background-color: rgba(255,255,255,0.05); border-radius: 8px; margin-top: 28px;">
                    <div>預估損益: <span style="color: white; font-weight: bold;">0 (+0.00%)</span></div>
                    <div>手續費總和: <span style="color: #cccccc;">0</span></div>
                    <div>交易稅: <span style="color: #cccccc;">0</span></div>
                    <div>價差: <span style="color: white; font-weight: bold;">+0.00</span></div>
                </div>
                """, unsafe_allow_html=True)
        st.markdown("---")

        if swing_calc_price is not None:
            if swing_calc_price != st.session_state.get('swing_base_price', 0):
                st.session_state.swing_base_price = swing_calc_price
                st.session_state.swing_view_price = apply_tick_rules(swing_calc_price)

            swing_limit_up, swing_limit_down = calculate_limits(st.session_state.swing_base_price)
            sb1, sb2, _ = st.columns([1, 1, 6])
            with sb1:
                if st.button("🔽 向下", key="swing_btn_down", width='stretch'):
                    if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = st.session_state.swing_base_price
                    st.session_state.swing_view_price = move_tick(st.session_state.swing_view_price, -swing_tick_count)
                    st.rerun()
            with sb2:
                if st.button("🔼 向上", key="swing_btn_up", width='stretch'):
                    if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = st.session_state.swing_base_price
                    st.session_state.swing_view_price = move_tick(st.session_state.swing_view_price, swing_tick_count)
                    st.rerun()

            swing_ticks_range = range(swing_tick_count, -(swing_tick_count + 1), -1)
            swing_calc_data = []
            s_base_p = st.session_state.swing_base_price
            if 'swing_view_price' not in st.session_state: st.session_state.swing_view_price = s_base_p
            s_view_p = st.session_state.swing_view_price
            s_fee_rate = 0.001425; s_tax_rate = 0.003
            
            for i in swing_ticks_range:
                p = move_tick(s_view_p, i)
                
                buy_price = s_base_p if swing_type in ["個股", "融資(多)"] else p
                sell_price = p if swing_type in ["個股", "融資(多)"] else s_base_p
                
                buy_fee = max(swing_min_fee, math.floor(buy_price * swing_shares * s_fee_rate * (swing_discount/10)))
                sell_fee = max(swing_min_fee, math.floor(sell_price * swing_shares * s_fee_rate * (swing_discount/10)))
                tax = math.floor(sell_price * swing_shares * s_tax_rate)
                total_fee = buy_fee + sell_fee
                
                stock_value_buy = buy_price * swing_shares
                stock_value_sell = sell_price * swing_shares
                
                profit = 0
                interest = 0
                borrow_fee = 0
                maintenance_ratio = 0.0
                call_price = 0.0
                
                if swing_type == "個股":
                    cost = stock_value_buy + buy_fee
                    income = stock_value_sell - sell_fee - tax
                    profit = income - cost
                    roi = (profit / cost * 100) if cost > 0 else 0
                elif swing_type == "融資(多)":
                    margin_loan = math.floor(stock_value_buy * (margin_ratio/100) / 1000) * 1000
                    self_prepare = stock_value_buy - margin_loan + buy_fee
                    interest = round(margin_loan * (annual_rate/100) * (swing_days/365))
                    net_sell = stock_value_sell - sell_fee - tax - margin_loan - interest
                    profit = net_sell - self_prepare
                    maintenance_ratio = (stock_value_sell / margin_loan) * 100 if margin_loan > 0 else 0
                    call_price = (margin_loan * 1.3) / swing_shares if margin_loan > 0 else 0
                    roi = (profit / self_prepare * 100) if self_prepare > 0 else 0
                elif swing_type == "融券(空)":
                    margin_deposit = math.ceil(stock_value_sell * (margin_ratio/100) / 100) * 100
                    borrow_fee = math.floor(stock_value_sell * (short_fee_rate/10000))
                    sell_guaranty = stock_value_sell - sell_fee - tax - borrow_fee
                    interest = round((margin_deposit + sell_guaranty) * (annual_rate/100) * (swing_days/365))
                    buy_cost = stock_value_buy + buy_fee
                    refund = margin_deposit + sell_guaranty + interest - buy_cost
                    profit = refund - margin_deposit
                    maintenance_ratio = ((margin_deposit + sell_guaranty) / stock_value_buy) * 100 if stock_value_buy > 0 else 0
                    call_price = (margin_deposit + sell_guaranty) / (1.3 * swing_shares) if swing_shares > 0 else 0
                    roi = (profit / margin_deposit * 100) if margin_deposit > 0 else 0

                diff = p - s_base_p
                diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
                if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
                
                is_base = (abs(p - s_base_p) < 0.001)
                
                row_data = {
                    "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%",
                    "手續費": int(total_fee), "交易稅": int(tax)
                }
                if swing_type in ["融資(多)", "融券(空)"]:
                    if swing_type == "融券(空)":
                        row_data["借券費"] = int(borrow_fee)
                    row_data["利息"] = int(interest)
                    row_data["維持率%"] = f"{maintenance_ratio:.1f}%" if maintenance_ratio > 0 else "-"
                    row_data["強制回補價"] = f"{call_price:.2f}" if call_price > 0 else "-"
                    
                row_data["_profit"] = profit
                row_data["_is_base"] = is_base
                if swing_type in ["融資(多)", "融券(空)"]:
                    row_data["_call"] = True if maintenance_ratio > 0 and maintenance_ratio < 130 else False
                else:
                    row_data["_call"] = False

                swing_calc_data.append(row_data)

            df_swing_calc = pd.DataFrame(swing_calc_data)
            
            def style_swing_row(row):
                is_base = row['_is_base']
                prof = row['_profit']
                is_call = row['_call']
                
                if is_base: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
                if is_call: return ['background-color: #ffcccc; color: #ff0000; font-weight: bold'] * len(row)
                if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
                if prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
                return ['color: gray'] * len(row)

            if not df_swing_calc.empty:
                st.dataframe(
                    df_swing_calc.style.apply(style_swing_row, axis=1), 
                    width='stretch',
                    hide_index=True, 
                    height=(len(df_swing_calc) + 1) * 35,
                    column_config={
                        "成交價": st.column_config.TextColumn(width="small"),
                        "漲跌": st.column_config.TextColumn(width="small"),
                        "預估損益": st.column_config.NumberColumn(width="small"),
                        "報酬率%": st.column_config.TextColumn(width="small"),
                        "手續費": st.column_config.NumberColumn(width="small"),
                        "交易稅": st.column_config.NumberColumn(width="small"),
                        "借券費": st.column_config.NumberColumn(width="small"),
                        "利息": st.column_config.NumberColumn(width="small"),
                        "維持率%": st.column_config.TextColumn(width="small"),
                        "強制回補價": st.column_config.TextColumn(width="small"),
                        "_profit": None, "_is_base": None, "_call": None
                    }
                )

    with tab2_3:
        st.markdown("##### 📈 期權交易室")
        st.markdown("""
        <style>
        .opt-card {
            background-color: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 10px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .opt-label {
            font-size: 13px;
            color: #aaa;
            margin-bottom: 5px;
        }
        .opt-value {
            font-size: 18px;
            font-weight: bold;
        }
        /* 做多與做空選擇文字的動態變色 */
        div[role="radiogroup"] label:has(input[value="🔴 做多 ▲"]:checked) p {
            color: #ff4b4b !important;
            font-weight: bold !important;
        }
        div[role="radiogroup"] label:has(input[value="🟢 做空 ▼"]:checked) p {
            color: #00e676 !important;
            font-weight: bold !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # ---------------- 回呼函數定義 ----------------
        def sync_taifex_margin():
            try:
                url = 'https://openapi.taifex.com.tw/v1/IndexFuturesAndOptionsMargining'
                headers = {
                    'accept': 'application/json',
                    'If-Modified-Since': 'Mon, 26 Jul 1997 05:00:00 GMT',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
                r = requests.get(url, headers=headers, timeout=5, verify=False)
                if r.status_code == 200:
                    data = r.json()
                    res = {}
                    sync_date = ""
                    for item in data:
                        sym_api = item.get("Contract", "").strip()
                        margin_api = item.get("InitialMargin", 0)
                        if isinstance(margin_api, str): margin_api = float(margin_api.replace(',', ''))
                        res[sym_api] = margin_api
                        # 正規化: 無論 API 回傳中文名稱或英文代碼，統一存為英文代碼
                        sym_clean = sym_api.replace(' ', '')
                        if sym_clean in ["MXF", "TMF"] or "微型臺" in sym_clean or "微型台" in sym_clean:
                            res["MXF"] = margin_api
                            res["TMF"] = margin_api
                        elif sym_clean == "MTX" or "小型臺指" in sym_clean or "小型台指" in sym_clean:
                            res["MTX"] = margin_api
                        elif sym_clean == "TX" or sym_clean in ["臺股期貨", "台股期貨", "臺指期貨", "台指期貨"]:
                            res["TX"] = margin_api
                        if not sync_date and "Date" in item:
                            d_str = str(item["Date"])
                            if len(d_str) == 8:
                                sync_date = f"{d_str[:4]}/{d_str[4:6]}/{d_str[6:]}"
                    
                    st.session_state.taifex_margin_data = res
                    if sync_date:
                        st.session_state.taifex_sync_date = sync_date
                    
                    # 立即更新當前選擇的合約保證金數值
                    opt_tx_type = st.session_state.get('opt_tx_type', '大台 (TX)')
                    # 修正期交所 API 的合約代號 (大台 TX, 小台 MTX, 微台 TMF)
                    sym = "TX" if "大台" in opt_tx_type else ("MTX" if "小台" in opt_tx_type else "MXF")
                    if sym in res:
                        st.session_state["margin_display_tx"] = f"{res[sym]:,.0f}"
                    
                    st.toast("已同步期交所最新保證金", icon="✅")
            except Exception as e:
                st.toast(f"取得期交所保證金失敗: {e}", icon="⚠️")

        @st.cache_data(ttl=3600, show_spinner=False)
        def fetch_ssf_margin_info():
            """透過期交所 OpenAPI 獲取個股期貨保證金比例與小型合約資訊"""
            margin_map = {}
            has_small_set = set()
            sync_date = ""
            group_level_map = {}   # 新增
            maint_map = {}         # 新增
            try:
                url_pct = "https://openapi.taifex.com.tw/v1/SingleStockFuturesMargining"
                r_pct = requests.get(url_pct, headers={'accept': 'application/json', 'If-Modified-Since': 'Mon, 26 Jul 1997 05:00:00 GMT'}, timeout=5, verify=False)
                if r_pct.status_code == 200:
                    data = r_pct.json()
                    stock_contracts = {}
                    for item in data:
                        code = str(item.get("UnderlyingSecurityCode", "")).strip()
                        contract = str(item.get("Contract", "")).strip()
                        m_val = str(item.get("InitialMarginRate", "0")).replace('%', '').strip()
                        
                        if code:
                            if code not in stock_contracts:
                                stock_contracts[code] = set()
                            if contract:
                                stock_contracts[code].add(contract)
                            
                            try: margin_map[code] = float(m_val)
                            except: pass
                            maint_val = str(item.get("MaintenanceMarginRate", "0")).replace('%', '').strip()
                            try: maint_map[code] = float(maint_val)
                            except: pass
                            g_val = str(item.get("GroupLevel", "")).strip()
                            if g_val: group_level_map[code] = g_val
                        # 擷取更新日期
                        if not sync_date and "Date" in item:
                            d_str = str(item["Date"])
                            if len(d_str) == 8:
                                sync_date = f"{d_str[:4]}/{d_str[4:6]}/{d_str[6:]}"

                    # 判斷是否含有小型股期 (若同一股號有多個合約代號，即代表有小型股期)
                    for code, contracts in stock_contracts.items():
                        if len(contracts) > 1:
                            has_small_set.add(code)
            except: pass
            return margin_map, maint_map, group_level_map, has_small_set, sync_date

        def do_clear_opt():
            for k in ['opt_entry_p', 'opt_exit_p', 'opt_sl_p', 'opt_manual_margin_tx', 'opt_manual_margin_opt', 'opt_rt_price', 'opt_ref_price']:
                if k in st.session_state:
                    st.session_state[k] = None
            st.session_state['opt_sf_search'] = None
            if 'opt_custom_margin' in st.session_state:
                del st.session_state['opt_custom_margin']
            st.session_state['opt_lots'] = 1
            st.session_state['opt_dir'] = "🔴 做多 ▲"
            st.session_state['opt_margin_level'] = "級距一 | 13.5% (一般股票)"

        def on_opt_tab_change():
            do_clear_opt()
            # 新增此行：確保切換商品類別(例如切換至台指期)時，自動觸發抓取預設合約的最新價格
            st.session_state.opt_rt_trigger = True
        
        def on_f_contract_change():
            st.session_state.opt_rt_trigger = True
        # --------------------------------------------

        if 'taifex_margin_data' not in st.session_state:
            st.session_state.taifex_margin_data = {}
            sync_taifex_margin()
        
        # 確保期貨清單有被載入 (穩定來源)
        if 'futures_list' not in st.session_state or not st.session_state.futures_list:
            st.session_state.futures_list = fetch_futures_list()

        # 取得 API 保證金與小型股期資料
        ssf_margin_map, ssf_maint_map, ssf_group_level_map, has_small_set, ssf_sync_date = fetch_ssf_margin_info()

        c_map_opt, _ = load_local_stock_names()
        sf_opts = []
        # 改回使用穩定的 futures_list 產生選單，避免 API 異常時選單空白
        for code, status in st.session_state.futures_list.items():
            name = c_map_opt.get(code, code)
            sf_opts.append(f"{code} {name}期貨 (一般 x2000)")
            # 結合網頁抓取的 "(小)" 標記與 API 回傳的 has_small_set 雙重確認
            if "(小)" in status or code in has_small_set:
                sf_opts.append(f"{code} 小型{name}期貨 (小型 x100)")
        sf_opts = sorted(sf_opts)

        col_left, col_right = st.columns([1.1, 1], gap="large")

        with col_left:
            st.markdown("###### ① 合約設定")
            opt_main_tab = st.selectbox(
                "合約類別", 
                ["台指期", "個股期貨", "選擇權"], 
                key="opt_main_tab",
                on_change=on_opt_tab_change
            )

            if opt_main_tab == "台指期":
                opt_tx_type = st.radio("合約規格", ["大台 (TX)", "小台 (MTX)", "微台 (TMF)"], horizontal=True, key="opt_tx_type", on_change=on_f_contract_change)
                mult = 200 if "大台" in opt_tx_type else (50 if "小台" in opt_tx_type else 10)
                tax_rate = 0.00002
            elif opt_main_tab == "個股期貨":
                # 1. 搜尋股期輸入股號只要出現對應的股期就好
                opt_sf_input = st.text_input("搜尋股期 (輸入代號或名稱)", placeholder="例如: 2330", key="opt_sf_input")
                filtered_sf_opts = [opt for opt in sf_opts if opt_sf_input in opt] if opt_sf_input else sf_opts
                
                search_stock_futures = st.selectbox(
                    "選擇對應股期", 
                    options=filtered_sf_opts, 
                    index=0 if filtered_sf_opts else None,
                    key="opt_sf_search",
                    on_change=on_f_contract_change
                )
                is_small = search_stock_futures is not None and "小型" in search_stock_futures
                opt_sub_type = st.radio("合約規格", ["一般 (x2000)", "小型 (x100)"], horizontal=True, index=1 if is_small else 0, on_change=on_f_contract_change)
                mult = 100 if "小型" in opt_sub_type else 2000
                tax_rate = 0.00002
            else:
                st.markdown("<div style='margin-bottom: 10px; font-size: 14px;'>合約規格：選擇權 (x50)</div>", unsafe_allow_html=True)
                mult = 50
                tax_rate = 0.001

            opt_dir = st.radio("部位方向", ["🔴 做多 ▲", "🟢 做空 ▼"], horizontal=True, key="opt_dir")
            opt_lots = st.number_input("口數", min_value=1, value=1, step=1, key="opt_lots")

           # 新增：當最新成交價為空（例如剛切換到期權交易室），自動觸發獲取預設的台指期大台價格
            if st.session_state.get('opt_rt_price') is None:
                st.session_state.opt_rt_trigger = True

            # --- 獲取最新即時價格邏輯 (支援夜盤) ---
            if st.session_state.get('opt_rt_trigger', False):
                st.session_state.opt_rt_trigger = False
                rt_p, ref_p = None, None
                try:
                    sj_logged = st.session_state.get('sj_logged_in', False)
                    sj_api = st.session_state.get('sj_api', None)
                    
                    if opt_main_tab in ["台指期", "個股期貨"] and sj_logged and sj_api:
                        contract = None
                        
                        tz_now = datetime.now(pytz.timezone('Asia/Taipei'))
                        today_str = tz_now.strftime("%Y%m%d")
                        now_time = tz_now.time()
                        
                        def is_valid_contract(c):
                            code_str = getattr(c, 'code', '')
                            if code_str.endswith('R1') or code_str.endswith('R2') or '/' in code_str:
                                return False
                                
                            # 優先取得 delivery_date，若無則取 delivery_month，並清除斜線與破折號以利比對
                            d_date = str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999')))
                            d_date = d_date.replace('/', '').replace('-', '')
                            
                            # 若為 8 碼精確日期 (YYYYMMDD)
                            if len(d_date) == 8:
                                if d_date < today_str:
                                    return False
                                if d_date == today_str and now_time >= dt_time(13, 45):
                                    return False
                            # 若僅有 6 碼年月 (YYYYMM)
                            elif len(d_date) == 6:
                                today_ym = today_str[:6]
                                if d_date < today_ym:
                                    return False
                                if d_date == today_ym:
                                    # 判斷今天是否為結算日(第三個星期三)
                                    cal = calendar.monthcalendar(tz_now.year, tz_now.month)
                                    wednesdays = [week[calendar.WEDNESDAY] for week in cal if week[calendar.WEDNESDAY] != 0]
                                    if len(wednesdays) >= 3:
                                        settle_day = wednesdays[2]
                                        if tz_now.day > settle_day:
                                            return False
                                        if tz_now.day == settle_day and now_time >= dt_time(13, 45):
                                            return False
                            return True

                        if opt_main_tab == "台指期":
                            try:
                                if "大台" in opt_tx_type:
                                    contracts = sj_api.Contracts.Futures.TXF
                                elif "小台" in opt_tx_type:
                                    contracts = sj_api.Contracts.Futures.MXF
                                else:
                                    contracts = sj_api.Contracts.Futures.TMF
                                    
                                valid_list = [c for c in contracts if is_valid_contract(c)]
                                if valid_list:
                                    contract = min(valid_list, key=lambda c: str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999'))).replace('/', '').replace('-', ''))
                            except: pass
                        elif opt_main_tab == "個股期貨":
                            code = search_stock_futures.split(" ")[0] if search_stock_futures else ""
                            if code:
                                try:
                                    is_small = False
                                    if 'opt_sub_type' in locals() and isinstance(opt_sub_type, str) and "小型" in opt_sub_type:
                                        is_small = True
                                    candidates = []
                                    for category in sj_api.Contracts.Futures:
                                        for c in category:
                                            if str(getattr(c, 'underlying_code', '')) == str(code):
                                                c_name = getattr(c, 'name', '')
                                                if is_small and "小型" in c_name: candidates.append(c)
                                                elif not is_small and "小型" not in c_name: candidates.append(c)
                                    if not candidates:
                                        for category in sj_api.Contracts.Futures:
                                            for c in category:
                                                if str(getattr(c, 'underlying_code', '')) == str(code): candidates.append(c)
                                    if candidates:
                                        valid_contracts = [c for c in candidates if is_valid_contract(c)]
                                        if valid_contracts:
                                            contract = min(valid_contracts, key=lambda c: str(getattr(c, 'delivery_date', getattr(c, 'delivery_month', '999999'))).replace('/', '').replace('-', ''))
                                except Exception: pass

                        if contract:
                            try:
                                snap = sj_api.snapshots([contract])
                                if snap and len(snap) > 0:
                                    s = snap[0]
                                    rt_p = s.close if s.close > 0 else s.open
                                    
                                    # 修正：精準讀取永豐快照的 change_price 反推基準價
                                    change_val = getattr(s, 'change_price', getattr(s, 'change', None))
                                    if change_val is not None and rt_p > 0:
                                        ref_p = rt_p - float(change_val)
                                    else:
                                        # 堅固備援：直接向 API 請求 K 棒，抓取當日 13:45 最後一筆收盤價
                                        tz_tw_loc = pytz.timezone('Asia/Taipei')
                                        now_loc = datetime.now(tz_tw_loc)
                                        start_str = (now_loc - timedelta(days=5)).strftime("%Y-%m-%d")
                                        end_str = now_loc.strftime("%Y-%m-%d")
                                        
                                        try:
                                            kbars = sj_api.kbars(contract, start=start_str, end=end_str)
                                            if kbars and hasattr(kbars, 'ts') and len(kbars.ts) > 0:
                                                df_k = pd.DataFrame({**kbars})
                                                df_k['ts'] = pd.to_datetime(df_k['ts'])
                                                if df_k['ts'].dt.tz is not None:
                                                    df_k['ts'] = df_k['ts'].dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
                                                
                                                day_mask = (df_k['ts'].dt.time >= dt_time(8, 45)) & (df_k['ts'].dt.time <= dt_time(13, 45))
                                                df_day = df_k[day_mask]
                                                if not df_day.empty:
                                                    daily_closes = df_day.groupby(df_day['ts'].dt.date)['Close'].last()
                                                    curr_d = now_loc.date()
                                                    
                                                    # 14:00 前 (含凌晨夜盤與上午日盤)：基準價為「前一交易日」的 13:45
                                                    if now_loc.time() < dt_time(14, 0):
                                                        past_closes = daily_closes[daily_closes.index < curr_d]
                                                        if not past_closes.empty: ref_p = float(past_closes.iloc[-1])
                                                    # 14:00 後 (下午夜盤開始)：基準價為「今日」的 13:45
                                                    else:
                                                        valid_closes = daily_closes[daily_closes.index <= curr_d]
                                                        if not valid_closes.empty: ref_p = float(valid_closes.iloc[-1])
                                        except: pass
                                        
                                        # 若真的都抓不到，最後才回退至快取的參考價
                                        if ref_p is None or ref_p == 0:
                                            ref_p = getattr(contract, 'reference', 0)
                                            
                                    # 防呆：若夜盤尚未有成交量，讓成交價顯示與基準價相同
                                    if rt_p == 0 and ref_p is not None and ref_p > 0:
                                        rt_p = ref_p
                            except Exception: pass
                except Exception: pass
                
                st.session_state.opt_rt_price = rt_p
                st.session_state.opt_ref_price = ref_p
            # ------------------------------------

            c_p1, c_p2 = st.columns(2)
            with c_p1:
                entry_p = st.number_input("進場價 (點)", value=None, format="%.2f", placeholder="輸入進場價", key="opt_entry_p")
                
                # --- 顯示最新成交價及重新整理按鈕 ---
                if opt_main_tab in ["台指期", "個股期貨"]:
                    # 修正：注入 CSS 實現 align-items: center 讓按鈕與文字垂直置中平行，並移除 margin 造成的落差
                    st.markdown("""
                    <style>
                    div[data-testid="stHorizontalBlock"]:has(button[title="更新最新價格"]) {
                        align-items: center !important;
                    }
                    div:has(> button[title="更新最新價格"]) button {
                        min-height: 32px !important;
                        font-size: 14px !important;
                        padding: 0px 5px !important;
                        margin: 0px !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)

                    c_rt1, c_rt2 = st.columns([5, 1])
                    with c_rt1:
                        rt_p = st.session_state.get('opt_rt_price', None)
                        ref_p = st.session_state.get('opt_ref_price', None)
                        if rt_p is not None and ref_p is not None:
                            color = "#ff4b4b" if rt_p > ref_p else ("#00e676" if rt_p < ref_p else "white")
                            diff = rt_p - ref_p
                            sign = "+" if diff > 0 else ""
                            # 新增：計算漲跌幅百分比
                            pct_chg = (diff / ref_p * 100) if ref_p else 0.0
                            st.markdown(f"<div style='font-size:20px; margin:0px;'>最新成交價: <span style='color:{color}; font-weight:bold;'>{rt_p:g}</span> <span style='font-size:16px; color:{color};'>({sign}{diff:g})({pct_chg:+.2f}%)</span></div>", unsafe_allow_html=True)
                        else:
                            st.markdown("<div style='font-size:20px; margin:0px; color:#aaa;'>最新成交價: 尚未更新</div>", unsafe_allow_html=True)
                    with c_rt2:
                        if st.button("🔄", key="btn_refresh_opt_rt", help="更新最新價格", use_container_width=True):
                            st.session_state.opt_rt_trigger = True
                            st.rerun()
                # ------------------------------------

            with c_p2:
                exit_p = st.number_input("出場/目標價 (點)", value=None, format="%.2f", placeholder="輸入目標價", key="opt_exit_p")

            st.markdown("###### ⇆ 停損及保證金設定")
            sl_p = st.number_input("停損價 (點) - 用於風報比", value=None, format="%.2f", placeholder="輸入停損價", key="opt_sl_p")
            
            # 手續費記憶
            config = load_config()
            if 'saved_opt_fee' not in st.session_state:
                st.session_state.saved_opt_fee = config.get('saved_opt_fee', None)
                
            opt_fee = st.number_input("單邊手續費 (元/口)", value=st.session_state.saved_opt_fee, step=1, placeholder="輸入手續費 (必填)")
            if opt_fee is not None and opt_fee != st.session_state.saved_opt_fee:
                st.session_state.saved_opt_fee = opt_fee
                config['saved_opt_fee'] = opt_fee
                try:
                    with open(CONFIG_FILE, "w") as f: json.dump(config, f)
                except: pass

            actual_margin_req = 0
            margin_display_val = "尚未同步或無資料"
            sync_text = "尚未同步"

            if opt_main_tab == "台指期":
                sync_text = st.session_state.get('taifex_sync_date', '尚未同步')
                opt_tx_type = st.session_state.get('opt_tx_type', '大台 (TX)')
                
                # 修正此處的合約代號對應
                sym = "TX" if "大台" in opt_tx_type else ("MTX" if "小台" in opt_tx_type else "MXF")
                
                fetched_margin = st.session_state.taifex_margin_data.get(sym, 0)
                
                # 偵測合約切換，若切換則強制更新 session_state 裡的值
                if st.session_state.get("last_opt_tx_type") != opt_tx_type:
                    st.session_state["last_opt_tx_type"] = opt_tx_type
                    if fetched_margin > 0:
                        st.session_state["margin_display_tx"] = f"{fetched_margin:,.0f}"
                    else:
                        st.session_state["margin_display_tx"] = ""

                st.markdown("<div style='font-size: 14px; margin-bottom: 5px;'>每口保證金 (原始)</div>", unsafe_allow_html=True)
                c_m1, c_m2 = st.columns([3, 1])
                with c_m1:
                    user_margin = st.text_input("每口保證金", label_visibility="collapsed", key="margin_display_tx")
                    try:
                        actual_margin_req = float(user_margin.replace(',', '').replace(' ', '')) if user_margin else 0
                    except ValueError:
                        actual_margin_req = fetched_margin
                with c_m2:
                    st.button("↺ 重新整理", key="refresh_tx_margin", use_container_width=True, on_click=sync_taifex_margin)
                if sync_text != "尚未同步":
                    st.markdown(f"<div style='font-size:13px; margin-top: -10px; margin-bottom: 10px;'><span style='color:#00e676;'>✔️</span> <span style='color:#ff4b4b;'>已同步</span> <span style='color:#aaa;'>期交所資料：{sync_text}</span></div>", unsafe_allow_html=True)
                    
            elif opt_main_tab == "個股期貨":
                sync_text = ssf_sync_date if ssf_sync_date else "尚未同步"
                
                sf_code = search_stock_futures.split(" ")[0] if search_stock_futures else ""
                margin_pct = ssf_margin_map.get(sf_code, 0)
                
                # 直接使用 API 的級距百分比計算標準合約保證金，避免套用到特殊調整合約的絕對金額
                calc_margin = 0
                if margin_pct > 0 and entry_p is not None:
                    calc_margin = round(entry_p * mult * (margin_pct / 100.0))
                
                # 偵測合約切換或進場價變更，若切換則強制更新 session_state 裡的值
                current_ssf_state = f"{sf_code}_{entry_p}_{mult}"
                if st.session_state.get("last_ssf_state") != current_ssf_state:
                    st.session_state["last_ssf_state"] = current_ssf_state
                    if calc_margin > 0:
                        st.session_state["margin_display_ssf"] = f"{calc_margin:,.0f}"
                    else:
                        st.session_state["margin_display_ssf"] = ""
# ── 保證金級距資訊卡片 ──────────────────────────
                group_level = ssf_group_level_map.get(sf_code, "")
                maint_pct   = ssf_maint_map.get(sf_code, 0)
                calc_maint  = round(entry_p * mult * (maint_pct / 100.0)) if maint_pct > 0 and entry_p is not None else 0

                if sf_code and (group_level or margin_pct > 0):
                    level_label = f"第 {group_level} 級" if group_level.isdigit() else (group_level if group_level else "—")
                    init_line  = f"原始 <b style='color:#ff9800;'>{calc_margin:,}</b> 元" if calc_margin > 0 else f"原始比例 <b>{margin_pct}%</b>"
                    maint_line = (f"　｜　維持 <b style='color:#4fc3f7;'>{calc_maint:,}</b> 元" if calc_maint > 0
                                  else (f"　｜　維持比例 <b>{maint_pct}%</b>" if maint_pct > 0 else ""))
                    st.markdown(f"""
                    <div style='background:#0d1b2a;border:1px solid #1e3a5f;border-radius:8px;padding:9px 14px;margin-bottom:12px;font-size:13px;'>
                        <span style='color:#00e5ff;font-weight:700;'>📊 保證金級距：{level_label}</span>
                        <span style='color:#888;margin-left:10px;'>原始 {margin_pct}%</span>
                        {"<span style='color:#888;'> ／ 維持 " + str(maint_pct) + "%</span>" if maint_pct > 0 else ""}
                        <div style='margin-top:5px;color:#e0e0e0;'>{init_line}{maint_line}</div>
                    </div>""", unsafe_allow_html=True)
                # ────────────────────────────────────────────────
                st.markdown("<div style='font-size: 14px; margin-bottom: 5px;'>每口保證金 (原始)</div>", unsafe_allow_html=True)
                c_m1, c_m2 = st.columns([3, 1])
                with c_m1:
                    user_margin = st.text_input("每口保證金", label_visibility="collapsed", placeholder=f"API 級距 {margin_pct}%" if margin_pct > 0 else "", key="margin_display_ssf")
                    try:
                        actual_margin_req = float(user_margin.replace(',', '').replace(' ', '')) if user_margin else 0
                    except ValueError:
                        actual_margin_req = calc_margin
                with c_m2:
                    if st.button("↺ 重新整理", key="refresh_ssf_margin", use_container_width=True):
                        fetch_ssf_margin_info.clear()
                        st.rerun()
                if sync_text != "尚未同步":
                    st.markdown(f"<div style='font-size:13px; margin-top: -10px; margin-bottom: 10px;'><span style='color:#00e676;'>✔️</span> <span style='color:#ff4b4b;'>已同步</span> <span style='color:#aaa;'>期交所資料：{sync_text}</span></div>", unsafe_allow_html=True)
                    
            else: # 選擇權
                margin_req = st.number_input("每口保證金 (原始)", value=None, step=1000.0, format="%.0f", key="opt_manual_margin_opt", placeholder="買方為權利金，賣方請手動輸入")
                if margin_req is not None:
                    actual_margin_req = margin_req
                elif entry_p is not None:
                    actual_margin_req = entry_p * mult
                else:
                    actual_margin_req = 0
            
            st.markdown("<div style='margin-top: 15px;'></div>", unsafe_allow_html=True)

        with col_right:
            st.markdown("###### 📈 損益結果")

            if entry_p is not None and exit_p is not None and opt_fee is not None:
                pt_diff = (exit_p - entry_p) if "做多" in opt_dir else (entry_p - exit_p)
                gross_pnl = pt_diff * mult * opt_lots

                tax_buy = round(entry_p * mult * tax_rate) * opt_lots
                tax_sell = round(exit_p * mult * tax_rate) * opt_lots
                total_tax = tax_buy + tax_sell
                total_fee = opt_fee * 2 * opt_lots

                net_pnl = gross_pnl - total_tax - total_fee

                pnl_color = "#ff4b4b" if net_pnl > 0 else ("#00e676" if net_pnl < 0 else "white")

                st.markdown(f"""
                <div class="opt-card">
                    <div class="opt-label">預估淨損益 (含手續費與稅)</div>
                    <div class="opt-value" style="color: {pnl_color}; font-size: 24px;">{int(net_pnl):,} 元</div>
                </div>
                """, unsafe_allow_html=True)

                cr1, cr2 = st.columns(2)
                with cr1:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">毛損益</div>
                        <div class="opt-value">{int(gross_pnl):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cr2:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">點差/口</div>
                        <div class="opt-value">{pt_diff:g} 點</div>
                    </div>
                    """, unsafe_allow_html=True)

                cr3, cr4 = st.columns(2)
                with cr3:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">手續費 (雙邊)</div>
                        <div class="opt-value">{int(total_fee):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cr4:
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">期交稅 (雙邊)</div>
                        <div class="opt-value">{int(total_tax):,} 元</div>
                    </div>
                    """, unsafe_allow_html=True)

                # 刪除原本冗長重複的區塊，只留下這一段：
                if actual_margin_req > 0:
                    roi = net_pnl / (actual_margin_req * opt_lots) * 100
                    st.markdown(f"""
                    <div class="opt-card">
                        <div class="opt-label">預估保證金總額</div>
                        <div class="opt-value">{int(actual_margin_req * opt_lots):,} 元 <span style="font-size: 14px; font-weight: normal; color: #aaa;">(報酬率: {roi:.2f}%)</span></div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("<br><div style='font-size:16px; font-weight:bold; color:#ddd; margin-bottom:5px;'>風報比 (R:R)</div>", unsafe_allow_html=True)
                if sl_p is not None:
                    risk_pt = (entry_p - sl_p) if "做多" in opt_dir else (sl_p - entry_p)
                    if risk_pt > 0:
                        reward_pt = pt_diff if pt_diff > 0 else 0
                        rrr = reward_pt / risk_pt if risk_pt != 0 else 0

                        st.markdown(f"<div style='text-align: right; font-size: 20px; font-weight: bold; margin-bottom: 5px;'>1 : {rrr:.2f}</div>", unsafe_allow_html=True)

                        total_rr = risk_pt + reward_pt
                        if total_rr > 0:
                            risk_pct = (risk_pt / total_rr) * 100
                            reward_pct = (reward_pt / total_rr) * 100
                        else:
                            risk_pct = 100; reward_pct = 0

                        st.markdown(f"""
                        <div style="width: 100%; height: 8px; display: flex; border-radius: 4px; overflow: hidden; margin-bottom: 5px;">
                            <div style="width: {risk_pct}%; background-color: #00e676;"></div>
                            <div style="width: {reward_pct}%; background-color: #ff4b4b;"></div>
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #aaa;">
                            <span>▼ 風險 {risk_pt:g} 點</span>
                            <span>▲ 報酬 {reward_pt:g} 點</span>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.error("停損價設定錯誤 (做多時停損應低於進場價，做空時應高於進場價)")
                else:
                    st.caption("輸入停損價後即可顯示風報比評估")
                    
                st.markdown("<br><div style='text-align: center; font-size: 12px; color: #888;'>💡 提示：手續費依各券商折扣不同 (大台≈100、小台≈50、微台≈25)<br>保證金請以期交所最新公告為準</div>", unsafe_allow_html=True)
            else:
                st.info("👈 請在左側填寫完整的 **進場價**、**出場/目標價** 與 **單邊手續費** 即可自動開始計算損益與風險。")
                
with tab_fibo:
    
    def format_fibo_tag(key):
        val = st.session_state[key].strip()
        if not val: 
            save_fibo_config()
            return
        if "(" in val and val.endswith(")"): 
            save_fibo_config()
            return
        
        code_map, name_map = load_local_stock_names()
        if val.isdigit():
            name = code_map.get(val, "")
            if name: st.session_state[key] = f"{name}({val})"
        else:
            matched_stocks = []
            for name, code in name_map.items():
                if val in name:
                    matched_stocks.append((name, code))
            if matched_stocks:
                def sort_key(item):
                    c = item[1]
                    if c.isdigit() and len(c) <= 4: return 0
                    elif c.isdigit(): return 1
                    return 2
                matched_stocks.sort(key=sort_key)
                best_match = matched_stocks[0]
                st.session_state[key] = f"{best_match[0]}({best_match[1]})"
        save_fibo_config()
    
    tab_fibo_chart, tab_fibo_manual = st.tabs(["📊 圖表分析", "🧮 手動計算"])

    with tab_fibo_chart:
        code_map_fibo, name_map_fibo = load_local_stock_names()
        fibo_stock_options = []
        for c, n in sorted(code_map_fibo.items()):
            if not st.session_state.get('allow_warrant_search', False) and is_warrant(c):
                continue
            fibo_stock_options.append(f"{n}({c})")
        # 下方 search_list 修改對應變數
        search_list = ["加權股價指數(TAIEX)", "臺股期貨(TX)", "微型臺指期貨(TMF)"] + fibo_stock_options

        def set_fibo_search(val):
            st.session_state.fibo_search_input = val
            st.session_state.fibo_trigger_search = True
            st.session_state.fibo_interval = "1d"

        with st.expander("⚙️ 設定快速標籤"):
            st.info("💡 將圖表字體大小設定獨立：")
            st.session_state.fibo_font_size = st.slider("圖表標籤字體大小", min_value=8, max_value=24, value=st.session_state.fibo_font_size)
            st.write("---")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.text_input("快速標籤 1", key="custom_tag_1", on_change=format_fibo_tag, args=("custom_tag_1",))
            c2.text_input("快速標籤 2", key="custom_tag_2", on_change=format_fibo_tag, args=("custom_tag_2",))
            c3.text_input("快速標籤 3", key="custom_tag_3", on_change=format_fibo_tag, args=("custom_tag_3",))
            c4.text_input("快速標籤 4", key="custom_tag_4", on_change=format_fibo_tag, args=("custom_tag_4",))
            c5.text_input("快速標籤 5", key="custom_tag_5", on_change=format_fibo_tag, args=("custom_tag_5",))

        st.write("📌 **快速查詢標籤** (點擊按鈕直接帶入)")
        btn_labels = [
            ("加權股價指數(TAIEX)", "加權股價指數(TAIEX)"),
            ("臺股期貨(TX)", "臺股期貨(TX)"),
            ("微型臺指期貨(TMF)", "微型臺指期貨(TMF)")
        ]
        for tag in [st.session_state.custom_tag_1, st.session_state.custom_tag_2, st.session_state.custom_tag_3, st.session_state.custom_tag_4, st.session_state.custom_tag_5]:
            if tag.strip(): btn_labels.append((tag.strip(), tag.strip()))
        
        if btn_labels:
            tag_cols = st.columns(len(btn_labels))
            for i, (label, val) in enumerate(btn_labels):
                if i < len(tag_cols):
                    tag_cols[i].button(label, on_click=set_fibo_search, args=(val,), width='stretch', key=f"btn_fibo_{i}")

        # 移除重複且未過濾的賦值，直接取得當前搜尋框的預設索引值
        current_val = st.session_state.fibo_search_input
        default_index = None
        
        if current_val:
            for i, opt in enumerate(search_list):
                if current_val in opt:
                    default_index = i
                    break

        def selectbox_changed():
            val = st.session_state.fibo_selectbox
            if val:
                st.session_state.fibo_search_input = val
                st.session_state.fibo_interval = "1d"
            else:
                st.session_state.fibo_search_input = ""

        selected_raw = st.selectbox(
            "🔍 搜尋股票 (可直接輸入股號或股名查找，或點擊上方快捷按鈕)",
            options=search_list,
            index=default_index,
            placeholder="請選擇或輸入...",
            key="fibo_selectbox",
            on_change=selectbox_changed
        )
        
        final_target = st.session_state.fibo_search_input
        
        st.write("---")
        st.write("⚙️ **圖表顯示設定**")
        col_m1, col_m2, col_m3, col_m4, col_v, col_w = st.columns([1, 1, 1, 1, 1.5, 2.5])
        s_ma5 = col_m1.checkbox("5MA (橘)", value=True)
        s_ma10 = col_m2.checkbox("10MA (淺藍)", value=True)
        s_ma20 = col_m3.checkbox("20MA (綠)", value=True)
        s_ma60 = col_m4.checkbox("60MA (黃)", value=True)
        s_vol = col_v.checkbox("📊 顯示成交量", value=True)
        ma_w = col_w.slider("均線粗細", min_value=1.0, max_value=5.0, value=st.session_state.ma_w, step=0.5, key="ma_w", on_change=save_fibo_config, label_visibility="collapsed")
        
        ma_flags = {'5': s_ma5, '10': s_ma10, '20': s_ma20, '60': s_ma60}

        st.write("---")
        interval_options = {"5m": "5分", "15m": "15分", "60m": "60分", "1d": "日", "1wk": "週", "1mo": "月"}
        try: default_radio_idx = list(interval_options.keys()).index(st.session_state.fibo_interval)
        except: default_radio_idx = 0 

        selected_interval_label = st.radio(
            "⏱️ 選擇時間標籤",
            options=list(interval_options.values()),
            index=default_radio_idx,
            horizontal=True
        )
        
        selected_interval = list(interval_options.keys())[list(interval_options.values()).index(selected_interval_label)]
        st.session_state.fibo_interval = selected_interval 
        
        if final_target.strip():
            plot_fibonacci_chart(final_target, selected_interval, font_size=st.session_state.fibo_font_size, ma_flags=ma_flags, ma_width=st.session_state.ma_w, show_vol=s_vol)
        else:
            st.info("請在上方選擇或輸入股票/期貨以顯示圖表。")

    with tab_fibo_manual:
        st.write("📌 **手動輸入高低點，計算費波納契回撤與延伸點位**")
        col_table, col_empty = st.columns([1, 2])
        
        with col_table:
            col_h, col_l = st.columns(2)
            with col_h:
                fibo_high = st.number_input("輸入波段高點：", value=None, step=1.0, format="%.2f")
            with col_l:
                fibo_low = st.number_input("輸入波段低點：", value=None, step=1.0, format="%.2f")
                
            if fibo_high is not None and fibo_low is not None:
                if fibo_high > 0 and fibo_low > 0 and fibo_high >= fibo_low:
                    diff = fibo_high - fibo_low
                    ratios_manual = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
                    
                    fibo_data = []
                    for r in ratios_manual:
                        price = fibo_low + (r * diff)
                        calc_price = round(price, 2)
                        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
                        fibo_data.append({
                            "比例": r_label,
                            "計算點位": f"{calc_price:.2f}",
                            "_raw_r": r 
                        })
                    
                    df_fibo = pd.DataFrame(fibo_data)
                    
                    def style_fibo_manual(row):
                        important_ratios = [0.0, 0.382, 0.5, 0.618, 1.0]
                        if row["_raw_r"] in important_ratios:
                            return ['background-color: #ffffcc; color: black; font-weight: bold;'] * len(row)
                        return [''] * len(row)
                        
                    table_height = (len(df_fibo) + 1) * 36
                    styled_fibo = df_fibo.style.apply(style_fibo_manual, axis=1)
                    
                    st.dataframe(
                        styled_fibo, 
                        width='stretch', 
                        height=table_height, 
                        hide_index=True,
                        column_config={"_raw_r": None}
                    )
                else:
                    st.warning("波段高點必須大於波段低點且大於0")
            else:
                st.info("請在上方輸入高低點數值開始計算。")


with tab_db:
    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["三大法人買賣超", "台指期籌碼快訊", "處置股"])
    
    with sub_tab1:
        st.markdown("#### 📊 台股三大法人每日買賣超統計")
        
        # 調整預設日期邏輯：若是週六或週日，預設切回上週五
        today_check = datetime.today()
        if today_check.weekday() == 5:
            default_date = today_check - timedelta(days=1)
        elif today_check.weekday() == 6:
            default_date = today_check - timedelta(days=2)
        else:
            default_date = today_check

        selected_date = st.date_input("選擇日期", default_date)
        date_str = selected_date.strftime("%Y%m%d")
        
        # 加入安全攔截，若發生錯誤則預設為 None
        try:
            df_inst = get_major_institutional_data(date_str)
        except Exception:
            df_inst = None
            
        if df_inst is not None:
            st.subheader(f"📅 {selected_date.strftime('%Y-%m-%d')} 統計結果")
            
            # 建立一個自訂的格式化函數，將原始數字轉為：千分位(+-XXX.XX億)
            def fmt_with_yi(val):
                try:
                    v = float(val)
                    yi_val = v / 100000000
                    return f"{int(v):,}  ({yi_val:+.2f}億)"
                except:
                    return str(val)
            
            # 將自訂格式套用到買進、賣出、買賣差額三個欄位
            fmt_dict = {col: fmt_with_yi for col in ['買進金額', '賣出金額', '買賣差額']}
            
            try:
                styled_df = df_inst.style.map(color_negative_positive, subset=['買賣差額']).format(fmt_dict)
            except AttributeError:
                styled_df = df_inst.style.applymap(color_negative_positive, subset=['買賣差額']).format(fmt_dict)
            
            # 使用 columns 進行縮排，不讓表格佔滿全螢幕
            col_tbl, _ = st.columns([1.5, 1])
            with col_tbl:
                st.dataframe(styled_df, width='stretch', hide_index=True)
            st.caption("數據來源：[台灣證券交易所 (TWSE)](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)")
        else:
            st.warning("該日期目前無資料（可能尚未開市或為休假日或證交所 API 觸發防護防阻）。")
                
        st.markdown("---")
        st.markdown("#### 📈 法人當日買賣超個股")
        inst_tabs = st.tabs(["外資當日買賣超", "投信當日買賣超", "自營商當日買賣超"])
        with inst_tabs[0]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_D.djhtm"), height=1185, width=800, scrolling=True)
        with inst_tabs[1]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DD.djhtm"), height=1185, width=800, scrolling=True)
        with inst_tabs[2]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DB.djhtm"), height=1185, width=800, scrolling=True)
        
    with sub_tab2:
        st.markdown("#### 📑 永豐期貨盤後籌碼自動化工具")
        
        if st.button("🔄 刷新最新報告清單"):
            get_report_list.clear()        # 只清報告清單
            fetch_and_parse_pdf.clear()   # 只清 PDF 快取
            st.rerun()

        reports = get_report_list()

        if not reports:
            st.warning("目前找不到相關報告，請檢查官網是否變動或稍後再試。")
        else:
            latest_report = reports[0]
            st.markdown(f"### 🔥 最新快訊: {latest_report['日期']} | {latest_report['title']}")
            
            with st.spinner("正在下載並自動預覽最新報告..."):
                data = fetch_and_parse_pdf(latest_report['url'])
                
                if data['ratio'] != "N/A" and data['ratio'] != "解析錯誤":
                    val = float(data['ratio'])
                    st.metric("散戶小台多空比", f"{val}%", delta=f"{val}%", delta_color="inverse")
                
                if data.get('images'):
                    col_img, _ = st.columns([1, 2])
                    with col_img:
                        for img in data['images']:
                            st.image(img, width='stretch')
                else:
                    st.warning("⚠️ 無法自動轉譯為圖片，請使用下方連結開啟。")
                    
            st.link_button("📥 點此進入原始報告下載頁面", latest_report['url'])
            
            st.divider()
            st.markdown("#### 📅 歷史報告清單")
            
            # 設定7天前的日期界線 (以台灣時間計算，精準到天)
            tz_tw = pytz.timezone('Asia/Taipei')
            limit_date = pd.Timestamp(datetime.now(tz_tw).date() - timedelta(days=7))
            
            for idx, report in enumerate(reports[1:], 1):
                show_report = False # 預設改為不顯示
                
                try:
                    # 改用 pandas 處理日期，可自動適應 - 或 / 等不同格式
                    rep_date = pd.to_datetime(report['日期'])
                    if rep_date.tzinfo is not None: 
                        rep_date = rep_date.tz_localize(None)
                        
                    if rep_date >= limit_date:
                        show_report = True
                except:
                    # 只有在明確寫 "近期發布" 時才例外顯示
                    if report['日期'] == "近期發布":
                        show_report = True
                        
                if show_report:
                    with st.expander(f"📅 {report['日期']} | {report['title']}"):
                        st.write(f"連結: [點此查看原始 PDF]({report['url']})")

    with sub_tab3:
        st.markdown("#### 🚨 處置股預測與公告")
        
        # 初始化處置股獨立重整計數器
        if 'disposal_refresh_idx' not in st.session_state:
            st.session_state.disposal_refresh_idx = 0
            
        # 新增重新整理按鈕
        if st.button("🔄 重新整理處置股", key="btn_refresh_disposal"):
            st.session_state.disposal_refresh_idx += 1
            st.rerun()
            
        # 將計數器加入網址參數，藉由改變網址強制 iframe 重新載入
        refresh_url = f"https://cmfaren.github.io/dispositionforecast/?t={st.session_state.disposal_refresh_idx}"
        
        components.iframe(
            refresh_url, 
            height=800, 
            scrolling=True
        )

with tab3:
    # ==========================================
    # 新增：自訂行事曆存取與 UI 介面
    # ==========================================
    CAL_OVERRIDE_FILE = "cal_override.json"
    
    def load_cal_overrides():
        if os.path.exists(CAL_OVERRIDE_FILE):
            try:
                with open(CAL_OVERRIDE_FILE, "r", encoding="utf-8") as f:
                    df = pd.DataFrame(json.load(f))
                    if not df.empty and "日期" in df.columns:
                        df["日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.date
                        return df
            except: pass
        
        # 預設給予一列空白資料，並確保日期欄位擁有正確的空值型別 (NaT)
        df = pd.DataFrame([{"日期": pd.NaT, "事件名稱": "", "文字顏色": "白色"}])
        df["日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.date
        return df
        
    def save_cal_overrides(df):
        try:
            # 確保日期格式為字串以利 JSON 儲存
            df_save = df.copy()
            df_save['日期'] = df_save['日期'].astype(str)
            df_save.to_json(CAL_OVERRIDE_FILE, orient="records", force_ascii=False)
        except: pass

    if 'cal_overrides' not in st.session_state:
        st.session_state.cal_overrides = load_cal_overrides()

    with st.expander("🛠️ 自訂與校正行事曆事件"):
        st.info("若發現系統預設日期或時間有誤，可在此手動新增或覆寫事件（例如：提前休市、自訂總經數據時間）。")
        edited_cal_df = st.data_editor(
            st.session_state.cal_overrides,
            num_rows="dynamic",
            column_config={
                "日期": st.column_config.DateColumn("日期 (YYYY-MM-DD)", required=True),
                "事件名稱": st.column_config.TextColumn("事件名稱", required=True),
                "文字顏色": st.column_config.SelectboxColumn(
                    "文字顏色",
                    options=["白色", "紅色", "綠色", "黃色", "藍色", "橘色", "紫紅色"],
                    default="白色"
                )
            },
            key="cal_override_editor",
            use_container_width=True
        )
        
        if st.button("💾 儲存行事曆設定", key="btn_save_cal_override"):
            st.session_state.cal_overrides = edited_cal_df
            save_cal_overrides(edited_cal_df)
            st.toast("自訂行事曆已儲存！", icon="✅")
            st.rerun()
    st.markdown("---")

    def change_month(delta):
        st.session_state.cal_month += delta
        if st.session_state.cal_month > 12:
            st.session_state.cal_month = 1
            st.session_state.cal_year += 1
        elif st.session_state.cal_month < 1:
            st.session_state.cal_month = 12
            st.session_state.cal_year -= 1
        
        if 'sel_year_box' in st.session_state: del st.session_state['sel_year_box']
        if 'sel_month_box' in st.session_state: del st.session_state['sel_month_box']

    col_sel_y, col_sel_m = st.columns(2)
    with col_sel_y:
        current_year_idx = range(2024, 2031).index(st.session_state.cal_year)
        new_year = st.selectbox("年份", range(2024, 2031), index=current_year_idx, key='sel_year_box')
        if new_year != st.session_state.cal_year:
            st.session_state.cal_year = new_year
            st.rerun()

    with col_sel_m:
        current_month_idx = st.session_state.cal_month - 1
        new_month = st.selectbox("月份", range(1, 13), index=current_month_idx, key='sel_month_box')
        if new_month != st.session_state.cal_month:
            st.session_state.cal_month = new_month
            st.rerun()

    sel_year = st.session_state.cal_year
    sel_month = st.session_state.cal_month

    col_prev, col_header, col_next = st.columns([1, 8, 1])
    with col_prev: st.button("◀️", on_click=change_month, args=(-1,), width='stretch')
    with col_next: st.button("▶️", on_click=change_month, args=(1,), width='stretch')
    with col_header: st.markdown(f"<div class='calendar-header'>{sel_year}/{sel_month:02}</div>", unsafe_allow_html=True)

    current_holidays = get_holidays(sel_year)
    def get_us_events(y, m):
        events = {}
        def add_evt(d, txt, color):
            if d not in events: events[d] = []
            events[d].append(f"<div style='color:{color}; font-size:0.8em; margin-top:2px; font-weight:bold;'>{txt}</div>")
        
        cal_temp = calendar.Calendar(firstweekday=6)
        month_days_list = list(cal_temp.itermonthdays2(y, m))
        
        # (1) 美股固定休市日期與節日對照表
        us_holiday_names = {
            2024: {
                (1,1): "元旦", (1,15): "馬丁路德金紀念日", (2,19): "總統日", (3,29): "耶穌受難日", 
                (5,27): "陣亡將士紀念日", (6,19): "六月節", (7,4): "獨立紀念日", (9,2): "勞動節", 
                (11,28): "感恩節", (12,25): "聖誕節"
            },
            2025: {
                (1,1): "元旦", (1,20): "馬丁路德金紀念日", (2,17): "總統日", (4,18): "耶穌受難日", 
                (5,26): "陣亡將士紀念日", (6,19): "六月節", (7,4): "獨立紀念日", (9,1): "勞動節", 
                (11,27): "感恩節", (12,25): "聖誕節"
            },
            2026: {
                (1,1): "元旦", (1,19): "馬丁路德金紀念日", (2,16): "總統日", (4,3): "耶穌受難日", 
                (5,25): "陣亡將士紀念日", (6,19): "六月節", (7,3): "獨立紀念日", (9,7): "勞動節", 
                (11,26): "感恩節", (12,25): "聖誕節"
            }
        }
        if y in us_holiday_names:
            for (hm, hd), name in us_holiday_names[y].items():
                if hm == m: 
                    add_evt(hd, f"美股{name}休市", "#B0BEC5")
        
        # (2) 美國非農人數公布 (自動判斷美股休市提前與夏令時間)
        for d, wd in month_days_list:
            if d != 0 and wd == 4:
                year_holidays = us_holiday_names.get(y, {})
                # 檢查該週五是否恰逢美股休市（例如 2026/7/3 獨立紀念日）
                if (m, d) in year_holidays:
                    # 提前至週四公布，且因必為夏令時間，固定顯示 20:30
                    add_evt(d - 1, "美國非農 (20:30)", "#00E5FF")
                else:
                    # 正常週五公布，依月份區分夏令(3~11月: 20:30) 與 冬令(12~2月: 21:30)
                    time_str = "20:30" if 3 <= m <= 11 else "21:30"
                    add_evt(d, f"美國非農 ({time_str})", "#00E5FF")
                break
                
        # (3) 四巫日 (3,6,9,12月 第三個週五)
        if m in [3, 6, 9, 12]:
            fridays = [d for d, wd in month_days_list if d != 0 and wd == 4]
            if len(fridays) >= 3:
                add_evt(fridays[2], "四巫日", "#FF00FF") # 洋紅色
                
        # (4) 13F報告 (通常在2,5,8,11月的14日或15日，以估算值為主)
        if m == 2: add_evt(14, "13F報告", "#FFD700") # 金黃色
        elif m == 5: add_evt(15, "13F報告", "#FFD700")
        elif m == 8: add_evt(14, "13F報告", "#FFD700")
        elif m == 11: add_evt(14, "13F報告", "#FFD700")
        
        # (5) MSCI季調 (2,5,8,11月最後一個交易日生效)
        if m in [2, 5, 8, 11]:
            weekdays = [d for d, wd in month_days_list if d != 0 and wd < 5]
            if weekdays:
                add_evt(weekdays[-1], "MSCI季調生效", "#FF9800") # 橘色

        # (6) FOMC (台灣時間，大多為週四凌晨 02:00 或 03:00，整理2024-2026日期)
        fomc_dates = {
            2024: [(2,1), (3,21), (5,2), (6,13), (8,1), (9,19), (11,8), (12,19)],
            2025: [(1,30), (3,20), (5,8), (6,19), (7,31), (9,18), (10,30), (12,11)],
            2026: [(1,29), (3,19), (5,7), (6,18), (7,30), (9,17), (10,29), (12,10)]
        }
        if y in fomc_dates:
            for fm, fd in fomc_dates[y]:
                if fm == m: 
                    time_str = "02:00" if 3 <= m <= 11 else "03:00"
                    add_evt(fd, f"FOMC ({time_str})", "#FF4500")
                
        # (7) 美國CPI公布 (台灣時間 20:30 或 21:30，整理2024-2026日期)
        cpi_dates = {
            2024: [(1,11), (2,13), (3,12), (4,10), (5,15), (6,12), (7,11), (8,14), (9,11), (10,10), (11,13), (12,11)],
            2025: [(1,15), (2,12), (3,12), (4,9), (5,14), (6,11), (7,16), (8,13), (9,10), (10,15), (11,12), (12,10)],
            2026: [(1,14), (2,11), (3,11), (4,15), (5,13), (6,10), (7,15), (8,12), (9,16), (10,15), (11,12), (12,16)]
        }
        if y in cpi_dates:
            for cm, cd in cpi_dates[y]:
                if cm == m: 
                    time_str = "20:30" if 3 <= m <= 11 else "21:30"
                    add_evt(cd, f"美國CPI ({time_str})", "#00FA9A")
        
        return events

    # 取得本月美國重要事件
    us_events = get_us_events(sel_year, sel_month)
    def is_market_closed_func(d_date):
        if d_date.weekday() >= 5: return True
        name = current_holidays.get((d_date.month, d_date.day), "")
        if name and name != "封關日": return True
        return False

    real_settlements = {} 
    def calculate_month_settlements(y, m):
        cal_obj = calendar.Calendar(firstweekday=6)
        days_in_month = cal_obj.itermonthdays(y, m)
        d_list = [d for d in days_in_month if d != 0]
        
        w_count, f_count = 0, 0
        month_raw_wed, month_raw_fri = [], []
        
        for d in d_list:
            curr = date(y, m, d)
            if curr.weekday() == 2: w_count += 1; month_raw_wed.append((curr, w_count))
            if curr.weekday() == 4: f_count += 1; month_raw_fri.append((curr, f_count))
                
        monthly_raw = month_raw_wed[2][0] if len(month_raw_wed) >= 3 else None
        real_monthly_date = None
        if monthly_raw:
            check = monthly_raw
            while is_market_closed_func(check): check += timedelta(days=1)
            real_monthly_date = check
        
        local_results = []
        if monthly_raw: local_results.append((monthly_raw, 'M', f"{m:02}月", real_monthly_date))

        for dt, idx in month_raw_wed:
            if dt != monthly_raw: local_results.append((dt, 'W', f"{str(y)[2:]}{m:02}W{idx}", real_monthly_date))
        for dt, idx in month_raw_fri: local_results.append((dt, 'F', f"{str(y)[2:]}{m:02}F{idx}", real_monthly_date))
        return local_results

    current_month_data = calculate_month_settlements(sel_year, sel_month)
    if sel_month == 1: prev_y, prev_m = sel_year - 1, 12
    else: prev_y, prev_m = sel_year, sel_month - 1
    prev_month_data = calculate_month_settlements(prev_y, prev_m)
    
    all_raw_data = prev_month_data + current_month_data
    
    for raw_date, s_type, s_code, m_date in all_raw_data:
        check_date = raw_date
        while is_market_closed_func(check_date):
            check_date += timedelta(days=1)
            if (check_date - raw_date).days > 30: break
        
        if check_date.year == sel_year and check_date.month == sel_month:
            if s_type == 'F' and check_date == m_date: continue
            if check_date not in real_settlements: real_settlements[check_date] = []
            real_settlements[check_date].append((s_type, s_code))

    week_days = ["週", "日", "一", "二", "三", "四", "五", "六"]
    cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
    for i, d in enumerate(week_days): cols[i].markdown(f"<div style='text-align: center; font-weight: bold;'>{d}</div>", unsafe_allow_html=True)

    cal_obj = calendar.Calendar(firstweekday=6)
    month_days = cal_obj.monthdayscalendar(sel_year, sel_month)

    # --- 新增：將自訂事件整理為字典以利快速渲染 ---
    override_dict = {}
    if not st.session_state.cal_overrides.empty:
        for _, r in st.session_state.cal_overrides.iterrows():
            try:
                if pd.notna(r["日期"]) and str(r["日期"]).strip():
                    d_obj = pd.to_datetime(r["日期"]).date()
                    if d_obj.year == sel_year and d_obj.month == sel_month:
                        if d_obj not in override_dict:
                            override_dict[d_obj] = []
                        color_map = {"白色": "#FFFFFF", "紅色": "#FF4B4B", "綠色": "#00E676", "黃色": "#FFD700", "藍色": "#00E5FF", "橘色": "#FF9800", "紫紅色": "#FF00FF"}
                        raw_col = str(r["文字顏色"]).strip() if pd.notna(r["文字顏色"]) else "白色"
                        col = color_map.get(raw_col, "#FFFFFF")
                        override_dict[d_obj].append(f"<div style='color:{col}; font-size:0.8em; margin-top:2px; font-weight:bold;'>{r['事件名稱']}</div>")
            except:
                pass
    # ---------------------------------------------

    for week in month_days:
        week_cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
        first_valid_day = next((d for d in week if d != 0), None)
        if first_valid_day:
            iso_week = date(sel_year, sel_month, first_valid_day).isocalendar()[1]
            week_cols[0].markdown(f"<div class='cal-box cal-week'>{iso_week}</div>", unsafe_allow_html=True)
        else: week_cols[0].markdown("")

        for i, day in enumerate(week):
            if day == 0:
                week_cols[i+1].markdown("")
                continue
            
            curr_date = date(sel_year, sel_month, day)
            holiday_name = current_holidays.get((sel_month, day), "")
            is_closed = is_market_closed_func(curr_date)
            bg_class = "cal-closed" if is_closed else "cal-open"
            border_style = "today-border" if curr_date == now_tw.date() else ""
            
            content_html = [f"<b>{day}</b>"]
            if holiday_name and holiday_name != "封關日": content_html.append(f"<div class='holiday-tag'>{holiday_name}</div>")
            if holiday_name == "封關日": content_html.append(f"<div style='color:#ff9800; font-size:0.8em;'>{holiday_name}</div>")
            
            # 加入外國重要股市事件
            if day in us_events:
                content_html.extend(us_events[day])
            
            # --- 新增：注入自訂與校正事件 ---
            if curr_date in override_dict:
                content_html.extend(override_dict[curr_date])
            # ------------------------------
            
            if curr_date in real_settlements:
                infos = real_settlements[curr_date]
                infos.sort(key=lambda x: 0 if x[0]=='M' else 1)
                for s_type, s_code in infos:
                    if s_type == 'M': content_html.append(f"<div class='settle-m'>台指期{s_code}結算<br>月選結算</div>")
                    elif s_type == 'W': content_html.append(f"<div class='settle-w'>週選(三) {s_code}</div>")
                    elif s_type == 'F': content_html.append(f"<div class='settle-f'>週選(五) {s_code}</div>")

            week_cols[i+1].markdown(f"<div class='cal-box {bg_class} {border_style}'>{''.join(content_html)}</div>", unsafe_allow_html=True)
