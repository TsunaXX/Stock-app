# 指數操作計畫背景快照驗證

本次僅調整指數操作計畫資料更新、快照及載入。股票／期貨選股排名、進出場與保利公式、模型損益及 Google Sheets 結構不變，其他頁面不新增隱藏常駐工作。

## 程式入口

| 檔案 | 入口與作用 |
| --- | --- |
| app.py | calculate_operation_snapshot：分離原計算與繪圖；get_stable_index_trade_plan：可傳入工作專屬快取 |
| app.py | index_operation_snapshot：首次開啟設定 session 工作、讀取安全快照、行情失效保留價位並關閉進場許可 |
| app.py | refresh_index_background_history：沿用共用 180 秒日 K 快取與有界慢來源佇列 |
| app.py | index_operation_model_specs／record_operation_models：沿用原訊號、許可時間與成交證據，背景顯式注入設定與游標 |
| app.py | 指數主／子分頁入口：已登入的指數頁不再在前景下載完整歷史，回訪不重算分析 |
| market_automation.py | IntradayBackground：字典安全複本、低優先排序、耗時退避、沿用原 session 斷線兩分鐘清理 |
| tests/test_index_background.py | 開啟／隱藏／返回、重複設定、資料失效、雙工作階段、失敗保留、模型等價及重複證據 |

初次開啟尚無日 K 時顯示既有載入提示，慢來源完成後由同一計時器取得計畫。每個 session 只保留最新完整結果；隱藏時不繪圖、不查歷史績效。API 登出時沿用原清理，工作階段離線兩分鐘後終止；服務休眠／重啟不提供全天候離線監控。

日 K 仍 180 秒有效，背景佇列每三十秒檢查，實際更新完成時間可能晚於快取到期。分 K 仍隨形成中 K 棒的串流變動重算必要條件，未刻意延長為三十秒，以免漏掉既有提早確認訊號。

## 可重現測試

```powershell
python -m py_compile app.py market_automation.py
python -m ruff check app.py market_automation.py model_tracking.py sec_financials.py tests --select E9,F63,F7,F82
python -m pytest -q
python tests/index_background_benchmark.py
python -m streamlit run tests/index_background_browser_fixture.py --server.port 8502 --server.address 127.0.0.1 --server.headless true
```

瀏覽器測試工具僅使用模擬串流與真正 session worker／snapshot adapter，不使用登入資料、不呼叫券商、不寫模型或 Google Sheets。開啟兩個瀏覽器工作階段，第一個切至股期頁五分鐘後返回，確認來源時間持續推進、每階段一個工作；關閉一端兩分鐘後，另一端工作與訂閱範圍仍存在。

## 效能比較：2026/10/19 本機 Python 3.12

以下是合成 180 根日 K、9,000 根分鐘 K，執行現有日線、15／5 分 K 與進場判斷函式的量測；不是正式站／真實 API 延遲。

| 項目 | 結果 |
| --- | ---: |
| 首次資料已在記憶體的計算 | 157.83 ms |
| 與舊前景流程同等計算，中位數／P95 | 16.06／20.15 ms |
| 讀取完整快照，中位數／P95 | 0.13／0.17 ms |
| 200 輪前景等價計算 CPU／200 次快照讀取 CPU | 3.344／0.016 秒 |
| 暖機後再讀 3,000 次，Python retained memory 差值 | 約 58 KB |
| 該次 Python traced memory 峰值 | 約 211 KB |
| 快照工作數／額外行情查詢／額外登入 | 1／0／0 |

CPU 數字僅含測試程序，記憶體是 tracemalloc 的 Python 配置量，不是整台主機 RSS；單次差值不能證明數日無洩漏。新機制的收益是回訪時讀快照，並非降低原策略每輪的計算量；切離頁面後仍有使用者要求的背景計算與到期日 K 更新。

## 驗收紀錄與限制

- 新增十一項指數背景測試已於本機通過，涵蓋生命週期、失敗保留、游標、資料時間、模型等價及共用訂閱。既有股期背景與選擇權／指數 AppTest 回歸通過。全套共 359 項，CI 在 Python 3.12／3.14 執行，兩版本均成功才合併。
- 本機 Chrome 兩個獨立工作階段通過：21:10 切離指數頁，21:16 返回顯示 21:16 最新來源與一個工作；關閉第一端後，21:19 第二端仍持續更新，有效 scope 與背景執行緒均由兩組降為一組。使用合成報價，不是實體手機或真實券商驗收。
- 尚未量測正式市場 API 流量、實體手機網路凍結、多日 CPU／RSS／HTTP 500。上述本機合成結果不可作為這些情境已通過的證據。
- 需完整重啟 Streamlit 套用背景類別更新，避免舊工作物件沿用舊版方法；工作取得入口也檢查類別並清理舊物件。
