---
name: stock-app-maintenance
description: 維護本專案的股票、期貨、選擇權、費波圖表、行事曆與 Google Sheet 持久化；當修改 app.py 的市場資料、計算、跨裝置同步或 Streamlit 介面時使用。
---

# 股期戰略室維護

修改前完整閱讀 [維護參考](../../../docs/MAINTENANCE_REFERENCE.md)。

## 工作方式

1. 先用 `rg` 找到資料取得、正規化、計算、顯示與持久化的完整路徑，不只修改錯誤訊息或單一畫面。
2. 保存既有資料口徑。Goodinfo、期交所、MOPS/FinMind 不可互相替代成不同定義的排行或日期。
3. 跨裝置資料必須分區比較版本；不可用股票快照時間決定費波標籤、期貨快照或公司事件的新舊。
4. 外部來源失敗時採 last-known-good 並顯示來源日期；不得把空值、0 或舊策略偽裝成新資料。
5. 調整 Streamlit widget 時，避免在 widget 已建立後改寫同 key 的 `session_state`。需要更新時用 callback、建立 widget 前賦值，或設定旗標後 `st.rerun()`。
6. UI 修改同時檢查 700px 以下手機版；資料表以內容可讀與可捲動為優先。

## 完成門檻

- 為新資料合併、價格邊界、日期或篩選規則增加行為測試。
- 執行 `python -m py_compile app.py` 與 `python -m pytest -q`。
- 涉及頁面流程時使用 Streamlit AppTest；外部網路在測試環境失敗不算成功路徑，需另驗證 fallback。
- 檢查 `git diff`，不得覆蓋使用者或其他任務的無關修改。
