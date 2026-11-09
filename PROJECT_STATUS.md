# Stock-app 專案狀態

最後來源核對：2026-11-09（Asia/Taipei）。已核對本機變更與 GitHub 主分支基準，功能發布及實際畫面證據以合併 PR 為準。

| 欄位 | 已核對內容 |
| --- | --- |
| 專案識別 | stock-app |
| 主要倉庫 | TsunaXX/Stock-app |
| 備份倉庫 | XTsunaX/Stock-app；本檔不代表備份已完成最新同步 |
| 開發分支 | main |
| 最近功能程式基準 | 8da9b525e247cb5143c64c068449ca1f26634e5d（開盤參考熱更新相容修復） |
| 正式 Release | 本次 Releases 查詢為空；不自行指定版本號 |
| 正式部署版本 | 上次已核對 PR #87 的 bdb7d846；開盤參考的正式站紀錄見 [PR #88](https://github.com/TsunaXX/Stock-app/pull/88) |
| 電腦本機版本／未提交修改 | 本次於 Windows 工作目錄完成指定功能與測試；日後狀態以當時 Git 狀態為準 |

## 當前結果
- 開盤參考已驗證可用舊快取追蹤器的資料庫連線讀取，資料庫暫時讀取失敗仍可顯示主表。完整 CI 與正式站核對見對應合併 PR。
- 本次修正股期當沖的觀望價位顯示，新增休市時的近七日開盤參考與 1R 啟動參考價。詳見維護參考 2026-11-09 項目。
- 前次功能為缺口參考績效及成交證據游標修正，詳見 [PR #87](https://github.com/TsunaXX/Stock-app/pull/87)。
- PR #87 記錄本機與 Python 3.12／3.14 CI 各 598 項通過、正式站桌面／手機尺寸及 Excel 驗收；這是既有發布證據，不是此次重跑。
- 既有詳細維護紀錄以 [docs/MAINTENANCE_REFERENCE.md](docs/MAINTENANCE_REFERENCE.md) 為準，不在此重複全部歷史問題。

## 待辦
- 待驗收：後續交易時段的長時間連續記錄，以及正式站 CPU／記憶體占用；PR #87 未宣稱完成此項。
- 開盤參考：本機指定回歸／AppTest 通過；完整 CI 及正式部署核對見 [PR #88](https://github.com/TsunaXX/Stock-app/pull/88)。下一次實際開盤前的正式行情時段驗收待核對。
- Bridge 替代進度：已建立本倉庫狀態格式與維護規則；仍待本機更新／推送後的手機查詢驗證。不能據此判定整體遷移成功或移除 Bridge。

## 閱讀與更新
一般進度查詢先讀本檔；版本與待辦改變時依 [AGENTS.md](AGENTS.md) 更新。查目前 GitHub HEAD 直接讀分支，不把上方功能基準當成最新文件提交。
