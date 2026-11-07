# Stock-app 專案狀態

最後來源核對：2026-11-07（Asia/Taipei）。此為 GitHub 與發布紀錄核對，未連接本機 Windows 或重新執行應用測試。

| 欄位 | 已核對內容 |
| --- | --- |
| 專案識別 | stock-app |
| 主要倉庫 | TsunaXX/Stock-app |
| 備份倉庫 | XTsunaX/Stock-app；本檔不代表備份已完成最新同步 |
| 開發分支 | main |
| 最近功能程式基準 | bdb7d846cb2b7e42c9d683ff5c33979c7cd509d1 |
| 正式 Release | 本次 Releases 查詢為空；不自行指定版本號 |
| 正式部署版本 | PR #87 記錄部署 bdb7d846；此次未直接讀取運行中伺服器 |
| 電腦本機版本／未提交修改 | 未核對 |

## 當前結果
- 最近功能基準為缺口參考績效及成交證據游標修正，詳見 [PR #87](https://github.com/TsunaXX/Stock-app/pull/87)。
- PR #87 記錄本機與 Python 3.12／3.14 CI 各 598 項通過、正式站桌面／手機尺寸及 Excel 驗收；這是既有發布證據，不是此次重跑。
- 既有詳細維護紀錄以 [docs/MAINTENANCE_REFERENCE.md](docs/MAINTENANCE_REFERENCE.md) 為準，不在此重複全部歷史問題。

## 待辦
- 待驗收：後續交易時段的長時間連續記錄，以及正式站 CPU／記憶體占用；PR #87 未宣稱完成此項。
- 待核對：本機實際工作目錄、commit、未提交修改與正式站目前運行版本。
- Bridge 替代進度：已建立本倉庫狀態格式與維護規則；仍待本機更新／推送後的手機查詢驗證。不能據此判定整體遷移成功或移除 Bridge。

## 閱讀與更新
一般進度查詢先讀本檔；版本與待辦改變時依 [AGENTS.md](AGENTS.md) 更新。查目前 GitHub HEAD 直接讀分支，不把上方功能基準當成最新文件提交。
