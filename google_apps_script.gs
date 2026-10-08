/**
 * 台股全盤戰略室 Google Sheet scope 儲存端。
 *
 * 部署方式：在綁定目標試算表的 Apps Script 中完整取代舊程式，執行一次
 * setupAppCacheSheet()；若舊版 JSON 原本存在其他工作表 A1，再執行一次
 * migrateLegacyCacheData()，最後建立「網頁應用程式」新版本。執行身分選擇
 * 自己，存取權限依目前私人使用設定。
 */

const SCOPES = [
  'stock_strategy',
  'fibo_strategy',
  'company_events',
  'strategy_signals',
  'futures_strategy',
];

const CACHE_SHEET_NAME = 'app_cache';
// One scope can exceed Google Sheets' 50,000-character cell ceiling.  Keep
// the same app_cache sheet, but store each JSON value in numbered chunks.
const HEADER = ['scope', 'part', 'data', 'updated_at'];
const MAX_CELL_CHARS = 44000;


function getStoreSheet_() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  if (!spreadsheet) throw new Error('找不到目前的 Google 試算表');
  let sheet = spreadsheet.getSheetByName(CACHE_SHEET_NAME);
  if (!sheet) sheet = spreadsheet.insertSheet(CACHE_SHEET_NAME);
  ensureHeader_(sheet);
  return sheet;
}


function ensureHeader_(sheet) {
  const current = sheet.getLastRow() >= 1
    ? sheet.getRange(1, 1, 1, HEADER.length).getDisplayValues()[0]
    : [];
  const legacyHeader = ['scope', 'data', 'updated_at'];
  const isLegacy = legacyHeader.every(function(value, index) {
    return String(current[index] || '').trim() === value;
  });
  if (isLegacy) {
    migrateLegacyRowsToChunks_(sheet);
    return;
  }
  const matches = HEADER.every(function(value, index) {
    return String(current[index] || '').trim() === value;
  });
  if (!matches) {
    sheet.getRange(1, 1, 1, HEADER.length).setNumberFormat('@').setValues([HEADER]);
  }
}


function migrateLegacyRowsToChunks_(sheet) {
  const lastRow = sheet.getLastRow();
  const legacyRows = lastRow > 1
    ? sheet.getRange(2, 1, lastRow - 1, 3).getDisplayValues()
    : [];
  const converted = [HEADER];
  legacyRows.forEach(function(row) {
    const scope = normalizeScope_(row[0]);
    const serialized = String(row[1] || '');
    if (!scope || !serialized) return;
    splitJsonChunks_(serialized).forEach(function(chunk, part) {
      converted.push([scope, part, chunk, String(row[2] || '')]);
    });
  });
  sheet.clearContents();
  sheet.getRange(1, 1, 1, HEADER.length).setNumberFormat('@').setValues([HEADER]);
  if (converted.length > 1) {
    sheet.getRange(2, 1, converted.length - 1, HEADER.length)
      .setNumberFormat('@')
      .setValues(converted.slice(1));
  }
}


function splitJsonChunks_(serialized) {
  const value = String(serialized || '');
  if (!value) return ['{}'];
  const chunks = [];
  for (let start = 0; start < value.length; start += MAX_CELL_CHARS) {
    chunks.push(value.slice(start, start + MAX_CELL_CHARS));
  }
  return chunks;
}


function getScopeRowMap_(sheet) {
  const result = {};
  const rowCount = Math.max(sheet.getLastRow() - 1, 0);
  if (!rowCount) return result;
  const values = sheet.getRange(2, 1, rowCount, HEADER.length).getDisplayValues();
  values.forEach(function(row, index) {
    const scope = normalizeScope_(row[0]);
    if (!scope) return;
    if (!result[scope]) {
      result[scope] = {rows: [], parts: [], updated_at: ''};
    }
    result[scope].rows.push(index + 2);
    const revision = /^(\d+)\|(\d+)\|(\d+)$/.exec(String(row[1]));
    result[scope].parts.push({
      generation: revision ? Number(revision[1]) : 0,
      count: revision ? Number(revision[3]) : 0,
      part: revision ? Number(revision[2]) : Number(row[1]) || 0,
      data: String(row[2] || ''),
      updated_at: String(row[3] || ''),
    });
    if (row[3]) result[scope].updated_at = String(row[3]);
  });
  return result;
}


function doGet(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
    const requested = normalizeScope_(e && e.parameter ? e.parameter.scope : '');
    const sheet = getStoreSheet_();
    const rows = getScopeRowMap_(sheet);

    if (requested) {
      return jsonResponse_(readScopeData_(sheet, requested, rows[requested]));
    }

    const scopes = {};
    SCOPES.forEach(function(scope) {
      const item = readScopeData_(sheet, scope, rows[scope]);
      scopes[scope] = {data: item.data, updated_at: item.updated_at};
    });
    return jsonResponse_({success: true, scopes: scopes});
  } catch (error) {
    return jsonResponse_({success: false, error: String(error)});
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}


function readScopeData_(sheet, scope, row) {
  if (!row || !row.parts || !row.parts.length) {
    return {success: true, scope: scope, data: scope === 'strategy_signals' ? {model_schema: 1, model_months: [], model_versions: {}} : null, updated_at: ''};
  }
  const generations = {};
  row.parts.forEach(function(part) {
    if (!generations[part.generation || 0]) generations[part.generation || 0] = [];
    generations[part.generation || 0].push(part);
  });
  const complete = Object.keys(generations).filter(function(key) {
    const parts = generations[key];
    return !parts[0].count || (parts.length === parts[0].count && parts.every(function(p) {
      return p.count === parts.length && p.part >= 0 && p.part < parts.length;
    }) && new Set(parts.map(function(p) {return p.part;})).size === parts.length);
  }).sort(function(a,b) {return Number(b) - Number(a);});
  if (!complete.length) throw new Error('儲存分段尚未完整提交');
  const serialized = generations[complete[0]]
    .sort(function(a, b) { return a.part - b.part; })
    .map(function(part) { return part.data; })
    .join('');
  return {
    success: true,
    scope: scope,
    data: scope === 'strategy_signals' ? Object.assign({model_schema: 1, model_months: [], model_versions: {}}, parseJsonSafe_(serialized)) : parseJsonSafe_(serialized),
    updated_at: String(generations[complete[0]][0].updated_at || row.updated_at || ''),
  };
}


function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
    const body = parseRequestBody_(e);
    const scope = normalizeScope_(body.scope);

    if (scope) {
      if (body.data === undefined || body.data === null) {
        return jsonResponse_({success: false, error: '缺少 data'});
      }
      const data = typeof body.data === 'string'
        ? parseJsonSafe_(body.data)
        : body.data;
      if (data === null || typeof data !== 'object') {
        return jsonResponse_({success: false, error: 'data 必須是有效 JSON 物件'});
      }
      const updatedAt = String(body.updated_at || new Date().toISOString());
      const row = saveScopeData_(scope, data, updatedAt);
      SpreadsheetApp.flush();
      return jsonResponse_({
        success: true,
        scope: scope,
        updated_at: updatedAt,
        row: row,
      });
    }

    // 舊版完整 payload 相容入口。
    if (body.data !== undefined && body.data !== null) {
      const payload = parseJsonSafe_(body.data);
      if (payload && typeof payload === 'object') {
        const saved = migratePayloadObject_(payload, body.updated_at);
        if (saved.length) {
          SpreadsheetApp.flush();
          return jsonResponse_({success: true, migrated: true, saved_scopes: saved});
        }
      }
    }
    return jsonResponse_({success: false, error: '缺少有效 scope'});
  } catch (error) {
    return jsonResponse_({success: false, error: String(error)});
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}


function saveScopeData_(scope, data, updatedAt) {
  if (/^strategy_signals:\d{6}$/.test(scope)) {
    if (!Array.isArray(data.model_trades) || data.model_trades.some(function(r) {
      return !r || !/^[a-f0-9]{64}$/.test(r['交易ID'] || '') || typeof r['商品鍵'] !== 'string' || !r['商品鍵'] ||
        typeof r['策略版本'] !== 'string' || !r['策略版本'] || isNaN(Date.parse(r['來源時間'] || '')) ||
        ['多頭','空頭','偏多','偏空'].indexOf(r['方向']) < 0 ||
        ['進場價','停損價','目標價'].some(function(k) {return typeof r[k] !== 'number' || !isFinite(r[k]) || r[k] <= 0;}) ||
        !Array.isArray(r['事件']) || !/^\d{4}-\d{2}-\d{2}$/.test(r['交易日'] || '') ||
        r['交易日'].slice(0,7).replace('-', '') !== scope.split(':')[1] ||
        ['股票','期貨'].indexOf(r['市場']) < 0 || ['當沖','波段'].indexOf(r['策略']) < 0 ||
        ['等待進場','模擬持倉','已平倉','訊號失效','資料不足','監控中斷'].indexOf(r['狀態']) < 0;
    })) throw new Error('模型交易資料格式或月份無效');
  }
  const sheet = getStoreSheet_();
  const rows = getScopeRowMap_(sheet);
  const existing = rows[scope] || null;
  if (scope === 'strategy_signals' || /^strategy_signals:\d{6}$/.test(scope)) {
    const prior = readScopeData_(sheet, scope, existing).data || {};
    data = mergeSignalPayload_(prior, data);
  }
  const chunks = splitJsonChunks_(JSON.stringify(data));
  const existingRow = existing && existing.rows.length ? existing.rows[0] : 0;
  const incomingUpdatedAt = String(updatedAt || new Date().toISOString());

  // A phone or desktop tab left open in the background can submit an older
  // stock snapshot after a newer official analysis has already been saved.
  // Reject only genuinely older, parseable stock timestamps; equal timestamps
  // remain writable so notes in the same snapshot can still be updated.
  if (existingRow && scope === 'stock_strategy') {
    const existingUpdatedAt = String(existing.updated_at || '').trim();
    const existingTime = Date.parse(existingUpdatedAt);
    const incomingTime = Date.parse(incomingUpdatedAt);
    if (
      !isNaN(existingTime) &&
      !isNaN(incomingTime) &&
      incomingTime < existingTime
    ) {
      throw new Error(
        '拒絕較舊的 stock_strategy 覆蓋較新的資料：' +
        incomingUpdatedAt + ' < ' + existingUpdatedAt
      );
    }
  }

  const signalScope = scope === 'strategy_signals' || /^strategy_signals:\d{6}$/.test(scope);
  if (!signalScope && existing && existing.rows.length) {
    existing.rows.sort(function(a, b) { return b - a; }).forEach(function(row) { sheet.deleteRow(row); });
  }
  const row = Math.max(sheet.getLastRow() + 1, 2);
  const generation = Math.max(Date.now() * 1000, existing ? existing.parts.reduce(function(n,p) {return Math.max(n, p.generation + 1);}, 0) : 0);
  const values = chunks.map(function(chunk, part) {
    return [scope, signalScope ? generation + '|' + part + '|' + chunks.length : part, chunk, incomingUpdatedAt];
  });
  sheet.getRange(row, 1, values.length, HEADER.length).setNumberFormat('@').setValues(values);
  SpreadsheetApp.flush();
  // Preserve the last complete generation until the new one has been written in full.
  if (signalScope && existing && existing.rows.length) {
    try {
      existing.rows.sort(function(a,b) {return b-a;}).forEach(function(oldRow) {sheet.deleteRow(oldRow);});
    } catch (_) {} // A leftover generation is harmless; reads select the newest complete one.
  }
  if (/^strategy_signals:\d{6}$/.test(scope)) {
    const month = scope.split(':')[1];
    const versions = {};
    versions[month] = String(generation);
    saveScopeData_('strategy_signals', {model_schema: 1, model_months: [month], model_versions: versions}, incomingUpdatedAt);
  }
  return row;
}


function migratePayloadObject_(payload, updatedAt) {
  const saved = [];
  const timestamp = String(updatedAt || new Date().toISOString());
  const stock = buildStockPayload_(payload);
  if (Object.keys(stock).length) {
    saveScopeData_(
      'stock_strategy',
      stock,
      stock.stock_data_updated_at || timestamp
    );
    saved.push('stock_strategy');
  }

  if (Array.isArray(payload.fibo_tags) && payload.fibo_tags.length >= 5) {
    const fiboUpdatedAt = String(payload.fibo_tags_updated_at || timestamp);
    saveScopeData_('fibo_strategy', {
      version: 3,
      fibo_tags: payload.fibo_tags.slice(0, 5),
      fibo_tags_updated_at: fiboUpdatedAt,
      fibo_tags_backup: {
        tags: payload.fibo_tags.slice(0, 5),
        updated_at: fiboUpdatedAt,
      },
    }, fiboUpdatedAt);
    saved.push('fibo_strategy');
  }

  if (payload.company_event_snapshot && typeof payload.company_event_snapshot === 'object') {
    saveScopeData_(
      'company_events',
      payload.company_event_snapshot,
      payload.company_event_snapshot.updated_at || timestamp
    );
    saved.push('company_events');
  }

  if (Array.isArray(payload.strategy_signal_log)) {
    saveScopeData_('strategy_signals', {
      version: 1,
      strategy_signal_log: payload.strategy_signal_log,
      strategy_signal_deleted_keys: Array.isArray(payload.strategy_signal_deleted_keys)
        ? payload.strategy_signal_deleted_keys
        : [],
    }, timestamp);
    saved.push('strategy_signals');
  }

  if (payload.futures_strategy_state && typeof payload.futures_strategy_state === 'object') {
    saveScopeData_(
      'futures_strategy',
      payload.futures_strategy_state,
      payload.futures_strategy_state.updated_at || timestamp
    );
    saved.push('futures_strategy');
  }
  return saved;
}


function buildStockPayload_(payload) {
  const result = {};
  [
    'version', 'stock_data', 'ignored_stocks', 'all_candidates',
    'saved_notes', 'cached_notes', 'stock_data_updated_at',
    'market_risk_data', 'display_settings', 'strategy_ranking_snapshots',
    'stock_swing_snapshot', 'quick_search_state',
  ].forEach(function(key) {
    if (payload[key] !== undefined) result[key] = payload[key];
  });
  return result;
}


// doPost holds ScriptLock across read/merge/write, including the month catalog.
function mergeModelTrades_(remote, incoming) {
  const merged = Object.create(null);
  [].concat(remote || [], incoming || []).forEach(function(item) {
    if (!item || !item['交易ID']) return;
    const key = item['交易ID'];
    const old = merged[key];
    let row = Object.assign({}, item);
    if (old) {
      const newer = String(item['來源時間'] || '') > String(old['來源時間'] || '');
      const terminal = item['狀態'] === '已平倉';
      const oldTerminal = old['狀態'] === '已平倉';
      let chosen = ((newer && !oldTerminal) || (terminal && !oldTerminal)) ? item : old;
      if (terminal && oldTerminal) {
        const oldKey = String(old['結案時間'] || old['來源時間'] || '') + String(old['進場時間'] || '');
        const newKey = String(item['結案時間'] || item['來源時間'] || '') + String(item['進場時間'] || '');
        chosen = Boolean(old['資料缺口']) !== Boolean(item['資料缺口']) ? (old['資料缺口'] ? item : old) : (newKey < oldKey ? item : old);
      }
      row = Object.assign({}, chosen);
      row['資料缺口'] = row['狀態'] === '已平倉' ? Boolean(row['資料缺口']) : Boolean(old['資料缺口'] || item['資料缺口']);
      if (old['模擬進場價'] != null && item['模擬進場價'] != null && old['模擬進場價'] !== item['模擬進場價']) {
        row['資料缺口'] = true;
        row['異常原因'] = '跨裝置模擬成交證據不一致，排除有效績效';
      }
      if (Object.keys(Object.assign({}, old['成本設定'], item['成本設定'])).some(function(k) {
        return (old['成本設定'] || {})[k] !== (item['成本設定'] || {})[k];
      })) {
        row['資料缺口'] = true;
        row['異常原因'] = '跨裝置成本設定不一致，排除有效績效';
      }
      const first = String(old['建立時間'] || '') <= String(item['建立時間'] || '') ? old : item;
      ['建立時間', '指標快照', '成本設定', '訊號價', '市場環境', '觸發條件', '條件快照'].forEach(function(k) {
        if (first[k] !== undefined) row[k] = first[k];
      });
      const events = {};
      [].concat(old['事件'] || [], item['事件'] || []).forEach(function(e) {
        if (e && e['事件ID']) events[e['事件ID']] = e;
      });
      row['事件'] = Object.keys(events).map(function(k) {return events[k];}).sort(function(a,b) {
        return String(a['時間']).localeCompare(String(b['時間']));
      });
    }
    merged[key] = row;
  });
  return Object.keys(merged).sort().map(function(k) {return merged[k];});
}

function mergeSignalPayload_(prior, incoming) {
  const result = Object.assign({}, prior, incoming, {model_schema: 1});
  result.model_months = Array.from(new Set([].concat(prior.model_months || [], incoming.model_months || []))).sort();
  result.model_versions = Object.assign({}, prior.model_versions || {});
  Object.keys(incoming.model_versions || {}).forEach(function(k) {
    if (String(incoming.model_versions[k]) > String(result.model_versions[k] || '')) result.model_versions[k] = incoming.model_versions[k];
  });
  if (prior.model_trades || incoming.model_trades) result.model_trades = mergeModelTrades_(prior.model_trades, incoming.model_trades);
  const deleted = Array.from(new Set([].concat(prior.strategy_signal_deleted_keys || [], incoming.strategy_signal_deleted_keys || [])));
  if (prior.strategy_signal_log || incoming.strategy_signal_log) {
    const manual = {};
    [].concat(prior.strategy_signal_log || [], incoming.strategy_signal_log || []).forEach(function(r) {
      if (!r || !r.dedupe_key || deleted.indexOf(r.dedupe_key) >= 0) return;
      const old = manual[r.dedupe_key];
      if (!old || String(r['最後更新'] || '') >= String(old['最後更新'] || '')) manual[r.dedupe_key] = r;
    });
    result.strategy_signal_log = Object.keys(manual).map(function(k) {return manual[k];});
  }
  result.strategy_signal_deleted_keys = deleted;
  return result;
}


function normalizeScope_(value) {
  const scope = String(value || '').trim();
  return SCOPES.indexOf(scope) >= 0 || /^strategy_signals:\d{6}$/.test(scope) ? scope : '';
}


function parseRequestBody_(e) {
  if (!e) return {};
  if (e.postData && e.postData.contents) {
    const raw = String(e.postData.contents || '').trim();
    if (raw) {
      try { return JSON.parse(raw); } catch (_) {}
    }
  }
  return e.parameter || {};
}


function parseJsonSafe_(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value !== 'string') return value;
  try { return JSON.parse(value); } catch (_) { return value; }
}


function jsonResponse_(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}


function setupAppCacheSheet() {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
    const sheet = getStoreSheet_();
    const rows = getScopeRowMap_(sheet);
    SCOPES.forEach(function(scope) {
      if (!rows[scope]) saveScopeData_(scope, {}, new Date().toISOString());
    });
    SpreadsheetApp.flush();
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}


function migrateLegacyCacheData() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const storeSheet = getStoreSheet_();
  const migrated = {};
  // The pre-scope deployment kept its full payload in a single cell (usually
  // A1).  Read that legacy value without touching the original sheet, then
  // save it through the current per-scope/chunked storage format.
  spreadsheet.getSheets().forEach(function(sheet) {
    if (sheet.getSheetId() === storeSheet.getSheetId()) return;
    const candidate = parseJsonSafe_(sheet.getRange('A1').getDisplayValue());
    const payload = candidate && candidate.data !== undefined
      ? parseJsonSafe_(candidate.data)
      : candidate;
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return;
    migratePayloadObject_(payload, new Date().toISOString()).forEach(function(scope) {
      migrated[scope] = true;
    });
  });
  SpreadsheetApp.flush();
  return jsonResponse_({success: true, migrated_scopes: Object.keys(migrated)});
}
