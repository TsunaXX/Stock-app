/**
 * 台股全盤戰略室 Google Sheet scope 儲存端。
 *
 * 部署方式：在綁定目標試算表的 Apps Script 中完整取代舊程式，執行一次
 * setupAppCacheSheet()，再建立「網頁應用程式」新版本。執行身分選擇自己，
 * 存取權限依目前私人使用設定。
 */

const SCOPES = [
  'stock_strategy',
  'fibo_strategy',
  'company_events',
  'strategy_signals',
  'futures_strategy',
];

const CACHE_SHEET_NAME = 'app_cache';
const HEADER = ['scope', 'data', 'updated_at'];
const MAX_CELL_CHARS = 48000;


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
  const matches = HEADER.every(function(value, index) {
    return String(current[index] || '').trim() === value;
  });
  if (!matches) {
    sheet.getRange(1, 1, 1, HEADER.length).setNumberFormat('@').setValues([HEADER]);
  }
}


function getScopeRowMap_(sheet) {
  const result = {};
  const rowCount = Math.max(sheet.getLastRow() - 1, 0);
  if (!rowCount) return result;
  const values = sheet.getRange(2, 1, rowCount, 1).getDisplayValues();
  values.forEach(function(row, index) {
    const scope = normalizeScope_(row[0]);
    if (scope && !result[scope]) result[scope] = index + 2;
  });
  return result;
}


function doGet(e) {
  try {
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
  }
}


function readScopeData_(sheet, scope, row) {
  if (!row) {
    return {success: true, scope: scope, data: null, updated_at: ''};
  }
  const values = sheet.getRange(row, 2, 1, 2).getDisplayValues()[0];
  return {
    success: true,
    scope: scope,
    data: parseJsonSafe_(values[0]),
    updated_at: String(values[1] || ''),
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
  const sheet = getStoreSheet_();
  const serialized = JSON.stringify(data);
  if (serialized.length > MAX_CELL_CHARS) {
    throw new Error(scope + ' 資料超過單格安全上限：' + serialized.length);
  }
  const rows = getScopeRowMap_(sheet);
  const existingRow = rows[scope] || 0;
  const row = existingRow || Math.max(sheet.getLastRow() + 1, 2);
  const incomingUpdatedAt = String(updatedAt || new Date().toISOString());

  // A phone or desktop tab left open in the background can submit an older
  // stock snapshot after a newer official analysis has already been saved.
  // Reject only genuinely older, parseable stock timestamps; equal timestamps
  // remain writable so notes in the same snapshot can still be updated.
  if (existingRow && scope === 'stock_strategy') {
    const existingUpdatedAt = String(
      sheet.getRange(existingRow, 3).getDisplayValue() || ''
    ).trim();
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

  sheet.getRange(row, 1, 1, 3).setNumberFormat('@').setValues([[
    scope,
    serialized,
    incomingUpdatedAt,
  ]]);
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
    'market_risk_data',
  ].forEach(function(key) {
    if (payload[key] !== undefined) result[key] = payload[key];
  });
  return result;
}


function normalizeScope_(value) {
  const scope = String(value || '').trim();
  return SCOPES.indexOf(scope) >= 0 ? scope : '';
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
  const sheet = getStoreSheet_();
  const values = sheet.getDataRange().getDisplayValues();
  const migrated = {};
  values.forEach(function(row) {
    row.forEach(function(cell) {
      const payload = parseJsonSafe_(String(cell || '').trim());
      if (!payload || typeof payload !== 'object') return;
      migratePayloadObject_(payload, new Date().toISOString()).forEach(function(scope) {
        migrated[scope] = true;
      });
    });
  });
  SpreadsheetApp.flush();
  return jsonResponse_({success: true, migrated_scopes: Object.keys(migrated)});
}
