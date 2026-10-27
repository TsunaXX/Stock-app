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
  // Read the small index first; a stock/calendar request must not load all model history JSON.
  const values = sheet.getRange(2, 1, rowCount, 2).getDisplayValues();
  const timestamps = sheet.getRange(2, 4, rowCount, 1).getDisplayValues();
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
      row: index + 2,
      updated_at: String(timestamps[index][0] || ''),
    });
    if (timestamps[index][0]) result[scope].updated_at = String(timestamps[index][0]);
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
      let item = readScopeData_(sheet, requested, rows[requested]);
      if (requested === 'strategy_signals') {
        const data = item.data || {};
        const months = data.model_months || [];
        const active = Object.assign({}, data.model_active_months || {});
        let changed = false;
        months.forEach(function(month) {
          if (Object.prototype.hasOwnProperty.call(active, month)) return;
          const stored = readScopeData_(sheet, 'strategy_signals:' + month, rows['strategy_signals:' + month]);
          active[month] = ((stored.data || {}).model_trades || []).some(function(r) {return ['等待進場','模擬持倉','監控中斷','資料不足'].indexOf(r['狀態']) >= 0;});
          changed = true;
        });
        if (changed) saveScopeData_('strategy_signals', {model_active_months: active}, new Date().toISOString());
        item.data = Object.assign({}, data, {model_reentry: 1, model_replan: 1, model_history: 1, model_events: 1, model_active_months: active});
      }
      const ids = e && e.parameter && e.parameter.trade_ids;
      if (ids !== undefined) {
        const keys = String(ids).split(',');
        if (!/^strategy_signals:\d{6}$/.test(requested) || keys.length > 20 || keys.some(function(k) {return !/^[a-f0-9]{64}$/.test(k);})) {
          throw new Error('交易回讀 ID 無效或超過 20 筆');
        }
        item.data = Object.assign({}, item.data || {}, {model_schema: 1,
          model_trades: ((item.data || {}).model_trades || []).filter(function(r) {return keys.indexOf(r['交易ID']) >= 0;})});
      }
      if (/^strategy_signals:\d{6}$/.test(requested) && item.data) item.data.model_version = item.version || '';
      return jsonResponse_(item);
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
    return {success: true, scope: scope, data: scope === 'strategy_signals' ? {model_schema: 1, model_sync: 2, model_months: [], model_versions: {}} : null, updated_at: ''};
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
  const selectedParts = generations[complete[0]];
  const firstRow = Math.min.apply(null, selectedParts.map(function(p) {return p.row;}));
  const lastRow = Math.max.apply(null, selectedParts.map(function(p) {return p.row;}));
  const data = sheet.getRange(firstRow, 3, lastRow - firstRow + 1, 1).getDisplayValues();
  const serialized = selectedParts
    .sort(function(a, b) { return a.part - b.part; })
    .map(function(part) { return String(data[part.row - firstRow][0] || ''); })
    .join('');
  return {
    success: true,
    scope: scope,
    data: scope === 'strategy_signals' ? Object.assign({model_months: [], model_versions: {}}, parseJsonSafe_(serialized), {model_schema: 1, model_sync: 2}) : parseJsonSafe_(serialized),
    version: String(complete[0]),
    updated_at: String(generations[complete[0]][0].updated_at || row.updated_at || ''),
  };
}


function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
    const body = parseRequestBody_(e);
    const scope = normalizeScope_(body.scope);
    if (body.action === 'archive_model_events') {
      if (!/^strategy_signals:\d{6}$/.test(scope)) throw new Error('封存月份無效');
      return jsonResponse_(archiveModelEvents_(scope));
    }
    if (/^strategy_events:/.test(scope)) throw new Error('原始事件僅可經封存流程寫入');

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
      const commit = {};
      const row = saveScopeData_(scope, data, updatedAt, commit);
      SpreadsheetApp.flush();
      return jsonResponse_({
        success: true,
        scope: scope,
        updated_at: updatedAt,
        row: row,
        model_version: commit.version,
        model_previous_version: commit.previous_version,
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


function deleteScopeRows_(sheet, rows) {
  const ordered = rows.slice().sort(function(a,b) {return b-a;});
  for (let i = 0; i < ordered.length;) {
    const last = ordered[i];
    let first = last;
    while (++i < ordered.length && ordered[i] === first - 1) first = ordered[i];
    sheet.deleteRows(first, last - first + 1);
  }
}


function saveScopeData_(scope, data, updatedAt, commit, internalArchive) {
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
    const prior = readScopeData_(sheet, scope, existing);
    if (commit) commit.previous_version = prior.version || '';
    const incomingTrades = data.model_trades || [];
    if (/^strategy_signals:\d{6}$/.test(scope) && !internalArchive) {
      const indexed = Object.create(null);
      ((prior.data || {}).model_trades || []).forEach(function(r) {indexed[r['交易ID']] = r;});
      incomingTrades.forEach(function(r) {
        const previous = indexed[r['交易ID']];
        if (stableModelJson_(previous && previous['事件封存'] || null) !== stableModelJson_(r['事件封存'] || null)) throw new Error('事件封存版本已變更，請重新同步；保留待同步資料');
      });
    }
    if (internalArchive) prior.data.model_trades = data.model_trades;
    data = mergeSignalPayload_(prior.data || {}, data);
    if (/^strategy_signals:\d{6}$/.test(scope)) validateModelExecutions_(data.model_trades, incomingTrades);
  }
  if (scope === 'strategy_signals') delete data.model_sync; // Capabilities belong to the deployed code, not persisted data.
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

  const signalScope = scope === 'strategy_signals' || /^strategy_(signals|events):\d{6}$/.test(scope);
  if (!signalScope && existing && existing.rows.length) {
    deleteScopeRows_(sheet, existing.rows);
  }
  const row = Math.max(sheet.getLastRow() + 1, 2);
  const generation = Math.max(Date.now() * 1000, existing ? existing.parts.reduce(function(n,p) {return Math.max(n, p.generation + 1);}, 0) : 0);
  const values = chunks.map(function(chunk, part) {
    return [scope, signalScope ? generation + '|' + part + '|' + chunks.length : part, chunk, incomingUpdatedAt];
  });
  sheet.getRange(row, 1, values.length, HEADER.length).setNumberFormat('@').setValues(values);
  SpreadsheetApp.flush();
  if (commit) commit.version = String(generation);
  // Preserve the last complete generation until the new one has been written in full.
  if (signalScope && existing && existing.rows.length) {
    try {
      deleteScopeRows_(sheet, existing.rows);
    } catch (_) {} // A leftover generation is harmless; reads select the newest complete one.
  }
  if (/^strategy_signals:\d{6}$/.test(scope)) {
    const month = scope.split(':')[1];
    const versions = {};
    versions[month] = String(generation);
    const active = {};
    active[month] = data.model_trades.some(function(r) {return ['等待進場','模擬持倉','監控中斷','資料不足'].indexOf(r['狀態']) >= 0;});
    saveScopeData_('strategy_signals', {model_schema: 1, model_months: [month], model_versions: versions, model_active_months: active}, incomingUpdatedAt);
  }
  return row;
}


function archiveModelEvents_(scope) {
  const sheet = getStoreSheet_(), index = getScopeRowMap_(sheet);
  const stored = readScopeData_(sheet, scope, index[scope]);
  const rows = (stored.data || {}).model_trades;
  if (!Array.isArray(rows)) throw new Error('封存來源不存在');
  const archiveScope = scope.replace('strategy_signals:', 'strategy_events:');
  let prior;
  try { prior = readScopeData_(sheet, archiveScope, index[archiveScope]); }
  catch (error) {
    if (rows.some(function(r) {return r['事件封存'];})) throw error;
    prior = {data: null}; // A failed first archive has no linked consumers; the full source is still intact.
  }
  const archived = Object.create(null);
  ((prior.data || {}).records || []).forEach(function(r) {archived[r['交易ID']] = r;});
  let changed = 0;
  rows.forEach(function(r) {
    if (r['識別版本'] === 2 || r['事件封存'] || (r['事件'] || []).length < 128) return;
    const events = r['事件'], text = JSON.stringify(events), sha = modelDigest_(text);
    if (archived[r['交易ID']] && archived[r['交易ID']].sha !== sha) throw new Error('既有原始事件封存不一致');
    archived[r['交易ID']] = {交易ID: r['交易ID'], sha: sha, events_json: text};
    const diagnostic = function(e) {
      const reason = String(e['原因'] || '');
      return !reason.startsWith('保利') && (['監控中斷','資料不足','等待進場'].indexOf(e['狀態']) >= 0 || reason.startsWith('恢復追蹤') || reason === '串流成交證據已就緒');
    };
    const diagnostics = Object.create(null);
    events.forEach(function(e) {
      if (diagnostic(e)) diagnostics[e['狀態'] + '|' + (e['原因'] || '')] = e['事件ID'];
    });
    const kept = events.filter(function(e) {
      return !diagnostic(e) || diagnostics[e['狀態'] + '|' + (e['原因'] || '')] === e['事件ID'];
    });
    const count = function(values) {return values.reduce(function(n,e) {return n + (e['次數'] || 1);}, 0);};
    r['事件封存'] = {scope: archiveScope, sha: sha, 事件數: count(events), 保留事件數: count(kept)};
    r['事件格式'] = 2; r['事件數'] = count(events); r['事件'] = kept;
    changed++;
  });
  if (changed) {
    saveScopeData_(archiveScope, {model_event_archive: 1, records: Object.keys(archived).sort().map(function(k) {return archived[k];})}, new Date().toISOString());
    const check = readScopeData_(sheet, archiveScope, getScopeRowMap_(sheet)[archiveScope]);
    const actual = (check.data || {}).records || [];
    if (actual.length !== Object.keys(archived).length || actual.some(function(r) {return !archived[r['交易ID']] || modelDigest_(r.events_json) !== archived[r['交易ID']].sha;})) throw new Error('原始事件封存回讀失敗，保留原主紀錄');
    saveScopeData_(scope, stored.data, new Date().toISOString(), {}, true);
  }
  return {success: true, archived_trades: changed, archive_scope: archiveScope};
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
    'stock_source_kind', 'stock_source_date', 'stock_source_updated_at',
  ].forEach(function(key) {
    if (payload[key] !== undefined) result[key] = payload[key];
  });
  return result;
}


function stableModelJson_(value) {
  if (Array.isArray(value)) return '[' + value.map(stableModelJson_).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().map(function(k) {return JSON.stringify(k) + ':' + stableModelJson_(value[k]);}).join(',') + '}';
  return JSON.stringify(value);
}

function modelDigest_(text) {
  return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, text, Utilities.Charset.UTF_8).map(function(n) {return ('0' + ((n + 256) % 256).toString(16)).slice(-2);}).join('');
}

function modelTime_(value) {
  const text = String(value || '');
  return Date.parse(/(Z|[+-]\d{2}:\d{2})$/.test(text) ? text : text + '+08:00');
}

function validateModelExecutions_(merged, incoming) {
  const indexed = Object.create(null);
  merged.forEach(function(row) {indexed[row['交易ID']] = row;});
  incoming.forEach(function(row) {
    if (row['識別版本'] !== 2 && !String(row['策略版本']).startsWith('V1.2')) return;
    const fields = {};
    ['市場','商品鍵','策略','策略版本','方向','到期時間'].forEach(function(k) {fields[k] = row[k] || '';});
    fields['計畫來源'] = row['計畫來源']; fields['計畫類型'] = row['計畫類型'];
    fields['交易日'] = row['策略'] === '當沖' ? row['交易日'] : '';
    fields['選擇權腿'] = (row['選擇權腿'] || []).slice().sort(function(a,b) {return String(a['代碼']).localeCompare(String(b['代碼'])) || a['買賣']-b['買賣'];});
    if (row['失效重觸規則'] === 'executable-reset-v1' && row['重新觸發來源ID']) fields['重新觸發來源ID'] = row['重新觸發來源ID'];
    const plan = modelDigest_('plan-v2|' + stableModelJson_(fields));
    const parentId = row['前次執行ID'] || '';
    const id = modelDigest_('execution-v2|' + plan + '|' + parentId);
    if (row['識別版本'] !== 2 || row['原始計畫ID'] !== plan || row['交易ID'] !== id || row['交易執行ID'] !== id ||
        ['股期戰略室','指數操作計畫','選擇權操作計畫'].indexOf(row['計畫來源']) < 0 || !row['計畫類型']) throw new Error('V1.2 計畫或執行 ID 無效');
    if (!parentId && row['進場次數'] !== 1) throw new Error('首次進場次數無效');
    if (row['重新觸發來源ID'] && !parentId) {
      const prior = indexed[row['重新觸發來源ID']], clear = row['重新觸發解除證據'] || {}, proof = row['成交證據識別'] || {};
      const invalidTime = prior && modelTime_(prior['訊號失效時間']);
      const clearTime = modelTime_(clear['來源時間']), entryTime = modelTime_(row['進場時間']);
      const same = prior && ['市場','商品鍵','策略','策略版本','方向','計畫來源','計畫類型','交易日'].every(function(k) {return prior[k] === row[k];});
      const released = same && (clear['進場許可'] === false || (['多頭','偏多'].indexOf(prior['方向'])>=0 ? clear['價格']<prior['進場價'] : clear['價格']>prior['進場價']));
      if (!same || prior['狀態']!=='訊號失效' || prior['模擬進場價']!=null || prior['資料缺口'] || !released ||
          row['失效重觸規則']!=='executable-reset-v1' || row['選擇權腿'] || !isFinite(invalidTime) || !isFinite(clearTime) || !isFinite(entryTime) ||
          !(invalidTime<clearTime && clearTime<entryTime) || proof['來源時間']!==row['進場時間'] ||
          !clear['串流識別'] || clear['串流識別']!==proof['串流識別'] || !(proof['序號']>clear['序號']) ||
          !(clear['價格']>0) || !(proof['價格']>0) || row['計畫可執行性']!=='通過' || !(row['淨RR']>=1) || !(row['原始RR']>=1.3)) {
        throw new Error('失效重新觸發缺少獨立計畫或完整解除／成交證據');
      }
    }
    if (parentId) {
      let parent = indexed[parentId];
      const cleared = row['重入解除證據'] || {};
      if (!parent && /^\d{6}$/.test(String(row['前次執行月份'] || ''))) {
        const sheet = getStoreSheet_(), scope = 'strategy_signals:' + row['前次執行月份'];
        const stored = readScopeData_(sheet, scope, getScopeRowMap_(sheet)[scope]);
        parent = ((stored.data || {}).model_trades || []).find(function(r) {return r['交易ID'] === parentId;});
      }
      const closeTime = parent && modelTime_(parent['結案時間']);
      const entryTime = modelTime_(row['進場時間']), clearTime = modelTime_(cleared['來源時間']);
      if (!parent || parent['原始計畫ID'] !== plan || parent['狀態'] !== '已平倉' || parent['資料缺口'] ||
          row['進場次數'] !== parent['進場次數'] + 1 || !isFinite(closeTime) || !isFinite(entryTime) || !isFinite(clearTime) ||
          entryTime <= closeTime || clearTime <= closeTime || clearTime > entryTime ||
          (row['策略'] === '當沖' && parent['交易日'] !== row['交易日']) ||
          (row['到期時間'] && entryTime >= modelTime_(row['到期時間']))) throw new Error('二次進場缺少已確認前筆或新許可／解除證據');
      const proof = row['成交證據識別'] || {};
      if (proof['來源時間'] !== row['進場時間'] || !isFinite(proof['價格']) || proof['價格'] <= 0) throw new Error('二次進場成交證據不符');
      if (row['選擇權腿']) {
        row['選擇權腿'].forEach(function(leg) {
          const premium = (proof['權利金證據'] || {})[leg['代碼']] || {};
          const elapsed = entryTime - modelTime_(premium['來源時間']);
          if (!isFinite(elapsed) || elapsed < 0 || elapsed > 2000 || !(premium['價格'] > 0) || !row['進場權利金'] || !(row['進場權利金'][leg['代碼']] > 0)) throw new Error('選擇權重新進場各腿證據不足');
        });
      }
    }
    indexed[id]['同步確認'] = true;
  });
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
      const newTime = String(item['來源時間'] || ''), oldTime = String(old['來源時間'] || '');
      const sameStream = item['串流識別'] === old['串流識別'];
      const newSeq = sameStream ? Number(item['最後序號']) || 0 : 0, oldSeq = sameStream ? Number(old['最後序號']) || 0 : 0;
      const newer = newTime > oldTime || (newTime === oldTime && (newSeq > oldSeq ||
        (newSeq === oldSeq && (item['事件'] || []).length > (old['事件'] || []).length)));
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
      if (row['識別版本'] === 2) ['進場價','停損價','目標價'].forEach(function(k) {row[k] = first[k];});
      ['最後進場等待原因','最後進場等待時間','訊號失效原因','訊號失效時間','未進場分類'].forEach(function(k) {
        if (!row[k] && (item[k] || old[k])) row[k] = item[k] || old[k];
      });
      const causes = Array.from(new Set([].concat(old['資料缺口原因'] || [], item['資料缺口原因'] || [])));
      if (causes.length) row['資料缺口原因'] = causes;
      const events = {};
      [].concat(old['事件'] || [], item['事件'] || []).forEach(function(e) {
        if (!e || !e['事件ID']) return;
        const previous = events[e['事件ID']];
        if (!previous || String(e['結束時間'] || e['時間']) > String(previous['結束時間'] || previous['時間']) ||
            (String(e['結束時間'] || e['時間']) === String(previous['結束時間'] || previous['時間']) && (e['次數'] || 1) >= (previous['次數'] || 1))) events[e['事件ID']] = e;
      });
      row['事件'] = Object.keys(events).map(function(k) {return events[k];}).sort(function(a,b) {
        return String(a['時間']).localeCompare(String(b['時間']));
      });
      const archive = row['事件封存'] || {};
      row['事件數'] = (archive['事件數'] || 0) - (archive['保留事件數'] || 0) + row['事件'].reduce(function(n,e) {return n + (e['次數'] || 1);}, 0);
      if (row['識別版本'] === 2) {
        row['同步確認'] = Boolean(old['同步確認'] || item['同步確認']);
        ['重入解除','重入來源時間','重入最後序號','重入串流識別','重入等待原因','新計畫解除','新計畫來源時間','新計畫最後序號','新計畫串流識別','新訊號觸發狀態'].forEach(function(k) {
          const candidates = [old,item].filter(function(r) {return r[k] !== undefined;});
          if (candidates.length) row[k] = candidates.sort(function(a,b) {return String(b[k.startsWith('新')?'新計畫來源時間':'重入來源時間'] || '').localeCompare(String(a[k.startsWith('新')?'新計畫來源時間':'重入來源時間'] || ''));})[0][k];
        });
      }
    }
    if (!row['計畫來源']) {
      const kind = row['計畫類型'] || '', keyText = String(row['商品鍵'] || '');
      row['計畫來源'] = row['選擇權腿'] ? '選擇權操作計畫' : (['日線／15分K','短波5分K'].indexOf(kind) >= 0 || /\|(日線／15分K|短波5分K)$/.test(keyText)) ? '指數操作計畫' :
        (['股票','期貨'].indexOf(row['市場']) >= 0 && !row['觸發商品']) ? '股期戰略室' : '未確認';
    }
    if (!row['計畫類型']) row['計畫類型'] = row['計畫來源'] === '股期戰略室' ? String(row['市場'] || '') + String(row['策略'] || '') : row['計畫來源'] === '指數操作計畫' ? String(row['商品鍵']).split('|').pop() : '未確認';
    merged[key] = row;
  });
  return Object.keys(merged).sort().map(function(k) {return merged[k];});
}

function mergeSignalPayload_(prior, incoming) {
  const result = Object.assign({}, prior, incoming, {model_schema: 1});
  result.model_months = Array.from(new Set([].concat(prior.model_months || [], incoming.model_months || []))).sort();
  result.model_versions = Object.assign({}, prior.model_versions || {});
  result.model_active_months = Object.assign({}, prior.model_active_months || {}, incoming.model_active_months || {});
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
  return SCOPES.indexOf(scope) >= 0 || /^strategy_(signals|events):\d{6}$/.test(scope) ? scope : '';
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
