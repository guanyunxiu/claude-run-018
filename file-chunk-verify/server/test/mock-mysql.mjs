/**
 * 极简内存版 mysql2/promise（CAS v3），仅实现业务代码实际用到的语句。
 * 通过 ESM loader 注入，业务代码零改动。
 */

const db = {
  files: new Map(), // id -> row
  cas: new Map(), // chunk_hash -> row
  fileChunks: [], // {id, file_id, chunk_index, chunk_hash, status}
  merged: new Map(), // merged_hash -> row
  // 列可空性元数据，模拟 information_schema（旧库 files.file_hash 为 NOT NULL）
  columns: {
    files: {
      // 默认新库为可空（与 CREATE TABLE 一致）；旧库迁移测试置 MOCK_LEGACY_NOTNULL=1
      file_hash: { isNullable: process.env.MOCK_LEGACY_NOTNULL === '1' ? 'NO' : 'YES' },
      merge_owner: { isNullable: 'YES' },
      merge_lease_until: { isNullable: 'YES' },
    },
  },
  // 记录迁移动作（测试断言“旧库启动时确实执行了 ALTER”）
  alterLog: [],
};

// 测试辅助：模拟旧库（file_hash NOT NULL）
globalThis.__mockSetFileHashNullable = (nullable) => {
  db.columns.files.file_hash.isNullable = nullable ? 'YES' : 'NO';
};

let fcAuto = 0;

function norm(sql) {
  return sql.replace(/\s+/g, ' ').trim();
}

function bind(sql, params) {
  let i = 0;
  return norm(sql).replace(/\?/g, () => {
    const v = params[i++];
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number') return String(v);
    if (v instanceof Date) return `'${v.toISOString().slice(0, 23)}'`;
    return `'${String(v).replace(/'/g, "''")}'`;
  });
}

class Connection {
  async query(sql, params = []) {
    return runQuery(sql, params);
  }
  async beginTransaction() {}
  async commit() {}
  async rollback() {}
  async end() {}
  release() {}
}

const num = (v) => (typeof v === 'number' ? v : Number(v));

function myLinks(fileId) {
  return db.fileChunks
    .filter((l) => l.file_id === fileId)
    .sort((a, b) => a.chunk_index - b.chunk_index);
}

/** 组装 file_chunks JOIN cas_chunks 查询结果 */
function joinRows(fileId) {
  return myLinks(fileId).map((l) => {
    const c = db.cas.get(l.chunk_hash);
    return {
      chunk_index: l.chunk_index,
      chunk_hash: l.chunk_hash,
      chunk_size: c ? Number(c.chunk_size) : 0,
      cas_size: c ? Number(c.chunk_size) : 0,
      status: l.status,
      storage_path: c ? c.storage_path : null,
    };
  });
}

async function runQuery(rawSql, params) {
  const n = bind(rawSql, params);
  let m;

  if (/^CREATE\b/i.test(n)) return [[]];

  /* ---- information_schema：列存在性 / 可空性（迁移幂等判断） ---- */
  if (n.includes('FROM information_schema.COLUMNS')) {
    let tableName = params[1];
    let columnName = params[2];
    if (params.length < 3) {
      const tm = n.match(/TABLE_NAME\s*=\s*'(\w+)'/);
      const cm = n.match(/COLUMN_NAME\s*=\s*'(\w+)'/);
      tableName = tm?.[1];
      columnName = cm?.[1];
    }
    const col = db.columns[tableName]?.[columnName];
    if (!col) return [[]];
    if (n.includes('IS_NULLABLE')) {
      return [[{ isNullable: col.isNullable, n: col.isNullable }]];
    }
    // SELECT 1 ... 存在性探测
    return [[{ '1': 1 }]];
  }

  /* ---- ALTER TABLE ... MODIFY/ADD COLUMN：执行迁移、更新元数据 ---- */
  if (/^ALTER TABLE/i.test(n)) {
    let mm = /TABLE `?(\w+)`? MODIFY `?(\w+)`? (.*)$/i.exec(n);
    if (mm) {
      const [, table, column, def] = mm;
      db.columns[table] ||= {};
      const nullable = /\bNULL\b/i.test(def) && !/NOT NULL/i.test(def);
      db.columns[table][column] = { isNullable: nullable ? 'YES' : 'NO' };
      db.alterLog.push({ kind: 'modify', table, column, def: def.trim() });
    } else {
      mm = /TABLE `?(\w+)`? ADD COLUMN `?(\w+)`? (.*)$/i.exec(n);
      if (mm) {
        const [, table, column, def] = mm;
        db.columns[table] ||= {};
        const nullable = /\bNULL\b/i.test(def);
        db.columns[table][column] = { isNullable: nullable ? 'YES' : 'NO' };
        db.alterLog.push({ kind: 'add', table, column, def: def.trim() });
      }
    }
    return [{ affectedRows: 0, info: 'mock alter' }];
  }

  /* ---------------- files ---------------- */

  if (n.startsWith('INSERT INTO files')) {
    m = n.match(
      /VALUES \('([^']*)', '((?:[^']|'')*)', '(\d+)', (\d+), (\d+), (?:('([a-f0-9]{64})')|NULL), '(\w+)'\)/,
    );
    if (!m) {
      // 兼容带列名清单的参数化 INSERT：直接从 params 构造（值已 bind 进 SQL 时）
      const mm = n.match(
        /INSERT INTO files \(([^)]+)\)\s*VALUES \((.*)\)$/s,
      );
      if (mm) {
        const cols = mm[1].split(',').map((s) => s.trim());
        const vals = mm[2].split(',').map((s) => s.trim());
        const row = {
          merged_hash: null,
          merged_path: null,
          created_at: new Date(),
          updated_at: new Date(),
        };
        cols.forEach((c, i) => {
          let v = vals[i];
          if (v === 'NULL') v = null;
          else if (/^'.*'$/.test(v)) v = v.slice(1, -1).replace(/''/g, "'");
          else if (/^\d+$/.test(v)) v = /size|chunks/.test(c) && c !== 'chunk_size' ? v : Number(v);
          row[c] = v;
        });
        if (typeof row.file_size === 'number') row.file_size = String(row.file_size);
        db.files.set(row.id, row);
        return [{ affectedRows: 1 }];
      }
      throw new Error('mock INSERT files: ' + n);
    }
    db.files.set(m[1], {
      id: m[1],
      file_name: m[2].replace(/''/g, "'"),
      file_size: m[3],
      chunk_size: Number(m[4]),
      total_chunks: Number(m[5]),
      file_hash: m[7] ?? null,
      merged_hash: null,
      merged_path: null,
      status: m[8],
      created_at: new Date(),
      updated_at: new Date(),
    });
    return [{ affectedRows: 1 }];
  }

  if ((m = n.match(/^SELECT \* FROM files WHERE id = '([^']+)' FOR UPDATE$/))) {
    const row = db.files.get(m[1]);
    return [row ? [{ ...row }] : []];
  }
  if ((m = n.match(/^SELECT (?!\*)(.*?) FROM files WHERE id\s*=\s*\?$/))) {
    // 参数化查询：WHERE id = ?（bind 后通常为字面量，此分支兜底）
    const row = db.files.get(String(params[0]));
    if (!row) return [[]];
    const cols = m[1].split(',').map((s) => s.trim());
    return [[Object.fromEntries(cols.map((c) => [c, row[c] ?? null]))]];
  }
  if ((m = n.match(/^SELECT (?!\*)(.*?) FROM files WHERE id\s*=\s*'([^']+)'$/))) {
    const row = db.files.get(m[2]);
    if (!row) return [[]];
    const cols = m[1].split(',').map((s) => s.trim());
    return [[Object.fromEntries(cols.map((c) => [c, row[c] ?? null]))]];
  }
  if ((m = n.match(/^SELECT \* FROM files WHERE id = '([^']+)'$/))) {
    const row = db.files.get(m[1]);
    return [row ? [{ ...row }] : []];
  }
  if ((m = n.match(/^SELECT id FROM files WHERE id = '([^']+)'$/))) {
    return [db.files.has(m[1]) ? [{ id: m[1] }] : []];
  }
  if ((m = n.match(/^SELECT status FROM files WHERE id = '([^']+)'$/))) {
    const row = db.files.get(m[1]);
    return [row ? [{ status: row.status }] : []];
  }
  // 全表列查询：SELECT a,b FROM files（无 WHERE）
  if (/^SELECT (?!\*)[a-z0-9_, ]+ FROM files\s*$/i.test(n)) {
    const cols = n.replace(/^SELECT (.*?) FROM files\s*$/i, '$1').split(',').map((s) => s.trim());
    return [[...db.files.values()].map((row) => Object.fromEntries(cols.map((c) => [c, row[c] ?? null])))];
  }

  // 通用列查询：SELECT a, b FROM files WHERE id=? 或 ='...'（FOR UPDATE 可选）
  {
    const gm = /^SELECT (?!\*)([a-z0-9_, ]+?) FROM files WHERE id\s*=\s*(?:\?|'([^']+)')\s*(FOR UPDATE)?$/.exec(n);
    if (gm) {
      const cols = gm[1].split(',').map((s) => s.trim()).filter(Boolean);
      const id = gm[2] ?? String(params[0]);
      const row = db.files.get(id);
      if (!row) return [[]];
      return [[Object.fromEntries(cols.map((c) => [c, row[c] ?? null]))]];
    }
  }
  if ((m = n.match(/^SELECT id, file_hash, status FROM files WHERE id\s*=\s*'([^']+)' FOR UPDATE$/))) {
    const row = db.files.get(m[1]);
    return [row ? [{ id: row.id, file_hash: row.file_hash, status: row.status }] : []];
  }

  // 秒传候选捐赠文件
  if (n.includes("FROM files WHERE file_hash =") && n.includes("status = 'completed'")) {
    m = n.match(/file_hash = '([a-f0-9]{64})'/);
    const rows = [...db.files.values()]
      .filter(
        (f) => f.file_hash === m[1] && f.status === 'completed' && f.merged_hash,
      )
      .sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))
      .map((f) => ({ ...f }));
    return [rows];
  }

  if (
    (m = n.match(
      /^UPDATE files SET status = 'merging' WHERE id = '([a-f0-9]{64})' AND status = 'uploading'$/,
    ))
  ) {
    const row = db.files.get(m[1]);
    if (row && row.status === 'uploading') {
      row.status = 'merging';
      return [{ affectedRows: 1 }];
    }
    return [{ affectedRows: 0 }];
  }

  // 合并抢占（含租约接管）：UPDATE files SET status='merging', merge_owner=?, merge_lease_until=?
  if (n.startsWith("UPDATE files SET status='merging', merge_owner=")) {
    const idM = /WHERE id='?([a-f0-9]{64})'?/.exec(n);
    const ownerM = /merge_owner='([^']*)'/.exec(n);
    const leaseM = /merge_lease_until='([^']+)'/.exec(n);
    const row = db.files.get(idM[1]);
    const leaseExpired = (r) =>
      !r.merge_lease_until || new Date(r.merge_lease_until).getTime() <= Date.now();
    if (row && (row.status === 'uploading' || (row.status === 'merging' && leaseExpired(row)))) {
      row.status = 'merging';
      row.merge_owner = ownerM[1];
      row.merge_lease_until = leaseM[1];
      return [{ affectedRows: 1 }];
    }
    return [{ affectedRows: 0 }];
  }

  // complete 成功：completed + merged_hash/path + 清空租约（字段顺序两种）
  if (
    (m = n.match(
      /^UPDATE files\s+SET status = 'completed', merged_hash = '([a-f0-9]{64})', merged_path = '([^']+)',\s*merge_owner = NULL, merge_lease_until = NULL\s+WHERE id = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.files.get(m[3]);
    if (row) {
      row.status = 'completed';
      row.merged_hash = m[1];
      row.merged_path = m[2];
      row.merge_owner = null;
      row.merge_lease_until = null;
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }

  // 回退 uploading 并清租约
  if ((m = n.match(/^UPDATE files SET status='uploading', merge_owner=NULL, merge_lease_until=NULL/))) {
    const idM = /WHERE id=\?/.test(n) ? params[0] : /id='?([a-f0-9]{64})'?/.exec(n)?.[1];
    const row = db.files.get(idM);
    if (row && (!/AND status='merging'/.test(n) || row.status === 'merging')) {
      row.status = 'uploading';
      row.merge_owner = null;
      row.merge_lease_until = null;
      return [{ affectedRows: 1 }];
    }
    return [{ affectedRows: 0 }];
  }

  if ((m = n.match(/^UPDATE files SET file_hash = '([a-f0-9]{64})' WHERE id = '([a-f0-9]{64})'$/))) {
    const row = db.files.get(m[2]);
    if (row) row.file_hash = m[1];
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if (
    (m = n.match(
      /^UPDATE files SET status = '(\w+)' WHERE id = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.files.get(m[2]);
    if (row) row.status = m[1];
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if (
    (m = n.match(
      /^UPDATE files SET status = 'completed', merged_hash = '([a-f0-9]{64})', merged_path = '([^']+)' WHERE id = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.files.get(m[3]);
    if (row) {
      row.status = 'completed';
      row.merged_hash = m[1];
      row.merged_path = m[2];
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if (
    (m = n.match(
      /^UPDATE files\s+SET status = 'completed', file_hash = '([a-f0-9]{64})', merged_hash = '([a-f0-9]{64})', merged_path = '([^']+)'\s+WHERE id = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.files.get(m[4]);
    if (row) {
      row.status = 'completed';
      row.file_hash = m[1];
      row.merged_hash = m[2];
      row.merged_path = m[3];
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if ((m = n.match(/^UPDATE files SET status = 'failed' WHERE id = '([a-f0-9]{64})'$/))) {
    const row = db.files.get(m[1]);
    if (row) row.status = 'failed';
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if ((m = n.match(/^DELETE FROM files WHERE id = '([a-f0-9]{64})'$/))) {
    const existed = db.files.delete(m[1]);
    return [{ affectedRows: existed ? 1 : 0 }];
  }

  /* ---------------- cas_chunks ---------------- */

  if (n.startsWith('SELECT * FROM cas_chunks WHERE chunk_hash')) {
    m = n.match(/chunk_hash = '([a-f0-9]{64})'/);
    const row = db.cas.get(m[1]);
    return [row ? [{ ...row }] : []];
  }
  if (n.startsWith('SELECT chunk_size FROM cas_chunks WHERE chunk_hash')) {
    m = n.match(/chunk_hash\s*=\s*'([a-f0-9]{64})'/);
    const row = db.cas.get(m[1]);
    return [row ? [{ chunk_size: row.chunk_size }] : []];
  }
  if (n.startsWith('SELECT ref_count FROM cas_chunks WHERE chunk_hash')) {
    m = n.match(/chunk_hash\s*=\s*'([a-f0-9]{64})'/);
    const row = db.cas.get(m[1]);
    return [row ? [{ ref_count: row.ref_count }] : []];
  }

  if (n.startsWith('INSERT INTO cas_chunks') || n.startsWith('INSERT IGNORE INTO cas_chunks')) {
    m = n.match(
      /VALUES \('([a-f0-9]{64})', (\d+), '([^']+)', (\d+)\)/,
    );
    const ignore = /^INSERT IGNORE/.test(n);
    const onDup = /ON DUPLICATE KEY UPDATE ref_count = ref_count \+ 1/.test(n);
    if (db.cas.has(m[1])) {
      if (ignore) return [{ affectedRows: 0 }];
      if (onDup) {
        db.cas.get(m[1]).ref_count += 1;
        return [{ affectedRows: 2 }];
      }
      // 模拟唯一键冲突
      const err = new Error('Duplicate entry');
      err.code = 'ER_DUP_ENTRY';
      throw err;
    }
    db.cas.set(m[1], {
      chunk_hash: m[1],
      chunk_size: BigInt(num(m[2])),
      storage_path: m[3],
      ref_count: Number(m[4]),
      created_at: new Date(),
    });
    return [{ affectedRows: 1 }];
  }

  if (
    (m = n.match(
      /^UPDATE cas_chunks SET ref_count = ref_count \+ 1 WHERE chunk_hash = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.cas.get(m[1]);
    if (row) row.ref_count += 1;
    return [{ affectedRows: row ? 1 : 0 }];
  }
  if (
    (m = n.match(
      /^UPDATE cas_chunks SET chunk_size = (\d+), storage_path = '([^']+)', ref_count = ref_count \+ 1 WHERE chunk_hash = '([a-f0-9]{64})'$/,
    ))
  ) {
    // 上传自愈：物理缺失后重写，更新大小/路径并 +1 引用
    const row = db.cas.get(m[3]);
    if (row) {
      row.chunk_size = BigInt(num(m[1]));
      row.storage_path = m[2];
      row.ref_count += 1;
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }
  if (
    (m = n.match(
      /^UPDATE cas_chunks SET ref_count = GREATEST\(ref_count - 1, 0\) WHERE chunk_hash = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.cas.get(m[1]);
    if (row) row.ref_count = Math.max(0, row.ref_count - 1);
    return [{ affectedRows: row ? 1 : 0 }];
  }

  // GC 候选：ref_count=0 且 created_at 早于 cutoff（尊重 minAge，保护刚创建的对象）
  if (
    n.startsWith(
      'SELECT chunk_hash, chunk_size, storage_path FROM cas_chunks WHERE ref_count = 0',
    )
  ) {
    const cutoff = params[0] instanceof Date ? params[0] : new Date(Date.now() - 300 * 1000);
    const rows = [...db.cas.values()]
      .filter((c) => c.ref_count === 0 && c.created_at < cutoff)
      .map((c) => ({ ...c }));
    return [rows];
  }
  if ((m = n.match(/^DELETE FROM cas_chunks WHERE chunk_hash = '([a-f0-9]{64})' AND ref_count = 0$/))) {
    const row = db.cas.get(m[1]);
    if (row && row.ref_count === 0) {
      db.cas.delete(m[1]);
      return [{ affectedRows: 1 }];
    }
    return [{ affectedRows: 0 }];
  }

  // 全局命中查询：... WHERE ref_count > 0 AND chunk_hash IN ('..','..')
  if (n.includes('FROM cas_chunks WHERE ref_count > 0 AND chunk_hash IN')) {
    const hashes = [...n.matchAll(/'([a-f0-9]{64})'/g)].map((x) => x[1]);
    const rows = hashes
      .filter((h) => db.cas.has(h) && db.cas.get(h).ref_count > 0)
      .map((h) => ({ chunk_hash: h }));
    return [rows];
  }

  /* ---------------- merged_blobs ---------------- */

  if (n.startsWith('SELECT merged_hash, storage_path FROM merged_blobs WHERE merged_hash')) {
    m = n.match(/merged_hash = '([a-f0-9]{64})'/);
    const row = db.merged.get(m[1]);
    return [row ? [{ merged_hash: row.merged_hash, storage_path: row.storage_path }] : []];
  }
  if (n.startsWith('SELECT merged_hash FROM merged_blobs WHERE merged_hash')) {
    m = n.match(/merged_hash = '([a-f0-9]{64})'/);
    const row = db.merged.get(m[1]);
    return [row ? [{ merged_hash: row.merged_hash }] : []];
  }
  if (n.startsWith('INSERT INTO merged_blobs')) {
    m = n.match(
      /VALUES \('([a-f0-9]{64})', '(\d+)', '([^']+)', (\d+)\)/,
    );
    if (db.merged.has(m[1])) {
      const err = new Error('Duplicate entry');
      err.code = 'ER_DUP_ENTRY';
      throw err;
    }
    db.merged.set(m[1], {
      merged_hash: m[1],
      file_size: BigInt(num(m[2])),
      storage_path: m[3],
      ref_count: Number(m[4]),
      created_at: new Date(),
    });
    return [{ affectedRows: 1 }];
  }
  if (
    (m = n.match(
      /^UPDATE merged_blobs SET ref_count = ref_count \+ 1 WHERE merged_hash = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.merged.get(m[1]);
    if (row) row.ref_count += 1;
    return [{ affectedRows: row ? 1 : 0 }];
  }
  if (
    (m = n.match(
      /^UPDATE merged_blobs SET ref_count = GREATEST\(ref_count - 1, 0\) WHERE merged_hash = '([a-f0-9]{64})'$/,
    ))
  ) {
    const row = db.merged.get(m[1]);
    if (row) row.ref_count = Math.max(0, row.ref_count - 1);
    return [{ affectedRows: row ? 1 : 0 }];
  }
  if (
    n.startsWith(
      'SELECT merged_hash, file_size, storage_path FROM merged_blobs WHERE ref_count = 0',
    )
  ) {
    const cutoff = params[0] instanceof Date ? params[0] : new Date(Date.now() - 300 * 1000);
    return [
      [...db.merged.values()]
        .filter((x) => x.ref_count === 0 && x.created_at < cutoff)
        .map((x) => ({ ...x })),
    ];
  }
  if ((m = n.match(/^DELETE FROM merged_blobs WHERE merged_hash = '([a-f0-9]{64})' AND ref_count = 0$/))) {
    const row = db.merged.get(m[1]);
    if (row && row.ref_count === 0) {
      db.merged.delete(m[1]);
      return [{ affectedRows: 1 }];
    }
    return [{ affectedRows: 0 }];
  }

  /* ---------------- file_chunks ---------------- */

  if (n.startsWith('INSERT INTO file_chunks')) {
    m = n.match(
      /VALUES \('([a-f0-9]{64})', (\d+), '([a-f0-9]{64})', '(\w+)'\)/,
    );
    const fileId = m[1];
    const index = Number(m[2]);
    const hash = m[3];
    const status = m[4];
    const existing = db.fileChunks.find(
      (l) => l.file_id === fileId && l.chunk_index === index,
    );
    if (existing) {
      // ON DUPLICATE KEY UPDATE
      existing.chunk_hash = hash;
      existing.status = status;
    } else {
      db.fileChunks.push({
        id: ++fcAuto,
        file_id: fileId,
        chunk_index: index,
        chunk_hash: hash,
        status,
      });
    }
    return [{ affectedRows: 1 }];
  }

  if (n.startsWith('SELECT id, chunk_hash FROM file_chunks WHERE')) {
    m = n.match(/file_id = '([a-f0-9]{64})' AND chunk_index = (\d+)/);
    const row = db.fileChunks.find(
      (l) => l.file_id === m[1] && l.chunk_index === Number(m[2]),
    );
    return [row ? [{ id: row.id, chunk_hash: row.chunk_hash }] : []];
  }

  if (n.includes('JOIN cas_chunks')) {
    m = n.match(/fc\.file_id = '([a-f0-9]{64})'/);
    const fileId = m[1];
    // 不同 SELECT 列：complete 需要 cas_size/storage_path；普通列表需要 chunk_size/status
    return [joinRows(fileId)];
  }

  if (n.startsWith('SELECT fc.chunk_index, fc.chunk_hash, cc.storage_path FROM file_chunks fc')) {
    m = n.match(/fc\.file_id = '([a-f0-9]{64})'/);
    return [
      myLinks(m[1]).map((l) => ({
        chunk_index: l.chunk_index,
        chunk_hash: l.chunk_hash,
        storage_path: db.cas.get(l.chunk_hash)?.storage_path ?? null,
      })),
    ];
  }

  if (n.startsWith('SELECT chunk_index, chunk_hash FROM file_chunks WHERE file_id')) {
    m = n.match(/file_id = '([a-f0-9]{64})'/);
    return [
      myLinks(m[1]).map((l) => ({ chunk_index: l.chunk_index, chunk_hash: l.chunk_hash })),
    ];
  }

  if (n.startsWith("UPDATE file_chunks SET chunk_hash")) {
    m = n.match(/WHERE id = (\d+)/);
    const id = Number(m[1]);
    const hm = n.match(/SET chunk_hash = '([a-f0-9]{64})'/);
    const row = db.fileChunks.find((l) => l.id === id);
    if (row) {
      row.chunk_hash = hm[1];
      row.status = 'uploaded';
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if (n.startsWith("UPDATE file_chunks SET status = 'verified' WHERE file_id")) {
    m = n.match(/file_id = '([a-f0-9]{64})'/);
    let count = 0;
    for (const l of db.fileChunks)
      if (l.file_id === m[1]) {
        l.status = 'verified';
        count += 1;
      }
    return [{ affectedRows: count }];
  }

  if (n.startsWith('DELETE FROM file_chunks WHERE file_id')) {
    m = n.match(/file_id = '([a-f0-9]{64})'/);
    const before = db.fileChunks.length;
    db.fileChunks = db.fileChunks.filter((l) => l.file_id !== m[1]);
    return [{ affectedRows: before - db.fileChunks.length }];
  }

  if (n.startsWith('SELECT COUNT(*) AS c FROM file_chunks WHERE file_id')) {
    m = n.match(/file_id = '([a-f0-9]{64})'/);
    return [[{ c: db.fileChunks.filter((l) => l.file_id === m[1]).length }]];
  }

  // GC 阶段 C 全表对账
  if (n === 'SELECT chunk_hash FROM cas_chunks') {
    return [[...db.cas.keys()].map((h) => ({ chunk_hash: h }))];
  }
  if (n === 'SELECT merged_hash FROM merged_blobs') {
    return [[...db.merged.keys()].map((h) => ({ merged_hash: h }))];
  }

  throw new Error('mock-mysql 未实现的 SQL: ' + n);
}

export async function createConnection() {
  return new Connection();
}

export function createPool() {
  return {
    async query(sql, params) {
      return runQuery(sql, params);
    },
    async getConnection() {
      return new Connection();
    },
  };
}

/** 测试钩子：直接访问内存库（仅用于构造真实接口难以制造的脏数据） */
export function __mockDb() {
  return db;
}

export default { createConnection, createPool };
