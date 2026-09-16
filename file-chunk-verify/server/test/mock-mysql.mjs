/**
 * 极简内存版 mysql2/promise（CAS v3），仅实现业务代码实际用到的语句。
 * 通过 ESM loader 注入，业务代码零改动。
 */

const db = {
  files: new Map(), // id -> row
  cas: new Map(), // chunk_hash -> row
  fileChunks: [], // {id, file_id, chunk_index, chunk_hash, status}
  merged: new Map(), // merged_hash -> row
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

  /* ---------------- files ---------------- */

  if (n.startsWith('INSERT INTO files')) {
    m = n.match(
      /VALUES \('([^']*)', '((?:[^']|'')*)', '(\d+)', (\d+), (\d+), (?:('([a-f0-9]{64})')|NULL), '(\w+)'\)/,
    );
    if (!m) throw new Error('mock INSERT files: ' + n);
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
  if ((m = n.match(/^SELECT id, file_hash, status FROM files WHERE id = '([^']+)' FOR UPDATE$/))) {
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

  if (n.startsWith('INSERT INTO cas_chunks')) {
    m = n.match(
      /VALUES \('([a-f0-9]{64})', (\d+), '([^']+)', (\d+)\)/,
    );
    if (db.cas.has(m[1])) {
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
    return [[...db.merged.values()].filter((x) => x.ref_count === 0).map((x) => ({ ...x }))];
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

export default { createConnection, createPool };
