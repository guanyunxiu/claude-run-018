/** 极简内存版 mysql2/promise，仅实现业务代码实际用到的语句 */

const db = {
  files: new Map(), // id -> row
  chunks: new Map(), // `${fileId}:${index}` -> row
};

function norm(sql) {
  return sql.replace(/\s+/g, ' ').trim();
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

let pIndex = 0;
function bind(sql, params) {
  let i = 0;
  return norm(sql).replace(/\?/g, () => {
    const v = params[i++];
    return typeof v === 'number' ? String(v) : `'${String(v).replace(/'/g, "''")}'`;
  });
}

async function runQuery(rawSql, params) {
  const sql = bind(rawSql, params);
  const n = norm(sql);

  if (/^CREATE\b/i.test(n)) return [[]];

  if (/^INSERT INTO files\b/i.test(n)) {
    // INSERT INTO files (id, file_name, file_size, chunk_size, total_chunks, file_hash, status)
    // VALUES ('id', 'name', 'size', cs, total, 'hash', 'uploading')
    const m = n.match(/VALUES \('([^']*)', '((?:[^']|'')*)', '(\d+)', (\d+), (\d+), '([a-f0-9]+)', '(\w+)'\)/);
    if (!m) throw new Error(`mock 无法解析 INSERT files: ${n}`);
    db.files.set(m[1], {
      id: m[1],
      file_name: m[2].replace(/''/g, "'"),
      file_size: m[3],
      chunk_size: Number(m[4]),
      total_chunks: Number(m[5]),
      file_hash: m[6],
      merged_hash: null,
      merged_path: null,
      status: m[7],
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    return [{ affectedRows: 1 }];
  }

  if (/^INSERT INTO chunks\b/i.test(n)) {
    // VALUES ('fileId', idx, 'hash', size, 'path', 'uploaded')
    const m = n.match(/VALUES \('([a-f0-9]+)', (\d+), '([a-f0-9]+)', (\d+), '([^']+)', '(\w+)'\)/);
    if (!m) throw new Error(`mock 无法解析 INSERT chunks: ${n}`);
    const key = `${m[1]}:${m[2]}`;
    db.chunks.set(key, {
      id: ++pIndex,
      file_id: m[1],
      chunk_index: Number(m[2]),
      chunk_hash: m[3],
      chunk_size: Number(m[4]),
      storage_path: m[5],
      status: 'uploaded',
    });
    return [{ affectedRows: 1 }];
  }

  if (/^SELECT \* FROM files WHERE id = '([^']+)' FOR UPDATE$/.test(n) ||
      /^SELECT \* FROM files WHERE id = '([^']+)'$/.test(n)) {
    const id = n.match(/id = '([^']+)'/)[1];
    const row = db.files.get(id);
    return [row ? [{ ...row }] : []];
  }

  if (/^SELECT id FROM files WHERE id = '([^']+)'$/.test(n)) {
    const id = n.match(/id = '([^']+)'/)[1];
    return db.files.has(id) ? [[{ id }]] : [[]];
  }

  const fileIdMatch = n.match(/file_id = '([a-f0-9]+)'/);

  if (n.startsWith('SELECT chunk_index, chunk_hash, chunk_size, status FROM chunks')) {
    const rows = [...db.chunks.values()]
      .filter((r) => r.file_id === fileIdMatch[1])
      .sort((a, b) => a.chunk_index - b.chunk_index)
      .map((r) => ({
        chunk_index: r.chunk_index,
        chunk_hash: r.chunk_hash,
        chunk_size: r.chunk_size,
        status: r.status,
      }));
    return [rows];
  }

  if (n.startsWith('SELECT chunk_index, chunk_hash, chunk_size, storage_path FROM chunks')) {
    const rows = [...db.chunks.values()]
      .filter((r) => r.file_id === fileIdMatch[1])
      .sort((a, b) => a.chunk_index - b.chunk_index)
      .map((r) => ({
        chunk_index: r.chunk_index,
        chunk_hash: r.chunk_hash,
        chunk_size: r.chunk_size,
        storage_path: r.storage_path,
      }));
    return [rows];
  }

  let m;
  if ((m = n.match(/^SELECT id, chunk_hash FROM chunks WHERE file_id = '[a-f0-9]+' AND chunk_index = (\d+)$/))) {
    const fid = fileIdMatch[1];
    const row = db.chunks.get(`${fid}:${m[1]}`);
    return [row ? [{ id: row.id, chunk_hash: row.chunk_hash }] : []];
  }

  if (n.startsWith('SELECT COUNT(*) AS c FROM chunks')) {
    let count = 0;
    for (const r of db.chunks.values()) if (r.file_id === fileIdMatch[1]) count++;
    return [[{ c: count }]];
  }

  if ((m = n.match(/^UPDATE files SET status = '(\w+)' WHERE id = '([a-f0-9]+)'$/))) {
    const row = db.files.get(m[2]);
    if (row) row.status = m[1];
    return [{ affectedRows: row ? 1 : 0 }];
  }

  if ((m = n.match(/^UPDATE chunks SET status = '(\w+)' WHERE file_id = '([a-f0-9]+)'$/))) {
    for (const r of db.chunks.values()) if (r.file_id === m[2]) r.status = m[1];
    return [{ affectedRows: 1 }];
  }

  if ((m = n.match(/^UPDATE files SET status = 'completed', merged_hash = '([a-f0-9]+)', merged_path = '([^']+)' WHERE id = '([a-f0-9]+)'$/))) {
    const row = db.files.get(m[3]);
    if (row) {
      row.status = 'completed';
      row.merged_hash = m[1];
      row.merged_path = m[2];
    }
    return [{ affectedRows: row ? 1 : 0 }];
  }

  throw new Error(`mock-mysql 未实现的 SQL: ${n}`);
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
