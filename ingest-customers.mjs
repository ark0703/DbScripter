
// ingest-customers.mjs
// Stream customer data from CSV, JSON/NDJSON, XML, and simple SQL into Postgres.
// - Handles large files with batching
// - Column mapping: rename/drop/select/defaults
// - Missing columns become NULL; auto-creates new columns
// - Optional type inference (boolean/int/float/timestamp/jsonb; default TEXT)
// - Optional UPSERT via --unique-key (creates a unique index if missing)
// - XML requires --xml-record-tag (will try to guess if omitted, but explicit is better)
// - SQL: parses simple INSERT dumps; or --allow-raw-sql to execute file as-is (DANGEROUS)
//
// Usage examples:
// node ingest-customers.mjs ./data --table customers --unique-key email --infer-types --mapping mapping.json
// node ingest-customers.mjs ./xml --table customers --xml-record-tag customer
// node ingest-customers.mjs ./dumps/customers.sql --table customers --allow-raw-sql
//
// Connection:
//   - Prefer DSN: --dsn "postgres://user:pass@host:5432/db"
//   - Or env vars (with dotenv): PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
// SSL (server certificates):
//   --ssl-ca ca.crt --ssl-cert client.crt --ssl-key client.key --ssl-reject-unauthorized
//
// Note: Requires Node 18+ (for fs/promises and native streams).
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import fg from 'fast-glob';
import { parse as csvParse } from 'csv-parse';
import readline from 'readline';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { SaxesParser } from 'saxes';
import crypto from 'crypto';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SUPPORTED_EXTS = new Set(['.csv', '.json', '.jsonl', '.ndjson', '.xml', '.sql']);

function log(...args) {
  const ts = new Date().toISOString().replace('T', ' ').replace('Z', '');
  console.log(`[${ts}]`, ...args);
}

// ---------- Utility Functions ----------
// --- helpers: date/timestamp coercion (single, smart version) ---
function pad2(n) { n = Number(n); return (n < 10 ? '0' : '') + n; }
function isEpochLike(v) { if (v == null) return false; const s = String(v).trim(); return /^\d{10}$/.test(s) || /^\d{13}$/.test(s); }
function epochToMillis(v) { const s = String(v).trim(); return s.length === 13 ? Number(s) : Number(s) * 1000; }
function validYMD(y, m, d) {
  y = Number(y); m = Number(m); d = Number(d);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && (dt.getUTCMonth() + 1) === m && dt.getUTCDate() === d;
}
function normalizeToDateSmart(v) {
  if (v == null || v === '') return null;
  const raw = String(v).trim();
  if (/^(na|n\/a|null|undefined)$/i.test(raw)) return null;

  // epoch seconds/ms
  if (isEpochLike(raw)) {
    const d = new Date(epochToMillis(raw));
    return isNaN(d) ? null : d.toISOString().slice(0, 10);
  }

  const s = raw.replace(/\//g, '-');

  // YYYY-?-?
  let m = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(s);
  if (m) {
    let [, y, a, b] = m; a = Number(a); b = Number(b);
    // If middle > 12 but last <= 12, assume YYYY-DD-MM -> swap
    if (a > 12 && b <= 12 && validYMD(Number(y), b, a)) return `${y}-${pad2(b)}-${pad2(a)}`;
    // else assume YYYY-MM-DD
    return validYMD(Number(y), a, b) ? `${y}-${pad2(a)}-${pad2(b)}` : null;
  }

  // DD-MM-YYYY or MM-DD-YYYY
  m = /^(\d{1,2})-(\d{1,2})-(\d{4})$/.exec(s);
  if (m) {
    let [, d, mo, y] = m; d = Number(d); mo = Number(mo);
    if (d > 12 && validYMD(Number(y), mo, d)) return `${y}-${pad2(mo)}-${pad2(d)}`;      // definitely DD-MM-YYYY
    if (mo > 12 && validYMD(Number(y), d, mo)) return `${y}-${pad2(d)}-${pad2(mo)}`;      // MM-DD-YYYY
    return validYMD(Number(y), mo, d) ? `${y}-${pad2(mo)}-${pad2(d)}` : null;             // default DD-MM-YYYY
  }

  // ISO or other parseable formats
  const d = new Date(s);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
}
function normalizeToTimestampSmart(v) {
  if (v == null || v === '') return null;
  const raw = String(v).trim();
  if (isEpochLike(raw)) {
    const d = new Date(epochToMillis(raw));
    return isNaN(d) ? null : d.toISOString();
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return new Date(raw + 'T00:00:00Z').toISOString();
  const d = new Date(raw);
  return isNaN(d) ? null : d.toISOString();
}
function applyCoercions(row, mappingCfg) {
  const cfg = mappingCfg?.coerce || {};
  for (const [k, v] of Object.entries(row)) {
    const t = cfg[k];
    if (!t) continue;
    if (t === 'date') row[k] = normalizeToDateSmart(v);
    else if (t === 'timestamp') row[k] = normalizeToTimestampSmart(v);
  }
  return row;
}
// --- end helpers ---
// Sanitize identifier to be a valid SQL column name


function sanitizeIdentifier(name) {
  if (name == null) return 'col';
  let s = String(name).normalize('NFC').trim();
  s = s.replace(/[^0-9a-zA-Z_]/g, '_');
  s = s.replace(/([a-z0-9])([A-Z])/g, '$1_$2');
  s = s.replace(/_+/g, '_').toLowerCase().replace(/^_+|_+$/g, '');
  s = s.replace(/^[0-9]+/, '');
  if (!s) s = 'col';
  return s;
}

function inferType(value) {
  if (value === null || value === undefined) return 'text';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'number') {
    if (Number.isInteger(value)) return 'bigint';
    return 'double precision';
  }
  if (typeof value === 'object') return 'jsonb';
  if (typeof value === 'string') {
    const v = value.trim();
    if (/^(true|false|t|f|yes|no|y|n)$/i.test(v)) return 'boolean';
    if (/^[+-]?\d+$/.test(v)) return 'bigint';
    if (/^[+-]?\d+\.\d+$/.test(v)) return 'double precision';
    const fmts = [
      /^\d{4}-\d{2}-\d{2}$/,
      /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/,
    ];
    if (fmts.some(rx => rx.test(v))) return 'timestamp';
  }
  return 'text';
}

function chooseWiderType(t1, t2) {
  if (t1 === t2) return t1;
  if (t1 === 'text' || t2 === 'text') return 'text';
  if ((t1 === 'jsonb' && t2 !== 'jsonb') || (t2 === 'jsonb' && t1 !== 'jsonb')) return 'text';
  if ((t1 === 'timestamp' && ['boolean', 'bigint', 'double precision'].includes(t2)) ||
    (t2 === 'timestamp' && ['boolean', 'bigint', 'double precision'].includes(t1))) return 'text';
  const order = { boolean: 1, bigint: 2, 'double precision': 3, timestamp: 3, jsonb: 4, text: 5 };
  return (order[t1] || 99) >= (order[t2] || 99) ? t1 : t2;
}

async function discoverFiles(pathsInput) {
  const patterns = [];
  for (const p of pathsInput) {
    const stat = await fsp.stat(p).catch(() => null);
    if (stat && stat.isFile()) {
      patterns.push(p);
    } else if (stat && stat.isDirectory()) {
      patterns.push(path.join(p, '**/*'));
    } else {
      patterns.push(p);
    }
  }
  const entries = await fg(patterns, { onlyFiles: true, dot: false });
  return entries.filter(fp => SUPPORTED_EXTS.has(path.extname(fp).toLowerCase()));
}

// ---------- Streamers ----------

async function* streamCSV(file) {
  const parser = fs.createReadStream(file)
    .pipe(csvParse({ columns: true, skip_empty_lines: true, relax_column_count: true }));
  for await (const record of parser) {
    const obj = {};
    for (const [k, v] of Object.entries(record)) {
      obj[k] = (v === '' ? null : v);
    }
    yield obj;
  }
}

async function* streamNDJSON(file) {
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      if (obj && typeof obj === 'object' && !Array.isArray(obj)) yield obj;
      else yield { _raw: obj };
    } catch (e) {
      // ignore badly formed lines
    }
  }
}

async function* streamJSONArray(file) {
  const { parser } = await import('stream-json');
  const { streamArray } = await import('stream-json/streamers/StreamArray.js');
  const pipeline = fs.createReadStream(file).pipe(parser()).pipe(streamArray());
  for await (const { value } of pipeline) {
    if (value && typeof value === 'object' && !Array.isArray(value)) yield value;
    else yield { _raw: value };
  }
}

async function* streamJSON(file) {
  // Peek first non-space char
  const fd = await fsp.open(file, 'r');
  const { buffer } = await fd.read(Buffer.alloc(4096), 0, 4096, 0).catch(() => ({ buffer: Buffer.alloc(0) }));
  await fd.close();
  const head = buffer.toString('utf8').trimStart();
  if (head.startsWith('{')) {
    // Probably NDJSON (objects per line)
    yield* streamNDJSON(file);
  } else {
    // Try array streamer
    try {
      yield* streamJSONArray(file);
      return;
    } catch (e) {
      // Fallback: load all (last resort)
      const txt = await fsp.readFile(file, 'utf8');
      const data = JSON.parse(txt);
      if (Array.isArray(data)) {
        for (const v of data) {
          if (v && typeof v === 'object' && !Array.isArray(v)) yield v;
          else yield { _raw: v };
        }
      } else if (data && typeof data === 'object') {
        yield data;
      } else {
        yield { _raw: data };
      }
    }
  }
}

async function* streamXML(file, recordTag) {
  const tag = recordTag;
  const parser = new SaxesParser({ xmlns: false });
  let stack = [];
  let current = null;
  let textBuf = '';

  function pushText() {
    const t = textBuf.trim();
    if (!t) return;
    const top = stack[stack.length - 1];
    if (top) {
      const key = top._currentChild;
      if (key) {
        // If same child repeats, keep last occurrence; could extend to arrays if needed
        current[key] = (current[key] === undefined) ? t : t;
      }
    }
    textBuf = '';
  }

  const inStream = fs.createReadStream(file, { encoding: 'utf8' });

  parser.on('opentag', (node) => {
    stack.push({ name: node.name, attrs: node.attributes, _currentChild: null });
    if (node.name === tag) {
      current = {};
    } else if (current) {
      // Track which child text is for
      const parent = stack[stack.length - 2];
      if (parent && parent.name === tag) {
        stack[stack.length - 1]._currentChild = node.name;
      }
    }
  });

  parser.on('text', (t) => {
    if (current) textBuf += t;
  });

  parser.on('closetag', (name) => {
    if (name === tag) {
      pushText();
      // include attributes on record as _attrname
      const top = stack[stack.length - 1];
      if (top && top.attrs) {
        for (const [ak, av] of Object.entries(top.attrs)) {
          current[`_${ak}`] = String(av);
        }
      }
      // child attributes were ignored; extend if needed
      const row = current;
      current = null;
      textBuf = '';
      // emit
      parser.pause();
      setImmediate(() => parser.resume());
      emitter(row);
    } else if (current) {
      pushText();
    }
    stack.pop();
  });

  let emitterResolve;
  let emitterRow;
  function emitter(row) {
    emitterRow = row;
    if (emitterResolve) {
      const r = emitterResolve;
      emitterResolve = null;
      r();
    }
  }

  const iter = {
    next: () => new Promise((resolve) => {
      if (emitterRow !== undefined) {
        const val = emitterRow;
        emitterRow = undefined;
        resolve({ value: val, done: false });
      } else {
        emitterResolve = () => {
          const val = emitterRow;
          emitterRow = undefined;
          resolve({ value: val, done: false });
        };
      }
    })
  };

  inStream.on('data', chunk => parser.write(chunk));
  inStream.on('end', () => {
    if (emitterResolve) {
      const r = emitterResolve;
      emitterResolve = null;
      r({ done: true });
    }
  });
  inStream.on('error', err => {
    throw err;
  });

  // Provide async iterator facade
  while (true) {
    const { value, done } = await iter.next();
    if (done) break;
    yield value;
  }
}

function parseInsertValues(sqlText, targetTable) {
  // very naive parser for INSERT INTO <table> (cols) VALUES (...),(...);
  const cleaned = sqlText
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--.*?$/gm, ' ')
    .replace(/\s+/g, ' ');

  const rx = new RegExp(`insert\\s+into\\s+${targetTable}\\s*\\((.*?)\\)\\s*values\\s*(.*?);`, 'ig');
  const results = [];
  let m;
  while ((m = rx.exec(cleaned)) !== null) {
    const cols = m[1].split(',').map(s => s.trim().replace(/^"(.*)"$/, '$1'));
    const valsStr = m[2];
    // split tuples by parentheses depth
    const tuples = [];
    let depth = 0, start = 0;
    for (let i = 0; i < valsStr.length; i++) {
      const ch = valsStr[i];
      if (ch === '(') {
        if (depth === 0) start = i + 1;
        depth++;
      } else if (ch === ')') {
        depth--;
        if (depth === 0) tuples.push(valsStr.slice(start, i));
      }
    }
    for (const tup of tuples) {
      // split by commas ignoring quotes
      const parts = [];
      let buf = '', inStr = false, quote = null, esc = false;
      for (const ch of tup) {
        if (inStr) {
          if (esc) { buf += ch; esc = false; continue; }
          if (ch === '\\') { esc = true; continue; }
          if (ch === quote) { inStr = false; buf += ch; continue; }
          buf += ch;
        } else {
          if (ch === "'" || ch === '"') { inStr = true; quote = ch; buf += ch; }
          else if (ch === ',') { parts.push(buf.trim()); buf = ''; }
          else { buf += ch; }
        }
      }
      parts.push(buf.trim());
      const row = {};
      for (let i = 0; i < cols.length; i++) {
        const p = parts[i] ?? null;
        if (p === null) { row[cols[i]] = null; continue; }
        if (/^null$/i.test(p)) { row[cols[i]] = null; continue; }
        if ((p.startsWith("'") && p.endsWith("'")) || (p.startsWith('"') && p.endsWith('"'))) {
          let v = p.slice(1, -1).replace(/\\'/g, "'").replace(/\\"/g, '"');
          row[cols[i]] = v;
          continue;
        }
        if (/^[+-]?\d+\.\d+$/.test(p)) { row[cols[i]] = parseFloat(p); continue; }
        if (/^[+-]?\d+$/.test(p)) { row[cols[i]] = parseInt(p, 10); continue; }
        row[cols[i]] = p;
      }
      results.push(row);
    }
  }
  return results;
}

async function* streamSQL(file, targetTable, allowRawSQL, client) {
  if (allowRawSQL) {
    const sql = await fsp.readFile(file, 'utf8');
    await client.query(sql);
    log(`Executed raw SQL file: ${file}`);
    return;
  }
  const sql = await fsp.readFile(file, 'utf8');
  const rows = parseInsertValues(sql, targetTable);
  for (const r of rows) yield r;
}

function applyMapping(row, mappingCfg) {
  if (!mappingCfg) return row;
  const out = {};
  const rename = mappingCfg.rename || {};
  const drop = new Set((mappingCfg.drop || []).map(sanitizeIdentifier));
  const whitelist = mappingCfg.select ? new Set(mappingCfg.select.map(sanitizeIdentifier)) : null;

  // Sanitize input keys first
  const sanitizedEntries = Object.entries(row).map(([k, v]) => [sanitizeIdentifier(k), v]);
  for (const [k, v] of sanitizedEntries) {
    const newKey = rename[k] ? sanitizeIdentifier(rename[k]) : k;
    if (drop.has(newKey)) continue;
    if (whitelist && !whitelist.has(newKey)) continue;
    if (!(newKey in out)) {
      out[newKey] = v;
    } else if ((out[newKey] == null || out[newKey] === '') && v != null && v !== '') {
      out[newKey] = v;
    }
  }
  // defaults
  if (mappingCfg.defaults) {
    for (const [k, v] of Object.entries(mappingCfg.defaults)) {
      const sk = sanitizeIdentifier(k);
      if (out[sk] === undefined || out[sk] === null) out[sk] = v;
    }
  }
  return out;
}

async function getExistingColumns(client, table) {
  const { rows } = await client.query(
    `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1`,
    [table]
  );
  const map = {};
  for (const r of rows) map[r.column_name] = r.data_type;
  return map;
}

async function ensureTable(client, table, columnsTypes) {
  const cols = Object.entries(columnsTypes);
  if (cols.length === 0) {
    // create minimal table
    await client.query(`CREATE TABLE IF NOT EXISTS ${quoteIdent(table)} (_ingest_stub text)`);
  } else {
    const defs = cols.map(([c, t]) => `${quoteIdent(c)} ${t}`).join(', ');
    await client.query(`CREATE TABLE IF NOT EXISTS ${quoteIdent(table)} (${defs})`);
  }
}

async function addMissingColumns(client, table, needed) {
  for (const [c, t] of Object.entries(needed)) {
    await client.query(`ALTER TABLE ${quoteIdent(table)} ADD COLUMN IF NOT EXISTS ${quoteIdent(c)} ${t}`);
  }
}

function quoteIdent(ident) {
  // very basic identifier quoting
  return '"' + ident.replace(/"/g, '""') + '"';
}

function buildInsertQuery(table, columns, rows, uniqueKeys) {
  // columns is an array of sanitized column names
  const valuePlaceholders = [];
  const values = [];
  let param = 1;
  for (const r of rows) {
    const rowVals = [];
    for (const c of columns) {
      rowVals.push(`$${param++}`);
      values.push(r[c] === undefined ? null : r[c]);
    }
    valuePlaceholders.push(`(${rowVals.join(',')})`);
  }
  const colsSQL = columns.map(quoteIdent).join(',');
  let text = `INSERT INTO ${quoteIdent(table)} (${colsSQL}) VALUES ${valuePlaceholders.join(',')}`;
  if (uniqueKeys && uniqueKeys.length > 0) {
    const nonKeys = columns.filter(c => !uniqueKeys.includes(c));
    if (nonKeys.length > 0) {
      const updates = nonKeys.map(c => `${quoteIdent(c)} = EXCLUDED.${quoteIdent(c)}`).join(', ');
      text += ` ON CONFLICT (${uniqueKeys.map(quoteIdent).join(',')}) DO UPDATE SET ${updates}`;
    } else {
      text += ` ON CONFLICT (${uniqueKeys.map(quoteIdent).join(',')}) DO NOTHING`;
    }
  }
  return { text, values };
}

async function ensureUniqueIndex(client, table, uniqueKeys) {
  if (!uniqueKeys || uniqueKeys.length === 0) return;
  // deterministic index name from keys
  const idxHash = crypto.createHash('sha1').update(uniqueKeys.join(',')).digest('hex').slice(0, 8);
  const idxName = `${table}_${idxHash}_ukey`;
  await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(idxName)} ON ${quoteIdent(table)} (${uniqueKeys.map(quoteIdent).join(',')})`);
}

function coerceForDB(value, targetType) {
  if (value === undefined) return null;
  if (value === null) return null;
  if (targetType === 'jsonb') {
    if (typeof value === 'string') {
      try { JSON.parse(value); return value; } catch { return JSON.stringify(value); }
    }
    return JSON.stringify(value);
  }
  // For date/time types, set invalid values to null
  if (['date', 'timestamp', 'timestamptz', 'time', 'timetz'].includes(targetType)) {
    if (typeof value === 'string') {
      // Basic ISO8601 check and range filter
      const isValid = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/.test(value);
      // Reject absurd years or malformed
      const year = parseInt(value.slice(0, 4), 10);
      if (!isValid || isNaN(year) || year < 1900 || year > 2100) return null;
    }
  }
  return value;
}

async function main() {
  const argv = yargs(hideBin(process.argv))
    .scriptName('ingest-customers')
    .usage('$0 <paths..> [options]')
    .positional('paths', { describe: 'Files or directories to ingest', type: 'string' })
    .option('dsn', { type: 'string', desc: 'Postgres DSN' })
    .option('table', { type: 'string', default: 'customers', desc: 'Target table name' })
    .option('unique-key', { type: 'string', desc: 'Comma-separated unique key columns for UPSERT' })
    .option('batch-size', { type: 'number', default: 1000, desc: 'Insert batch size' })
    .option('infer-types', { type: 'boolean', default: false, desc: 'Infer column types (default TEXT)' })
    .option('xml-record-tag', { type: 'string', desc: 'XML record tag name (e.g., customer)' })
    .option('allow-raw-sql', { type: 'boolean', default: false, desc: 'Execute .sql files as-is (DANGEROUS)' })
    .option('mapping', { type: 'string', desc: 'Path to mapping JSON' })
    .option('ssl-ca', { type: 'string', desc: 'Path to CA certificate' })
    .option('ssl-cert', { type: 'string', desc: 'Path to client certificate' })
    .option('ssl-key', { type: 'string', desc: 'Path to client private key' })
    .option('ssl-reject-unauthorized', { type: 'boolean', default: false, desc: 'Reject unauthorized SSL certs' })
    .option('dry-run', { type: 'boolean', default: false, desc: 'Analyze only; do not write to DB' })
    .help()
    .argv;

  const pathsInput = argv._.map(String).concat(argv.paths || []);
  if (!pathsInput.length) {
    console.error('Provide at least one file or directory');
    process.exit(1);
  }

  const files = await discoverFiles(pathsInput);
  if (files.length === 0) {
    log('No supported input files found.');
    process.exit(1);
  }
  log(`Discovered ${files.length} file(s).`);

  // Load mapping file if provided
  let mappingCfg = null;
  if (argv.mapping) {
    const txt = await fsp.readFile(argv.mapping, 'utf8');
    mappingCfg = JSON.parse(txt);
  }

  // Connection
  const ssl = (argv.sslCa || argv.sslCert || argv.sslKey)
    ? {
      ca: argv.sslCa ? await fsp.readFile(argv.sslCa, 'utf8') : undefined,
      cert: argv.sslCert ? await fsp.readFile(argv.sslCert, 'utf8') : undefined,
      key: argv.sslKey ? await fsp.readFile(argv.sslKey, 'utf8') : undefined,
      rejectUnauthorized: argv.sslRejectUnauthorized ?? false,
    }
    : undefined;

  const client = new Client(Object.assign(
    argv.dsn ? { connectionString: argv.dsn } : {
      host: process.env.PGHOST || 'localhost',
      port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'postgres',
      database: process.env.PGDATABASE || 'postgres',
    },
    ssl ? { ssl } : {}
  ));

  await client.connect();

  const table = sanitizeIdentifier(argv.table);
  const uniqueKeys = argv['unique-key'] ? argv['unique-key'].split(',').map(s => sanitizeIdentifier(s.trim())).filter(Boolean) : null;
  const batchSize = argv['batch-size'] || 1000;
  const inferTypes = !!argv['infer-types'];
  const xmlRecordTag = argv['xml-record-tag'] || null;
  const allowRawSQL = !!argv['allow-raw-sql'];
  const dryRun = !!argv['dry-run'];

  // First pass: optionally infer types over a sample
  let inferredTypes = {}; // { col: type }
  let sampleCount = 0;
  if (inferTypes) {
    log('Scanning sample to infer types...');
    for (const file of files) {
      const ext = path.extname(file).toLowerCase();
      const streamer = (ext === '.csv') ? streamCSV(file)
        : (ext === '.xml') ? streamXML(file, xmlRecordTag || 'customer')
          : (ext === '.sql') ? streamSQL(file, table, false, client)
            : streamJSON(file);
      let i = 0;
      for await (const rawRow of streamer) {
        const mapped = applyMapping(rawRow, mappingCfg);
        for (const [k, v] of Object.entries(mapped)) {
          const sk = sanitizeIdentifier(k);
          const t = inferType(v);
          inferredTypes[sk] = inferredTypes[sk] ? chooseWiderType(inferredTypes[sk], t) : t;
        }
        i++; sampleCount++;
        if (sampleCount >= 5000) break;
      }
      if (sampleCount >= 5000) break;
    }
    log(`Inference complete on ~${sampleCount} rows; found ${Object.keys(inferredTypes).length} columns.`);
  } else {
    log('Type inference disabled; defaulting to TEXT for new columns.');
  }

  try {
    if (!dryRun) {
      await ensureTable(client, table, inferredTypes);
      if (uniqueKeys) await ensureUniqueIndex(client, table, uniqueKeys);
    } else {
      log('[DRY RUN] Would create/alter table with columns:');
      for (const [c, t] of Object.entries(inferredTypes)) log(`  - ${c}: ${t}`);
    }

    let existing = dryRun ? {} : await getExistingColumns(client, table);
  let total = 0;
  let batch = [];
  let totalRows = 0;
  let skippedRows = 0;
  const duplicatesFile = path.resolve('duplicates.jsonl');
  const seenRows = new Map();
  const duplicateRows = [];

    async function flushBatch() {
      if (batch.length === 0) return;
      // union of keys
      const columnsSet = new Set();
      for (const r of batch) for (const k of Object.keys(r)) columnsSet.add(k);
      const columns = Array.from(columnsSet).sort();

      // ensure columns exist with types
      const needed = {};
      for (const c of columns) {
        const t = inferredTypes[c] || 'text';
        if (!existing[c] && !dryRun) needed[c] = t;
      }
      if (Object.keys(needed).length && !dryRun) {
        await addMissingColumns(client, table, needed);
        existing = { ...existing, ...needed };
      }

      // Deduplicate batch by unique key(s) if present
      let dedupedBatch = batch;
      if (uniqueKeys && uniqueKeys.length > 0) {
        const keyFn = r => uniqueKeys.map(k => (r[k] !== undefined && r[k] !== null) ? String(r[k]).trim() : '').join('||');
        const seen = new Map();
        const firebaseSeen = new Set();
        for (const r of batch) {
          const key = keyFn(r);
          // Track duplicates
          if (key) {
            if (seen.has(key)) {
              duplicateRows.push(r);
            }
            seen.set(key, r); // last occurrence wins
          }
        }
        // If some rows have missing/empty unique key, keep them but don't deduplicate
        let deduped = Array.from(seen.values()).concat(batch.filter(r => !keyFn(r)));
        // Remove rows with duplicate firebase_uid
        deduped = deduped.filter(r => {
          const firebase = r.firebase_uid || r.firebase || '';
          if (!firebase) return true;
          if (firebaseSeen.has(firebase)) return false;
          firebaseSeen.add(firebase);
          return true;
        });
        dedupedBatch = deduped;
      }

      // coerce jsonb, etc.
      // Columns to treat as date/time from mapping config
      const dateTimeColumns = new Set([
        ...(mappingCfg?.coerce ? Object.keys(mappingCfg.coerce).filter(k => ['date', 'timestamp', 'timestamptz', 'time', 'timetz'].includes(mappingCfg.coerce[k])) : []),
        ...(mappingCfg?.select ? mappingCfg.select.filter(k => /date|time|timestamp/i.test(k)) : [])
      ]);

      const coerced = dedupedBatch.map(r => {
        const o = {};
        for (const c of columns) {
          const t = inferredTypes[c] || 'text';
          let v = r[c];
          // If type, column name, or mapping config suggests date/time, set invalid values to null
          const isDateTimeType = ['date', 'timestamp', 'timestamptz', 'time', 'timetz'].includes(t);
          const isDateTimeName = /date|time|timestamp/i.test(c);
          const isDateTimeMapped = dateTimeColumns.has(c);
          if ((isDateTimeType || isDateTimeName || isDateTimeMapped) && typeof v === 'string') {
            const isValid = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/.test(v);
            const year = parseInt(v.slice(0, 4), 10);
            if (!isValid || isNaN(year) || year < 1900 || year > 2100) v = null;
          }
          o[c] = coerceForDB(v, t);
        }
        return o;
      });

  const failedRowsFile = path.resolve('failed_rows.jsonl');
  const notInsertedFile = path.resolve('not_inserted_rows.jsonl');
      let inserted = true;
      if (!dryRun) {
        const { text, values } = buildInsertQuery(table, columns, coerced, uniqueKeys);
        try {
          await client.query(text, values);
        } catch (err) {
          inserted = false;
          // Log error and continue execution
          for (const row of dedupedBatch) {
            const email = row.email || row.Email || '';
            const firebase = row.firebase_uid || row.firebase || '';
            log(`[ERROR ROW] email: ${email}, firebase_uid: ${firebase}`);
            // Store failed constraint rows
            fs.appendFileSync(failedRowsFile, JSON.stringify({ ...row, error: err.message }) + '\n');
          }
          log(`[ERROR] Skipping batch due to DB error: ${err.message}`);
          // Do not throw, just skip this batch
        }
      }
      // Store all not-inserted rows
      if (!inserted) {
        for (const row of dedupedBatch) {
          fs.appendFileSync(notInsertedFile, JSON.stringify(row) + '\n');
        }
      }
      total += dedupedBatch.length;
      batch = [];
      log(`Inserted ${total} rows so far...`);
    }

    // Second pass: ingest data
    for (const file of files) {
      log(`Processing: ${file}`);
      const ext = path.extname(file).toLowerCase();
      const streamer = (ext === '.csv') ? streamCSV(file)
        : (ext === '.xml') ? streamXML(file, xmlRecordTag || 'customer')
          : (ext === '.sql') ? streamSQL(file, table, allowRawSQL, client)
            : streamJSON(file);

      for await (const rawRow of streamer) {
        let row = applyMapping(rawRow, mappingCfg);
        // sanitize keys
        const sanitized = {};
        for (const [k, v] of Object.entries(row)) {
          const sk = sanitizeIdentifier(k);
          sanitized[sk] = v;
          if (inferTypes) {
            const t = inferType(v);
            inferredTypes[sk] = inferredTypes[sk] ? chooseWiderType(inferredTypes[sk], t) : t;
          } else {
            inferredTypes[sk] = inferredTypes[sk] || 'text';
          }
        }
        const badBlob = JSON.stringify(sanitized).toLowerCase();
        if (badBlob.includes('<html') || badBlob.includes('<!doctype') || badBlob.includes('404 not found')) {
          skippedRows++;
          continue; // skip HTML error chunks accidentally in the CSV
        }

        // Count all rows processed
        totalRows++;

        // Enforce required columns for your table
        if (!sanitized.email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(sanitized.email))) {
          skippedRows++;
          continue;
        }
        if (!sanitized.first_name || !sanitized.last_name) {
          skippedRows++;
          continue;
        }
        // after building `sanitized` object
        if (mappingCfg) applyCoercions(sanitized, mappingCfg);

        batch.push(sanitized);
        if (batch.length >= batchSize) await flushBatch();
      }
    }
    await flushBatch();
    // Write duplicate rows to file
    if (duplicateRows.length > 0) {
      for (const row of duplicateRows) {
        fs.appendFileSync(duplicatesFile, JSON.stringify(row) + '\n');
      }
    }
    log(`Total rows in file: ${totalRows}`);
    log(`Skipped rows (validation failed): ${skippedRows}`);
    log(`Done. Rows ingested: ${total}`);
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error('ERROR:', err);
  process.exit(1);
});
