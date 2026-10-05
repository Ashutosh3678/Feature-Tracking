'use strict';

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');

const ENV_FILE = path.join(__dirname, '.env');
if (fs.existsSync(ENV_FILE) && typeof process.loadEnvFile === 'function') {
  try { process.loadEnvFile(ENV_FILE); } catch (err) { console.warn('Could not read .env: ' + err.message); }
}

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const MONGODB_URI = (process.env.MONGODB_URI || '').trim();
const MONGODB_DB = (process.env.MONGODB_DB || 'feature_tracker').trim();
const COLLECTION = 'features';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const INDEX_FILE = path.join(__dirname, 'index.html');

const { ID_RE, MAX_DOCS, sanitize } = require('./lib/shared');

const MAX_BODY = 1024 * 1024;
const POLL_MS = 5000;

// In-memory cache of the "features" collection, keyed by feature id (f1, f2, ...).
let docs = {};
let storage = null;
const clients = new Set();

/* ---------- storage: MongoDB ---------- */
async function createMongoStorage() {
  let MongoClient;
  try {
    ({ MongoClient } = require('mongodb'));
  } catch (err) {
    throw new Error('The "mongodb" package is not installed. Run "npm install" first.');
  }
  const client = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 10000 });
  await client.connect();
  const col = client.db(MONGODB_DB).collection(COLLECTION);

  async function loadAll() {
    const out = {};
    for await (const d of col.find({})) {
      if (typeof d._id !== 'string' || !ID_RE.test(d._id)) continue;
      const { _id, ...rest } = d;
      out[_id] = rest;
    }
    return out;
  }

  return {
    label: 'MongoDB (' + MONGODB_DB + '.' + COLLECTION + ')',
    load: loadAll,
    put: (id, doc) => col.replaceOne({ _id: id }, doc, { upsert: true }),
    remove: (id) => col.deleteOne({ _id: id }),
    // Picks up writes made by other server instances or directly in the database.
    watch(onChange) {
      let polling = false;
      const refresh = async () => {
        try {
          const all = await loadAll();
          if (JSON.stringify(all) !== JSON.stringify(docs)) onChange(all);
        } catch (err) {
          console.error('MongoDB refresh failed: ' + err.message);
        }
      };
      const startPolling = (reason) => {
        if (polling) return;
        polling = true;
        console.log('MongoDB change streams unavailable (' + reason + '); polling every ' + POLL_MS / 1000 + 's instead.');
        setInterval(refresh, POLL_MS).unref();
      };
      try {
        const stream = col.watch([], { fullDocument: 'updateLookup' });
        stream.on('change', refresh);
        stream.on('error', (err) => {
          stream.close().catch(() => {});
          startPolling(err.codeName || err.message);
        });
      } catch (err) {
        startPolling(err.message);
      }
    },
    close: () => client.close()
  };
}

/* ---------- storage: local JSON file (used when MONGODB_URI is not set) ---------- */
function createFileStorage() {
  const file = path.join(DATA_DIR, 'features.json');
  let chain = Promise.resolve();

  function persist() {
    const body = JSON.stringify({ features: docs }, null, 2);
    const tmp = file + '.tmp';
    chain = chain.catch(() => {}).then(async () => {
      await fs.promises.mkdir(DATA_DIR, { recursive: true });
      await fs.promises.writeFile(tmp, body);
      try {
        await fs.promises.rename(tmp, file);
      } catch (err) {
        // Synced folders (OneDrive, Dropbox) sometimes lock the target file.
        await fs.promises.writeFile(file, body);
        fs.promises.unlink(tmp).catch(() => {});
      }
    });
    return chain;
  }

  return {
    label: 'local file (' + file + ')',
    async load() {
      try {
        const src = JSON.parse(await fs.promises.readFile(file, 'utf8')).features || {};
        const out = {};
        for (const id of Object.keys(src)) {
          if (ID_RE.test(id) && src[id] && typeof src[id] === 'object' && !Array.isArray(src[id])) out[id] = src[id];
        }
        return out;
      } catch (err) {
        if (err.code !== 'ENOENT') console.error('Could not read ' + file + ': ' + err.message);
        return {};
      }
    },
    put: () => persist(),
    remove: () => persist(),
    close: () => chain.catch(() => {})
  };
}

/* ---------- live updates (Server-Sent Events) ---------- */
function snapshotEvent() {
  return 'event: snapshot\ndata: ' + JSON.stringify({ docs }) + '\n\n';
}
function broadcast() {
  const msg = snapshotEvent();
  for (const res of clients) {
    try { res.write(msg); } catch (err) { clients.delete(res); }
  }
}
setInterval(() => {
  for (const res of clients) {
    try { res.write(': ping\n\n'); } catch (err) { clients.delete(res); }
  }
}, 25000).unref();

/* ---------- HTTP ---------- */
function setSecurityHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('Content-Security-Policy',
    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; " +
    "img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'");
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooBig = false;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY) tooBig = true;
      else chunks.push(chunk);
    });
    req.on('end', () => {
      if (tooBig) return reject(Object.assign(new Error('Payload too large'), { status: 413 }));
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || 'null'));
      } catch (err) {
        reject(Object.assign(new Error('Invalid JSON'), { status: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function serveIndex(res) {
  fs.readFile(INDEX_FILE, (err, buf) => {
    if (err) return sendJson(res, 500, { error: 'index.html not found next to server.js' });
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
}

async function handleDoc(req, res, id) {
  if (req.method === 'GET') {
    return docs[id] ? sendJson(res, 200, { id, doc: docs[id] }) : sendJson(res, 404, { error: 'Not found' });
  }
  if (req.method === 'PUT') {
    const body = await readJson(req);
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return sendJson(res, 400, { error: 'Body must be a JSON object' });
    }
    if (!docs[id] && Object.keys(docs).length >= MAX_DOCS) {
      return sendJson(res, 409, { error: 'Too many features' });
    }
    const doc = Object.assign(sanitize(body, 0), { updatedAt: Date.now() });
    docs = Object.assign({}, docs, { [id]: doc });
    broadcast();
    await storage.put(id, doc);
    return sendJson(res, 200, { ok: true });
  }
  if (req.method === 'DELETE') {
    if (docs[id]) {
      docs = Object.assign({}, docs);
      delete docs[id];
      broadcast();
      await storage.remove(id);
    }
    return sendJson(res, 200, { ok: true });
  }
  res.setHeader('Allow', 'GET, PUT, DELETE');
  return sendJson(res, 405, { error: 'Method not allowed' });
}

const server = http.createServer(async (req, res) => {
  setSecurityHeaders(res);
  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch (err) {
    return sendJson(res, 400, { error: 'Bad URL' });
  }

  try {
    if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) return serveIndex(res);

    if (pathname === '/api/health') {
      return sendJson(res, 200, { ok: true, storage: storage.label, features: Object.keys(docs).length });
    }

    if (pathname === '/api/features') {
      if (req.method !== 'GET') return sendJson(res, 405, { error: 'Method not allowed' });
      return sendJson(res, 200, { docs, stream: true });
    }

    if (pathname === '/api/stream' && req.method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no'
      });
      res.write('retry: 3000\n\n');
      res.write(snapshotEvent());
      clients.add(res);
      req.on('close', () => clients.delete(res));
      return;
    }

    const m = pathname.match(/^\/api\/features\/([^/]+)$/);
    if (m) {
      let id;
      try { id = decodeURIComponent(m[1]); } catch (err) { id = ''; }
      if (!ID_RE.test(id)) return sendJson(res, 400, { error: 'Invalid feature id' });
      return await handleDoc(req, res, id);
    }

    return sendJson(res, 404, { error: 'Not found' });
  } catch (err) {
    if (!err.status) console.error(err);
    if (!res.headersSent) sendJson(res, err.status || 500, { error: err.status ? err.message : 'Could not save. Please retry.' });
  }
});

async function main() {
  storage = MONGODB_URI ? await createMongoStorage() : createFileStorage();
  docs = await storage.load();
  if (storage.watch) {
    storage.watch((all) => { docs = all; broadcast(); });
  }
  server.listen(PORT, HOST, () => {
    console.log('Feature Tracker running:');
    console.log('  Local:   http://localhost:' + PORT);
    for (const list of Object.values(os.networkInterfaces())) {
      for (const ni of list || []) {
        if (ni.family === 'IPv4' && !ni.internal) console.log('  Network: http://' + ni.address + ':' + PORT);
      }
    }
    console.log('  Storage: ' + storage.label + (MONGODB_URI ? '' : '  (set MONGODB_URI in .env to use MongoDB)'));
  });
}

function shutdown() {
  for (const res of clients) { try { res.end(); } catch (err) { /* ignore */ } }
  server.close(() => {
    Promise.resolve(storage && storage.close()).catch(() => {}).then(() => process.exit(0));
  });
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  console.error('Failed to start: ' + err.message);
  if (MONGODB_URI) console.error('Check MONGODB_URI in .env and that the database is reachable.');
  process.exit(1);
});
