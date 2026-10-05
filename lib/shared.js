'use strict';

const ID_RE = /^f[1-9]\d{0,3}$/;
const MAX_DOCS = 1000;
const COLLECTION = 'features';

// Drops operator-like keys ($..., dotted, _id, __proto__) so request bodies are safe to store in MongoDB.
function sanitize(value, depth) {
  if (depth > 8) return undefined;
  if (Array.isArray(value)) {
    return value.slice(0, 5000).map((v) => sanitize(v, depth + 1)).filter((v) => v !== undefined);
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value)) {
      if (key.startsWith('$') || key.includes('.') || key === '_id' || key === '__proto__') continue;
      const v = sanitize(value[key], depth + 1);
      if (v !== undefined) out[key] = v;
    }
    return out;
  }
  if (typeof value === 'string') return value.slice(0, 5000);
  if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
  if (typeof value === 'boolean' || value === null) return value;
  return undefined;
}

// One client per process; serverless instances reuse it across invocations.
let clientPromise = null;
function getClient() {
  const uri = (process.env.MONGODB_URI || '').trim();
  if (!uri) return Promise.reject(new Error('MONGODB_URI is not set'));
  if (!clientPromise) {
    const { MongoClient } = require('mongodb');
    clientPromise = new MongoClient(uri, { serverSelectionTimeoutMS: 10000, maxPoolSize: 5 })
      .connect()
      .catch((err) => { clientPromise = null; throw err; });
  }
  return clientPromise;
}
async function getCollection() {
  const client = await getClient();
  return client.db((process.env.MONGODB_DB || 'feature_tracker').trim()).collection(COLLECTION);
}

async function loadAll(col) {
  const out = {};
  for await (const d of col.find({})) {
    if (typeof d._id !== 'string' || !ID_RE.test(d._id)) continue;
    const { _id, ...rest } = d;
    out[_id] = rest;
  }
  return out;
}

module.exports = { ID_RE, MAX_DOCS, COLLECTION, sanitize, getClient, getCollection, loadAll };
