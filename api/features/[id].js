'use strict';

const { ID_RE, MAX_DOCS, sanitize, getCollection } = require('../../lib/shared');

function parseBody(body) {
  if (typeof body === 'string') {
    try { return JSON.parse(body); } catch (err) { return null; }
  }
  return body;
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const id = String((req.query && req.query.id) || '');
  if (!ID_RE.test(id)) return res.status(400).json({ error: 'Invalid feature id' });

  try {
    const col = await getCollection();

    if (req.method === 'GET') {
      const d = await col.findOne({ _id: id });
      if (!d) return res.status(404).json({ error: 'Not found' });
      const { _id, ...doc } = d;
      return res.status(200).json({ id, doc });
    }

    if (req.method === 'PUT') {
      const body = parseBody(req.body);
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return res.status(400).json({ error: 'Body must be a JSON object' });
      }
      const exists = await col.countDocuments({ _id: id }, { limit: 1 });
      if (!exists && (await col.estimatedDocumentCount()) >= MAX_DOCS) {
        return res.status(409).json({ error: 'Too many features' });
      }
      const doc = Object.assign(sanitize(body, 0), { updatedAt: Date.now() });
      await col.replaceOne({ _id: id }, doc, { upsert: true });
      return res.status(200).json({ ok: true });
    }

    if (req.method === 'DELETE') {
      await col.deleteOne({ _id: id });
      return res.status(200).json({ ok: true });
    }

    res.setHeader('Allow', 'GET, PUT, DELETE');
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Could not save. Please retry.' });
  }
};
