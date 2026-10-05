'use strict';

const { getCollection } = require('../lib/shared');

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const col = await getCollection();
    const features = await col.estimatedDocumentCount();
    return res.status(200).json({ ok: true, storage: 'MongoDB (' + col.dbName + '.' + col.collectionName + ')', features });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message === 'MONGODB_URI is not set' ? err.message : 'Database unavailable' });
  }
};
