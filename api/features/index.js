'use strict';

const { getCollection, loadAll } = require('../../lib/shared');

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method not allowed' });
  }
  try {
    const docs = await loadAll(await getCollection());
    return res.status(200).json({ docs });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Database unavailable' });
  }
};
