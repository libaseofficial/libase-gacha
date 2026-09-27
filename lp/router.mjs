import express from 'express';
import { loadConfig, createPgStore, makeShopify, createOfferService } from './offers.mjs';

export function createLpRouter({ pool, shop, getAccessToken, env = process.env, service: suppliedService }) {
  let config, service;
  try {
    config = loadConfig(env);
    service = suppliedService || createOfferService(config, { store: createPgStore(pool), shopify: makeShopify({ shop, getAccessToken }) });
  } catch (error) {
    console.error(`LP configuration: ${error.message}`);
    config = { enabled: false, origin: 'https://libase.shop' };
  }
  const router = express.Router();
  router.use((req, res, next) => {
    res.set({ 'Cache-Control': 'no-store', 'Vary': 'Origin', 'X-Content-Type-Options': 'nosniff' });
    if (req.method === 'GET' && req.path === '/health') return res.json({ enabled: config.enabled, configured: Boolean(service) });
    if (req.headers.origin !== config.origin) return res.status(403).json({ error: 'Origin not allowed.' });
    res.set({ 'Access-Control-Allow-Origin': config.origin, 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' });
    if (!['/offer', '/status'].includes(req.path)) return res.status(404).json({ error: 'Not found.' });
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });
    if (!req.is('application/json')) return res.status(415).json({ error: 'JSON required.' });
    if (!service) return res.status(503).json({ error: 'LP is not configured.' });
    next();
  });
  router.use(express.json({ limit: '2kb', strict: true }));
  for (const route of ['offer', 'status']) router.post(`/${route}`, async (req, res) => {
    try { res.json(await service[route](req.body)); }
    catch (error) {
      if (!error.status || error.status >= 500) console.error(`LP ${route}: ${error.status ? error.message : 'Database or upstream request failed.'}`);
      res.status(error.status || 503).json({ error: 'Offer unavailable. Please try again later.' });
    }
  });
  router.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    res.status(error.status === 413 ? 413 : 400).json({ error: 'Invalid request.' });
  });
  return { router, markUsed: discounts => service?.markUsed(discounts) || Promise.resolve() };
}
