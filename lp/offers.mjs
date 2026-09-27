import { createHmac, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { CREATE_DISCOUNT, FIND_DISCOUNT } from './operations.mjs';

export const SCHEMA = readFileSync(new URL('./schema.sql', import.meta.url), 'utf8');
export const endOfJstDay = now => (Math.floor((now + 32400000) / 86400000) + 1) * 86400000 - 32400000;

export class OfferError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

export function loadConfig(env = process.env) {
  const config = {
    enabled: env.LP_ENABLED === 'true',
    campaign: env.LP_CAMPAIGN_ID || 'libase-swipe-2026',
    origin: env.LP_STOREFRONT_ORIGIN || 'https://libase.shop',
    secret: env.LP_VISITOR_SECRET || '',
    dailyLimit: Number(env.LP_MAX_NEW_OFFERS_PER_DAY || 1000)
  };
  if (config.enabled) {
    if (config.secret.length < 32) throw new Error('LP_VISITOR_SECRET must be at least 32 random characters.');
    if (!/^[a-zA-Z0-9_-]{1,80}$/.test(config.campaign)) throw new Error('Invalid LP_CAMPAIGN_ID.');
    const url = new URL(config.origin);
    if (url.protocol !== 'https:' || url.origin !== config.origin) throw new Error('LP_STOREFRONT_ORIGIN must be an HTTPS origin.');
    if (!Number.isInteger(config.dailyLimit) || config.dailyLimit < 1) throw new Error('Invalid LP_MAX_NEW_OFFERS_PER_DAY.');
  }
  return config;
}

export function makeShopify({ shop, getAccessToken, fetcher = fetch }) {
  async function graphql(query, variables) {
    const accessToken = await getAccessToken();
    if (!accessToken) throw new OfferError(503, 'Shopify access token unavailable.');
    const response = await fetcher(`https://${shop}/admin/api/2026-07/graphql.json`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': accessToken },
      body: JSON.stringify({ query, variables }),
      signal: AbortSignal.timeout(8000)
    });
    if (!response.ok) throw new OfferError(503, `Shopify HTTP ${response.status}.`);
    const result = await response.json();
    if (result.errors?.length || !result.data) throw new OfferError(503, 'Shopify API error; check read_discounts/write_discounts permissions.');
    return result.data;
  }
  return {
    async find(code) { return (await graphql(FIND_DISCOUNT, { code })).codeDiscountNodeByCode?.codeDiscount || null; },
    async create(record) {
      const input = {
        title: 'LIBASE LP限定・本日10%OFF',
        code: record.code, context: { all: 'ALL' },
        startsAt: new Date(Number(record.created_at)).toISOString(),
        endsAt: new Date(Number(record.ends_at)).toISOString(),
        usageLimit: 1, appliesOncePerCustomer: true,
        customerGets: { value: { percentage: 0.1 }, items: { all: true }, appliesOnOneTimePurchase: true, appliesOnSubscription: false },
        combinesWith: { orderDiscounts: false, productDiscounts: false, shippingDiscounts: false }
      };
      let result = (await graphql(CREATE_DISCOUNT, { input })).discountCodeBasicCreate;
      const unsupportedPurchaseTypes = result.userErrors?.length && result.userErrors.every(error =>
        error.code === 'INVALID' &&
        ['appliesOnSubscription', 'appliesOnOneTimePurchase'].includes(error.field?.at(-1)) &&
        error.message?.includes('not permitted without the shop using subscriptions')
      );
      if (!result.codeDiscountNode?.id && unsupportedPurchaseTypes) {
        const { appliesOnSubscription, appliesOnOneTimePurchase, ...customerGets } = input.customerGets;
        result = (await graphql(CREATE_DISCOUNT, { input: { ...input, customerGets } })).discountCodeBasicCreate;
      }
      if (result.userErrors?.length || !result.codeDiscountNode?.id) {
        console.error('LP discount validation:', JSON.stringify((result.userErrors || []).map(({ field, code, message }) => ({ field, code, message }))));
        throw new OfferError(503, 'Shopify did not accept the LP discount.');
      }
      return result.codeDiscountNode.id;
    }
  };
}

export function createPgStore(pool) {
  let ready;
  function initialize() {
    if (!ready) ready = pool.query(SCHEMA).catch(error => { ready = null; throw error; });
    return ready;
  }
  async function transaction(lockKey, action) {
    await initialize();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL lock_timeout = '10s'");
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [lockKey]);
      const value = await action(client);
      await client.query('COMMIT');
      return value;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }
  return {
    initialize,
    async reserve(record, limit) {
      return transaction(`libase-lp-new:${record.campaign}`, async client => {
        const existing = (await client.query('SELECT * FROM libase_lp_visits WHERE visitor_key=$1', [record.visitor_key])).rows[0];
        if (existing) return existing;
        const count = (await client.query('SELECT COUNT(*) AS n FROM libase_lp_visits WHERE campaign=$1 AND created_at >= $2', [record.campaign, endOfJstDay(record.created_at) - 86400000])).rows[0].n;
        if (Number(count) >= limit) throw new OfferError(429, 'LP daily issuance limit reached.');
        return (await client.query('INSERT INTO libase_lp_visits(visitor_key,campaign,created_at,ends_at,code) VALUES($1,$2,$3,$4,$5) RETURNING *', [record.visitor_key, record.campaign, record.created_at, record.ends_at, record.code])).rows[0];
      });
    },
    async withVisit(key, action) {
      return transaction(`libase-lp-visit:${key}`, async client => {
        const record = (await client.query('SELECT * FROM libase_lp_visits WHERE visitor_key=$1 FOR UPDATE', [key])).rows[0];
        const save = async (state, checkedAt) => client.query('UPDATE libase_lp_visits SET state=$1, checked_at=$2 WHERE visitor_key=$3', [state, checkedAt, key]);
        return action(record, save);
      });
    },
    async markUsed(codes) {
      await initialize();
      await pool.query("UPDATE libase_lp_visits SET state='used' WHERE code = ANY($1::text[])", [codes]);
    }
  };
}

export function createOfferService(config, { store, shopify, clock = Date.now }) {
  const digest = value => createHmac('sha256', config.secret).update(value).digest('hex');
  const keyFor = visitorId => digest(`${config.campaign}:${visitorId}`);
  const tokenFor = key => `${key}.${digest(`eligibility:${key}`)}`;
  const flights = new Map();
  function keyFromToken(token) {
    if (typeof token !== 'string' || !/^[a-f0-9]{64}\.[a-f0-9]{64}$/.test(token)) throw new OfferError(401, 'Invalid visitor token.');
    const [key, signature] = token.split('.');
    if (!timingSafeEqual(Buffer.from(signature, 'hex'), Buffer.from(digest(`eligibility:${key}`), 'hex'))) throw new OfferError(401, 'Invalid visitor token.');
    return key;
  }
  function reply(record, status) {
    return { status, campaign: config.campaign, scope: 'all', serverNow: new Date(clock()).toISOString(),
      ...(record ? { expiresAt: new Date(Number(record.ends_at)).toISOString() } : {}),
      ...(status === 'active' ? { code: record.code, percentage: 10, visitorToken: tokenFor(record.visitor_key) } : {}) };
  }
  async function ensure(key, mayCreate) {
    return store.withVisit(key, async (record, save) => {
      if (!record || record.campaign !== config.campaign) return reply(null, 'ineligible');
      if (clock() >= Number(record.ends_at) || record.state === 'expired') return reply(record, 'expired');
      if (record.state === 'used') return reply(record, 'used');
      if (record.state === 'active' && clock() - Number(record.checked_at) < 60000) return reply(record, 'active');
      let found = await shopify.find(record.code);
      if (!found && record.state === 'active') { await save('expired', clock()); return reply(record, 'expired'); }
      if (!found) {
        if (!mayCreate) return reply(record, 'pending');
        if (clock() >= Number(record.ends_at)) return reply(record, 'expired');
        await shopify.create(record);
        found = { status: 'ACTIVE', endsAt: new Date(Number(record.ends_at)).toISOString(), asyncUsageCount: 0, usageLimit: 1, customerGets: { items: { __typename: 'AllDiscountItems' }, value: { percentage: 0.1 } } };
      }
      if (Date.parse(found.endsAt) !== Number(record.ends_at) || found.usageLimit !== 1 || found.customerGets?.items?.__typename !== 'AllDiscountItems' || found.customerGets?.value?.percentage !== 0.1) throw new OfferError(503, 'LP discount settings have changed.');
      const state = found.asyncUsageCount >= 1 ? 'used' : found.status === 'ACTIVE' && clock() < Number(record.ends_at) ? 'active' : 'expired';
      await save(state, clock());
      return reply(record, state);
    });
  }
  function run(key, mayCreate) {
    const flightKey = `${mayCreate}:${key}`;
    if (flights.has(flightKey)) return flights.get(flightKey);
    const promise = ensure(key, mayCreate).finally(() => flights.delete(flightKey));
    flights.set(flightKey, promise);
    return promise;
  }
  function validateCampaign(body) {
    if (!body || body.campaign !== config.campaign) throw new OfferError(400, 'Invalid campaign.');
  }
  return {
    async offer(body) {
      if (!config.enabled) return reply(null, 'disabled');
      validateCampaign(body);
      if (typeof body.visitorId !== 'string' || !/^[a-f0-9]{48}$/.test(body.visitorId)) throw new OfferError(400, 'Invalid visitor ID.');
      const key = keyFor(body.visitorId), now = clock();
      const record = await store.reserve({ visitor_key: key, campaign: config.campaign, created_at: now, ends_at: endOfJstDay(now), code: `LP${digest(`code:${key}`).slice(0,24).toUpperCase()}` }, config.dailyLimit);
      return run(record.visitor_key, true);
    },
    async status(body) {
      if (!config.enabled) return reply(null, 'disabled');
      validateCampaign(body);
      return run(keyFromToken(body.visitorToken), false);
    },
    async markUsed(discounts = []) {
      if (!config.enabled) return;
      const codes = discounts.map(item => String(item.code || '').toUpperCase()).filter(code => /^LP[A-F0-9]{24}$/.test(code));
      if (codes.length) await store.markUsed(codes);
    }
  };
}
