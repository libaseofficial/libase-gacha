import express from 'express';
import cors from 'cors';
import pkg from 'pg';
import basicAuth from 'express-basic-auth';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import crypto from 'crypto';
import multer from 'multer';
import fs from 'fs';

const { Pool } = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use((req, res, next) => {
  if (req.path === '/webhook/orders-paid' || req.path === '/webhook/orders-cancelled' || req.path === '/webhook/customers-created' || req.path === '/webhook/customers-deleted') {
    express.raw({ type: 'application/json' })(req, res, next);
  } else {
    express.json()(req, res, next);
  }
});
app.use(express.static(join(__dirname, 'public')));
app.use('/uploads', express.static(join(__dirname, 'uploads')));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const SHOPIFY_SHOP = 's62nix-7r.myshopify.com';
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
let ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || '';

// Shopify Files API only. Keep this on a currently supported stable version.
const SHOPIFY_FILES_API_VERSION = process.env.SHOPIFY_FILES_API_VERSION || '2026-07';
const SHOPIFY_FILES_GRAPHQL_URL = `https://${SHOPIFY_SHOP}/admin/api/${SHOPIFY_FILES_API_VERSION}/graphql.json`;
const TEMPORARY_SHOPIFY_UPLOAD_HOST = 'shopify-staged-uploads.storage.googleapis.com';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isTemporaryShopifyUploadUrl(value) {
  if (!value) return false;
  try {
    const parsed = new URL(String(value));
    return parsed.hostname === TEMPORARY_SHOPIFY_UPLOAD_HOST && parsed.pathname.startsWith('/tmp/');
  } catch (_) {
    return false;
  }
}

function normalizePermanentImageUrl(value) {
  if (value === null || value === undefined || String(value).trim() === '') return null;

  let parsed;
  try {
    parsed = new URL(String(value).trim());
  } catch (_) {
    throw new Error('画像URLの形式が正しくありません');
  }

  if (parsed.protocol !== 'https:') {
    throw new Error('画像URLはhttpsから始まるURLを使用してください');
  }

  if (isTemporaryShopifyUploadUrl(parsed.toString())) {
    throw new Error('Shopifyの一時アップロードURLは保存できません。正式なCDN URLを使用してください');
  }

  return parsed.toString();
}

async function shopifyFilesGraphql(query, variables = {}) {
  const response = await fetch(SHOPIFY_FILES_GRAPHQL_URL, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': ACCESS_TOKEN,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });

  const responseText = await response.text();
  let payload;
  try {
    payload = JSON.parse(responseText);
  } catch (_) {
    throw new Error(`Shopify API returned invalid JSON: ${response.status} ${responseText.slice(0, 300)}`);
  }

  const actualApiVersion = response.headers.get('x-shopify-api-version');
  if (actualApiVersion && actualApiVersion !== SHOPIFY_FILES_API_VERSION) {
    console.warn(`⚠️ Shopify API version fallback: requested=${SHOPIFY_FILES_API_VERSION} actual=${actualApiVersion}`);
  }

  if (!response.ok) {
    throw new Error(`Shopify API HTTP error: ${response.status} ${JSON.stringify(payload)}`);
  }
  if (payload.errors?.length) {
    throw new Error(`Shopify GraphQL error: ${JSON.stringify(payload.errors)}`);
  }

  return payload.data;
}

async function waitForShopifyImageReady(fileId, maxAttempts = 30, intervalMs = 1000) {
  const query = `
    query GetUploadedImage($id: ID!) {
      node(id: $id) {
        ... on MediaImage {
          id
          fileStatus
          image {
            url
          }
        }
      }
    }
  `;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const data = await shopifyFilesGraphql(query, { id: fileId });
    const file = data?.node;

    if (!file) {
      throw new Error(`Shopify上の画像ファイルが見つかりません: ${fileId}`);
    }
    if (file.fileStatus === 'FAILED') {
      throw new Error(`Shopifyの画像処理に失敗しました: ${fileId}`);
    }

    const permanentUrl = file.image?.url || null;
    if (file.fileStatus === 'READY' && permanentUrl) {
      if (isTemporaryShopifyUploadUrl(permanentUrl)) {
        throw new Error('Shopifyから一時URLが返されたため保存を中止しました');
      }
      return permanentUrl;
    }

    if (attempt === 1 || attempt % 5 === 0) {
      console.log(`📷 Shopify image processing: id=${fileId} status=${file.fileStatus} attempt=${attempt}/${maxAttempts}`);
    }
    await sleep(intervalMs);
  }

  throw new Error('Shopifyの画像処理が時間内に完了しませんでした。少し待ってから再度お試しください');
}

async function getShopifyProductForReview(productId) {
  const data = await shopifyFilesGraphql(
    `
      query GetReviewProduct($id: ID!) {
        product(id: $id) {
          handle
          title
          featuredImage {
            url
          }
        }
      }
    `,
    { id: `gid://shopify/Product/${productId}` }
  );

  return data?.product || null;
}

async function loadAccessToken() {
  try {
    const result = await pool.query(
      "SELECT value FROM settings WHERE key = 'shopify_access_token'"
    );
    if (result.rows.length > 0 && result.rows[0].value) {
      ACCESS_TOKEN = result.rows[0].value;
      console.log('Access token loaded from Supabase');
    }
  } catch (e) {
    console.error('Failed to load access token:', e.message);
  }
}

loadAccessToken();

const adminAuth = basicAuth({
  users: { 'admin': process.env.ADMIN_PASSWORD || 'libase2024' },
  challenge: true
});

async function draw() {
  const result = await pool.query('SELECT * FROM rewards WHERE stock > 0');
  const rewards = result.rows;
  if (rewards.length === 0) return null;
  const total = rewards.reduce((sum, r) => sum + r.probability, 0);
  let rand = Math.random() * total;
  for (const r of rewards) {
    if (rand < r.probability) return r;
    rand -= r.probability;
  }
  return rewards[rewards.length - 1];
}

async function issueRewardCode(reward) {
  if (!ACCESS_TOKEN) return null;
  if (reward.reward_type === 'manual') return null;

  if (reward.reward_type === 'external') {
    const result = await pool.query(
      "SELECT * FROM external_codes WHERE reward_id = $1 AND status = 'available' LIMIT 1",
      [reward.id]
    );
    if (result.rows.length === 0) return null;
    const externalCode = result.rows[0];
    await pool.query(
      "UPDATE external_codes SET status = 'used', used_at = NOW() WHERE id = $1",
      [externalCode.id]
    );
    return { code: externalCode.code, pin: externalCode.pin_code || null };
  }

  try {
    const priceRuleRes = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2025-01/price_rules.json`,
      {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          price_rule: {
            title: `GACHA-${reward.name}`,
            target_type: reward.reward_type === 'shipping' ? 'shipping_line' : 'line_item',
            target_selection: 'all',
            allocation_method: 'across',
            value_type: reward.reward_type === 'shipping' ? 'percentage' : 'fixed_amount',
            value: reward.reward_type === 'shipping' ? '-100.0' : `-${reward.discount_amount}.0`,
            customer_selection: 'all',
            starts_at: new Date().toISOString(),
            ends_at: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString(),
            usage_limit: 1
          }
        })
      }
    );
    const priceRuleData = await priceRuleRes.json();
    const priceRuleId = priceRuleData.price_rule.id;
    const code = 'GACHA-' + Math.random().toString(36).substring(2, 10).toUpperCase();
    const discountRes = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2025-01/price_rules/${priceRuleId}/discount_codes.json`,
      {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ discount_code: { code } })
      }
    );
    const discountData = await discountRes.json();
    return discountData.discount_code.code;
  } catch (e) {
    console.error('issueRewardCode error:', e);
    return null;
  }
}

app.get('/', (_req, res) => res.send('LIBASE Gacha Server is running'));

app.get('/install', (_req, res) => {
  const url = `https://${SHOPIFY_SHOP}/admin/oauth/authorize?client_id=${CLIENT_ID}&scope=read_price_rules,write_price_rules,read_discounts,write_discounts,read_customers,read_orders,write_files&redirect_uri=https://libase-gacha.onrender.com/callback&state=gacha123`;
  res.redirect(url);
});

app.get('/callback', async (req, res) => {
  const { code } = req.query;
  const response = await fetch(`https://${SHOPIFY_SHOP}/admin/oauth/access_token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ client_id: CLIENT_ID, client_secret: CLIENT_SECRET, code })
  });
  const data = await response.json();
  ACCESS_TOKEN = data.access_token;
  await pool.query(
    "INSERT INTO settings (key, value) VALUES ('shopify_access_token', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
    [ACCESS_TOKEN]
  );
  res.send('インストール完了しました。このページを閉じてください。');
});

app.get('/points', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId || !ACCESS_TOKEN) return res.json({ ok: false, points: 0 });
  try {
    const response = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2025-01/customers/${customerId}/metafields.json`,
      { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
    );
    const data = await response.json();
    const pointField = data.metafields.find(m => m.namespace === 'poingpong' && m.key === 'points_after_change');
    const lastGrantedField = data.metafields.find(m => m.namespace === 'poingpong' && m.key === 'last_point_granted_at');
    const points = pointField ? parseInt(pointField.value) : 0;
    const lastGrantedAt = lastGrantedField ? lastGrantedField.value : null;
    res.json({ ok: true, points, lastGrantedAt });
  } catch (e) {
    res.json({ ok: false, points: 0 });
  }
});

// 独自ポイント残高取得
app.get('/my-points', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId) return res.json({ ok: false, points: 0 });
  try {
    const result = await pool.query(
      'SELECT points FROM customer_points WHERE customer_id = $1 AND shop_domain = $2',
      [customerId, SHOPIFY_SHOP]
    );
    const points = result.rows.length > 0 ? result.rows[0].points : 0;
    res.json({ ok: true, points });
  } catch (e) {
    res.json({ ok: false, points: 0 });
  }
});

app.get('/history', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId) return res.json({ ok: false, history: [] });
  try {
    const result = await pool.query(
      `SELECT gh.reward_name, gh.points_used, gh.created_at, gh.reward_code,
              r.image_url
       FROM gacha_history gh
       LEFT JOIN rewards r ON r.name = gh.reward_name
       WHERE gh.customer_id = $1
       ORDER BY gh.created_at DESC LIMIT 10`,
      [customerId]
    );
    res.json({ ok: true, history: result.rows });
  } catch (e) {
    res.json({ ok: false, history: [] });
  }
});

app.post('/verify', async (req, res) => {
  const { customerId, coupon } = req.body;
  try {
    await pool.query('INSERT INTO spin_tickets (customer_id, coupon_code) VALUES ($1, $2)', [customerId, coupon]);
    res.json({ ok: true });
  } catch {
    res.json({ ok: false });
  }
});

app.post('/spin', async (req, res) => {
  const { customerId, coupon } = req.body;
  try {
    const ticketResult = await pool.query(
      "SELECT * FROM spin_tickets WHERE coupon_code = $1 AND status = 'verified'",
      [coupon]
    );
    if (ticketResult.rows.length === 0) return res.json({ ok: false });
    const ticket = ticketResult.rows[0];

    if (ACCESS_TOKEN) {
      const shopifyRes = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2025-01/discount_codes/lookup.json?code=${coupon}`,
        { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
      );
      if (!shopifyRes.ok) return res.json({ ok: false, message: '無効なコードです' });
      const shopifyData = await shopifyRes.json();
      const { id, price_rule_id } = shopifyData.discount_code;
      await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2025-01/price_rules/${price_rule_id}/discount_codes/${id}.json`,
        { method: 'DELETE', headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
      );
    }

    const reward = await draw();
    if (!reward) return res.json({ ok: false, message: '景品がありません' });
    const rewardCode = await issueRewardCode(reward);

    await pool.query('UPDATE rewards SET stock = stock - 1 WHERE id = $1', [reward.id]);
    await pool.query("UPDATE spin_tickets SET status = 'used' WHERE id = $1", [ticket.id]);
    await pool.query(
      'INSERT INTO gacha_history (customer_id, reward_name, reward_code) VALUES ($1, $2, $3)',
      [customerId, reward.name, rewardCode]
    );

    res.json({ ok: true, reward: reward.name, rarity: reward.rarity || 'normal', rewardCode, rewardType: reward.reward_type });
  } catch (e) {
    console.error(e);
    res.json({ ok: false, message: 'エラーが発生しました' });
  }
});

// ポイントでガチャを回す
const GACHA_COST = 500;

app.post('/spin-with-points', async (req, res) => {
  const { customerId } = req.body;
  if (!customerId) return res.json({ ok: false, message: 'ログインが必要です' });

  try {
    const pointResult = await pool.query(
      'SELECT points FROM customer_points WHERE customer_id = $1 AND shop_domain = $2',
      [customerId, SHOPIFY_SHOP]
    );
    if (pointResult.rows.length === 0 || pointResult.rows[0].points < GACHA_COST) {
      return res.json({ ok: false, message: `ポイントが不足しています（必要：${GACHA_COST}pt）` });
    }

    const reward = await draw();
if (!reward) return res.json({ ok: false, message: '景品がありません' });
const rewardResult = await issueRewardCode(reward);

let rewardCodeStr = null;
let rewardPin = null;
if (rewardResult && typeof rewardResult === 'object') {
  rewardCodeStr = rewardResult.code;
  rewardPin = rewardResult.pin;
} else {
  rewardCodeStr = rewardResult;
}
await pool.query('UPDATE rewards SET stock = stock - 1 WHERE id = $1', [reward.id]);
await pool.query(
  'UPDATE customer_points SET points = points - $1, updated_at = NOW() WHERE customer_id = $2 AND shop_domain = $3',
  [GACHA_COST, customerId, SHOPIFY_SHOP]
);

await pool.query(
  "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason) VALUES ($1, $2, $3, 'gacha', 'ガチャ消費')",
  [customerId, SHOPIFY_SHOP, -GACHA_COST]
);

await pool.query(
  'INSERT INTO gacha_history (customer_id, reward_name, reward_code, points_used) VALUES ($1, $2, $3, $4)',
  [customerId, reward.name, rewardCodeStr, GACHA_COST]
);

console.log(`✅ ガチャ: customer=${customerId} -${GACHA_COST}pt → ${reward.name}`);
res.json({
  ok: true,
  reward: reward.name,
  rarity: reward.rarity || 'normal',
  rewardCode: rewardCodeStr,
  rewardPin: rewardPin,
  rewardType: reward.reward_type,
  imageUrl: reward.image_url || null
});
  } catch (e) {
    console.error('spin-with-points error:', e);
    res.json({ ok: false, message: 'エラーが発生しました' });
  }
});

// 管理画面
app.get('/admin', adminAuth, (_req, res) => res.sendFile(join(__dirname, 'public', 'admin.html')));

app.get('/admin/api/rewards', adminAuth, async (_req, res) => {
  const result = await pool.query('SELECT * FROM rewards ORDER BY id');
  res.json(result.rows);
});

app.post('/admin/api/rewards', adminAuth, async (req, res) => {
  const { name, probability, stock, rarity, reward_type, discount_amount, image_url } = req.body;
  const result = await pool.query(
    'INSERT INTO rewards (name, probability, stock, rarity, reward_type, discount_amount, image_url) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *',
    [name, probability, stock, rarity, reward_type, discount_amount, image_url]
  );
  res.json(result.rows[0]);
});

app.put('/admin/api/rewards/:id', adminAuth, async (req, res) => {
  const { name, probability, stock, rarity, reward_type, discount_amount, image_url } = req.body;
  const result = await pool.query(
    'UPDATE rewards SET name=$1, probability=$2, stock=$3, rarity=$4, reward_type=$5, discount_amount=$6, image_url=$7 WHERE id=$8 RETURNING *',
    [name, probability, stock, rarity, reward_type, discount_amount, image_url, req.params.id]
  );
  res.json(result.rows[0]);
});

app.delete('/admin/api/rewards/:id', adminAuth, async (req, res) => {
  await pool.query('DELETE FROM rewards WHERE id = $1', [req.params.id]);
  res.json({ ok: true });
});

app.get('/admin/api/history', adminAuth, async (_req, res) => {
  const result = await pool.query('SELECT * FROM gacha_history ORDER BY created_at DESC LIMIT 50');
  const history = result.rows;
  if (!ACCESS_TOKEN || history.length === 0) return res.json(history);

  const customerIds = [...new Set(history.map(h => h.customer_id))];
  const customerMap = {};
  for (const id of customerIds) {
    try {
      const r = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2025-01/customers/${id}.json`,
        { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
      );
      const data = await r.json();
      if (data.customer) {
        customerMap[id] = {
          name: `${data.customer.first_name} ${data.customer.last_name}`.trim(),
          email: data.customer.email
        };
      }
    } catch (e) {
      customerMap[id] = { name: '-', email: '-' };
    }
  }

  res.json(history.map(h => ({
    ...h,
    customer_name: customerMap[h.customer_id]?.name || '-',
    customer_email: customerMap[h.customer_id]?.email || '-'
  })));
});

app.get('/admin/api/external-codes/:rewardId', adminAuth, async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM external_codes WHERE reward_id = $1 ORDER BY created_at DESC',
    [req.params.rewardId]
  );
  res.json(result.rows);
});

app.post('/admin/api/external-codes', adminAuth, async (req, res) => {
  const { reward_id, codes, pin_codes } = req.body;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i].trim();
    const pin = pin_codes && pin_codes[i] ? pin_codes[i].trim() : null;
    if (!code) continue;
    await pool.query(
      'INSERT INTO external_codes (reward_id, code, pin_code) VALUES ($1, $2, $3)',
      [reward_id, code, pin]
    );
  }
  res.json({ ok: true });
});

app.delete('/admin/api/external-codes/:id', adminAuth, async (req, res) => {
  await pool.query('DELETE FROM external_codes WHERE id = $1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/upload', upload.single('file'), async (req, res) => {
  console.log('📷 /upload called');

  if (!req.file) {
    console.warn('⚠️ upload: ファイルなし');
    return res.status(400).json({ ok: false, message: 'ファイルがありません' });
  }

  if (!ACCESS_TOKEN) {
    console.error('❌ upload: ACCESS_TOKEN がありません');
    return res.status(500).json({ ok: false, message: 'Shopify連携トークンがありません' });
  }

  try {
    const fileData = fs.readFileSync(req.file.path);
    const mimeType = req.file.mimetype || 'image/jpeg';
    const filename = req.file.originalname || `review-${Date.now()}.jpg`;

    if (!mimeType.startsWith('image/')) {
      return res.status(400).json({ ok: false, message: '画像ファイルを選択してください' });
    }

    console.log('📷 upload file:', {
      filename,
      mimeType,
      size: req.file.size,
      apiVersion: SHOPIFY_FILES_API_VERSION
    });

    const stagingData = await shopifyFilesGraphql(
      `
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `,
      {
        input: [
          {
            filename,
            mimeType,
            resource: 'IMAGE',
            fileSize: String(req.file.size),
            httpMethod: 'POST'
          }
        ]
      }
    );

    const stagingErrors = stagingData?.stagedUploadsCreate?.userErrors || [];
    if (stagingErrors.length > 0) {
      throw new Error(`stagedUploadsCreate failed: ${JSON.stringify(stagingErrors)}`);
    }

    const target = stagingData?.stagedUploadsCreate?.stagedTargets?.[0];
    if (!target?.url || !target?.resourceUrl) {
      throw new Error('stagedUploadsCreate target が取得できません');
    }

    const formData = new FormData();
    target.parameters.forEach((parameter) => {
      formData.append(parameter.name, parameter.value);
    });
    formData.append('file', new Blob([fileData], { type: mimeType }), filename);

    const uploadToStorageRes = await fetch(target.url, {
      method: 'POST',
      body: formData
    });
    const uploadToStorageText = await uploadToStorageRes.text();

    console.log('📷 storage upload status:', uploadToStorageRes.status);
    if (!uploadToStorageRes.ok) {
      throw new Error(`storage upload failed: ${uploadToStorageRes.status} ${uploadToStorageText.slice(0, 500)}`);
    }

    const fileCreateData = await shopifyFilesGraphql(
      `
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
              fileStatus
              alt
              createdAt
              ... on MediaImage {
                image {
                  url
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `,
      {
        files: [
          {
            originalSource: target.resourceUrl,
            contentType: 'IMAGE',
            alt: filename
          }
        ]
      }
    );

    const fileCreateErrors = fileCreateData?.fileCreate?.userErrors || [];
    if (fileCreateErrors.length > 0) {
      throw new Error(`fileCreate failed: ${JSON.stringify(fileCreateErrors)}`);
    }

    const file = fileCreateData?.fileCreate?.files?.[0];
    if (!file?.id) {
      throw new Error('ShopifyのファイルIDが取得できません');
    }

    // fileCreate is asynchronous. Never fall back to target.resourceUrl because it is temporary.
    let permanentUrl = file.image?.url || null;
    if (!permanentUrl || file.fileStatus !== 'READY') {
      permanentUrl = await waitForShopifyImageReady(file.id);
    }

    permanentUrl = normalizePermanentImageUrl(permanentUrl);
    if (!permanentUrl) {
      throw new Error('正式な画像URLが取得できません');
    }

    console.log('✅ upload success:', { fileId: file.id, url: permanentUrl });
    return res.json({ ok: true, url: permanentUrl, fileId: file.id });
  } catch (e) {
    console.error('❌ Upload error:', e);
    return res.status(500).json({
      ok: false,
      message: '画像の正式保存に失敗しました。時間を置いて再度お試しください',
      error: e.message
    });
  } finally {
    try {
      if (req.file?.path && fs.existsSync(req.file.path)) {
        fs.unlinkSync(req.file.path);
      }
    } catch (cleanupError) {
      console.warn('⚠️ upload temp file cleanup failed:', cleanupError.message);
    }
  }
});

// レビューAPI
const REVIEW_POINTS = 500;

app.post('/reviews', async (req, res) => {
  const {
    customerId,
    purchaseId,
    productId,
    productName,
    authorName,
    email,
    rating,
    title,
    body,
    imageUrl
  } = req.body;
  const normalizedRating = parseInt(rating, 10);

  if (!customerId || !productId || !body) {
    return res.status(400).json({ ok: false, message: '必須項目が不足しています' });
  }
  if (!Number.isInteger(normalizedRating) || normalizedRating < 1 || normalizedRating > 5) {
    return res.status(400).json({ ok: false, message: '評価は1〜5で入力してください' });
  }
  if (purchaseId && !/^\d+$/.test(String(purchaseId))) {
    return res.status(400).json({ ok: false, message: '購入情報が正しくありません' });
  }

  let permanentImageUrl;
  try {
    permanentImageUrl = normalizePermanentImageUrl(imageUrl);
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }

  const shopDomain = SHOPIFY_SHOP;
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    // 同一顧客・同一商品への同時投稿を直列化し、ポイントの二重付与を防ぐ。
    await client.query(
      'SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))',
      [`${shopDomain}:${customerId}`, purchaseId ? `purchase:${purchaseId}` : `product:${productId}`]
    );

    let purchase = null;
    if (purchaseId) {
      const purchaseResult = await client.query(
        `SELECT id, product_id, product_name, order_id, order_name
         FROM review_purchases
         WHERE id = $1
           AND customer_id = $2
           AND shop_domain = $3
           AND reviewed_at IS NULL
           AND cancelled_at IS NULL
         FOR UPDATE`,
        [purchaseId, customerId, shopDomain]
      );
      purchase = purchaseResult.rows[0] || null;

      if (!purchase) {
        await client.query('ROLLBACK');
        return res.status(409).json({ ok: false, message: 'この購入分はすでにレビュー済み、またはレビュー対象外です' });
      }
    } else {
      // 旧画面との互換用。候補が1件だけなら購入IDなしでも安全に処理できる。
      const purchaseResult = await client.query(
        `SELECT id, product_id, product_name, order_id, order_name
         FROM review_purchases
         WHERE customer_id = $1
           AND shop_domain = $2
           AND product_id = $3
           AND reviewed_at IS NULL
           AND cancelled_at IS NULL
         ORDER BY purchased_at ASC NULLS LAST, id ASC
         LIMIT 2
         FOR UPDATE`,
        [customerId, shopDomain, productId]
      );

      if (purchaseResult.rows.length > 1) {
        await client.query('ROLLBACK');
        return res.status(409).json({
          ok: false,
          message: '複数の購入履歴があります。ページを再読み込みしてから再度お試しください'
        });
      }
      purchase = purchaseResult.rows[0] || null;
    }

    const savedProductId = purchase?.product_id || productId;
    const savedProductName = purchase?.product_name || productName;

    if (!purchase) {
      // マイグレーション前の購入履歴は購入IDを持たないため、従来どおり商品単位で1回だけ許可する。
      const duplicateResult = await client.query(
        'SELECT id FROM reviews WHERE customer_id = $1 AND shop_domain = $2 AND product_id = $3 LIMIT 1',
        [customerId, shopDomain, savedProductId]
      );
      if (duplicateResult.rows.length > 0) {
        await client.query('ROLLBACK');
        return res.status(409).json({ ok: false, message: 'この購入分はすでにレビュー済みです' });
      }
    }

    const reviewResult = await client.query(
      `INSERT INTO reviews
       (customer_id, shop_domain, product_id, product_name, author_name, email, rating, title, body, image_url, status, purchase_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       RETURNING id`,
      [
        customerId,
        shopDomain,
        savedProductId,
        savedProductName,
        authorName || '匿名',
        email || null,
        normalizedRating,
        title || null,
        body,
        permanentImageUrl,
        'hidden',
        purchase?.id || null
      ]
    );
    const reviewId = reviewResult.rows[0].id;

    if (purchase) {
      await client.query(
        `UPDATE review_purchases
         SET reviewed_at = NOW(), review_id = $1, updated_at = NOW()
         WHERE id = $2`,
        [reviewId, purchase.id]
      );
    }

    await client.query(
      `INSERT INTO customer_points (customer_id, shop_domain, points, total_earned) VALUES ($1, $2, $3, $3)
       ON CONFLICT (customer_id, shop_domain) DO UPDATE SET points = customer_points.points + $3, total_earned = customer_points.total_earned + $3, updated_at = NOW()`,
      [customerId, shopDomain, REVIEW_POINTS]
    );

    await client.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason) VALUES ($1, $2, $3, 'review', $4)",
      [
        customerId,
        shopDomain,
        REVIEW_POINTS,
        purchase?.order_name
          ? `レビュー投稿ポイント（${purchase.order_name}）`
          : 'レビュー投稿ポイント'
      ]
    );

    await client.query('COMMIT');
    console.log(`✅ レビューポイント付与: customer=${customerId} purchase=${purchase?.id || 'legacy'} +${REVIEW_POINTS}pt`);
    return res.json({ ok: true, points: REVIEW_POINTS, reviewId });
  } catch (e) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (_) {
        // 元のエラーを優先する。
      }
    }
    console.error('Review error:', e);
    if (e.code === '23505') {
      return res.status(409).json({ ok: false, message: 'この購入分はすでにレビュー済みです' });
    }
    return res.status(500).json({ ok: false, message: 'レビューの投稿に失敗しました' });
  } finally {
    client?.release();
  }
});

app.get('/reviews', async (req, res) => {
  const { productId } = req.query;
  if (!productId) return res.json({ ok: false, reviews: [] });
  try {
    const result = await pool.query(
      "SELECT author_name, rating, title, body, image_url, reply, replied_at, created_at FROM reviews WHERE product_id = $1 AND status = 'published' ORDER BY created_at DESC",
      [productId]
    );
    res.json({ ok: true, reviews: result.rows });
  } catch (e) {
    res.json({ ok: false, reviews: [] });
  }
});

app.get('/review-summary', async (req, res) => {
  const { productId } = req.query;
  if (!productId) {
    return res.json({ ok: false, avg: 0, count: 0 });
  }

  try {
    const result = await pool.query(
      `SELECT 
         ROUND(AVG(rating)::numeric, 1) AS avg,
         COUNT(*)::int AS count
       FROM reviews
       WHERE product_id = $1
         AND status = 'published'`,
      [productId]
    );

    const row = result.rows[0];
    res.json({
      ok: true,
      avg: row?.avg ? Number(row.avg) : 0,
      count: row?.count ? Number(row.count) : 0
    });
  } catch (e) {
    console.error('review-summary error:', e);
    res.json({ ok: false, avg: 0, count: 0 });
  }
});


app.get('/admin/api/reviews', adminAuth, async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT r.*,
              rp.order_id AS review_order_id,
              rp.order_name AS review_order_name,
              rp.purchased_at AS review_purchased_at
       FROM reviews r
       LEFT JOIN review_purchases rp ON rp.id = r.purchase_id
       ORDER BY r.created_at DESC`
    );
    res.json(result.rows);
  } catch (e) {
    console.error('Admin reviews load error:', e);
    res.json([]);
  }
});

app.post('/admin/api/reviews', adminAuth, async (req, res) => {
  const { productId, productName, authorName, email, rating, title, body, imageUrl, status } = req.body;
  const normalizedRating = parseInt(rating, 10);
  const normalizedStatus = status === 'hidden' ? 'hidden' : 'published';

  let permanentImageUrl;
  try {
    permanentImageUrl = normalizePermanentImageUrl(imageUrl);
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }

  if (!productId || !productName || !body) {
    return res.json({ ok: false, message: '必須項目が不足しています' });
  }
  if (!Number.isInteger(normalizedRating) || normalizedRating < 1 || normalizedRating > 5) {
    return res.json({ ok: false, message: '評価は1〜5で入力してください' });
  }

  try {
    const customerId = `admin_manual_${Date.now()}`;
    const result = await pool.query(
      `INSERT INTO reviews
       (customer_id, shop_domain, product_id, product_name, author_name, email, rating, title, body, image_url, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING id`,
      [
        customerId,
        SHOPIFY_SHOP,
        productId,
        productName,
        authorName || '匿名',
        email || null,
        normalizedRating,
        title || null,
        body,
        permanentImageUrl,
        normalizedStatus
      ]
    );
    res.json({ ok: true, id: result.rows[0].id });
  } catch (e) {
    console.error('Admin review create error:', e);
    res.json({ ok: false, message: 'レビューの追加に失敗しました' });
  }
});


app.put('/admin/api/reviews/:id', adminAuth, async (req, res) => {
  const { productId, productName, authorName, email, rating, title, body, imageUrl, status } = req.body;
  const normalizedRating = parseInt(rating, 10);
  const normalizedStatus = status === 'hidden' ? 'hidden' : 'published';

  let permanentImageUrl;
  try {
    permanentImageUrl = normalizePermanentImageUrl(imageUrl);
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }

  if (!productId || !productName || !body) {
    return res.json({ ok: false, message: '必須項目が不足しています' });
  }
  if (!Number.isInteger(normalizedRating) || normalizedRating < 1 || normalizedRating > 5) {
    return res.json({ ok: false, message: '評価は1〜5で入力してください' });
  }

  try {
    const result = await pool.query(
      `UPDATE reviews
       SET product_id = $1,
           product_name = $2,
           author_name = $3,
           email = $4,
           rating = $5,
           title = $6,
           body = $7,
           image_url = $8,
           status = $9,
           updated_at = NOW()
       WHERE id = $10
       RETURNING id`,
      [
        productId,
        productName,
        authorName || '匿名',
        email || null,
        normalizedRating,
        title || null,
        body,
        permanentImageUrl,
        normalizedStatus,
        req.params.id
      ]
    );

    if (result.rows.length === 0) return res.json({ ok: false, message: 'レビューが見つかりません' });
    res.json({ ok: true });
  } catch (e) {
    console.error('Admin review update error:', e);
    res.json({ ok: false, message: 'レビューの保存に失敗しました' });
  }
});

app.delete('/admin/api/reviews/:id', adminAuth, async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM reviews WHERE id = $1 RETURNING id', [req.params.id]);
    if (result.rows.length === 0) return res.json({ ok: false, message: 'レビューが見つかりません' });
    res.json({ ok: true });
  } catch (e) {
    console.error('Admin review delete error:', e);
    res.json({ ok: false });
  }
});

app.post('/admin/api/reviews/:id/edit', adminAuth, async (req, res) => {
  const { productId, productName, authorName, email, rating, title, body, imageUrl, status } = req.body;
  const normalizedRating = parseInt(rating, 10);
  const normalizedStatus = status === 'hidden' ? 'hidden' : 'published';

  let permanentImageUrl;
  try {
    permanentImageUrl = normalizePermanentImageUrl(imageUrl);
  } catch (e) {
    return res.status(400).json({ ok: false, message: e.message });
  }

  if (!productId || !productName || !body) {
    return res.json({ ok: false, message: '必須項目が不足しています' });
  }
  if (!Number.isInteger(normalizedRating) || normalizedRating < 1 || normalizedRating > 5) {
    return res.json({ ok: false, message: '評価は1〜5で入力してください' });
  }

  try {
    const result = await pool.query(
      `UPDATE reviews
       SET product_id = $1,
           product_name = $2,
           author_name = $3,
           email = $4,
           rating = $5,
           title = $6,
           body = $7,
           image_url = $8,
           status = $9,
           updated_at = NOW()
       WHERE id = $10
       RETURNING id`,
      [
        productId,
        productName,
        authorName || '匿名',
        email || null,
        normalizedRating,
        title || null,
        body,
        permanentImageUrl,
        normalizedStatus,
        req.params.id
      ]
    );

    if (result.rows.length === 0) return res.json({ ok: false, message: 'レビューが見つかりません' });
    res.json({ ok: true });
  } catch (e) {
    console.error('Admin review update error:', e);
    res.json({ ok: false, message: 'レビューの保存に失敗しました' });
  }
});

app.post('/admin/api/reviews/:id/delete', adminAuth, async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM reviews WHERE id = $1 RETURNING id', [req.params.id]);
    if (result.rows.length === 0) return res.json({ ok: false, message: 'レビューが見つかりません' });
    res.json({ ok: true });
  } catch (e) {
    console.error('Admin review delete error:', e);
    res.json({ ok: false });
  }
});

app.post('/admin/api/reviews/:id/reply', adminAuth, async (req, res) => {
  const { reply } = req.body;
  try {
    await pool.query(
      'UPDATE reviews SET reply = $1, replied_at = NOW(), updated_at = NOW() WHERE id = $2',
      [reply, req.params.id]
    );
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false });
  }
});

app.post('/admin/api/reviews/:id/status', adminAuth, async (req, res) => {
  const { status } = req.body;
  try {
    await pool.query('UPDATE reviews SET status = $1, updated_at = NOW() WHERE id = $2', [status, req.params.id]);
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: false });
  }
});

app.get('/latest-reviews', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '20', 10), 50);

  try {
    const latestResult = await pool.query(
      `SELECT 
         product_id,
         product_name,
         author_name,
         rating,
         title,
         body,
         image_url,
         reply,
         replied_at,
         created_at
       FROM reviews
       WHERE status = 'published'
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );

    const summaryResult = await pool.query(
      `SELECT 
         ROUND(AVG(rating)::numeric, 2) AS avg,
         COUNT(*)::int AS count
       FROM reviews
       WHERE status = 'published'`
    );

    const summary = summaryResult.rows[0];

    res.json({
      ok: true,
      summary: {
        avg: summary?.avg ? Number(summary.avg) : 0,
        count: summary?.count ? Number(summary.count) : 0
      },
      reviews: latestResult.rows
    });
  } catch (e) {
    console.error('latest-reviews error:', e);
    res.json({
      ok: false,
      summary: { avg: 0, count: 0 },
      reviews: []
    });
  }
});

// Webhook
const SHOPIFY_WEBHOOK_SECRET = process.env.SHOPIFY_WEBHOOK_SECRET || '';
const POINT_RATE = 1;

app.post('/webhook/orders-paid', async (req, res) => {
  const hmac = req.headers['x-shopify-hmac-sha256'];
  const hash = crypto.createHmac('sha256', SHOPIFY_WEBHOOK_SECRET).update(req.body).digest('base64');
  if (hmac !== hash) {
    console.warn('Webhook: invalid signature');
    return res.status(401).send('Unauthorized');
  }

  const order = JSON.parse(req.body);
  const customerId = order.customer?.id?.toString();
  const email = order.customer?.email || '';
  if (!customerId) return res.status(200).send('no customer');
  const orderId = order.id?.toString();
  if (!orderId) return res.status(200).send('no order id');

  try {
    const orderName = order.name || (order.order_number ? `#${order.order_number}` : orderId);
    const purchasedAt = order.processed_at || order.created_at || new Date().toISOString();
    const productCache = new Map();

    // ポイント付与済みのWebhook再送でも、購入単位のレビュー権利は必ず補完する。
    for (const [itemIndex, item] of (order.line_items || []).entries()) {
      if (!item.product_id) continue;

      let productHandle = null;
      let productImageUrl = item.image?.src || null;
      try {
        const cacheKey = String(item.product_id);
        let productData = productCache.get(cacheKey);
        if (productData === undefined) {
          productData = ACCESS_TOKEN ? await getShopifyProductForReview(item.product_id) : null;
          productCache.set(cacheKey, productData);
        }
        productHandle = productData?.handle || null;
        productImageUrl = productImageUrl || productData?.featuredImage?.url || null;
      } catch (e) {
        console.error('product handle fetch error:', e);
      }

      if (!productHandle || productHandle.trim() === '') {
        productHandle = item.product_id?.toString() || item.sku || item.title;
        console.warn('⚠️ productHandle取得失敗。代替IDで保存:', {
          product_id: item.product_id,
          title: item.title,
          fallback: productHandle
        });
      }

      const lineItemId = item.id?.toString() || `${orderId}:${item.product_id}:${itemIndex}`;
      const parsedQuantity = parseInt(item.quantity || '1', 10);
      const quantity = Number.isInteger(parsedQuantity) && parsedQuantity > 0 ? parsedQuantity : 1;

      await pool.query(
        `INSERT INTO review_purchases
         (customer_id, shop_domain, order_id, order_name, line_item_id, product_id, product_name, image_url, quantity, purchased_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (shop_domain, order_id, line_item_id)
         DO UPDATE SET
           customer_id = EXCLUDED.customer_id,
           order_name = EXCLUDED.order_name,
           product_id = EXCLUDED.product_id,
           product_name = EXCLUDED.product_name,
           image_url = COALESCE(EXCLUDED.image_url, review_purchases.image_url),
           quantity = EXCLUDED.quantity,
           purchased_at = EXCLUDED.purchased_at,
           updated_at = NOW()`,
        [
          customerId,
          SHOPIFY_SHOP,
          orderId,
          orderName,
          lineItemId,
          productHandle,
          item.title,
          productImageUrl,
          quantity,
          purchasedAt
        ]
      );

      // 既存画面・既存データとの互換用。リピート購入の判定は review_purchases を使用する。
      await pool.query(
        `INSERT INTO customer_products (customer_id, shop_domain, product_id, product_name, image_url)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (customer_id, shop_domain, product_id)
         DO UPDATE SET
           product_name = EXCLUDED.product_name,
           image_url = COALESCE(EXCLUDED.image_url, customer_products.image_url)`,
        [customerId, SHOPIFY_SHOP, productHandle, item.title, productImageUrl]
      );

      console.log('✅ レビュー購入権利保存:', {
        customerId,
        orderId,
        lineItemId,
        productId: productHandle
      });
    }

    const duplicatePoints = await pool.query(
      "SELECT id FROM point_logs WHERE order_id = $1 AND type = 'purchase'",
      [orderId]
    );
    if (duplicatePoints.rows.length > 0) return res.status(200).send('already processed');

    const totalPrice = parseFloat(order.subtotal_price || order.total_price || 0);
    const pointsToAdd = Math.floor(totalPrice / 100) * POINT_RATE;
    if (pointsToAdd <= 0) return res.status(200).send('purchase saved; no points');

    await pool.query(
      `INSERT INTO customer_points (customer_id, shop_domain, email, points, total_earned) VALUES ($1, $2, $3, $4, $4)
       ON CONFLICT (customer_id, shop_domain) DO UPDATE SET points = customer_points.points + $4, total_earned = customer_points.total_earned + $4, email = EXCLUDED.email, updated_at = NOW()`,
      [customerId, SHOPIFY_SHOP, email, pointsToAdd]
    );

    await pool.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason, order_id) VALUES ($1, $2, $3, 'purchase', $4, $5)",
      [customerId, SHOPIFY_SHOP, pointsToAdd, `注文 ${orderName} 購入ポイント`, orderId]
    );

    console.log(`✅ ポイント付与: customer=${customerId} +${pointsToAdd}pt (${orderName})`);
    return res.status(200).send('ok');
  } catch (e) {
    console.error('Webhook error:', e);
    return res.status(500).send('error');
  }
});

// Supabase ping
setInterval(async () => {
  try {
    await pool.query('SELECT 1');
    console.log('DB ping OK');
  } catch (e) {
    console.error('DB ping failed:', e.message);
  }
}, 1000 * 60 * 60 * 24 * 6);

// 管理画面: ポイント一覧
app.get('/admin/api/points', adminAuth, async (_req, res) => {
  try {
    const result = await pool.query(
      'SELECT customer_id, email, points, total_earned FROM customer_points WHERE shop_domain = $1 ORDER BY points DESC',
      [SHOPIFY_SHOP]
    );
    res.json(result.rows);
  } catch (e) {
    res.json([]);
  }
});

// 管理画面: ポイント手動変更
app.post('/admin/api/points/:customerId', adminAuth, async (req, res) => {
  const { customerId } = req.params;
  const { type, amount, reason } = req.body;

  try {
    let newPoints;
    if (type === 'add') {
      await pool.query(
        'UPDATE customer_points SET points = points + $1, total_earned = total_earned + $1, updated_at = NOW() WHERE customer_id = $2 AND shop_domain = $3',
        [amount, customerId, SHOPIFY_SHOP]
      );
      newPoints = amount;
    } else if (type === 'subtract') {
      await pool.query(
        'UPDATE customer_points SET points = GREATEST(points - $1, 0), updated_at = NOW() WHERE customer_id = $2 AND shop_domain = $3',
        [amount, customerId, SHOPIFY_SHOP]
      );
      newPoints = -amount;
    } else if (type === 'set') {
      const current = await pool.query(
        'SELECT points FROM customer_points WHERE customer_id = $1 AND shop_domain = $2',
        [customerId, SHOPIFY_SHOP]
      );
      const currentPoints = current.rows[0]?.points || 0;
      newPoints = amount - currentPoints;
      await pool.query(
        'UPDATE customer_points SET points = $1, updated_at = NOW() WHERE customer_id = $2 AND shop_domain = $3',
        [amount, customerId, SHOPIFY_SHOP]
      );
    }

    await pool.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason) VALUES ($1, $2, $3, 'manual', $4)",
      [customerId, SHOPIFY_SHOP, newPoints, reason]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.json({ ok: false });
  }
});

// キャンセル時ポイント取り消し
app.post('/webhook/orders-cancelled', async (req, res) => {
  const hmac = req.headers['x-shopify-hmac-sha256'];
  const hash = crypto
    .createHmac('sha256', SHOPIFY_WEBHOOK_SECRET)
    .update(req.body)
    .digest('base64');
  if (hmac !== hash) {
    console.warn('Webhook: invalid signature');
    return res.status(401).send('Unauthorized');
  }

  const order = JSON.parse(req.body);
  const customerId = order.customer?.id?.toString();
  if (!customerId) return res.status(200).send('no customer');

  try {
    // キャンセル済み注文からは新しいレビューとレビュー500ptを獲得できないようにする。
    await pool.query(
      `UPDATE review_purchases
       SET cancelled_at = COALESCE(cancelled_at, NOW()), updated_at = NOW()
       WHERE shop_domain = $1 AND order_id = $2`,
      [SHOPIFY_SHOP, order.id.toString()]
    );

    // 元の付与ログを確認
    const log = await pool.query(
      "SELECT points_change FROM point_logs WHERE order_id = $1 AND type = 'purchase'",
      [order.id.toString()]
    );
    if (log.rows.length === 0) return res.status(200).send('no points to cancel');

    const pointsToRemove = log.rows[0].points_change;

    // ポイントを減算（0未満にはならない）
    await pool.query(
      `UPDATE customer_points SET points = GREATEST(points - $1, 0), updated_at = NOW()
       WHERE customer_id = $2 AND shop_domain = $3`,
      [pointsToRemove, customerId, SHOPIFY_SHOP]
    );

    // ログに記録
    await pool.query(
      `INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason, order_id)
       VALUES ($1, $2, $3, 'manual', $4, $5)`,
      [customerId, SHOPIFY_SHOP, -pointsToRemove, `注文 #${order.order_number} キャンセルによるポイント取り消し`, order.id.toString()]
    );

    console.log(`✅ ポイント取り消し: customer=${customerId} -${pointsToRemove}pt (注文#${order.order_number})`);
    res.status(200).send('ok');
  } catch (e) {
    console.error('Cancel webhook error:', e);
    res.status(500).send('error');
  }
});

// 新規会員登録ポイント付与
app.post('/webhook/customers-created', async (req, res) => {
  const hmac = req.headers['x-shopify-hmac-sha256'];
  const hash = crypto.createHmac('sha256', SHOPIFY_WEBHOOK_SECRET).update(req.body).digest('base64');
  if (hmac !== hash) {
    console.warn('Webhook: invalid signature');
    return res.status(401).send('Unauthorized');
  }

  const customer = JSON.parse(req.body);
  const customerId = customer.id?.toString();
  const email = customer.email || '';
  if (!customerId) return res.status(200).send('no customer');

  const SIGNUP_POINTS = 100;

  try {
    const dup = await pool.query(
      "SELECT id FROM point_logs WHERE customer_id = $1 AND type = 'signup'",
      [customerId]
    );
    if (dup.rows.length > 0) return res.status(200).send('already processed');

    await pool.query(
      `INSERT INTO customer_points (customer_id, shop_domain, email, points, total_earned) VALUES ($1, $2, $3, $4, $4)
       ON CONFLICT (customer_id, shop_domain) DO UPDATE SET points = customer_points.points + $4, total_earned = customer_points.total_earned + $4, email = EXCLUDED.email, updated_at = NOW()`,
      [customerId, SHOPIFY_SHOP, email, SIGNUP_POINTS]
    );

    await pool.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason) VALUES ($1, $2, $3, 'signup', '新規会員登録ポイント')",
      [customerId, SHOPIFY_SHOP, SIGNUP_POINTS]
    );

    console.log(`✅ 新規登録ポイント付与: customer=${customerId} +${SIGNUP_POINTS}pt`);
    res.status(200).send('ok');
  } catch (e) {
    console.error('Signup webhook error:', e);
    res.status(500).send('error');
  }
});

// 顧客削除時のポイント削除
app.post('/webhook/customers-deleted', async (req, res) => {
  const hmac = req.headers['x-shopify-hmac-sha256'];
  const hash = crypto.createHmac('sha256', SHOPIFY_WEBHOOK_SECRET).update(req.body).digest('base64');
  if (hmac !== hash) {
    console.warn('Webhook: invalid signature');
    return res.status(401).send('Unauthorized');
  }

  const customer = JSON.parse(req.body);
  const customerId = customer.id?.toString();
  if (!customerId) return res.status(200).send('no customer');

  try {
    await pool.query('DELETE FROM customer_points WHERE customer_id = $1 AND shop_domain = $2', [customerId, SHOPIFY_SHOP]);
    await pool.query('DELETE FROM point_logs WHERE customer_id = $1 AND shop_domain = $2', [customerId, SHOPIFY_SHOP]);
    await pool.query('DELETE FROM gacha_history WHERE customer_id = $1', [customerId]);
    await pool.query('DELETE FROM reviews WHERE customer_id = $1 AND shop_domain = $2', [customerId, SHOPIFY_SHOP]);
    await pool.query('DELETE FROM review_purchases WHERE customer_id = $1 AND shop_domain = $2', [customerId, SHOPIFY_SHOP]);
    await pool.query('DELETE FROM customer_products WHERE customer_id = $1 AND shop_domain = $2', [customerId, SHOPIFY_SHOP]);

    console.log(`✅ 顧客削除: customer=${customerId}`);
    res.status(200).send('ok');
  } catch (e) {
    console.error('Customer delete webhook error:', e);
    res.status(500).send('error');
  }
});

// ガチャ回数取得
app.get('/gacha-count', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId) return res.json({ ok: false, count: 0 });
  try {
    const result = await pool.query(
      'SELECT COUNT(*) FROM gacha_history WHERE customer_id = $1',
      [customerId]
    );
    res.json({ ok: true, count: parseInt(result.rows[0].count) });
  } catch (e) {
    res.json({ ok: false, count: 0 });
  }
});

app.get('/my-orders', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId) return res.json({ ok: false, products: [] });
  try {
    const result = await pool.query(
      `WITH ranked_purchases AS (
         SELECT rp.id AS purchase_id,
                rp.product_id,
                rp.product_name,
                rp.image_url,
                rp.order_name,
                rp.purchased_at,
                COUNT(*) OVER (PARTITION BY rp.product_id) AS reviewable_count,
                ROW_NUMBER() OVER (
                  PARTITION BY rp.product_id
                  ORDER BY rp.purchased_at ASC NULLS LAST, rp.id ASC
                ) AS purchase_rank
         FROM review_purchases rp
         WHERE rp.customer_id = $1
           AND rp.shop_domain = $2
           AND rp.reviewed_at IS NULL
           AND rp.cancelled_at IS NULL
       ),
       purchase_candidates AS (
         SELECT purchase_id,
                product_id,
                product_name,
                image_url,
                order_name,
                purchased_at,
                reviewable_count
         FROM ranked_purchases
         WHERE purchase_rank = 1
       ),
       legacy_candidates AS (
         SELECT NULL::bigint AS purchase_id,
                cp.product_id,
                cp.product_name,
                cp.image_url,
                NULL::text AS order_name,
                NULL::timestamptz AS purchased_at,
                1::bigint AS reviewable_count
         FROM customer_products cp
         JOIN customer_points cpt
           ON cpt.customer_id = cp.customer_id
          AND cpt.shop_domain = cp.shop_domain
         WHERE cp.customer_id = $1
           AND cp.shop_domain = $2
           AND NOT EXISTS (
             SELECT 1
             FROM review_purchases rp
             WHERE rp.customer_id = cp.customer_id
               AND rp.shop_domain = cp.shop_domain
               AND rp.product_id = cp.product_id
           )
           AND NOT EXISTS (
             SELECT 1
             FROM reviews r
             WHERE r.product_id = cp.product_id
               AND r.shop_domain = cp.shop_domain
               AND (
                 r.customer_id = cp.customer_id
                 OR (cpt.email IS NOT NULL AND r.email = cpt.email)
               )
           )
       )
       SELECT * FROM purchase_candidates
       UNION ALL
       SELECT * FROM legacy_candidates
       ORDER BY purchased_at DESC NULLS LAST, product_name ASC`,
      [customerId, SHOPIFY_SHOP]
    );
    const products = result.rows.map(r => ({
      purchaseId: r.purchase_id || null,
      productId: r.product_id,
      productName: r.product_name,
      imageUrl: r.image_url || null,
      orderName: r.order_name || null,
      purchasedAt: r.purchased_at || null,
      reviewableCount: Number(r.reviewable_count || 1)
    }));
    res.json({ ok: true, products });
  } catch (e) {
    console.error('my-orders error:', e);
    res.json({ ok: false, products: [] });
  }
});

// 公開用景品一覧
app.get('/rewards', async (_req, res) => {
  try {
    const result = await pool.query(
      'SELECT name, rarity, reward_type, discount_amount, image_url, stock FROM rewards WHERE stock > 0 ORDER BY probability DESC'
    );
    res.json(result.rows);
  } catch (e) {
    res.json([]);
  }
});



app.listen(PORT, () => {
  console.log(`Gacha running on port ${PORT}`);
});
