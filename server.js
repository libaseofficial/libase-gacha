import express from 'express';
import cors from 'cors';
import pkg from 'pg';
import basicAuth from 'express-basic-auth';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import crypto from 'crypto';
import multer from 'multer';

const { Pool } = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use((req, res, next) => {
  if (req.path === '/webhook/orders-paid' || req.path === '/webhook/orders-cancelled' || req.path === '/webhook/customers-created') {
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
    return externalCode.code;
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
  const url = `https://${SHOPIFY_SHOP}/admin/oauth/authorize?client_id=${CLIENT_ID}&scope=read_price_rules,write_price_rules,read_discounts,write_discounts,read_customers,read_orders&redirect_uri=https://libase-gacha.onrender.com/callback&state=gacha123`;
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
      'SELECT reward_name, points_used, created_at, reward_code FROM gacha_history WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 10',
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
    const rewardCode = await issueRewardCode(reward);

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
      [customerId, reward.name, rewardCode, GACHA_COST]
    );

    console.log(`✅ ガチャ: customer=${customerId} -${GACHA_COST}pt → ${reward.name}`);
    res.json({
      ok: true,
      reward: reward.name,
      rarity: reward.rarity || 'normal',
      rewardCode,
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
  const { reward_id, codes } = req.body;
  const values = codes.map(code => `(${reward_id}, '${code}')`).join(',');
  await pool.query(`INSERT INTO external_codes (reward_id, code) VALUES ${values}`);
  res.json({ ok: true });
});

app.delete('/admin/api/external-codes/:id', adminAuth, async (req, res) => {
  await pool.query('DELETE FROM external_codes WHERE id = $1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.json({ ok: false, message: 'ファイルがありません' });
  }

  const fileUrl = `https://libase-gacha.onrender.com/uploads/${req.file.filename}`;
  res.json({ ok: true, url: fileUrl });
});

// レビューAPI
const REVIEW_POINTS = 500;

app.post('/reviews', async (req, res) => {
  const { customerId, productId, productName, authorName, email, rating, title, body, imageUrl } = req.body;
  if (!customerId || !productId || !rating) return res.json({ ok: false, message: '必須項目が不足しています' });

  const shopDomain = SHOPIFY_SHOP;
  try {
    const dup = await pool.query(
      'SELECT id FROM reviews WHERE customer_id = $1 AND product_id = $2',
      [customerId, productId]
    );
    if (dup.rows.length > 0) return res.json({ ok: false, message: 'この商品はすでにレビュー済みです' });

    await pool.query(
      'INSERT INTO reviews (customer_id, shop_domain, product_id, product_name, author_name, email, rating, title, body, image_url, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
      [
        customerId,
        shopDomain,
        productId,
        productName,
        authorName || '匿名',
        email || null,
        rating,
        title || null,
        body,
        imageUrl || null,
        'hidden'
      ]
    );

    await pool.query(
      `INSERT INTO customer_points (customer_id, shop_domain, points, total_earned) VALUES ($1, $2, $3, $3)
       ON CONFLICT (customer_id, shop_domain) DO UPDATE SET points = customer_points.points + $3, total_earned = customer_points.total_earned + $3, updated_at = NOW()`,
      [customerId, shopDomain, REVIEW_POINTS]
    );

    await pool.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason) VALUES ($1, $2, $3, 'review', 'レビュー投稿ポイント')",
      [customerId, shopDomain, REVIEW_POINTS]
    );

    console.log(`✅ レビューポイント付与: customer=${customerId} +${REVIEW_POINTS}pt`);
    res.json({ ok: true, points: REVIEW_POINTS });
  } catch (e) {
    console.error('Review error:', e);
    res.json({ ok: false, message: 'エラーが発生しました' });
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

// 公開用景品一覧
app.get('/rewards', async (_req, res) => {
  const result = await pool.query(
    'SELECT name, rarity, reward_type, discount_amount, image_url, stock FROM rewards WHERE stock > 0 ORDER BY probability DESC'
  );
  res.json(result.rows);
});

app.get('/admin/api/reviews', adminAuth, async (_req, res) => {
  try {
    const result = await pool.query('SELECT * FROM reviews ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (e) {
    res.json([]);
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

  const dup = await pool.query(
    "SELECT id FROM point_logs WHERE order_id = $1 AND type = 'purchase'",
    [order.id.toString()]
  );
  if (dup.rows.length > 0) return res.status(200).send('already processed');

  const totalPrice = parseFloat(order.subtotal_price || order.total_price || 0);
  const pointsToAdd = Math.floor(totalPrice / 100) * POINT_RATE;
  if (pointsToAdd <= 0) return res.status(200).send('no points');

  try {
    await pool.query(
      `INSERT INTO customer_points (customer_id, shop_domain, email, points, total_earned) VALUES ($1, $2, $3, $4, $4)
       ON CONFLICT (customer_id, shop_domain) DO UPDATE SET points = customer_points.points + $4, total_earned = customer_points.total_earned + $4, email = EXCLUDED.email, updated_at = NOW()`,
      [customerId, SHOPIFY_SHOP, email, pointsToAdd]
    );

    await pool.query(
      "INSERT INTO point_logs (customer_id, shop_domain, points_change, type, reason, order_id) VALUES ($1, $2, $3, 'purchase', $4, $5)",
      [customerId, SHOPIFY_SHOP, pointsToAdd, `注文 #${order.order_number} 購入ポイント`, order.id.toString()]
    );

    console.log(`✅ ポイント付与: customer=${customerId} +${pointsToAdd}pt (注文#${order.order_number})`);
    // ↓ここに追加
for (const item of order.line_items || []) {
  if (!item.product_id) continue;
  await pool.query(
`INSERT INTO customer_products (customer_id, shop_domain, product_id, product_name, image_url)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (customer_id, shop_domain, product_id) DO NOTHING`,
[customerId, SHOPIFY_SHOP,item.product_handle, item.title, item.image?.src || null]
  );
}
    res.status(200).send('ok');
  } catch (e) {
    console.error('Webhook error:', e);
    res.status(500).send('error');
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
      `SELECT cp.product_id, cp.product_name, cp.image_url
       FROM customer_products cp
       WHERE cp.customer_id = $1 AND cp.shop_domain = $2
       AND NOT EXISTS (
         SELECT 1 FROM reviews r
         WHERE r.customer_id = cp.customer_id
         AND r.product_id = cp.product_id
       )`,
      [customerId, SHOPIFY_SHOP]
    );
    const products = result.rows.map(r => ({
      productId: r.product_id,
      productName: r.product_name,
      imageUrl: r.image_url || null
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
