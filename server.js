import express from 'express';
import cors from 'cors';
import pkg from 'pg';
import basicAuth from 'express-basic-auth';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const { Pool } = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(join(__dirname, 'public')));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const SHOPIFY_SHOP = 's62nix-7r.myshopify.com';
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
let ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || '';

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

app.get('/', (_req, res) => {
  res.send('LIBASE Gacha Server is running');
});

app.get('/install', (_req, res) => {
  const url = `https://${SHOPIFY_SHOP}/admin/oauth/authorize?client_id=${CLIENT_ID}&scope=read_price_rules,write_price_rules,read_discounts,write_discounts,read_customers&redirect_uri=https://libase-gacha.onrender.com/callback&state=gacha123`;
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
  res.send('インストール完了しました。このページを閉じてください。');
});

app.get('/points', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId || !ACCESS_TOKEN) {
    return res.json({ ok: false, points: 0 });
  }
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

app.get('/history', async (req, res) => {
  const { customerId } = req.query;
  if (!customerId) return res.json({ ok: false, history: [] });
  try {
    const result = await pool.query(
      'SELECT reward_name, points_used, created_at FROM gacha_history WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 10',
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
    await pool.query(
      'INSERT INTO spin_tickets (customer_id, coupon_code) VALUES ($1, $2)',
      [customerId, coupon]
    );
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
    if (ticketResult.rows.length === 0) {
      return res.json({ ok: false });
    }
    const ticket = ticketResult.rows[0];

    if (ACCESS_TOKEN) {
      const shopifyRes = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2025-01/discount_codes/lookup.json?code=${coupon}`,
        { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
      );
      console.log('lookup status:', shopifyRes.status);
      if (!shopifyRes.ok) {
        return res.json({ ok: false, message: '無効なコードです' });
      }
      const shopifyData = await shopifyRes.json();
      console.log('shopifyData:', JSON.stringify(shopifyData));
      const { id, price_rule_id } = shopifyData.discount_code;
      console.log('deleting id:', id, 'price_rule_id:', price_rule_id);
      const deleteRes = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2025-01/price_rules/${price_rule_id}/discount_codes/${id}.json`,
        {
          method: 'DELETE',
          headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN }
        }
      );
      console.log('delete status:', deleteRes.status);
    }
    
    const reward = await draw();
    if (!reward) return res.json({ ok: false, message: '景品がありません' });
    
    await pool.query('UPDATE rewards SET stock = stock - 1 WHERE id = $1', [reward.id]);
    await pool.query("UPDATE spin_tickets SET status = 'used' WHERE id = $1", [ticket.id]);
    await pool.query(
      'INSERT INTO gacha_history (customer_id, reward_name) VALUES ($1, $2)',
      [customerId, reward.name]
    );
    
    res.json({ ok: true, reward: reward.name, rarity: reward.rarity || 'normal' });
    } catch (e) {
      console.error(e);
      res.json({ ok: false, message: 'エラーが発生しました' });
    }
    });

// 管理画面
app.get('/admin', adminAuth, (_req, res) => {
  res.sendFile(join(__dirname, 'public', 'admin.html'));
});

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
  const result = await pool.query(
    'SELECT * FROM gacha_history ORDER BY created_at DESC LIMIT 50'
  );
  res.json(result.rows);
});

app.listen(PORT, () => {
  console.log(`Gacha running on port ${PORT}`);
});