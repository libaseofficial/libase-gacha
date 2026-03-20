import express from 'express';
import cors from 'cors';
import Database from 'better-sqlite3';

const app = express();
const db = new Database('./gacha.db');
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const SHOPIFY_SHOP = 's62nix-7r.myshopify.com';
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
let ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || '';

db.exec(`
CREATE TABLE IF NOT EXISTS spin_tickets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT,
  coupon_code TEXT UNIQUE,
  status TEXT DEFAULT 'verified'
);

CREATE TABLE IF NOT EXISTS rewards (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  probability INTEGER,
  stock INTEGER
);
`);

const count = db.prepare('SELECT COUNT(*) as c FROM rewards').get().c;
if (count === 0) {
  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)').run('500円OFF', 70, 100);
  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)').run('1000円OFF', 25, 50);
  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)').run('無料', 5, 5);
}

function draw() {
  const rewards = db.prepare('SELECT * FROM rewards WHERE stock > 0').all();
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
  const url = `https://${SHOPIFY_SHOP}/admin/oauth/authorize?client_id=${CLIENT_ID}&scope=read_price_rules,write_price_rules,read_discounts,write_discounts&redirect_uri=https://libase-gacha.onrender.com/callback&state=gacha123`;
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

// ポイント残高取得
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
    const points = pointField ? parseInt(pointField.value) : 0;
    res.json({ ok: true, points });
  } catch (e) {
    res.json({ ok: false, points: 0 });
  }
});

app.post('/verify', (req, res) => {
  const { customerId, coupon } = req.body;
  try {
    db.prepare('INSERT INTO spin_tickets (customer_id, coupon_code) VALUES (?, ?)').run(customerId, coupon);
    res.json({ ok: true });
  } catch {
    res.json({ ok: false });
  }
});

app.post('/spin', async (req, res) => {
  const { customerId, coupon } = req.body;

  const ticket = db.prepare(
    "SELECT * FROM spin_tickets WHERE coupon_code = ? AND status = 'verified'"
  ).get(coupon);

  if (!ticket) return res.json({ ok: false });

  if (ACCESS_TOKEN) {
    const shopifyRes = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2025-01/discount_codes/lookup.json?code=${coupon}`,
      { headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN } }
    );
    if (!shopifyRes.ok) {
      return res.json({ ok: false, message: '無効なコードです' });
    }
    const shopifyData = await shopifyRes.json();
    const { id, price_rule_id } = shopifyData.discount_code;
    await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2025-01/price_rules/${price_rule_id}/discount_codes/${id}.json`,
      {
        method: 'DELETE',
        headers: { 'X-Shopify-Access-Token': ACCESS_TOKEN }
      }
    );
  }

  const reward = draw();
  db.prepare('UPDATE rewards SET stock = stock - 1 WHERE id = ?').run(reward.id);
  db.prepare("UPDATE spin_tickets SET status = 'used' WHERE id = ?").run(ticket.id);
  res.json({ ok: true, reward: reward.name });
});

app.listen(PORT, () => {
  console.log(`Gacha running on port ${PORT}`);
});