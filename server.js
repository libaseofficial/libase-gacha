import express from 'express';
import Database from 'better-sqlite3';

const app = express();
const db = new Database('./gacha.db');

app.use(express.json());

// DB
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

// 初期データ
const count = db.prepare('SELECT COUNT(*) as c FROM rewards').get().c;

if (count === 0) {
  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)')
    .run('500円OFF', 70, 100);

  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)')
    .run('1000円OFF', 25, 50);

  db.prepare('INSERT INTO rewards (name, probability, stock) VALUES (?, ?, ?)')
    .run('無料', 5, 5);
}

// 抽選
function draw() {
  const rewards = db.prepare('SELECT * FROM rewards WHERE stock > 0').all();
  const total = rewards.reduce((sum, r) => sum + r.probability, 0);

  let rand = Math.random() * total;

  for (const r of rewards) {
    if (rand < r.probability) return r;
    rand -= r.probability;
  }
}

// verify
app.post('/verify', (req, res) => {
  const { customerId, coupon } = req.body;

  try {
    db.prepare('INSERT INTO spin_tickets (customer_id, coupon_code) VALUES (?, ?)')
      .run(customerId, coupon);

    res.json({ ok: true });
  } catch {
    res.json({ ok: false });
  }
});

// spin
app.post('/spin', (req, res) => {
  const { customerId, coupon } = req.body;

  const ticket = db.prepare(
    "SELECT * FROM spin_tickets WHERE coupon_code = ? AND status = 'verified'"
  ).get(coupon);

  if (!ticket) {
    return res.json({ ok: false });
  }

  const reward = draw();

  db.prepare('UPDATE rewards SET stock = stock - 1 WHERE id = ?')
    .run(reward.id);

  db.prepare("UPDATE spin_tickets SET status = 'used' WHERE id = ?")
    .run(ticket.id);

  res.json({ ok: true, reward: reward.name });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Gacha running on port ${PORT}`);
});