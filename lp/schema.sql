CREATE TABLE IF NOT EXISTS libase_lp_visits (
  visitor_key TEXT PRIMARY KEY,
  campaign TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  ends_at BIGINT NOT NULL,
  code TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','active','expired','used')),
  checked_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS libase_lp_visits_campaign_created ON libase_lp_visits(campaign, created_at);
ALTER TABLE libase_lp_visits ENABLE ROW LEVEL SECURITY;
