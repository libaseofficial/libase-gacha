-- Allow repeat-purchase reviews without allowing unlimited 500pt awards.
--
-- New rule:
--   one Shopify order line -> at most one review -> at most one 500pt award
--
-- Run this migration BEFORE deploying the matching server.js.

BEGIN;

-- The original schema enforced one review per customer/product forever.
-- Existing duplicate rows are preserved; only the obsolete constraint is removed.
ALTER TABLE public.reviews
  DROP CONSTRAINT IF EXISTS reviews_customer_id_product_id_key;

CREATE TABLE IF NOT EXISTS public.review_purchases (
  id BIGSERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,
  shop_domain TEXT NOT NULL,
  order_id TEXT NOT NULL,
  order_name TEXT,
  line_item_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  product_name TEXT NOT NULL,
  image_url TEXT,
  quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
  purchased_at TIMESTAMPTZ,
  reviewed_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  review_id INTEGER REFERENCES public.reviews(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (shop_domain, order_id, line_item_id)
);

ALTER TABLE public.reviews
  ADD COLUMN IF NOT EXISTS purchase_id BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'reviews_purchase_id_fkey'
      AND conrelid = 'public.reviews'::regclass
  ) THEN
    ALTER TABLE public.reviews
      ADD CONSTRAINT reviews_purchase_id_fkey
      FOREIGN KEY (purchase_id)
      REFERENCES public.review_purchases(id);
  END IF;
END $$;

-- A purchase can never issue a second review, even under simultaneous requests.
CREATE UNIQUE INDEX IF NOT EXISTS reviews_purchase_id_unique
  ON public.reviews(purchase_id)
  WHERE purchase_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS review_purchases_customer_queue_idx
  ON public.review_purchases(customer_id, shop_domain, reviewed_at, cancelled_at, purchased_at);

CREATE INDEX IF NOT EXISTS review_purchases_product_idx
  ON public.review_purchases(product_id);

COMMIT;

-- Optional verification after deployment:
-- SELECT id, customer_id, order_name, product_id, reviewed_at, cancelled_at
-- FROM public.review_purchases
-- ORDER BY id DESC
-- LIMIT 20;
