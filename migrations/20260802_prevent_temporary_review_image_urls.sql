-- Prevent new Shopify staged-upload URLs from being saved to reviews.image_url.
-- NOT VALID keeps existing broken rows in place so they can be repaired first,
-- while still enforcing the rule for all new inserts and updates.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'reviews_image_url_must_be_permanent'
      AND conrelid = 'public.reviews'::regclass
  ) THEN
    ALTER TABLE public.reviews
      ADD CONSTRAINT reviews_image_url_must_be_permanent
      CHECK (
        image_url IS NULL
        OR image_url !~ '^https://shopify-staged-uploads\.storage\.googleapis\.com/tmp/'
      ) NOT VALID;
  END IF;
END $$;

-- After all existing staged URLs have been replaced with permanent CDN URLs,
-- run this separately to validate the historical rows too:
-- ALTER TABLE public.reviews VALIDATE CONSTRAINT reviews_image_url_must_be_permanent;
