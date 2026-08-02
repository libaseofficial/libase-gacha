# Review image permanent URL fix

## What was wrong

`fileCreate` processes images asynchronously. The previous code used:

```js
file?.image?.url || target.resourceUrl
```

When Shopify had not finished processing the image yet, `image.url` was null and the temporary staged-upload URL was returned and saved to Supabase. That temporary URL later expired.

## What changed

- Shopify Files GraphQL calls use stable API version `2026-07`.
- After `fileCreate`, the server polls the file ID until `fileStatus` is `READY` and `image.url` exists.
- The server never falls back to `target.resourceUrl`.
- All review create/update endpoints reject `shopify-staged-uploads.../tmp/` URLs.
- The upload response now also includes the Shopify file ID.
- A Supabase CHECK constraint migration is included as a second safety layer.

## Optional Render environment variable

```text
SHOPIFY_FILES_API_VERSION=2026-07
```

The code already defaults to `2026-07`, so this variable is optional.

## Supabase safety migration

Run this file in Supabase SQL Editor after deploying the code:

```text
migrations/20260802_prevent_temporary_review_image_urls.sql
```

The constraint is added as `NOT VALID`, so existing broken URLs remain editable while all new inserts and updates are protected.
