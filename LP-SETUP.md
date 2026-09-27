# LIBASE LP限定・全品10%OFFの追加

2026-09-28 / 既存 `libase-gacha` 向け。

## 追加内容

- `server.js`：LPルーターの接続、LP経路だけのJSON/CORS処理、署名確認済み購入Webhookから利用済み状態の通知。
- `lp/router.mjs`：`/lp/offer`（LPで受取）、`/lp/status`（既存の権利を確認）、`/lp/health`。
- `lp/offers.mjs`：Shopifyとの連携、初回LPアクセスから24時間で失効、署名付き訪問トークン、PostgreSQL保存。
- `lp/operations.mjs`：Shopify Admin GraphQL 2026-07。
- `lp/schema.sql`：新しい `libase_lp_visits` テーブル。LPを有効にして初めてアクセスすると自動作成する。
- `lp/test.mjs`：ローカルテスト。

`package.json`・依存関係・起動コマンド・レビュー管理画面・既存のマイグレーションは変更していない。既存 `DATABASE_URL` とShopifyアクセストークンを再利用する。新たなRenderサービスや永続ディスクを作ることは前提にしていない。既存DBの空き容量・接続枠・実際の権限は管理画面での確認が必要。

## 導入

1. ZIPを解凍し、GitHubの `libaseofficial/libase-gacha` に `server.js` を差し替え、`lp` フォルダを追加する。`LP-SETUP.md` は案内用。
2. Renderの自動デプロイ完了を確認する。この段階では `LP_ENABLED` が未設定ならLP機能は無効で、既存機能は今までの構成で動く。
3. Renderの `libase-gacha` → Environmentで、以下の環境変数を追加してデプロイする。

| 変数 | 値 |
|---|---|
| `LP_ENABLED` | `true` |
| `LP_VISITOR_SECRET` | ランダムな64桁の16進文字列 |
| `LP_CAMPAIGN_ID` | `libase-swipe-2026` |
| `LP_STOREFRONT_ORIGIN` | `https://libase.shop` |
| `LP_MAX_NEW_OFFERS_PER_DAY` | `1000`（省略可） |

秘密の文字列は自分のPCのターミナルで `openssl rand -hex 32` を実行して生成し、Renderの環境変数へ貼り付ける。通常運用中は秘密の文字列とキャンペーンIDを変更しない。GitHubやShopifyテーマには入れない。

`DATABASE_URL`、`SHOPIFY_CLIENT_ID`、`SHOPIFY_CLIENT_SECRET`、既存のShopifyトークン、管理画面パスワード等は現在の設定を継続して使用する。

4. `https://libase-gacha.onrender.com/lp/health` を開いて `enabled: true` と `configured: true` を確認する。これは設定が読み込めたことの確認で、DB接続やShopify権限までの確認ではない。
5. 更新版テーマをShopifyへ追加し、「テーマ設定 → LIBASE LP特典」を開く。ベースURLに `https://libase-gacha.onrender.com/lp`、キャンペーンIDに同じ `libase-swipe-2026` を設定し、特典を有効にする。
6. 未公開テーマの通常プレビューでLPを開き、実際の受取・移動・カート・チェックアウトを確認する。テーマエディタ内は見本表示でありクーポンを発行しない。

既存コードのインストール要求には `read_discounts` と `write_discounts` が含まれている。ただし、実際のインストール済みアプリの付与権限は未確認。権限エラー時はShopify側の既存アプリで両権限を確認・更新する。再認証が必要になる場合がある。

## 期限と権利

- 初回訪問の記録を先にDBへ確定し、Shopifyで同じ期限の10%OFFクーポンを作る。
- 期限は最初のLPアクセスから24時間（86,400,000ミリ秒）。例：9月27日23:30 JSTに訪問した場合、9月28日23:30 JSTに失効。残り時間と実際の締切日時を表示する。
- `offer` は受取専用。`status` は署名を検証して既存の権利を返すだけで、新しい訪問記録やコードを作らない。
- ブラウザには訪問IDと署名付きトークンを保存し、DBでは生の訪問IDを保存しない。
- 同じ訪問IDは再読み込み・再訪問・タブの開き直しでも最初の期限を維持する。新しいブラウザセッションごとに24時間へ戻さない。日次のクーポン更新処理やCronは不要。
- PostgreSQLのトランザクション・アドバイザリロックで重複を防ぎ、Shopifyへの応答が途切れても決定的な同じコードで復帰する。
- 新規発行上限は日本時間の暦日ごとに1,000件。これは発行数の集計期間であり、個別クーポンの24時間の有効期間とは別。再訪問は新規発行数に含めない。
- `libase_lp_visits` にはRLSを有効化し、Supabase等の公開クライアント用ロール向けのポリシーは追加しない。DBテーブルの所有者、またはRLSを回避できるサーバーロールで接続する。
- 利用済みWebhookで早めに表示を止め、Shopifyの利用回数も最大約60秒間隔で再確認する。Shopifyの利用回数は非同期更新のため表示の反映に遅れ得る。実際の使用回数はShopifyが1回に制限する。
- ログインによる本人確認ではない。別端末・別ブラウザ・保存情報の削除は新しい訪問として扱われ得る。コード共有も完全には防げず、共有先が先に利用すると元の訪問者は使えない。

## 以前の当日限定版からの更新

テーマとサーバーの両方を更新する。環境変数やDBスキーマの追加変更は不要。更新後の新規訪問記録は `created_at + 24時間` で作成する。

既存の訪問記録とShopifyコードの期限は変更しない。発行済み・発行予約済みのものは元の `ends_at` を維持し、期限切れでも再発行しない。キャンペーンIDや秘密の文字列を変更したり、訪問記録を削除したりして期限をリセットしない。

## 割引と既存の割引

全商品の通常購入を対象に、現在の販売価格から10%OFF。セール価格の商品もさらに10%OFF。ギフトカード・定期購入は対象外。送料には適用しない。1コード1回、他のクーポン・商品割引・注文割引・送料割引との併用は無効。

Shopifyがより有利な既存割引を選ぶケースや、アプリ独自価格・追加料金・端数処理は実ストアで確認する。テーマは商品データを基に「LP限定 10%OFF適用時」の価格を表示し、カート合計はShopifyの計算結果を表示する。

## 停止・戻し方

- 新規発行と権利確認を止める：Renderの `LP_ENABLED=false` を保存してデプロイ。
- 特典表示を止める：テーマ設定の「LP限定特典を有効にする」をオフ。
- 発行済みのShopifyクーポンは、サーバーやテーマをオフにしただけでは無効化されない。すぐ停止したい場合はShopify管理画面のディスカウントで対象コードを無効にする。それ以外は各コードに設定された期限で自動失効。
- アプリのコードを戻す：導入前のGitHubコミットに戻してデプロイ。新テーブルは残しても既存のレビュー・ガチャ・ポイントには使用しない。

## 検証

`node --test lp/test.mjs`

9件のローカルテストで、深夜23:30の訪問から翌日の同時刻までの24時間、日付をまたぐ再訪、期限直前・期限到達、旧記録の期限維持、同時受取、応答欠落、署名改ざん、読み取り専用status、発行上限、利用済み、Shopifyへの全商品指定、CORS・JSONサイズ制限をテスト。別途PGliteで実際のPostgreSQL SQL、トランザクション、RLS、新テーブル以外のデータ保持を確認。実際のRender DB・本番Shopify APIへの接続、実決済、既存機能全体の回帰確認は未実施。

参考：
- https://shopify.dev/docs/api/admin-graphql/2026-07/mutations/discountCodeBasicCreate
- https://shopify.dev/docs/api/admin-graphql/2026-07/input-objects/DiscountCodeBasicInput
- https://shopify.dev/docs/api/ajax/reference/cart
- https://help.shopify.com/en/manual/discounts/managing-discounts
