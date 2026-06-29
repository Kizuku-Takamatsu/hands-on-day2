# Databricks notebook source
# MAGIC %md
# MAGIC # ④ Genieとダッシュボード — 取引データに答える"データ担当"を作る
# MAGIC
# MAGIC 受注トランザクションに、自然言語で答える Genie を用意します（⑤で再利用）。最後にアプリ化まで通します。
# MAGIC
# MAGIC ## このノートの目的（所要：約45分）
# MAGIC - 取引ログ（`orders_silver` / `sales_monthly_gold`）の **Genie Space** を作る（⑤で再利用）
# MAGIC - AI/BI ダッシュボードで可視化する
# MAGIC - Genie を **Playground** に接続し、**Databricks Apps** のチャットアプリとして公開する
# MAGIC
# MAGIC > このノートは画面操作（GUI）が中心です。準備・確認はコードで、作成は手順に沿ってクリックで進めます。

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 準備確認：使うテーブルと、自分のスキーマ名
# MAGIC Genie / ダッシュボードで使うテーブルが用意できているか確認します（名前は Genie 作成時に選びます）。

# COMMAND ----------

# DBTITLE 1,セル4
# このセルを実行すると、Genie の名前、ダッシュボード用SQL、General Instructions が「コピペ用」に出力されます
genie_name = "tx-genie-" + schema.replace("handson_", "")
dashboard_sql = (
    f"SELECT order_month, market_segment, total_sales, n_orders, avg_order_value\n"
    f"FROM {catalog}.{schema}.sales_monthly_gold"
)

# Genie Space の General Instructions テンプレート
general_instructions = f"""このGenie Spaceは取引データの分析を支援します。

## 使用するテーブル
- {catalog}.{schema}.orders_silver: 受注トランザクション（クレンジング済み）
- {catalog}.{schema}.sales_monthly_gold: 月次売上集計

## 主要なカラムと意味
- order_id: 注文ID
- order_date: 注文日
- order_amount: 注文金額
- market_segment: 市場セグメント
- total_sales: 総売上（集計値）
- n_orders: 注文件数
- avg_order_value: 平均注文額
- order_month: 注文月（YYYY-MM形式）

## 用語の対応
- 「売上」「売上高」→ total_sales
- 「注文数」「受注件数」→ n_orders
- 「平均単価」「平均注文額」→ avg_order_value
- 「セグメント」「市場区分」→ market_segment

常に日本語で回答してください。"""

print("===== ① Genie Space の名前（コピーして使う）=====")
print(genie_name)
print()
print("===== ② Genie Space の General Instructions（コピーして使う）=====")
print(general_instructions)
print()
print("===== ③ ダッシュボードの『データ』タブに貼り付けるSQL（コピーして使う）=====")
print(dashboard_sql)
print()
print(f"使うテーブル : {catalog}.{schema}.orders_silver / sales_monthly_gold")
display(spark.sql("SELECT * FROM sales_monthly_gold ORDER BY order_month LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,セル5
# MAGIC %md
# MAGIC ## ステップ1：Genie Space を作る（名前は ⑤ で参照するため固定）
# MAGIC 1. 左メニュー **「Genie」→「新規」**
# MAGIC 2. データに、上で確認した **`<スキーマ>.orders_silver` と `sales_monthly_gold`** を追加（SQL Warehouse 2X-Small を起動）
# MAGIC 3. 名前は **上の準備セルで出力された名前（`tx-genie-…`）をコピーして**設定（⑤で参照するため固定）
# MAGIC 4. **General Instructions を設定**：上の準備セルで出力された **General Instructions をコピーして貼り付け**（テーブルやカラムの意味、用語の対応をGenieに教えて精度向上）
# MAGIC 5. 日本語で質問して動作確認：
# MAGIC    - 「市場セグメント別の総売上を多い順に教えて」
# MAGIC    - 「注文 ORD-XXX の金額と注文日は？」（XXX は `orders_silver` の実 `order_id` に置換）
# MAGIC    - 「月別の受注件数の推移を折れ線で」
# MAGIC 6. Genie が生成した **SQL を開いて確認**（裏で何を実行したか見える＝ガバナンス上も安心）
# MAGIC
# MAGIC > 💡 General Instructions で用語を定義すると、「売上」や「注文数」などの日本語が正しくカラムに対応され、回答精度が向上します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ2：AI/BI ダッシュボードで可視化
# MAGIC 1. 左メニュー **「ダッシュボード」→ 新規作成**
# MAGIC 2. **「データ」タブ**に、**上の準備セルで出力されたSQLをコピーして貼り付け**、データセットを作成
# MAGIC    （内容は下記。実際は準備セルの出力＝自分のカタログ・スキーマ入りをコピーしてください）
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_month, market_segment, total_sales, n_orders, avg_order_value
# MAGIC FROM <自分のカタログ>.<自分のスキーマ>.sales_monthly_gold
# MAGIC ```
# MAGIC
# MAGIC 3. **「キャンバス」タブ**でビジュアルを追加：
# MAGIC    - 折れ線：X=`order_month` / Y=`total_sales` / 色=`market_segment`（月次売上推移）
# MAGIC    - 棒：`market_segment` 別の `total_sales`
# MAGIC    - カウンター：`total_sales` の合計（KPIカード）
# MAGIC 4. **フィルター**（`market_segment`）を1つ置く
# MAGIC 5. 右上 **「公開 / Publish」** → 共有リンクで他メンバーに見せる
# MAGIC 6. ダッシュボード上部の **「Ask Genie」** から、その場で追加の自然言語質問もできる

# COMMAND ----------

# DBTITLE 1,セル7
# MAGIC %md
# MAGIC ## ステップ3：Playground に接続して、アプリにする
# MAGIC 1. 左メニュー **「Playground」** を開く
# MAGIC 2. ツール/エージェントとして、作った **Genie Space（`tx-genie…`）** を追加し、日本語で質問して動作確認
# MAGIC 3. 右上の **「Get code（コードを取得）」→「Export to Databricks Apps（Databricks Appsにエクスポート）」**
# MAGIC 4. エクスポート画面で：
# MAGIC    - アプリ名は **`agent-` で始まる小文字英数とハイフン**（例 `agent-tx-genie-thori`）
# MAGIC    - 説明を入力
# MAGIC 5. **MLflow Experiment の設定（エクスポート画面内で作成）：**
# MAGIC    - 「新規作成」をクリック
# MAGIC    - **「Unity Catalog (Beta)」** と **「エクスペリメント内」** の選択肢が表示されるので、**「エクスペリメント内」を選択**
# MAGIC    - Experiment を作成
# MAGIC 6. 「エクスポート」をクリック
# MAGIC 7. 生成された **チャットUI付きアプリ**が Databricks Apps にデプロイされる → ブラウザでアプリを開いて Genie に質問
# MAGIC
# MAGIC > ⚠️ Free Edition は Apps 最大3個・起動から24hで自動停止（再起動可）。アプリ名は `agent-...` 形式が必須。
# MAGIC > 💡 SQLを書けない人が、ブラウザのアプリから自然言語でデータに聞ける＝民主化の到達点。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - 取引データの Genie（`tx-genie`）を作り、ダッシュボードとアプリで誰でも使える形にした
# MAGIC - ただし Genie は「社内ルール」は知らない —— 次の⑤で③の `rules_index`（ルール担当）と束ねます
# MAGIC 次は **⑤（ツール作成 → スーパーバイザー）** へ。