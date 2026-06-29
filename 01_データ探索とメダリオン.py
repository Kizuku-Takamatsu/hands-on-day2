# Databricks notebook source
# MAGIC %md
# MAGIC ## 01. メダリオンアーキテクチャに基づいたデータエンジニアリング概要
# MAGIC
# MAGIC ## 本ノートブックの目的：Databricksにおけるデータ処理の基礎と[メダリオンアーキテクチャ](https://www.databricks.com/jp/glossary/medallion-architecture)について理解を深める
# MAGIC
# MAGIC 題材は、本研修を通じて使う **受注データ（`samples.tpch` の注文・顧客）** です。これを生データに見立てて Bronze→Silver→Gold を体験します。

# COMMAND ----------

# MAGIC %md
# MAGIC ![メダリオンアーキテクチャ](https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_01__introduction/delta-lake-medallion-architecture-2.jpeg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### メダリオンアーキテクチャとは
# MAGIC データを、Bronze、Silver、Goldの３層の論理レイヤーで管理する手法です。Databricks では、すべてのレイヤーを Delta Lake 形式で保持することが推奨されています。
# MAGIC
# MAGIC | #    | データレイヤー | 概要 | 類義語 |
# MAGIC | ---- | -------------- | ---- | ------ |
# MAGIC | 1    | Bronze | 未加工データを保持するレイヤー | Raw |
# MAGIC | 2    | Silver | クレンジング・適合済みデータを保持するレイヤー | Enriched |
# MAGIC | 3    | Gold   | ビジネスレベルのキュレート済みデータを保持するレイヤー | Curated |
# MAGIC
# MAGIC 次のメリットがあります。
# MAGIC - データレイヤーごとの役割分担が可能となること
# MAGIC - データレイクにてデータ品質が担保できるようになること
# MAGIC - ローデータから再度テーブルの再作成が容易となること
# MAGIC
# MAGIC **Bronzeの特徴について**
# MAGIC - 取り込んだローデータのコピーを、スキーマ展開を許可するなど、そのまま保持。
# MAGIC - ロード日時などの監査列（システム列）を必要に応じて付加。
# MAGIC - データ型を文字型として保持するなどの対応によりデータ損失の発生を低減。
# MAGIC - データを削除する場合には、物理削除ではなく、論理削除が推奨。
# MAGIC
# MAGIC **Silverの特徴について**
# MAGIC - Bronze のデータに処理を行い、クレンジング・適合済みデータを保持。
# MAGIC - スキーマを適用し、dropDuplicates関数を利用した重複排除などによるデータ品質チェック処理を実施。
# MAGIC - 最小限、あるいは「適度な」変換およびデータクレンジングルールのみを適用。
# MAGIC - Bronze との関係性が、「1 対多」方式となることもある。
# MAGIC
# MAGIC **Goldの特徴について**
# MAGIC - 企業や部門のデータプロジェクトにおいてビジネス上の課題を解決するように編成・集計したデータを保持。
# MAGIC - BI・機械学習・Genie がそのまま使えるデータセット。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ0：題材データを確認する
# MAGIC `samples.tpch.orders`（架空の受注）の規模感を見ます。

# COMMAND ----------

# 取り込み元（生データ）置き場と、Auto Loader チェックポイント置き場の Volume を作成
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.checkpoints")
orders_raw = f"/Volumes/{catalog}/{schema}/raw/orders"
custs_raw  = f"/Volumes/{catalog}/{schema}/raw/customers"
chk_dir    = f"/Volumes/{catalog}/{schema}/checkpoints"
print("orders_raw:", orders_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Bronze テーブルのパイプラインを作成する
# MAGIC 実務では外部システムから CSV 等が「生データ」として届きます。ここでは tpch の受注・顧客を CSV に書き出し、
# MAGIC **わざと"汚い"生データ**（全列が文字列・重複行を含む）にして、後の Silver でのクレンジング効果が分かるようにします。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例：`med_orders__bronze`（受注の生データ取り込み）

# COMMAND ----------

# 受注データを「全列文字列」で取り出す（＝届いたままの生データのイメージ）。研修用に少量サンプル。
orders_base = spark.sql("""
  SELECT CAST(o_orderkey  AS STRING) AS order_id,
         CAST(add_months(o_orderdate, 336) AS STRING) AS order_date,  -- 研修用に
         CAST(o_orderpriority AS STRING) AS order_priority,
         CAST(o_totalprice    AS STRING) AS order_amount,
         CAST(o_custkey       AS STRING) AS cust_id
  FROM samples.tpch.orders
  WHERE add_months(o_orderdate, 336) BETWEEN DATE'2020-01-01' AND DATE'2026-05-31'
""").sample(fraction=0.02, seed=42)

# クレンジングパートで差分がわかりやすいよう、生データに欠損があるなど、汚いデータで届いた設定にします
from pyspark.sql import functions as F
dup_rows   = orders_base.sample(fraction=0.30, seed=1)                                  
bad_amount = (orders_base.sample(fraction=0.08, seed=2)
              .withColumn("order_id", F.concat(F.lit("90"), F.col("order_id")))
              .withColumn("order_amount", F.lit("")))
bad_date   = (orders_base.sample(fraction=0.08, seed=3)
              .withColumn("order_id", F.concat(F.lit("91"), F.col("order_id")))
              .withColumn("order_date", F.lit("")))
orders_dirty = orders_base.unionByName(dup_rows).unionByName(bad_amount).unionByName(bad_date)
orders_dirty.write.mode("overwrite").option("header", True).csv(orders_raw)

# 作成した不良データの統計を表示
print("=== 生データ(CSV)の作成結果 ===")
print(f"正常データ: {orders_base.count()} 行")
print(f"重複データ: {dup_rows.count()} 行(意図的に追加)")
print(f"金額欠損: {bad_amount.count()} 行(order_id が '90' で始まる)")
print(f"日付欠損: {bad_date.count()} 行(order_id が '91' で始まる)")
print(f"合計: {orders_dirty.count()} 行")
print("\n【不良データのサンプル】")
display(orders_dirty.filter((F.col("order_amount") == "") | (F.col("order_date") == "")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 書き出したファイルを UI で見てみよう
# MAGIC 左メニュー **「カタログ」→ 自分のカタログ → 自分のスキーマ → Volumes → `raw` → `orders`** を開くと、
# MAGIC 今書き出した CSV ファイルが置かれているのが確認できます（Volume＝ファイルの置き場）。

# COMMAND ----------

# CSV の中身を先頭だけチェック（生データの確認）
first_csv = [f.path for f in dbutils.fs.ls(orders_raw) if f.path.endswith(".csv")][0]
df_preview = spark.read.format("csv").option("header", True).option("inferSchema", False).load(first_csv)
display(df_preview.limit(10))

# COMMAND ----------

# DBTITLE 1,セル13
from pyspark.sql import functions as F

# CSV ファイルから読み込んで _metadata を利用
df = (spark.read.format("csv").option("header", True).option("inferSchema", False).load(orders_raw))
df = (df.select("*", "_metadata")
        .withColumn("_datasource", F.col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", F.col("_metadata.file_modification_time"))
        .drop("_metadata"))

# Bronze テーブルに書き込む前に、読み込んだデータの内容を確認
print("=== CSV から読み込んだデータの統計(Bronze 書き込み前) ===")
total_count = df.count()
# 空文字列とNULLの両方をチェック
bad_amount_count = df.filter((F.col('order_amount') == '') | F.col('order_amount').isNull()).count()
bad_date_count = df.filter((F.col('order_date') == '') | F.col('order_date').isNull()).count()
dup_order_ids = df.groupBy("order_id").count().filter(F.col("count") > 1)
dup_count = dup_order_ids.count()
print(f"総行数: {total_count}")
print(f"金額欠損の行数: {bad_amount_count}")
print(f"日付欠損の行数: {bad_date_count}")
print(f"重複している order_id の数: {dup_count}")
print(f"\n💡 この不良データが Bronze テーブルにそのまま保存され、Silver でクレンジングされます")

# Bronze テーブルを作成(全列 STRING = 生データのまま)
spark.sql(f"""
CREATE OR REPLACE TABLE {ns}.med_orders__bronze (
  order_id STRING, order_date STRING, order_priority STRING,
  order_amount STRING, cust_id STRING,
  _datasource STRING,
  _ingest_timestamp TIMESTAMP
) USING delta
""")
df.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable(f"{ns}.med_orders__bronze")

# Bronze テーブル作成後の確認
print("\n=== Bronze テーブルの統計(書き込み後) ===")
bronze_total = spark.table(f'{ns}.med_orders__bronze').count()
# NULLと空文字列の両方をチェック
bronze_bad_count = spark.sql(f"SELECT count(*) FROM {ns}.med_orders__bronze WHERE order_amount='' OR order_amount IS NULL OR order_date='' OR order_date IS NULL").first()[0]
print(f"Bronze 総行数(重複・不良込み): {bronze_total}")
print(f"うち 金額または日付が空の不良行: {bronze_bad_count}")

print("\n【Bronze の不良データサンプル(金額欠損)】")
display(spark.sql(f"SELECT order_id, order_date, order_amount, cust_id FROM {ns}.med_orders__bronze WHERE order_amount='' OR order_amount IS NULL LIMIT 3"))
print("【Bronze の不良データサンプル(日付欠損)】")
display(spark.sql(f"SELECT order_id, order_date, order_amount, cust_id FROM {ns}.med_orders__bronze WHERE order_date='' OR order_date IS NULL LIMIT 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `med_customers__bronze`（顧客マスタの生データ取り込み）
# MAGIC 受注と同じ要領で、顧客マスタ（顧客名・市場セグメント・国）の Bronze を作ります。

# COMMAND ----------

# 顧客マスタを「全列文字列」で生CSV化
custs_base = spark.sql("""
  SELECT CAST(c.c_custkey AS STRING)  AS cust_id,
         c.c_name                     AS customer_name,
         c.c_mktsegment               AS market_segment,
         n.n_name                     AS nation
  FROM samples.tpch.customer c
  JOIN samples.tpch.nation n ON c.c_nationkey = n.n_nationkey
""")
custs_base.write.mode("overwrite").option("header", True).csv(custs_raw)

# Bronze（全列 STRING ＋ 監査列）
spark.sql(f"""
CREATE OR REPLACE TABLE {ns}.med_customers__bronze (
  cust_id STRING, customer_name STRING, market_segment STRING, nation STRING,
  _datasource STRING, _ingest_timestamp TIMESTAMP
) USING delta
""")
dfc = (spark.read.format("csv").option("header", True).option("inferSchema", False).load(custs_raw))
dfc = (dfc.select("*", "_metadata")
          .withColumn("_datasource", F.col("_metadata.file_path"))
          .withColumn("_ingest_timestamp", F.col("_metadata.file_modification_time"))
          .drop("_metadata"))
dfc.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable(f"{ns}.med_customers__bronze")
print("med_customers__bronze 行数:", spark.table(f"{ns}.med_customers__bronze").count())
display(spark.sql(f"SELECT * FROM {ns}.med_customers__bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Silver テーブルのパイプラインを作成する
# MAGIC Bronze（全列文字列・重複あり）を、**型変換**し、**主キーごとに最新の取り込み分を採用**して **MERGE でアップサート**します。
# MAGIC Bronze との違い（件数・型）がはっきり分かるようにします。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例：`med_orders__silver`（型付け・重複排除・MERGE）

# COMMAND ----------

# Silver（正しい型 ＋ 監査列）。空テーブルを作り、MERGE でアップサート
spark.sql(f"""
CREATE OR REPLACE TABLE {ns}.med_orders__silver (
  order_id LONG, order_date DATE, order_priority STRING,
  order_amount DOUBLE, cust_id LONG,
  _datasource STRING, _ingest_timestamp TIMESTAMP
) USING delta
""")

# 1.主キーごとに最新 _ingest_timestamp を採用 → 2.型変換 → 3.重複排除
brz_to_slv = spark.sql(f"""
  WITH latest AS (   -- 主キーごとに最新の取り込み分だけ残す
    SELECT order_id, MAX(_ingest_timestamp) AS max_ts
    FROM {ns}.med_orders__bronze GROUP BY order_id
  )
  SELECT CAST(b.order_id     AS LONG)   AS order_id,
         CAST(b.order_date   AS DATE)   AS order_date,
         upper(trim(b.order_priority)) AS order_priority,   -- 表記ゆれを正規化（前後空白除去・大文字化）
         CAST(b.order_amount AS DOUBLE) AS order_amount,
         CAST(b.cust_id      AS LONG)   AS cust_id,
         b._datasource, b._ingest_timestamp
  FROM {ns}.med_orders__bronze b
  JOIN latest l ON b.order_id = l.order_id AND b._ingest_timestamp = l.max_ts
  WHERE b.order_amount IS NOT NULL AND b.order_amount <> ''   -- 金額欠損の不良行を除去
    AND b.order_date  IS NOT NULL AND b.order_date  <> ''     -- 日付欠損の不良行を除去
""").dropDuplicates(["order_id"])

# 一時ビュー経由で MERGE（アップサート）。何度実行しても重複しない＝冪等
brz_to_slv.createOrReplaceTempView("_tmp_orders_silver")
spark.sql(f"""
  MERGE INTO {ns}.med_orders__silver AS tgt
  USING _tmp_orders_silver AS src
    ON tgt.order_id = src.order_id
  WHEN MATCHED AND tgt._ingest_timestamp < src._ingest_timestamp THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# 確認：重複排除＋不良行除去で件数が減り、型も正しく・優先度も正規化されている（Bronzeとの違い）
print("Bronze 行数（重複・不良込み）            :", spark.table(f"{ns}.med_orders__bronze").count())
print("Silver 行数（重複排除・不良除去・型付き）:", spark.table(f"{ns}.med_orders__silver").count())
display(spark.sql(f"SELECT * FROM {ns}.med_orders__silver LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,セル19
# MAGIC %md
# MAGIC ### `med_customers__silver`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {ns}.med_customers__silver (
  cust_id LONG, customer_name STRING, market_segment STRING, nation STRING,
  _datasource STRING, _ingest_timestamp TIMESTAMP
) USING delta
""")
c_brz_to_slv = spark.sql(f"""
  WITH latest AS (
    SELECT cust_id, MAX(_ingest_timestamp) AS max_ts
    FROM {ns}.med_customers__bronze GROUP BY cust_id
  )
  SELECT CAST(b.cust_id AS LONG) AS cust_id, b.customer_name, b.market_segment, b.nation,
         b._datasource, b._ingest_timestamp
  FROM {ns}.med_customers__bronze b
  JOIN latest l ON b.cust_id = l.cust_id AND b._ingest_timestamp = l.max_ts
""").dropDuplicates(["cust_id"])
c_brz_to_slv.createOrReplaceTempView("_tmp_customers_silver")
spark.sql(f"""
  MERGE INTO {ns}.med_customers__silver AS tgt
  USING _tmp_customers_silver AS src
    ON tgt.cust_id = src.cust_id
  WHEN MATCHED AND tgt._ingest_timestamp < src._ingest_timestamp THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
display(spark.sql(f"SELECT * FROM {ns}.med_customers__silver LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Gold テーブルのパイプラインを作成する
# MAGIC Silver を結合・集計して、ビジネス利用しやすい形（マート）にします。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例：`med_sales_by_month__gold`（月 × セグメントの売上）
# MAGIC 受注Silverと顧客Silverを結合し、月（**YYYY-MM**）× 市場セグメント で売上を集計します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 受注(med_orders__silver) × 顧客(med_customers__silver) を結合し、月次×セグメントで集計
# MAGIC CREATE OR REPLACE TABLE med_sales_by_month__gold AS
# MAGIC SELECT date_format(o.order_date, 'yyyy-MM') AS order_month,   -- 月を YYYY-MM の文字列で
# MAGIC        c.market_segment,
# MAGIC        count(*)                      AS n_orders,        -- 受注件数
# MAGIC        round(sum(o.order_amount), 0) AS total_sales,     -- 売上合計
# MAGIC        round(avg(o.order_amount), 0) AS avg_order_value  -- 平均注文単価
# MAGIC FROM med_orders__silver o
# MAGIC JOIN med_customers__silver c ON o.cust_id = c.cust_id
# MAGIC GROUP BY date_format(o.order_date, 'yyyy-MM'), c.market_segment;
# MAGIC
# MAGIC SELECT * FROM med_sales_by_month__gold ORDER BY order_month, market_segment LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### グラフで見る（display の組込みグラフ）
# MAGIC 下のセルを実行 → 表の上の **＋ → 可視化（Visualization）** で「折れ線」を選び、X=`order_month` / Y=`total_sales` / グループ=`market_segment` にすると、月次の売上推移が見えます。

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {ns}.med_sales_by_month__gold ORDER BY order_month"))

# COMMAND ----------

# DBTITLE 1,セル26
# MAGIC %md
# MAGIC ### `med_top_customers__gold`（売上上位の顧客）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客別の売上合計を集計し、上位を見やすくしたマート
# MAGIC CREATE OR REPLACE TABLE med_top_customers__gold AS
# MAGIC SELECT c.customer_name, c.market_segment, c.nation,
# MAGIC        count(*)                      AS n_orders,
# MAGIC        round(sum(o.order_amount), 0) AS total_sales
# MAGIC FROM med_orders__silver o
# MAGIC JOIN med_customers__silver c ON o.cust_id = c.cust_id
# MAGIC GROUP BY c.customer_name, c.market_segment, c.nation;
# MAGIC
# MAGIC SELECT * FROM med_top_customers__gold ORDER BY total_sales DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader で「増分取り込み」を体験する（任意）
# MAGIC Auto Loader（`cloudFiles`）は、フォルダに**新しく届いたファイルだけ**を自動で増分取り込みします。
# MAGIC 「①既存ファイルを取り込む → ②新しいファイルを1つ追加 → ③もう一度実行すると追加分だけ増える」を確認します。

# COMMAND ----------

al_chk = f"{chk_dir}/orders_al"
dbutils.fs.rm(al_chk, True)  # 学習用に初期化

def run_autoloader():
    """orders_raw を Auto Loader で読み、med_orders__autoloader へ増分書き込み（届いている分だけ1回）"""
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", al_chk)
        .option("header", True)
        .load(orders_raw)
        .writeStream
        .option("checkpointLocation", al_chk)
        .trigger(availableNow=True)
        .toTable(f"{ns}.med_orders__autoloader")).awaitTermination()

n1 = None
try:
    run_autoloader()                                   # ① 1回目：今ある CSV を取り込む
    n1 = spark.table(f"{ns}.med_orders__autoloader").count()
    print(f"① 1回目の取り込み後: {n1} 行")
except Exception as e:
    print("ℹ️ Auto Loader はこの環境ではスキップしました（学習の本筋ではないのでOK）:", str(e)[:160])

# COMMAND ----------

# ② 新しい受注ファイルを1つ追加（別の受注が新たに届いた想定）
if n1 is not None:
    orders_base.sample(fraction=0.01, seed=99).coalesce(1) \
        .write.mode("append").option("header", True).csv(orders_raw)
    print("新しいCSVを追加しました。raw フォルダのCSV数:",
          len([f for f in dbutils.fs.ls(orders_raw) if f.path.endswith('.csv')]))

# COMMAND ----------

# ③ もう一度実行 → 追加されたファイルだけが増分取り込みされる
if n1 is not None:
    try:
        run_autoloader()
        n2 = spark.table(f"{ns}.med_orders__autoloader").count()
        print(f"① 1回目: {n1} 行  →  ③ 新ファイル追加後: {n2} 行（+{n2 - n1} 行が増分取り込みされた）")
        print("→ 既に取り込んだファイルは再取り込みされず、新しいファイルだけが追加されています。")
    except Exception as e:
        print("ℹ️ 2回目はスキップ:", str(e)[:160])

# 事後処理：ストリーム停止
for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 補足：Unity Catalog の **Volume** には、CSV のような構造化ファイルだけでなく、PDF やテキストなどの**非構造化ファイル**も置けます。
# MAGIC > **③RAGナレッジ**では、その仕組みで社内ルール文書をAIが検索できるようにします。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - **Bronze**（生・全文字列・重複あり）→ **Silver**（型付き・最新採用・MERGEアップサート）→ **Gold**（YYYY-MM の月次集計・顧客別マート）でデータを育てた
# MAGIC - 受注・顧客の2つのパイプラインを作り、Gold で結合した
# MAGIC - **Auto Loader** で新規ファイルの増分取り込みを確認した
# MAGIC
# MAGIC > 本ノートの `med_*` は学習用（少量サンプル）。**②以降は、同じ tpch 受注データの全量版** `orders_silver` / `sales_monthly_gold`（`_setup` 作成）を使います。データソースは一貫して tpch の受注データです。
# MAGIC 次は **②AI関数とインサイト** へ。