# Databricks notebook source
# MAGIC %md
# MAGIC # 共通セットアップ（_setup）
# MAGIC
# MAGIC このノートブックは各ノートブックの先頭から `%run ./_setup` で**自動的に呼ばれます**。
# MAGIC 受講者が直接開く必要はありません。
# MAGIC
# MAGIC やっていること（すべて冪等＝何度実行してもOK）：
# MAGIC 1. ログインユーザー名から、各自専用のスキーマ名を自動生成（入力不要）
# MAGIC 2. 書き込み可能なカタログを自動判定し、スキーマを作成
# MAGIC 3. 教材用テーブル（Silver / Gold）を `CREATE TABLE IF NOT EXISTS` で用意
# MAGIC    （既に存在すれば何もしない＝高速）

# COMMAND ----------

import re

# --- 1) ログインユーザーから一意なスキーマ名を自動生成（ウィジェット入力なし）---
user = spark.sql("SELECT current_user()").first()[0]
schema = "handson_" + re.sub(r"[^0-9a-zA-Z]", "_", user.split("@")[0]).lower()

# --- 2) 書き込み可能なカタログを自動判定（Free Editionは既定で workspace を想定）---
catalog = "workspace"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
except Exception:
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# 後続ノートブックから使う完全修飾名のプレフィックス
ns = f"{catalog}.{schema}"

# --- 3) 教材用テーブルを用意（IF NOT EXISTS なので2回目以降は一瞬で終わる）---
# Silver: サンプルの受注データ(samples.tpch)を、顧客・国と結合して分析しやすく整形
#   ※ samples.tpch は 1992〜1998 年の架空データなので、研修用にそのまま見ると古いです。
#     ここでは日付を +28年シフトして、2020-01-01 〜 2026-05-31 の範囲のみを採用しています。
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ns}.orders_silver AS
SELECT
  o.o_orderkey                          AS order_id,
  add_months(o.o_orderdate, 336)        AS order_date,
  o.o_orderpriority                     AS order_priority,
  o.o_totalprice                        AS order_amount,
  c.c_name                              AS customer_name,
  c.c_mktsegment                        AS market_segment,
  n.n_name                              AS nation
FROM samples.tpch.orders o
JOIN samples.tpch.customer c ON o.o_custkey   = c.c_custkey
JOIN samples.tpch.nation   n ON c.c_nationkey = n.n_nationkey
WHERE add_months(o.o_orderdate, 336) BETWEEN DATE'2020-01-01' AND DATE'2026-05-31'
""")

# Gold: 月 × 顧客セグメントの売上サマリ（Genie / ダッシュボードで使う）
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ns}.sales_monthly_gold AS
SELECT
  date_trunc('month', order_date) AS order_month,
  market_segment,
  count(*)                        AS n_orders,
  round(sum(order_amount), 0)     AS total_sales,
  round(avg(order_amount), 0)     AS avg_order_value
FROM {ns}.orders_silver
GROUP BY 1, 2
""")

print(f"✅ セットアップ完了")
print(f"   ユーザー : {user}")
print(f"   カタログ : {catalog}")
print(f"   スキーマ : {schema}")
print(f"   テーブル : {ns}.orders_silver / {ns}.sales_monthly_gold")
