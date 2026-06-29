# Databricks notebook source
# MAGIC %md
# MAGIC # Day2 ハンズオン｜00. はじめに（環境チェック）
# MAGIC
# MAGIC ## このハンズオンの進め方（重要）
# MAGIC - **基本は「上から順にセルを実行するだけ」** です。各セルで **`Shift + Enter`**（実行して次へ）を押し続けるか、
# MAGIC   画面右上の **「すべて実行 / Run all」** を押せば、最後まで自動で進みます。
# MAGIC - 文字を入力したり設定をいじる操作は **基本ありません**。迷ったら次のセルを実行してください。
# MAGIC - ノートブックは **00 → 09 の番号順** に開いて進めます（コード中心の回は Run All で完走）。
# MAGIC
# MAGIC ## 今日の流れ（一本のシナリオ：現場の問い合わせに答えるAIアシスタントを組み上げる）
# MAGIC | ノートブック | 内容 |
# MAGIC |---|---|
# MAGIC | **00_はじめに** | 環境チェック（今ここ） |
# MAGIC | **01_データ探索とメダリオン** | 生→Bronze→Silver→Gold でデータを整える |
# MAGIC | **02_AI関数とインサイト** | SQLからAIを呼ぶ |
# MAGIC | **03_RAGナレッジ** | 社内ルール文書を検索できるようにする（ノーコード） |
# MAGIC | **04_Genieとダッシュボード** | 取引データのGenie＋ダッシュボード＋アプリ化 |
# MAGIC | **05_ツール作成 / 06_スーパーバイザーAgent** | 道具を作り、統合エージェントで束ねる（山場） |
# MAGIC | **07_MLflowトレース / 08_評価と改善** | 可視化・品質評価（任意） |
# MAGIC | **09_本番への道筋** | Serving / AI Gateway（結び） |
# MAGIC
# MAGIC > 使う題材は Databricks 組込みのサンプルデータ `samples.tpch`（架空の「受注・顧客」データ）＋ 社内ルール文書です。
# MAGIC > 準備は不要で、`samples.tpch` は Free Edition に最初から入っています。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ① まずは下のセルを実行（環境の確認）
# MAGIC `Shift + Enter` を押してください。

# COMMAND ----------

print("Spark version :", spark.version)
print("ログインユーザー :", spark.sql("SELECT current_user()").first()[0])
print("既定カタログ   :", spark.sql("SELECT current_catalog()").first()[0])
print("\nここまで表示されれば、サーバーレス環境は正常に動いています。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ② 共通セットアップを実行
# MAGIC 下のセルは、各自専用のスキーマ作成と教材テーブルの用意を**自動**で行います（入力不要）。

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### ③ サンプルデータが見えるか確認
# MAGIC 組込みサンプル `samples.tpch` の受注データを5行だけ覗いてみます。

# COMMAND ----------

display(spark.sql("SELECT * FROM samples.tpch.orders"))

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) AS `件数` FROM samples.tpch.orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ④ 自分のテーブルが用意できたか確認
# MAGIC セットアップで作られた、あなた専用のテーブル一覧です。

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {ns}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ ここまで来たらDay2の準備は完了です。
# MAGIC 次は **`01_データ探索とメダリオン`** を開いて、同じように上から実行してください。