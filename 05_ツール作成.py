# Databricks notebook source
# MAGIC %md
# MAGIC # ⑤-1 ツール作成 — エージェントの「道具」を用意する
# MAGIC
# MAGIC 統合エージェント（⑥）が使う**ツール**を作ります。役割の違う2つの情報源を「道具」にするのがポイントです。
# MAGIC
# MAGIC | ツール | 種類 | 役割 |
# MAGIC |---|---|---|
# MAGIC | `search_order_info` | SQL UC Function | 取引データ（`orders_silver`）から注文情報を引く |
# MAGIC | `search_rules` | Python関数（Vector Search） | 社内ルール文書（③の `rules_index`）を検索 |
# MAGIC | `record_request` | Python関数 | 問い合わせ内容を記録する |

# COMMAND ----------

# MAGIC %md
# MAGIC ## なぜ「ツール」を作るのか — エージェントの手足
# MAGIC
# MAGIC ### エージェントとツールの関係
# MAGIC AIエージェント（LLM）は「**考えることはできるが、直接データを触れない**」存在です。
# MAGIC - ❌ LLM単体では、データベースを検索できない
# MAGIC - ❌ LLM単体では、外部システムを呼び出せない
# MAGIC - ❌ LLM単体では、ファイルを読み書きできない
# MAGIC
# MAGIC → **ツール**を用意することで、LLMが「現実世界」と接続できます。
# MAGIC
# MAGIC ### なぜ Unity Catalog 関数（UC Function）として作るのか
# MAGIC #### 永続化と再利用
# MAGIC - **Pythonの関数だけ**だと → ノートブックを閉じたら消える。他の人は使えない。
# MAGIC - **UC Functionにすると** → カタログに永続化される。他のノートブック・エージェント・ダッシュボードから再利用できる。
# MAGIC
# MAGIC #### ガバナンスと権限管理
# MAGIC - Unity Catalog の**アクセス制御**が効く（誰がこのツールを使えるか管理できる）
# MAGIC - **監査ログ**に記録される（誰がいつ使ったか追跡できる）

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vector Search が使えない環境への配慮（フォールバック）
# MAGIC このノートブックの `search_rules` 関数は、2段階で動作します：
# MAGIC ```python
# MAGIC try:
# MAGIC     # (1) まず Vector Search で検索を試みる
# MAGIC     result = vector_search_index.similarity_search(query)
# MAGIC except:
# MAGIC     # (2) Vector Search が無ければ、Volume のファイルを直接読む
# MAGIC     result = grep_files_in_volume(query)
# MAGIC ```
# MAGIC
# MAGIC **理由**: ③のRAGナレッジが未実行でも、このノートブックが単独で動くようにするため。
# MAGIC
# MAGIC > 💡 **本番のベストプラクティス**: Vector Search は高速で精度が高いが、開発初期やテスト環境では、シンプルなファイル検索から始めても構いません。

# COMMAND ----------

# MAGIC %pip install -q -U databricks-vectorsearch mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツール1：`search_order_info`（SQL UC Function）
# MAGIC 「ツール」は **Unity Catalog の関数（UC Function）** として作ると、永続化され、エージェントや他ノートから再利用できます。
# MAGIC ここでは注文ID から注文情報を返す SQL 関数を作ります。

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {ns}.search_order_info(p_order_id BIGINT)
RETURNS TABLE(order_id BIGINT, order_date DATE, order_priority STRING,
              order_amount DOUBLE, customer_name STRING, market_segment STRING)
COMMENT '注文IDから注文情報（日付・金額・優先度・顧客・セグメント）を返す'
RETURN
  SELECT order_id, order_date, order_priority, order_amount, customer_name, market_segment
  FROM {ns}.orders_silver
  WHERE order_id = p_order_id
""")
print("✅ UC Function を作成: search_order_info")

# 動作確認（実在する order_id を1つ取得して引いてみる）
sample_id = spark.sql(f"SELECT order_id FROM {ns}.orders_silver LIMIT 1").first()[0]
print("テスト order_id =", sample_id)
display(spark.sql(f"SELECT * FROM {ns}.search_order_info({sample_id})"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツール2：`search_rules`（社内ルール文書の検索）
# MAGIC ③で作った Vector Search インデックス `rules_index` を検索します。
# MAGIC （`rules_index` が未作成の環境でも動くよう、**Volume のファイルを直接読むフォールバック**を入れてあります）

# COMMAND ----------

import glob, os
RULES_VOLUME = f"/Volumes/{catalog}/{schema}/rules_docs"
VS_INDEX = f"{catalog}.{schema}.rules_index"

def search_rules(query: str, k: int = 3) -> str:
    """社内ルール文書を検索して、関連箇所のテキストを返す。"""
    # (1) まず Vector Search インデックスを試す
    try:
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient(disable_notice=True)
        idx = vsc.get_index(index_name=VS_INDEX)
        res = idx.similarity_search(query_text=query, columns=["path", "content"], num_results=k)
        rows = res.get("result", {}).get("data_array", [])
        if rows:
            return "\n---\n".join(f"[{r[0]}] {r[1][:400]}" for r in rows)
    except Exception:
        pass
    # (2) フォールバック：Volume のファイルを読んでキーワード一致
    hits = []
    for path in glob.glob(RULES_VOLUME + "/*"):
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        except Exception:
            continue
        score = sum(1 for ch in set(query) if ch in text)
        hits.append((score, os.path.basename(path), text))
    hits.sort(reverse=True)
    return "\n---\n".join(f"[{name}] {text[:400]}" for _, name, text in hits[:k]) or "該当する社内ルールが見つかりませんでした。"

print(search_rules("返品は何日以内に可能ですか？"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツール3：`record_request`（問い合わせの記録）
# MAGIC 複雑な依頼を記録する Write 系ツール。UUID を採番して記録テーブルに追記します。

# COMMAND ----------

import uuid
from datetime import datetime
spark.sql(f"""CREATE TABLE IF NOT EXISTS {ns}.requests
            (request_id STRING, content STRING, created_at TIMESTAMP)""")

def record_request(content: str) -> str:
    rid = str(uuid.uuid4())[:8]
    spark.sql(f"INSERT INTO {ns}.requests VALUES ('{rid}', '{content.replace(chr(39), '')}', current_timestamp())")
    return f"記録しました（依頼ID: {rid}）"

print(record_request("カテゴリ別の客単価を深掘り分析してほしい"))
display(spark.sql(f"SELECT * FROM {ns}.requests ORDER BY created_at DESC LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - **取引データの道具**（`search_order_info`：UC Function）と **社内ルールの道具**（`search_rules`：Vector Search）を用意した
# MAGIC - これらを ⑥ で **スーパーバイザーエージェント**に渡し、「データ × ルール」を統合して答えさせます
# MAGIC
# MAGIC > 💡 ②で触れた「カスタムAI関数（UC Function）」が、ここでは"エージェントのツール"として実体化しました。
# MAGIC 次は **⑥ スーパーバイザーAgent** へ。