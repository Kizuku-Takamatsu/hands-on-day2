# Databricks notebook source
# MAGIC %md
# MAGIC # ⑦ MLflow トレース — エージェントの動きを可視化（任意）
# MAGIC
# MAGIC エージェントが「どのツールを・どの順で・どれくらいの時間で」呼んだかを、**MLflow Tracing** で可視化します。
# MAGIC
# MAGIC ## このノートの目的（所要：約15分・任意）
# MAGIC - MLflow のトレースで、エージェント実行の中身を見る
# MAGIC - 「ブラックボックスにしない」運用の第一歩を体験する
# MAGIC
# MAGIC ## 進め方
# MAGIC 最初のセルで `%pip install` → 再起動 → 上から再実行。使えない環境ではスキップして⑨へ。

# COMMAND ----------

# MAGIC %md
# MAGIC ## トレースがないとどうなるか — ブラックボックス問題
# MAGIC
# MAGIC ### エージェントは「中身が見えない」
# MAGIC エージェントに質問すると、答えが返ってきます。しかし、**トレースがない**と：
# MAGIC - ❌ どのツールを呼んだのか？
# MAGIC - ❌ どの順番で呼んだのか？
# MAGIC - ❌ 各ツールの実行時間は？
# MAGIC - ❌ ツールからの戻り値は？
# MAGIC
# MAGIC → 結果がおかしくても、**「なぜそうなったのか」が分からない** = デバッグできない。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 本番運用でトレースが必須な理由
# MAGIC
# MAGIC #### 1. 問題の原因特定
# MAGIC ユーザー：「エージェントが間違った答えを返しました」
# MAGIC 開発者：トレースを見る → 「ツールAが古いデータを返している」と分かる
# MAGIC
# MAGIC #### 2. パフォーマンス最適化
# MAGIC トレースで各ツールの所要時間を見る → 「ツールBが3秒かかっている」→ キャッシュを入れる
# MAGIC
# MAGIC #### 3. 監査・コンプライアンス
# MAGIC 「このエージェントが、どのデータにアクセスして答えたのか」を後から検証できる。
# MAGIC
# MAGIC > 💡 **重要**: MLflowトレースは「エージェントの思考プロセスを可視化する」ツール。autolog() を1行書くだけで、自動記録されます。

# COMMAND ----------

# DBTITLE 1,セル2
# バージョン互換性を確保してインストール
%pip install -q -U mlflow databricks-langchain langchain-core "langgraph>=0.2.0,<0.3.0" databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## トレースを有効にして、エージェント（または統合処理）を1回実行
# MAGIC `mlflow.langchain.autolog()` でトレースを自動記録します。実行後、ノート上やサイドの **MLflow / トレース** で確認できます。

# COMMAND ----------

# DBTITLE 1,セル5
import glob, os, json, datetime
RULES_VOLUME = f"/Volumes/{catalog}/{schema}/rules_docs"

def tool_order_info(order_id):
    # ⑤のUC関数があれば使い、無ければ orders_silver を直接参照（単体でも動く）
    try:
        rows = spark.sql(f"SELECT * FROM {ns}.search_order_info({int(order_id)})").collect()
    except Exception:
        rows = spark.sql(f"""SELECT order_id, order_date, order_priority, order_amount,
                                    customer_name, market_segment
                             FROM {ns}.orders_silver WHERE order_id = {int(order_id)}""").collect()
    return json.dumps(rows[0].asDict(), ensure_ascii=False, default=str) if rows else "なし"

def tool_search_rules(query, k=3):
    hits=[]
    for p in glob.glob(RULES_VOLUME+"/*"):
        try: t=open(p,encoding="utf-8").read()
        except: continue
        hits.append((sum(1 for c in set(query) if c in t), os.path.basename(p), t))
    hits.sort(reverse=True)
    return "\n".join(f"[{n}] {t[:300]}" for _,n,t in hits[:k]) or "なし"

DEMO_ID = spark.sql(f"SELECT order_id FROM {ns}.orders_silver LIMIT 1").first()[0]
question = f"注文 {DEMO_ID} は返品できますか？"

try:
    import mlflow
    mlflow.langchain.autolog()
    from databricks_langchain import ChatDatabricks
    from langchain_core.tools import tool
    from langgraph.prebuilt import create_react_agent

    @tool
    def get_order_info(order_id: int) -> str:
        "注文情報を返す"
        return tool_order_info(order_id)
    @tool
    def get_rules(query: str) -> str:
        "社内ルールを検索"
        return tool_search_rules(query)

    agent = create_react_agent(ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct"),
                               tools=[get_order_info, get_rules])
    res = agent.invoke({"messages": [{"role": "user", "content": question}]})
    print(res["messages"][-1].content)
    print("\n✅ LangGraph/LLMが正常に動作しました。エージェントがツールを呼び出したMLflowトレースを記録しました。")
    print("   サイド/上部の『トレース』タブで、ツール呼び出しの流れと所要時間を確認してください。")
except Exception as e:
    print("⚠️ LLM/LangGraph が使えないため、トレースのデモはスキップします（Ⅹへ）。")
    print("   エラー内容:", str(e)[:200])
    print("\n本番では、上記の autolog によりエージェントの各ステップ（ツール呼び出し・入出力・レイテンシ）が")
    print("MLflow Experiments の『トレース』に自動記録され、UIで可視化できます。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - `mlflow.langchain.autolog()` の1行で、エージェントの実行が**トレース**として記録される
# MAGIC - 「どのツールを呼んだか・なぜその答えになったか」を後から追える＝運用・デバッグの土台
# MAGIC 次は（任意）**⑧評価と改善** へ。