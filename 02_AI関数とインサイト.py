# Databricks notebook source
# MAGIC %md
# MAGIC # ② AI関数とインサイト — SQLの中からAIを呼ぶ
# MAGIC
# MAGIC 数字に「ことばの補足」を付ける、いちばん手軽な「データ × 生成AI」の入り口です。
# MAGIC
# MAGIC ## このノートの目的（所要：約15分）
# MAGIC - SQL から組込みのAI関数（`ai_query` / `ai_gen` 等）を呼び、集計結果に自然言語コメントを付ける
# MAGIC - 「特別な基盤を組まなくても、SQL一行でAIが呼べる」を体感する
# MAGIC
# MAGIC ## 進め方
# MAGIC 上から `Shift + Enter`。AI関数が未対応の環境では自動でスキップします（エラーで止まりません）。

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. AI関数が使えるか確認
# MAGIC `ai_gen()` は、自然言語の指示にAIが答える組込み関数です。まず短い質問で動作確認します。

# COMMAND ----------

ai_available = False
try:
    r = spark.sql("SELECT ai_gen('「こんにちは」と一言だけ返して') AS msg").first()[0]
    ai_available = True
    print("✅ AI関数が使えます。応答:", r)
except Exception as e:
    print("ℹ️ この環境ではAI関数(ai_gen)が利用できないようです。以降のセルは自動スキップします。")
    print("   参考:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. セグメント別の売上に、AIで一言コメントを付ける
# MAGIC 集計結果（数字）をAIに渡して、経営報告向けの短いコメントを生成します。
# MAGIC （AI関数が使えない環境では自動でスキップされます）

# COMMAND ----------

if ai_available:
    sql = f"""
    WITH seg AS (
      SELECT market_segment,
             round(sum(order_amount)/1e6, 1) AS sales_million
      FROM orders_silver
      GROUP BY market_segment
    )
    SELECT market_segment AS `セグメント`,
           sales_million  AS `売上_百万`,
           ai_gen(
             '次の顧客セグメントの売上規模について、経営報告向けに30字以内の日本語で一言コメント。' ||
             'セグメント=' || market_segment || ' / 売上=' || sales_million || '百万'
           ) AS `AIコメント`
    FROM seg
    ORDER BY sales_million DESC
    """
    display(spark.sql(sql))
else:
    print("⏭️ スキップしました（AI関数が未対応の環境）。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ほかにもある組込みAI関数
# MAGIC SQLから呼べる組込みAI関数の例。社内データの分類・要約・感情分析などにそのまま使えます。
# MAGIC
# MAGIC | 関数 | 用途 |
# MAGIC |---|---|
# MAGIC | `ai_gen(prompt)` | 自由な指示に回答を生成 |
# MAGIC | `ai_summarize(text)` | 長文の要約 |
# MAGIC | `ai_classify(text, ARRAY('A','B'))` | カテゴリ分類 |
# MAGIC | `ai_analyze_sentiment(text)` | 感情分析（ポジ/ネガ） |
# MAGIC | `ai_translate(text, 'en')` | 翻訳 |
# MAGIC
# MAGIC 下は分類の例です（使えない環境ではスキップ）。

# COMMAND ----------

if ai_available:
    try:
        demo = spark.sql("""
          SELECT order_priority,
                 ai_classify(order_priority, ARRAY('至急', '通常')) AS `ai_判定`
          FROM orders_silver
          GROUP BY order_priority
          ORDER BY order_priority
        """)
        display(demo)
    except Exception as e:
        print("ℹ️ この関数はスキップしました:", str(e)[:200])
else:
    print("⏭️ スキップしました（AI関数が未対応の環境）。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## （補足）自分専用の「カスタムAI関数」も作れます — ただし本研修では扱いません
# MAGIC 組込みの `ai_query` のほかに、Unity Catalog の **カスタム関数（UC Function）** として
# MAGIC 「自社専用の処理 ＋ AI呼び出し」をSQL関数化して再利用できます。
# MAGIC 本研修では**範囲外**（時間の都合）としますが、「組込み・自作の両方のAI関数を部品化して使える」と覚えておいてください。
# MAGIC
# MAGIC > 💡 実は **⑤（統合エージェント）** では、この UC Function を「**エージェントのツール**」として実際に作ります。お楽しみに。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - `ai_query` / `ai_gen` などで、**SQLの中から直接AIを呼べる**
# MAGIC - 分類・要約・感情分析・翻訳も関数1つ
# MAGIC - カスタムAI関数（UC Function）で自社専用の部品化も可能（本研修は範囲外）
# MAGIC 次は **③RAGナレッジ**（社内文書をAIが読めるようにする）へ。