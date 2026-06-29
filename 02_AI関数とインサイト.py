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
# MAGIC ## 4. 実践：カスタムAI関数を作ってみよう
# MAGIC 組込みの `ai_gen` のほかに、Unity Catalog の **カスタム関数（UC Function）** として
# MAGIC 「自社専用の処理 ＋ AI呼び出し」をSQL関数化して再利用できます。
# MAGIC
# MAGIC ここでは `analyze_sales_trend` という関数を作り、セグメント別の売上トレンドを分析させます。
# MAGIC
# MAGIC > 💡 この UC Function は **⑤（統合エージェント）** で「**エージェントのツール**」としても活用できます。

# COMMAND ----------

# DBTITLE 1,ステップ1: 関数の説明
# MAGIC %md
# MAGIC ### ステップ1：カスタムAI関数を定義する
# MAGIC
# MAGIC `analyze_sales_trend` 関数をUC Functionとして作成します。
# MAGIC
# MAGIC **入力：**
# MAGIC - `segment`: 顧客セグメント名
# MAGIC - `recent_months_data`: 直近3ヶ月の売上データ（JSON形式）
# MAGIC
# MAGIC **出力：**
# MAGIC - AIが生成したトレンド分析と戦略提案（日本語、150字以内）

# COMMAND ----------

# DBTITLE 1,ステップ1: 関数作成
if ai_available:
    # 既存の関数があれば削除
    try:
        spark.sql(f"DROP FUNCTION IF EXISTS {ns}.analyze_sales_trend")
    except:
        pass
    
    # UC Function としてカスタムAI関数を作成
    spark.sql(f"""
    CREATE FUNCTION {ns}.analyze_sales_trend(
      segment STRING,
      recent_months_data STRING
    )
    RETURNS STRING
    RETURN ai_gen(
      '次の顧客セグメントの直近3ヶ月の売上トレンドを分析し、' ||
      '経営層向けに（①トレンド要約 ②戦略提案）を150字以内の日本語で返して。' ||
      'セグメント=' || segment || ' / ' ||
      '直近3ヶ月のデータ=' || recent_months_data
    )
    """)
    
    print(f"✅ カスタムAI関数 `{ns}.analyze_sales_trend` を作成しました")
    print("\n💡 この関数は他のノートブックやSQLクエリからも呼び出し可能です")
else:
    print("⏭️ スキップしました（AI関数が未対応の環境）。")

# COMMAND ----------

# DBTITLE 1,ステップ2: 関数の説明
# MAGIC %md
# MAGIC ### ステップ2：作成した関数を実際に使ってみる
# MAGIC
# MAGIC `sales_monthly_gold` テーブルから各セグメントの直近3ヶ月の売上データを取得し、
# MAGIC 作成した `analyze_sales_trend` 関数にAI分析させます。

# COMMAND ----------

# DBTITLE 1,ステップ2: 関数実行
if ai_available:
    sql = f"""
    WITH recent_data AS (
      -- 各セグメントの直近3ヶ月の売上推移を取得
      SELECT 
        market_segment,
        concat_ws(', ',
          collect_list(
            concat(
              date_format(order_month, 'yyyy-MM'), ': ',
              cast(round(total_sales/1e6, 1) as string), '百万'
            )
          )
        ) AS monthly_trend
      FROM (
        SELECT market_segment, order_month, total_sales
        FROM {ns}.sales_monthly_gold
        WHERE order_month >= add_months(current_date(), -3)
        ORDER BY market_segment, order_month
      )
      GROUP BY market_segment
    )
    SELECT 
      market_segment AS `セグメント`,
      monthly_trend AS `直近3ヶ月の推移`,
      {ns}.analyze_sales_trend(market_segment, monthly_trend) AS `AI分析・戦略提案`
    FROM recent_data
    ORDER BY market_segment
    """
    
    print("✅ カスタムAI関数を実行中...（AIが分析を生成します）\n")
    display(spark.sql(sql))
else:
    print("⏭️ スキップしました（AI関数が未対応の環境）。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - `ai_gen` / `ai_classify` などで、**SQLの中から直接AIを呼べる**
# MAGIC - 分類・要約・感情分析・翻訳も関数1つ
# MAGIC - **カスタムAI関数（UC Function）で自社専用の部品化が可能** ← 実際に `analyze_sales_trend` を作成！
# MAGIC - UC Function は **⑤統合エージェントのツール** としても活用できる
# MAGIC
# MAGIC 次は **③RAGナレッジ**（社内文書をAIが読めるようにする）へ。

# COMMAND ----------

