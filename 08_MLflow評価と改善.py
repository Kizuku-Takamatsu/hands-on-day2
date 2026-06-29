# Databricks notebook source
# MAGIC %md
# MAGIC # ⑧ MLflow 評価と改善 — エージェントの品質を測る（任意）
# MAGIC
# MAGIC 「なんとなく良さそう」で終わらせず、**評価データで採点 → 改善 → 再評価**の改善サイクルを体験します。
# MAGIC
# MAGIC ## このノートの目的（所要：約25分・任意）
# MAGIC - `mlflow.genai.evaluate()` で、エージェントの回答を採点する
# MAGIC - スコアラー（Correctness / RelevanceToQuery / Guidelines）で品質を可視化する
# MAGIC - 結果を MLflow Experiment で比較する
# MAGIC
# MAGIC ## 進め方
# MAGIC `%pip install` → 再起動 → 上から再実行。使えない環境では概念のみ確認してスキップ。
# MAGIC > 参考スタイル：taka_yayoi（弥生）氏の MLflow 評価記事。

# COMMAND ----------

# MAGIC %md
# MAGIC ## なぜ評価するのか — 品質管理の考え方
# MAGIC
# MAGIC AIエージェントの答えは、「なんとなく良さそう」でも本番では通用しません。
# MAGIC **評価データによる採点（客観的指標）**が必要です。
# MAGIC
# MAGIC - ❌ サンプル回答の見た目で判断 → ブラックボックス化
# MAGIC - ✅ スコアラー（正確さ・関連性など）で定量評価 → 改善点が見えやすい
# MAGIC
# MAGIC **評価なしだとどうなるか？**
# MAGIC - どこを直せば良いかわからない
# MAGIC - 品質が低いまま本番に進んでしまう
# MAGIC
# MAGIC > 💡 MLflow評価は「品質管理の最初の一歩」。コード1行で比較・改善→再評価のサイクルを回せます。

# COMMAND ----------

# DBTITLE 1,セル2
# バージョン互換性を確保してインストール
%pip install -q -U mlflow databricks-agents databricks-langchain "langgraph>=0.2.0,<0.3.0" databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 評価データを用意して採点する
# MAGIC 「質問」と「期待される観点（ガイドライン）」を数件用意し、エージェントの回答を採点します。

# COMMAND ----------

# DBTITLE 1,セル5
import glob, os, json
RULES_VOLUME = f"/Volumes/{catalog}/{schema}/rules_docs"

def tool_search_rules(query, k=3):
    hits=[]
    for p in glob.glob(RULES_VOLUME+"/*"):
        try: t=open(p,encoding="utf-8").read()
        except: continue
        hits.append((sum(1 for c in set(query) if c in t), os.path.basename(p), t))
    hits.sort(reverse=True)
    return "\n".join(f"[{n}] {t[:300]}" for _,n,t in hits[:k]) or "なし"

# 評価対象アプリ（シンプルにルール検索＋要約。実際は⑥のエージェントを渡す）
def my_app(question: str) -> str:
    return tool_search_rules(question)

eval_data = [
    {"inputs": {"question": "返品は何日以内に可能ですか？"},
     "expectations": {"guidelines": "注文日から30日以内である旨に触れている"}},
    {"inputs": {"question": "優先出荷の条件は？"},
     "expectations": {"guidelines": "order_priority が 1-URGENT / 2-HIGH である旨に触れている"}},
    {"inputs": {"question": "出荷後にキャンセルできますか？"},
     "expectations": {"guidelines": "出荷後はキャンセル不可（返品扱い）である旨に触れている"}},
]

try:
    import mlflow
    from mlflow.genai.scorers import RelevanceToQuery, Guidelines
    results = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=lambda question: my_app(question),
        scorers=[RelevanceToQuery(), Guidelines(name="rule_coverage",
                 guidelines="回答は社内ルールの該当条件に正しく触れていること")],
    )
    print("✅ MLflow評価が正常に実行されました。MLflow Experiments の『評価』で各質問のスコア・トレースを確認できます。")
except Exception as e:
    print("⚠️ この環境では mlflow.genai.evaluate をスキップします（概念のみ）。")
    print("   エラー内容:", str(e)[:200])
    print("\n本番では、上記のように評価データ＋スコアラー（Correctness / RelevanceToQuery / Guidelines 等）で")
    print("エージェントを採点し、プロンプト改善→再評価の差分を Experiments で比較できます。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 改善サイクル（考え方）
# MAGIC 1. 評価でスコアが低い質問を特定
# MAGIC 2. プロンプトやツールの使い分けルールを修正
# MAGIC 3. もう一度 `evaluate` → スコアが上がったかを比較（Experiments で並べて確認）
# MAGIC
# MAGIC ## （任意）レビュー＆フィードバック
# MAGIC 回答に人が 👍/👎・コメントを付ける **Review App / フィードバック**で、現場の評価を評価データとして蓄積できます。
# MAGIC
# MAGIC ## まとめ
# MAGIC - エージェントは「作って終わり」ではなく、**評価して改善**するもの
# MAGIC - MLflow が採点・トレース・比較の土台になる
# MAGIC 次は **⑨ 本番への道筋** へ。