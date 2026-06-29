# Databricks notebook source
# MAGIC %md
# MAGIC # ⑥ 統合エージェント — スーパーバイザーで束ねる（本日の山場）
# MAGIC
# MAGIC 「取引データの担当」と「社内ルールの担当」を、1人のマネージャー（スーパーバイザー）が束ねます。
# MAGIC **単体では答えられない質問が、組み合わせると答えられる**——これがマルチエージェントの価値です。
# MAGIC
# MAGIC ## このノートの目的（所要：約45分）
# MAGIC - ⑤のツール（取引データ／社内ルール）を1つのエージェントに統合する
# MAGIC - 同じ質問を「単体 / 単体 / 統合」で比べ、挙動の違いを確認する
# MAGIC
# MAGIC ## 進め方（重要）
# MAGIC 最初のセルで `%pip install` → 再起動 → もう一度上から実行（Run All なら自動で続行）。
# MAGIC ※ LangGraph や LLM エンドポイントが使えない環境でも、**手動オーケストレーションのフォールバック**で必ず最後まで動きます。

# COMMAND ----------

# DBTITLE 1,セル2
# バージョン互換性を確保してインストール
%pip install -q -U databricks-langchain langchain-core "langgraph>=0.2.0,<0.3.0" databricks-vectorsearch mlflow
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## スーパーバイザーパターンとは — なぜ「束ねる」のか
# MAGIC
# MAGIC ### マルチエージェントの基本構造
# MAGIC ```
# MAGIC            ┌─────────────────────┐
# MAGIC            │ スーパーバイザー     │ ← ユーザーの質問を受ける
# MAGIC            │ (マネージャー役)    │
# MAGIC            └──────┬──────────────┘
# MAGIC                   │ どのツールを使うか判断
# MAGIC           ┌───────┴───────┐
# MAGIC           ▼               ▼
# MAGIC     ┌─────────┐     ┌─────────┐
# MAGIC     │データ担当│     │ルール担当│ ← それぞれが専門の道具を持つ
# MAGIC     │ ツール  │     │ ツール  │
# MAGIC     └─────────┘     └─────────┘
# MAGIC ```
# MAGIC
# MAGIC ### なぜ単体エージェントではダメなのか
# MAGIC **問題**: 「注文 ORD-XXX は返品できますか？」という質問に答えるには：
# MAGIC 1. **取引データ** から注文の事実（注文日・金額）を調べる
# MAGIC 2. **社内ルール** から返品条件（30日以内・10万以上は要承認）を調べる
# MAGIC 3. **事実 × ルール** を照らし合わせて判断する
# MAGIC
# MAGIC → 単体エージェントは「1つのツールしか持てない」ため、どちらか一方の情報だけで答えることになり、**「判断できません」** と言うしかない。
# MAGIC
# MAGIC ### マルチエージェントの3つのメリット
# MAGIC 1. **情報の統合**: 異なる種類の情報源（構造化データ・文書・API等）を組み合わせられる
# MAGIC 2. **責任の分離**: 各ツールは自分の専門だけに集中 → メンテナンスしやすい
# MAGIC 3. **拡張性**: 新しいツール（与信チェック・在庫照会等）を後から追加しやすい
# MAGIC
# MAGIC > 💡 **重要**: スーパーバイザーは「どのツールをいつ使うか」を判断する役割。LLM（大規模言語モデル）が、質問を理解してツールの呼び出しを決めます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツールを（このノートでも）用意する
# MAGIC ⑤で作った道具を、このノートのエージェントから使えるように関数として定義します。
# MAGIC - `tool_order_info(order_id)`：UC Function `search_order_info` を呼ぶ（取引データ）
# MAGIC - `tool_search_rules(query)`：Vector Search `rules_index`（無ければファイル検索）で社内ルールを引く

# COMMAND ----------

import glob, os, datetime, json
RULES_VOLUME = f"/Volumes/{catalog}/{schema}/rules_docs"
VS_INDEX = f"{catalog}.{schema}.rules_index"

# 社内ルール文書が無ければ（③未実行でも動くよう）最小限を配置（冪等）
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.rules_docs")
try:
    if not os.listdir(RULES_VOLUME):
        for _n, _b in {
            "返品ポリシー.md": "返品は注文日(order_date)から30日以内。金額(order_amount)が100,000以上は要承認。",
            "出荷・優先度規程.txt": "order_priority が 1-URGENT / 2-HIGH は優先出荷。他は通常便。",
            "キャンセルポリシー.md": "出荷前はキャンセル可。出荷後は不可（返品扱い）。",
        }.items():
            with open(f"{RULES_VOLUME}/{_n}", "w", encoding="utf-8") as _f:
                _f.write(_b)
except Exception:
    pass

def tool_order_info(order_id: int) -> str:
    """注文IDから注文情報を返す。⑤のUC関数があれば使い、無ければ orders_silver を直接参照（単体でも動く）。"""
    try:
        df = spark.sql(f"SELECT * FROM {ns}.search_order_info({int(order_id)})")
        rows = df.collect()
    except Exception:
        rows = spark.sql(f"""SELECT order_id, order_date, order_priority, order_amount,
                                    customer_name, market_segment
                             FROM {ns}.orders_silver WHERE order_id = {int(order_id)}""").collect()
    if not rows:
        return f"注文 {order_id} は見つかりませんでした。"
    return json.dumps({k: str(v) for k, v in rows[0].asDict().items()}, ensure_ascii=False)

def tool_search_rules(query: str, k: int = 3) -> str:
    """社内ルール文書を検索して関連テキストを返す。"""
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
    hits = []
    for path in glob.glob(RULES_VOLUME + "/*"):
        try:
            with open(path, encoding="utf-8") as f: text = f.read()
        except Exception: continue
        score = sum(1 for ch in set(query) if ch in text)
        hits.append((score, os.path.basename(path), text))
    hits.sort(reverse=True)
    return "\n---\n".join(f"[{n}] {t[:400]}" for _, n, t in hits[:k]) or "該当ルールなし。"

# 実在する order_id を1つ用意（デモ質問で使う）
DEMO_ID = spark.sql(f"SELECT order_id FROM {ns}.orders_silver ORDER BY order_amount DESC LIMIT 1").first()[0]
print("デモに使う order_id =", DEMO_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## まったく同じ質問を3者に投げて比べる
# MAGIC **同一の質問**を、持っている道具だけが違う3つのAIエージェントに投げます。
# MAGIC 全員に「**答えられない場合は、無理に推測せず『〜が分からないため判断できません』と理由を述べる**」と指示してあるので、差がはっきり出ます。
# MAGIC
# MAGIC 共通の質問：「注文 ORD-XXX は返品できますか？理由も教えてください。」
# MAGIC - **① データのみのAI**（注文情報ツールだけ）→ 事実は言えるが、ルールを知らず「判断できません」
# MAGIC - **② ルールのみのAI**（ルール検索ツールだけ）→ ルールは言えるが、注文の実データが無く「判断できません」
# MAGIC - **③ 統合AI**（両方）→ 事実 × ルールで返品可否を判断できる

# COMMAND ----------

# DBTITLE 1,セル7
# 3者に投げる「まったく同じ質問」と、全員共通のシステムプロンプト（答えられないなら答えられないと言う）
QUESTION = f"注文 {DEMO_ID} は返品できますか？理由も教えてください。"
COMMON_PROMPT = (
    "あなたは受発注アシスタントです。与えられたツールだけを使って日本語で答えてください。"
    "ツールで必要な情報が得られず答えられない場合は、推測で答えず、"
    "『〜が分からないため判断できません』と理由を明示してください。"
)
# 利用可能なFMエンドポイント（以下から選択）：
# - databricks-meta-llama-3-3-70b-instruct（推奨・高性能）
# - databricks-meta-llama-3-1-8b-instruct（軽量・高速）
# - databricks-llama-4-maverick（最新）
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

answers = {}
try:
    from databricks_langchain import ChatDatabricks
    from langchain_core.tools import tool
    from langgraph.prebuilt import create_react_agent

    @tool
    def get_order_info(order_id: int) -> str:
        "注文IDから注文情報（日付・金額・優先度・顧客・セグメント）を返す"
        return tool_order_info(order_id)

    @tool
    def get_rules(query: str) -> str:
        "社内の受発注ルール（返品・出荷・与信・キャンセル）を検索して返す"
        return tool_search_rules(query)

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT)

    def ask(tools):
        ag = create_react_agent(llm, tools=tools, prompt=COMMON_PROMPT)
        r = ag.invoke({"messages": [{"role": "user", "content": QUESTION}]})
        return r["messages"][-1].content

    # 同じ質問・同じプロンプトで、ツール構成だけを変えて3回
    answers["① データのみのAI"] = ask([get_order_info])
    answers["② ルールのみのAI"] = ask([get_rules])
    answers["③ 統合AI（両方）"] = ask([get_order_info, get_rules])
    print("✅ LangGraph/LLMが正常に動作しました。3つのAIエージェントから実際のLLM応答を取得しました。")
except Exception as e:
    print("⚠️ LangGraph/LLM が使えないため、手動オーケストレーション（フォールバック）で3パターンの回答を再現します。")
    print("   エラー内容:", str(e)[:200])

# COMMAND ----------

# DBTITLE 1,セル8
# フォールバック：LangGraph/LLMが使えない場合のみ、3者の「答え方の違い」を再現
if not answers:
    print("🔧 フォールバックモード：手動オーケストレーションで3パターンの回答を生成します。\n")
    info = json.loads(tool_order_info(DEMO_ID))               # 注文の事実
    rules = tool_search_rules("返品 何日以内 金額 要承認")     # 社内ルール
    # ① データのみ：ルールを参照できないので「判断できません」
    answers["① データのみのAI"] = (
        f"注文{DEMO_ID}の事実：注文日={info['order_date']} / 金額={float(info['order_amount']):,.0f}。"
        "ただし返品ルールを参照できないため、返品可否は判断できません。"
    )
    # ② ルールのみ：注文の実データが無いので「判断できません」
    answers["② ルールのみのAI"] = (
        "返品ルール：注文日から30日以内・金額10万以上は要承認。"
        "ただしこの注文の実データ（注文日・金額）が分からないため、この注文が返品可能かは判断できません。"
    )
    # ③ 統合：事実×ルールで判断できる
    order_date = datetime.date.fromisoformat(info["order_date"])
    days = (datetime.date(2026, 5, 31) - order_date).days     # 研修の基準日
    amount = float(info["order_amount"]); within = days <= 30; approval = amount >= 100000
    verdict = "返品可能（注文日から30日以内）" if within else "返品不可（注文日から30日超過）"
    if within and approval:
        verdict = "返品可能だが、金額が10万以上のため『要承認』"
    answers["③ 統合AI（両方）"] = (
        f"注文{DEMO_ID}は 注文日={info['order_date']} / 金額={amount:,.0f} / 基準日からの経過={days}日。"
        f"返品ルール（30日以内・10万以上は要承認）に照らすと → {verdict}。"
    )

# 結果表示
print("\n" + "="*64)
if answers and "① データのみのAI" in answers:
    print("🤖 以下は、LangGraph/LLMエージェントからの実際の応答です")
else:
    print("📝 以下は、フォールバックロジックで生成した回答です")
print("="*64)
print("【共通の質問】", QUESTION, "\n")
for k in ["① データのみのAI", "② ルールのみのAI", "③ 統合AI（両方）"]:
    print("=" * 64)
    print(k)
    print("-" * 64)
    print(answers.get(k, "(回答なし)"))
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ — 組み合わせると挙動が変わる
# MAGIC - 単体（データのみ／ルールのみ）では「返品できるか」に答えられない
# MAGIC - **統合エージェントは、注文の事実（Genie/データツール）× 返品ルール（RAG）を合わせて判断**できた
# MAGIC - これがスーパーバイザー（マルチエージェント）の価値です
# MAGIC
# MAGIC ## （補足・デモ動画）ノーコードでも作れる：Agent Bricks のスーパーバイザー
# MAGIC UI（Agent Bricks）でも、Genie スペース＋ナレッジアシスタントを束ねたスーパーバイザーを作れます。
# MAGIC ただし**ナレッジアシスタントは Free Edition 非対応**のため、ここは講師の Premium デモ動画で見せます。
# MAGIC
# MAGIC ▶ 〔講師デモ動画：Agent Bricks スーパーバイザー（Genie＋ナレッジアシスタント）作成（Premium収録）を差し込む〕
# MAGIC
# MAGIC 次は（任意）**⑦MLflowトレース / ⑧評価**、最後に **⑨本番への道筋** へ。