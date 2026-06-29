# Databricks notebook source
# MAGIC %md
# MAGIC # ③ RAGナレッジ — 社内ルール文書をAIが読めるようにする（ノーコード）
# MAGIC
# MAGIC 取引データ（②/④のSQL・Genie）とは別に、**社内の受発注ルール（文書）**をAIが検索・回答できるようにします（RAG）。
# MAGIC このパートは**ノーコード**で進めます。
# MAGIC
# MAGIC ## このノートの目的（所要：約40分）
# MAGIC 1. 複数形式の社内文書（md / txt / csv / html / pdf）を Volume に用意する
# MAGIC 2. その文書をソースに、**Genie Code に日本語で指示**してノーコードでベクトル検索インデックスを作る
# MAGIC 3. **Playground** で、そのインデックスを読むエージェントを作り、文書内容を読めることを確認する
# MAGIC
# MAGIC > 作るインデックス名は **`rules_index`** に固定します（⑤の統合エージェントで再利用するため）。

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：社内文書（複数形式）を Volume に置く
# MAGIC まず文書の置き場（Volume `rules_docs`）を作り、サンプルの受発注ルール文書を配置します。
# MAGIC 下のセルは学習用にコードで配置しますが、配布された `受発注ルール文書_サンプル.zip` を
# MAGIC 画面から Volume にアップロードしてもOKです（カタログ → 自分のスキーマ → rules_docs → アップロード）。

# COMMAND ----------

import os
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.rules_docs")
base = f"/Volumes/{catalog}/{schema}/rules_docs"

docs = {
"返品ポリシー.md": """# 返品ポリシー（受発注ルール）
- 返品は、注文日（order_date）から30日以内に申請してください。30日を過ぎた返品は受け付けません。
- 注文金額（order_amount）が 100,000 以上の返品は、マネージャーの承認が必要です（要承認）。
- 返品理由は「不良品」「数量誤り」「顧客都合」のいずれかを記録します。
- 優先出荷の案件でも、返品ルール（30日以内・10万以上は要承認）は同じです。
""",
"出荷・優先度規程.txt": """出荷・優先度規程（受発注ルール）
1. order_priority が "1-URGENT" または "2-HIGH" の注文は「優先出荷」の対象です。
2. それ以外（3-MEDIUM / 4-NOT SPECIFIED / 5-LOW）は通常便で出荷します。
3. 優先出荷に追加料金は発生しません（サービス）。
4. 優先出荷対象は、当日15時までの受注分を当日出荷します。
""",
"割引・与信ルール.csv": """market_segment,credit_limit_jpy,discount_rule
AUTOMOBILE,50000000,年間売上1億以上の大口は3%割引
BUILDING,80000000,年間売上1億以上の大口は5%割引
FURNITURE,30000000,キャンペーン期間のみ割引
MACHINERY,60000000,大口は4%割引
HOUSEHOLD,20000000,割引なし
""",
"受発注FAQ.html": """<!doctype html><html lang="ja"><body>
<h1>受発注FAQ</h1>
<h2>Q. 注文をキャンセルしたい</h2><p>出荷前はキャンセル可能。出荷後はキャンセル不可で返品扱い（返品ポリシー参照）。</p>
<h2>Q. 優先出荷にできますか</h2><p>order_priority が 1-URGENT / 2-HIGH の注文が優先出荷の対象です。</p>
<h2>Q. 与信枠を超えそう</h2><p>market_segment ごとの与信枠は「割引・与信ルール」を参照。超過時は受注を保留し承認を得ます。</p>
</body></html>
""",
"キャンセルポリシー.md": """# キャンセルポリシー（受発注ルール）
- 出荷前の注文はキャンセル可能です。
- 出荷後はキャンセルできません。返品ポリシーに従ってください。
- 優先出荷（order_priority が 1-URGENT / 2-HIGH）の注文は、受注確定後30分以内のみキャンセル可能です。
""",
}
for name, body in docs.items():
    with open(f"{base}/{name}", "w", encoding="utf-8") as f:
        f.write(body)
print("社内ルール文書を配置しました:", base)
display(dbutils.fs.ls(base))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ2：「ビジュアルデータの準備」から Genie Code に頼んでインデックスを作る
# MAGIC **⚠️ 重要：投げる場所に注意。** ノートブック内のアシスタントにプロンプトを投げると、**このノートブックを編集してしまいます**。
# MAGIC 必ず次の場所から実行してください：
# MAGIC
# MAGIC 1. 左サイドバー **「ビジュアルデータの準備」** を開く（Lakeflow Designer / ノーコードのデータ準備）
# MAGIC 2. 右上 **「＋ 作成」** をクリック
# MAGIC 3. そこで開く **Genie Code** に、下のセルで出力されるプロンプトを貼り付けて実行
# MAGIC
# MAGIC - Genie Code が**パイプラインを自動で組み**、ベクトル検索インデックスを作ってくれます（＝ノーコード）。
# MAGIC - 完了後、作られたインデックス（`rules_index`）を確認します。
# MAGIC - うまくいかないときは「インデックス名は rules_index にして」と一言追記すると確実です。
# MAGIC
# MAGIC > 💡 これが Genie Code の価値：やりたいことを日本語で言えば、データパイプラインをAIが組んでくれる。
# MAGIC > ⚠️ Free Edition の Vector Search は 1エンドポイント・1ユニット・Direct Vector Access 不可（Delta Sync 方式）。
# MAGIC
# MAGIC 👇 次のセルを実行すると、**コピペ用のプロンプト**（自分のカタログ・スキーマ入り）が出力されます。

# COMMAND ----------

# 下の出力をそのままコピーして、「ビジュアルデータの準備」→「＋作成」→ Genie Code に貼り付けてください
prompt = (
    f"ビジュアルデータの準備から、Volume `{catalog}.{schema}.rules_docs` にあるファイルを読み込み、"
    f"ベクトル検索インデックステーブル rules_index を作成してください。"
)
print("===== Genie Code に貼り付けるプロンプト（ここをコピー）=====\n")
print(prompt)
print("\n===========================================================")
print("貼り付け先 → 左サイドバー「ビジュアルデータの準備」→ 右上「＋作成」→ Genie Code")

# COMMAND ----------

# （確認）配置した社内ルール文書の一覧。これらが Genie Code のインデックス化対象になります。
display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/rules_docs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3：Playground で「文書を読むエージェント」を作って確認
# MAGIC 1. 左メニュー **「Playground」** を開く
# MAGIC 2. **「ツール」を追加 → 「AI検索インデックス（Vector Search Index）」** から `rules_index` を選ぶ
# MAGIC 3. 日本語で質問し、文書の内容を読めることを確認：
# MAGIC    - 「返品は何日以内？金額の条件は？」（→ 返品ポリシー）
# MAGIC    - 「優先出荷の条件は？」（→ 出荷・優先度規程）
# MAGIC    - 「出荷後にキャンセルできる？」（→ キャンセルポリシー / FAQ）
# MAGIC 4. 回答の**根拠（どの文書から引いたか）**も確認する
# MAGIC
# MAGIC これで「社内文書に自然言語で答えるエージェント（RAG）」が完成です。
# MAGIC ⑤では、この `rules_index` を"ルール検索ツール"としてスーパーバイザーに渡し、取引データのツールと統合します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC - 複数形式の社内文書を Volume に置き、**Genie Code にノーコードで指示**して Vector Search インデックス（`rules_index`）を作った
# MAGIC - **Playground のエージェント**が、その文書内容を自然言語で読めることを確認した
# MAGIC - ⑤で、この `rules_index` を"ルール検索ツール"にして、取引データのツールと統合します
# MAGIC 次は **④Genieとダッシュボード** へ。