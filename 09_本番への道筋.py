# Databricks notebook source
# MAGIC %md
# MAGIC # ⑨ 本番への道筋 — サービングエンドポイント / AI Gateway（結び）
# MAGIC
# MAGIC 今日は Free Edition でエージェントを作り、その場でテストしました。
# MAGIC ここでは「**業務に乗せる**」ための仕組み（サービング・AI Gateway）と、本番（有償エディション）でできることを俯瞰します。
# MAGIC （このノートは説明中心。実行する必要はありません）

# COMMAND ----------

# MAGIC %md
# MAGIC ## サービングエンドポイントとは
# MAGIC 作ったエージェントを、業務システムやアプリから**常時呼べる REST API** にするのが **モデルサービング（サービングエンドポイント）** です。
# MAGIC
# MAGIC - エージェント／モデルを **REST API として公開** → 社内アプリ・基幹システム・チャットツールから同じエージェントを呼べる
# MAGIC - Unity Catalog のガバナンス・課金・監査・MLflow のトレースと**一体で運用**できる
# MAGIC - ④で作った「アプリ」も、裏側はサービングエンドポイント／エージェントを呼んでいるイメージ
# MAGIC
# MAGIC > ⚠️ Free Edition のサービングは制限あり（アクティブ数上限・GPU/Provisioned Throughput 不可・一部モデル不可）。
# MAGIC > 本番では、⑥のスーパーバイザーをサービング化し、全社のアプリから利用します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Gateway — モデル利用を一括で統制する“関所”
# MAGIC サービングエンドポイント化したら、その手前に **AI Gateway（Unity AI Gateway）** を効かせます。
# MAGIC
# MAGIC ### どこで設定する？（最新UI）
# MAGIC **サービングエンドポイントの作成／編集画面の「AI Gateway」セクション**で、各機能を個別に設定（コードでも可）。**エンドポイント単位**で効きます。
# MAGIC
# MAGIC | 機能 | 何ができる |
# MAGIC |---|---|
# MAGIC | レート制限 | ユーザー／エンドポイント単位で **QPM（毎分クエリ数）** 上限 |
# MAGIC | 使用量トラッキング | `system.serving.endpoint_usage` / `served_entities` に自動記録 |
# MAGIC | ガードレール | **PII 検出/マスキング**・不適切コンテンツのブロック・カスタムガードレール（入力/出力） |
# MAGIC | ペイロードロギング | リクエスト/レスポンスを推論テーブルに記録（監査・品質改善） |
# MAGIC | 外部モデル統合/ルーティング | GPT / Claude / Gemini 等を同じ口から管理・切替（安いモデルへ逃がす） |
# MAGIC
# MAGIC > 💡 Day1 で触れた「token maxing → value maxing（安いモデルへ賢く逃がす）」「統制とセットで全社展開」を、実際に効かせる場所がこの AI Gateway です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Free Edition でできること と、本番（有償）で解放されること（2026年6月末時点）
# MAGIC 2026年6月のアップデートで、**Free Edition には実務者向けの主要機能がほぼ含まれる**ようになりました。
# MAGIC 本研修で触れた機能の多くは **Free Edition で動きます**。本番（有償）で効いてくるのは主に「規模・運用・統制・セキュリティ」です。
# MAGIC
# MAGIC ### ✅ Free Edition に含まれる（＝今日触れた範囲）
# MAGIC | 領域 | 含まれる機能 |
# MAGIC |---|---|
# MAGIC | データエンジニアリング | Delta Lake / メダリオン / Lakeflow Connect / 宣言型パイプライン / **Lakeflow Designer（ノーコード）** |
# MAGIC | SQL・BI | Databricks SQL / AI/BIダッシュボード / **Genie** / **Genie Code** / AI関数（`ai_query` 等） |
# MAGIC | 生成AI | **Agent Bricks** / Vector Search / Model Serving・Foundation Model APIs / AI Playground / **スーパーバイザーエージェント** |
# MAGIC | ML | MLflow（実験・トレース・評価）/ **サーバーレスGPU** |
# MAGIC | データベース | **Lakebase（Postgres互換）** |
# MAGIC | ガバナンス | Unity Catalog（アクセス制御・リネージ・監査）/ Databricks Apps |
# MAGIC
# MAGIC > ※ 2026/06/17 のアップデートで Genie Code・GPU・Lakebase・Agent Bricks・Lakeflow Designer が追加されました。
# MAGIC > ロールアウト進行中で、公式の制限ページ（2026/06/01時点）と差がある項目もあるため、アカウントごとに実機で確認してください。
# MAGIC
# MAGIC ### 🔒 Free Edition の制限・非対応（＝本番＝有償で解放される）
# MAGIC | 区分 | Free Edition の制限・非対応 |
# MAGIC |---|---|
# MAGIC | コンピュート規模 | サーバーレスのみ・サイズ/使用量に上限、SQL Warehouse 1台(2X-Small)、Jobs 同時5、パイプライン種別ごと1本、GPU/カスタムコンピュート不可、フェアユースのクォータ（超過で当日停止） |
# MAGIC | Model Serving | **provisioned throughput 不可・GPUサービング不可**・アクティブ数に上限・一部モデル不可 |
# MAGIC | Vector Search | **1エンドポイント/1ユニット・Direct Vector Access 不可** |
# MAGIC | Apps | 最大3個・起動から24hで自動停止 |
# MAGIC | エージェント（一部） | **Knowledge Assistant は非対応**（スーパーバイザーの候補には表示されるが作成不可。本研修ではデモ動画で紹介） |
# MAGIC | 管理 | 1ワークスペース/1メタストア・アカウントコンソール/Admin API 不可・SLA/サポートなし・**商用利用不可** |
# MAGIC | セキュリティ | SSO/SCIM・Private Link/IPアクセスリスト・顧客管理キー(CMK)・コンプライアンス強制 は非対応 |
# MAGIC | その他 | R / Scala・カスタムワークスペースストレージ・Online tables・Clean rooms は非対応 |
# MAGIC
# MAGIC > 進め方の目安：まず Free Edition で「Delta化 → Genie/RAG → 統合エージェント」を社内データで体験し、
# MAGIC > 価値が見えたら **規模・運用・セキュリティ**の要件に応じて本番（有償）へ移行するのが王道です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 今日のまとめ（Day2 全体）
# MAGIC ① データを整える（メダリオン）→ ② SQLでAI → ③ 社内文書のRAG → ④ Genie＋アプリ → ⑤⑥ ツール＆統合エージェント → ⑦⑧ 可視化・評価 → ⑨ 本番化。
# MAGIC
# MAGIC **単体を束ねると、単体ではできない回答ができる**——これがマルチエージェントの価値であり、Databricks が「データ × AI を1つの基盤で」掲げる理由です。
# MAGIC
# MAGIC お疲れさまでした。
