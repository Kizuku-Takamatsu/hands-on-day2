# Day2 ノートブック（一本のシナリオ：現場の問い合わせに答えるAIアシスタントを組み上げる）

**番号順（00 → 09）**に開いて進めます。コード中心の回は「すべて実行 / Run all」で完走、UI中心の回（③④の一部）は記載の手順に沿って操作します。

| ファイル | パート | 役割 | 形態 |
|---|---|---|---|
| `_setup.py` | 共通 | スキーマ自動作成＋教材テーブル（冪等）。各ノート先頭で `%run ./_setup` | （自動） |
| `00_はじめに.py` | 0 | 環境チェック | コード |
| `01_データ探索とメダリオン.py` | ① | 生→Bronze→Silver→Gold＋Auto Loader、Volume、Lakeflow Designer(任意) | コード＋UI手順 |
| `02_AI関数とインサイト.py` | ② | `ai_query`/`ai_gen`、カスタム関数は範囲外 | コード |
| `03_RAGナレッジ.py` | ③ | 多形式文書→Genie Codeでノーコードに `rules_index`→Playgroundでエージェント確認 | ノーコード＋確認コード |
| `04_Genieとダッシュボード.py` | ④ | Genie＋ダッシュボード＋Playground→Apps | UI手順＋確認コード |
| `05_ツール作成.py` | ⑤-1 | UC関数 `search_order_info`／`search_rules`／`record_request` | コード(`%pip`) |
| `06_スーパーバイザーAgent.py` | ⑤-2 | LangGraphで統合＋3者比較（山場）。LLM不可環境は手動統合にフォールバック | コード(`%pip`) |
| `07_MLflowトレース.py` | ⑥ | エージェント実行の可視化（任意） | コード(`%pip`) |
| `08_MLflow評価と改善.py` | ⑦ | `mlflow.genai.evaluate` で採点→改善（任意） | コード(`%pip`) |
| `09_本番への道筋.py` | 結 | Serving／AI Gateway／本番でできること | 説明(md) |

関連配布物（`day2/` 直下）：`受発注ルール文書_サンプル.zip`（③で使う多形式文書。中身は `day2/rag_docs/`）。

## シナリオの役割分担
- **Genie（④）**：取引ログ（`orders_silver`/`sales_monthly_gold`）への自然言語クエリ
- **RAGナレッジ（③ `rules_index`）**：社内の受発注ルール文書の検索
- **スーパーバイザー（⑥）**：両者を束ね「データ × ルール」を統合判断（例：「注文ORD-XXXは返品できる？」）

## 設計上の工夫
- 入力操作なし（スキーマは `current_user()` から自動生成）／冪等（`IF NOT EXISTS`・`OR REPLACE`）
- コード中心の回は Run All 完走。`%pip` が要る 05–08 は「最初に1回 pip→再起動→上から再実行」
- エラーで止めない（未提供機能は try/except でスキップ＋本番説明）／カタログ差を `current_catalog()` で吸収
- **Free Edition 非対応のナレッジアシスタント等は使わず**、講師の Premium デモ動画プレースホルダ（③⑥）に集約

## ⚠ 旧ファイルの削除（push 前に）
旧構成のファイルが残っている場合は削除してください（このリポジトリでは使いません）：
`01_データ探索.py` / `03_Genieとダッシュボード.py`
→ 例：`git rm "day2/notebooks/01_データ探索.py" "day2/notebooks/03_Genieとダッシュボード.py"`
