# S3ステージ CDC パイプライン

S3ステージに配置されたParquetファイルを自動的にSnowflakeテーブルにロードするCDC（Change Data Capture）パイプラインです。

## 概要

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│   S3 Bucket │────▶│  S3_STAGE       │────▶│  S3_STAGE_STREAM     │
│  (Parquet)  │     │  (External)     │     │  (Directory Stream)  │
└─────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                       │
                                                       ▼
                                            ┌──────────────────────┐
                                            │  LOAD_CAR_PRODUCT    │
                                            │  _TASK               │
                                            │  (1分間隔で実行)      │
                                            └──────────┬───────────┘
                                                       │
                                    ┌──────────────────┴──────────────────┐
                                    ▼                                      ▼
                           ┌───────────────┐                      ┌───────────────┐
                           │    INSERT     │                      │    DELETE     │
                           │  (新規ファイル) │                      │ (削除ファイル) │
                           └───────┬───────┘                      └───────┬───────┘
                                   │                                      │
                                   ▼                                      ▼
                           ┌─────────────────────────────────────────────────────┐
                           │              CAR_PRODUCT_LOADED                     │
                           │              (ロード先テーブル)                       │
                           └─────────────────────────────────────────────────────┘
```

## 使用するオブジェクト

| オブジェクト | 種類 | 説明 |
|-------------|------|------|
| `DEMO.PALTAC.S3_STAGE` | External Stage | S3バケットへの接続 |
| `DEMO.PALTAC.S3_STAGE_STREAM` | Directory Stream | ステージのファイル変更を追跡 |
| `DEMO.PALTAC.PARQUET_FORMAT` | File Format | Parquet形式の定義 |
| `DEMO.PALTAC.CAR_PRODUCT_LOADED` | Table | ロード先テーブル |
| `DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK` | Task | 自動ロードタスク |

## パス形式の注意点

ステージとストリームでファイルパスの形式が異なります。

| ソース | カラム | パス形式 |
|--------|--------|----------|
| ステージ | `METADATA$FILENAME` | `unload/from_snowflake/car_product_partX.parquet...` |
| ストリーム | `RELATIVE_PATH` | `from_snowflake/car_product_partX.parquet...` |

### JOIN時の対応

```sql
-- ステージとストリームをJOINする際は 'unload/' を付与
JOIN @DEMO.PALTAC.S3_STAGE f
  ON f.METADATA$FILENAME = 'unload/' || s.RELATIVE_PATH
```

### SOURCE_FILE_NAME の保存

`SOURCE_FILE_NAME`には`RELATIVE_PATH`を保存します。これにより、DELETE時にストリームの`RELATIVE_PATH`とそのまま比較できます。

```sql
-- INSERT時: RELATIVE_PATHを保存
s.RELATIVE_PATH::VARCHAR  -- → 'from_snowflake/car_product_part1.parquet...'

-- DELETE時: そのまま比較（'unload/'不要）
WHERE SOURCE_FILE_NAME IN (SELECT RELATIVE_PATH FROM STREAM WHERE ACTION = 'DELETE')
```

## テーブルスキーマ

### CAR_PRODUCT_LOADED

| カラム | 型 | 説明 |
|--------|-----|------|
| DATE | DATE | 日付 |
| COMPANY_NAME | VARCHAR | 会社名 |
| PRODUCT_ID | VARCHAR | 商品ID |
| PRODUCT_NAME | VARCHAR | 商品名 |
| SELLING_PRICE | NUMBER | 販売価格 |
| POINTS | NUMBER | ポイント |
| SHIPPING_FEE | NUMBER | 送料 |
| STOCK_QUANTITY | NUMBER | 在庫数 |
| COST | NUMBER | 原価 |
| SOURCE_FILE_NAME | VARCHAR | ロード元ファイル名（追跡用） |
| LOADED_AT | TIMESTAMP_NTZ | ロード日時（追跡用） |

## 運用コマンド

### タスクの開始

```sql
ALTER TASK DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK RESUME;
```

### タスクの停止

```sql
ALTER TASK DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK SUSPEND;
```

### タスクの状態確認

```sql
SHOW TASKS LIKE 'LOAD_CAR_PRODUCT_TASK' IN SCHEMA DEMO.PALTAC;
```

### タスク実行履歴の確認

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'LOAD_CAR_PRODUCT_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));
```

### ロード結果の確認

```sql
-- 総レコード数
SELECT COUNT(*) AS TOTAL_LOADED FROM DEMO.PALTAC.CAR_PRODUCT_LOADED;

-- ファイル別のレコード数
SELECT SOURCE_FILE_NAME, COUNT(*) AS RECORD_COUNT 
FROM DEMO.PALTAC.CAR_PRODUCT_LOADED 
GROUP BY SOURCE_FILE_NAME
ORDER BY SOURCE_FILE_NAME;
```

### ストリームの状態確認

```sql
-- 未処理の変更を確認
SELECT * FROM DEMO.PALTAC.S3_STAGE_STREAM;

-- ストリームにデータがあるか確認
SELECT SYSTEM$STREAM_HAS_DATA('DEMO.PALTAC.S3_STAGE_STREAM');
```

## ファイル構成

```
paltac-poc/
├── README.md                    # このファイル
└── Step1_unload_parquet.sql     # CDCパイプライン構築SQL
```

## 参考ドキュメント

### Snowflake公式ドキュメント

- [Directory Tables](https://docs.snowflake.com/en/user-guide/data-load-dirtables)
- [Streams on Directory Tables](https://docs.snowflake.com/en/user-guide/data-load-dirtables-pipeline)
- [CREATE STREAM (Directory Table)](https://docs.snowflake.com/en/sql-reference/sql/create-stream#directory-tables)
- [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [CREATE TASK](https://docs.snowflake.com/en/sql-reference/sql/create-task)
- [COPY INTO <table>](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Querying Staged Data](https://docs.snowflake.com/en/user-guide/querying-stage)

### 関連機能

- [SYSTEM$STREAM_HAS_DATA](https://docs.snowflake.com/en/sql-reference/functions/system_stream_has_data)
- [METADATA$FILENAME](https://docs.snowflake.com/en/user-guide/querying-metadata)
- [Parquet File Format](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-parquet)

