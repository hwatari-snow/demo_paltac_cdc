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

