--------------------------------------------------------------------------------
-- S3ステージからParquetファイルを自動ロードするCDCパイプライン
-- 
-- 概要:
--   ストリームを使用してS3ステージのファイル変更を検知し、
--   新規ファイルのみをロード、削除されたファイルのレコードを削除する
--
-- パス形式の注意:
--   - METADATA$FILENAME: 'unload/from_snowflake/car_product_partX.parquet...'
--   - RELATIVE_PATH:     'from_snowflake/car_product_partX.parquet...'
--   → JOIN時は 'unload/' || RELATIVE_PATH で一致させる
--   → SOURCE_FILE_NAMEにはRELATIVE_PATHを保存（DELETE時の比較用）
--------------------------------------------------------------------------------

USE DATABASE DEMO;
USE SCHEMA PALTAC;

-- =============================================================================
-- 1. 前提オブジェクトの作成
-- =============================================================================

-- ステージのディレクトリテーブルを有効化（ファイル変更の追跡に必要）
ALTER STAGE DEMO.PALTAC.S3_STAGE SET DIRECTORY = (ENABLE = TRUE);

-- ディレクトリテーブル上にストリームを作成（CDC用）
CREATE OR REPLACE STREAM DEMO.PALTAC.S3_STAGE_STREAM ON STAGE DEMO.PALTAC.S3_STAGE;

-- Parquetファイルフォーマットを作成
CREATE FILE FORMAT IF NOT EXISTS DEMO.PALTAC.PARQUET_FORMAT TYPE = PARQUET;

-- ロード先テーブルを作成
CREATE OR REPLACE TABLE DEMO.PALTAC.CAR_PRODUCT_LOADED (
    DATE              DATE,
    COMPANY_NAME      VARCHAR,
    PRODUCT_ID        VARCHAR,
    PRODUCT_NAME      VARCHAR,
    SELLING_PRICE     NUMBER,
    POINTS            NUMBER,
    SHIPPING_FEE      NUMBER,
    STOCK_QUANTITY    NUMBER,
    COST              NUMBER,
    SOURCE_FILE_NAME  VARCHAR,                                  -- 追跡用: ロード元ファイル名
    LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- 追跡用: ロード日時
);

-- =============================================================================
-- 2. CDCタスクの作成
-- =============================================================================

CREATE OR REPLACE TASK DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK
  TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('DEMO.PALTAC.S3_STAGE_STREAM')
AS
BEGIN
  -- -------------------------------------------------------------------------
  -- INSERT: ストリームで検知した新規ファイルのみをロード
  -- 
  -- ポイント:
  --   - ストリーム(RELATIVE_PATH)とステージ(METADATA$FILENAME)をJOIN
  --   - パス形式が異なるため 'unload/' を付与して一致させる
  --   - SOURCE_FILE_NAMEにはRELATIVE_PATHを保存（DELETE時の比較用）
  -- -------------------------------------------------------------------------
  INSERT INTO DEMO.PALTAC.CAR_PRODUCT_LOADED
  SELECT 
    f.$1:DATE::DATE,
    f.$1:COMPANY_NAME::VARCHAR,
    f.$1:PRODUCT_ID::VARCHAR,
    f.$1:PRODUCT_NAME::VARCHAR,
    f.$1:SELLING_PRICE::NUMBER,
    f.$1:POINTS::NUMBER,
    f.$1:SHIPPING_FEE::NUMBER,
    f.$1:STOCK_QUANTITY::NUMBER,
    f.$1:COST::NUMBER,
    s.RELATIVE_PATH::VARCHAR,           -- SOURCE_FILE_NAME: DELETE時の比較に使用
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM DEMO.PALTAC.S3_STAGE_STREAM s
  JOIN @DEMO.PALTAC.S3_STAGE (FILE_FORMAT => 'DEMO.PALTAC.PARQUET_FORMAT') f
    ON f.METADATA$FILENAME = 'unload/' || s.RELATIVE_PATH
  WHERE s.METADATA$ACTION = 'INSERT'
    AND s.RELATIVE_PATH LIKE '%from_snowflake/car_product_part%.parquet%';

  -- -------------------------------------------------------------------------
  -- DELETE: 削除されたファイルに対応するレコードを削除
  -- 
  -- ポイント:
  --   - SOURCE_FILE_NAMEとRELATIVE_PATHは同じ形式なのでそのまま比較可能
  --   - 'unload/' の付与は不要
  -- -------------------------------------------------------------------------
  DELETE FROM DEMO.PALTAC.CAR_PRODUCT_LOADED
  WHERE SOURCE_FILE_NAME IN (
    SELECT RELATIVE_PATH
    FROM DEMO.PALTAC.S3_STAGE_STREAM
    WHERE METADATA$ACTION = 'DELETE'
      AND RELATIVE_PATH LIKE '%from_snowflake/car_product_part%.parquet%'
  );
END;

-- =============================================================================
-- 3. タスクの開始
-- =============================================================================

ALTER TASK DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK RESUME;

-- =============================================================================
-- 4. 確認用クエリ（必要に応じて実行）
-- =============================================================================

-- タスクの状態を確認
-- SHOW TASKS LIKE 'LOAD_CAR_PRODUCT_TASK' IN SCHEMA DEMO.PALTAC;

-- タスクを停止する場合
-- ALTER TASK DEMO.PALTAC.LOAD_CAR_PRODUCT_TASK SUSPEND;

-- ロード結果を確認
-- SELECT COUNT(*) AS TOTAL_LOADED FROM DEMO.PALTAC.CAR_PRODUCT_LOADED;
-- SELECT SOURCE_FILE_NAME, COUNT(*) FROM CAR_PRODUCT_LOADED GROUP BY SOURCE_FILE_NAME;

-- ストリームの状態を確認
-- SELECT * FROM DEMO.PALTAC.S3_STAGE_STREAM;
