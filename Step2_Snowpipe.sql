CREATE OR REPLACE PIPE DEMO.PALTAC.CAR_PRODUCT_PIPE
  AUTO_INGEST = TRUE  -- S3イベント通知による自動取り込みを有効化
AS
COPY INTO DEMO.PALTAC.CAR_PRODUCT_LOADED
FROM (
  SELECT 
    $1:DATE::DATE,
    $1:COMPANY_NAME::VARCHAR,
    $1:PRODUCT_ID::VARCHAR,
    $1:PRODUCT_NAME::VARCHAR,
    $1:SELLING_PRICE::NUMBER,
    $1:POINTS::NUMBER,
    $1:SHIPPING_FEE::NUMBER,
    $1:STOCK_QUANTITY::NUMBER,
    $1:COST::NUMBER,
    METADATA$FILENAME::VARCHAR,         -- ロード元ファイル名を保存
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @DEMO.PALTAC.S3_STAGE/from_snowflake/
)
PATTERN = '.*car_product_part.*\\.parquet'
FILE_FORMAT = DEMO.PALTAC.PARQUET_FORMAT;

-- =============================================================================
-- 3. S3イベント通知の設定（AWS側で必要）
-- =============================================================================

-- Snowpipeの通知チャネル（SQS ARN）を取得
-- このARNをS3バケットのイベント通知に設定する必要があります
SHOW PIPES LIKE 'CAR_PRODUCT_PIPE' IN SCHEMA DEMO.PALTAC;


select * from DEMO.PALTAC.CAR_PRODUCT_LOADED;
