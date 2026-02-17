%flink.ssql
-- ==========================================
-- 1. ソーステーブルの定義 
-- ==========================================
DROP TABLE IF EXISTS source_stream;
CREATE TABLE source_stream (
    `timestamp` STRING,
    `machine_id` STRING,
    `product_id` STRING,
    `md_length (m)` DOUBLE,
    `td_position (mm)` INT,
    `thickness (µm)` DOUBLE,
    `line_speed (m/min)` DOUBLE,
    `event_time` AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'spc-stream',
    'aws.region' = 'ap-northeast-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'scan.stream.recordpublisher' = 'EFO',
    'scan.stream.efo.consumername' = 'spc-anomaly-detection-consumer'
);

-- ==========================================
-- 2. 参照データテーブル の定義
-- ==========================================
DROP TABLE IF EXISTS reference_data;
CREATE TABLE reference_data (
    `product_id` STRING,
    `date_key` STRING,
    `mean` DOUBLE,
    `std_dev` DOUBLE,
    `ucl_i_d` DOUBLE,
    `lcl_i_d` DOUBLE,
    `ucl_d` DOUBLE,
    `lcl_d` DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://sample-coating-data/calculation-results/',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

-- ==========================================
-- 3. 出力テーブルの定義
-- ==========================================
DROP TABLE IF EXISTS output_stream_sink;
CREATE TABLE output_stream_sink (
    `AnomalyType` STRING,
    `TargetTime` TIMESTAMP(3),
    `MeasuredValue` DOUBLE,
    `UpperLimit` DOUBLE,
    `LowerLimit` DOUBLE,
    `BaselineRef` STRING,
    `ProductId` STRING
) WITH (
    'connector' = 'kinesis',
    'stream' = 'OutputStream',
    'aws.region' = 'ap-northeast-1',
    'format' = 'json'
);

-- ==========================================
-- 4. 異常検知ロジック
-- ==========================================
INSERT INTO output_stream_sink
WITH SourcePrep AS (
    SELECT
        event_time,
        `thickness (µm)` AS thickness,
        product_id,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS date_key
    FROM source_stream
),
CalcRowNum AS (
    SELECT
        event_time,
        date_key,
        product_id,
        thickness,
        ROW_NUMBER() OVER (
            PARTITION BY date_key, product_id 
            ORDER BY event_time
        ) AS row_num
    FROM SourcePrep
),
CalcMovAvg AS (
    SELECT
        event_time,
        date_key,
        product_id,
        thickness,
        row_num,
        AVG(thickness) OVER (
            PARTITION BY date_key, product_id
            ORDER BY event_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS moving_avg
    FROM CalcRowNum
),
FinalJoined AS (
    SELECT
        c.event_time,
        c.date_key,
        c.product_id,
        c.thickness,
        c.moving_avg,
        c.row_num,
        r.ucl_d, r.lcl_d, r.ucl_i_d, r.lcl_i_d
    FROM CalcMovAvg c
    INNER JOIN reference_data r 
        ON c.date_key = r.date_key
        AND c.product_id = r.product_id
),
-- ここで判定を行い、AnomalyTypeを確定させる
JudgedData AS (
    SELECT
        CASE
            WHEN MOD(row_num, 5) = 0 THEN
                CASE
                    WHEN moving_avg > ucl_d OR moving_avg < lcl_d THEN 'Confirmed'
                    ELSE 'Normal'
                END
            ELSE
                CASE
                    WHEN thickness > ucl_i_d OR thickness < lcl_i_d THEN 'Provisional'
                    ELSE 'Normal'
                END
        END AS AnomalyType,
        
        event_time AS TargetTime,
        
        CASE WHEN MOD(row_num, 5) = 0 THEN moving_avg ELSE thickness END AS MeasuredValue,
        CASE WHEN MOD(row_num, 5) = 0 THEN ucl_d ELSE ucl_i_d END AS UpperLimit,
        CASE WHEN MOD(row_num, 5) = 0 THEN lcl_d ELSE lcl_i_d END AS LowerLimit,
        date_key AS BaselineRef,
        product_id AS ProductId
    FROM FinalJoined
)
-- Normal以外をフィルタリングして出力
SELECT 
    AnomalyType,
    TargetTime,
    MeasuredValue,
    UpperLimit,
    LowerLimit,
    BaselineRef,
    ProductId
FROM JudgedData
WHERE AnomalyType <> 'Normal';