CREATE EXTERNAL TABLE IF NOT EXISTS coating_data (
  `timestamp` string,
  `thickness` double,
  `product_id` string
)
PARTITIONED BY (datehour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://XXXXXXXXXXX/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.datehour.format' = 'yyyy/MM/dd/HH',
  'projection.datehour.type' = 'date',
  'projection.datehour.range' = '2024/01/01/00,NOW',
  'projection.datehour.interval' = '1',
  'projection.datehour.interval.unit' = 'HOURS',
  'storage.location.template' = 's3://XXXXXXXXXXXX/${datehour}/'
)