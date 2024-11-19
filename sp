CREATE OR REPLACE PROCEDURE ${aether_5g_core_module_tgt_dataset_name}.aether_amf_performance_sp(process_ts STRING,trans_ts STRING,window_hour int64,window_interval int64)
options(strict_mode=False)
BEGIN
  --Insert entry into audit table
MERGE `${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname}` tgt
USING
  (SELECT
    CONCAT("5g core amf hourly process") as prc_name,
    safe_cast(trans_ts AS datetime) AS start_time,
    safe_cast(SPLIT(trans_ts,' ')[
    OFFSET
      (0)] AS date) src_prc_dt 
  ) src
ON ( src.prc_name=tgt.PROCESS_NAME AND src.START_TIME=tgt.START_TIME)
  WHEN NOT MATCHED THEN INSERT (
    PROCESS_MODULE,
    SCHEDULER,
    PROCESS_NAME,
    SOURCE_NAME,
    TARGET_NAME,
    START_TIME,
    END_TIME,
    PROCESS_DT,
    NUM_RECORDS_AFFECTED,
    STATUS,
    RETURN_MESSAGE)
  VALUES
  ("aether",
    "airflow",
    prc_name,
    "${aether_5g_core_module_src_project_id}.${aether_5g_core_module_src_dataset_name}.${aether_5g_core_module_src_amf_tblname}",
    "${aether_5g_core_module_tgt_project_id}.${aether_5g_core_module_tgt_dataset_name}.${aether_5g_core_module_tgt_amf_tblname}",
     start_time,
     NULL,
     src_prc_dt,
     NULL,
     "Started",
     NULL
  );

merge into `${aether_5g_core_module_tgt_project_id}.${aether_5g_core_module_tgt_dataset_name}.${aether_5g_core_module_tgt_amf_tblname}` tgt
using (
select
 event_time as event_time,
"ericsson" as  vendor,
fqdn,
KEY as metric_name,
safe_cast(increase_value as bignumeric) as metric_increase_value,
safe_cast(sum_value as bignumeric) as metric_sum_value,
trans_dt,
datetime(process_ts) as schedule_time,
current_timestamp as updated_timestamp
from
(
WITH event_timestamps as
(
select distinct DATETIME_TRUNC(timestamp(`timestamp`),HOUR) as trans_hr from  `${aether_5g_core_module_src_project_id}.${aether_5g_core_module_src_dataset_name}.${aether_5g_core_module_src_amf_tblname}`
  where DATETIME_TRUNC(timestamp(insert_date_utc),HOUR) in
  unnest(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_SUB(DATETIME_TRUNC(timestamp(trans_ts),HOUR),INTERVAL window_interval-1 HOUR),DATETIME_TRUNC(timestamp(trans_ts),HOUR),INTERVAL window_hour HOUR))
--   and trans_dt=date(timestamp(trans_ts))
   and trans_dt is not null-- need to check
),

base_data AS (
  select *,
  MD5(labels) as checksum
  from(
  SELECT
  distinct
    TIMESTAMP_SECONDS(CAST(FLOOR(UNIX_SECONDS(`timestamp`)/(window_hour*60*60)) * (window_hour*60*60) AS INT64)) AS event_time,
    fqdn,
    TO_JSON_STRING(JSON_REMOVE(SAFE.PARSE_JSON(labels),'$.__name__','$.jobid','$.localdn')) as labels,
    JSON_VALUE(labels,'$.instance') as instance,
    lower(name) as KEY,
    SAFE_CAST(nullif(value,'NaN') AS FLOAT64) AS value,
    DATE(`timestamp`) AS trans_dt,
    `timestamp`,
  FROM
    `${aether_5g_core_module_src_project_id}.${aether_5g_core_module_src_dataset_name}.${aether_5g_core_module_src_amf_tblname}`
  WHERE
    DATETIME_TRUNC(timestamp(`timestamp`),HOUR) in (select trans_hr from event_timestamps)
    and insert_date_utc > (select min(trans_hr) from event_timestamps)
and trans_dt in (select date(trans_hr) from event_timestamps)
and trans_dt is not null
)),

window_data AS (
select *,if(value<prev_value,1,0) has_reset from (
select
   trans_dt,
   event_time,
   fqdn,
   labels,
   checksum,
   instance,
   KEY,
   value,
   `timestamp`,
SAFE_CAST(LAG(value) OVER (PARTITION BY fqdn, checksum, instance, KEY, DATETIME_TRUNC(timestamp,HOUR) ORDER BY `timestamp`) AS FLOAT64) AS prev_value
from base_data)
),

reset_adjusted AS (
  SELECT
    event_time,
    fqdn,
    instance,
    trans_dt,
    MAX(labels) AS labels,
    checksum,
    KEY,
    sum(value) as sum_value,
    SUM(CASE
        WHEN prev_value is NULL THEN 0
        WHEN has_reset =1 THEN value
        ELSE value - IFNULL(prev_value,0)
    END
      ) AS increase_value
  FROM
    window_data
  GROUP BY
  trans_dt,
    event_time,
    instance,
    fqdn,
    checksum,
    KEY )

SELECT
  event_time,
  fqdn,
  trans_dt,
  KEY,
  sum(increase_value) as increase_value,
  sum(sum_value) as sum_value,
FROM
  reset_adjusted
group BY
  trans_dt,
    event_time,
    fqdn,
    KEY
order by
 trans_dt,
event_time,
fqdn,
KEY)
) src
on tgt.trans_dt=src.trans_dt
and tgt.event_time=src.event_time
and tgt.fqdn=src.fqdn
and tgt.metric_name=src.metric_name
and tgt.vendor=src.vendor
and tgt.trans_dt is not null

WHEN MATCHED
    THEN UPDATE SET
    tgt.metric_increase_value=src.metric_increase_value,
    tgt.metric_sum_value=src.metric_sum_value,
    tgt.schedule_time=src.schedule_time,
    tgt.updated_timestamp=src.updated_timestamp

when not matched then
insert
(event_time,
vendor,
fqdn,
metric_name,
metric_increase_value,
metric_sum_value,
trans_dt,
schedule_time,
updated_timestamp
)
values(
src.event_time,
src.vendor,
src.fqdn,
src.metric_name,
src.metric_increase_value,
src.metric_sum_value,
src.trans_dt,
src.schedule_time,
src.updated_timestamp
);

--update audit table with completed status
UPDATE
  `${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname}`
SET
  END_TIME=CURRENT_DATETIME(),
  NUM_RECORDS_AFFECTED=@@ROW_COUNT,
  STATUS="Completed",
  RETURN_MESSAGE="Success"
WHERE
  PROCESS_NAME=CONCAT("5g core amf hourly process")
  AND start_time=safe_cast(trans_ts AS datetime);
SELECT
  "Process Completed Successfully"; EXCEPTION
    WHEN ERROR THEN

--update audit table with error status
UPDATE `${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname}`
SET END_TIME=CURRENT_DATETIME(),
STATUS="Error",
RETURN_MESSAGE=CONCAT(@@error.message,'------***----',@@error.statement_text)
WHERE PROCESS_NAME=CONCAT("5g core amf hourly process") AND start_time=safe_cast(trans_ts AS datetime); RAISE USING message=@@error.message;
END;
