
  create or replace   view M5_FORECASTING.DBT_DEV.stg_calendar
  
  
  
  
  as (
    select
    d as day_id,
    cast(date as date) as full_date,
    wm_yr_wk,
    event_name_1,
    event_type_1,
    snap_ca,
    snap_tx,
    snap_wi,
    to_number(replace(lower(d), 'd_', '')) as day_num
from M5_FORECASTING.RAW_DATA.CALENDAR
  );

