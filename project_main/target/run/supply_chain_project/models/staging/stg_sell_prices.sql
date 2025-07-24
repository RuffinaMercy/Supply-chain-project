
  create or replace   view M5_FORECASTING.DBT_DEV.stg_sell_prices
  
  
  
  
  as (
    select
    store_id,
    item_id,
    wm_yr_wk,
    sell_price
from M5_FORECASTING.RAW_DATA.SELL_PRICES
  );

