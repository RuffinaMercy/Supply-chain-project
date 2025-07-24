
  
    

create or replace transient table M5_FORECASTING.DBT_DEV.mart_sales_with_features
    
    
    
    as (-- This model's ONLY job is to add features to the already-joined table.
-- File: models/marts/mart_sales_with_features.sql



with final_joined as (
    -- We now refer to our new intermediate model
    select * from M5_FORECASTING.DBT_DEV.int_sales_joined
)

select
    *,
    -- Lag Feature (Sales from 4 weeks ago)
    lag(target_sales, 28) over (partition by id order by full_date) as sales_lag_28,

    -- Rolling Window Features (7-day and 28-day windows)
    avg(target_sales) over (partition by id order by full_date rows between 6 preceding and current row) as sales_rolling_avg_7,
    stddev(target_sales) over (partition by id order by full_date rows between 6 preceding and current row) as sales_rolling_std_7,

    avg(target_sales) over (partition by id order by full_date rows between 27 preceding and current row) as sales_rolling_avg_28,
    stddev(target_sales) over (partition by id order by full_date rows between 27 preceding and current row) as sales_rolling_std_28

from final_joined
    )
;


  