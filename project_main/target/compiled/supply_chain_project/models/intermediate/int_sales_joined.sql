-- File: models/intermediate/int_sales_joined.sql
-- FINAL PRODUCTION VERSION

with unpivoted_sales as (
    select * from M5_FORECASTING.DBT_DEV.int_sales_unpivoted
),

calendar as (
    select * from M5_FORECASTING.DBT_DEV.stg_calendar
),

prices as (
    select * from M5_FORECASTING.DBT_DEV.stg_sell_prices
),

-- Identify all item-store IDs that have at least one price entry
good_ids as (
    select distinct
        store_id,
        item_id
    from prices
),

sales_with_cal as (
    select
        s.id,
        s.item_id,
        s.store_id,
        c.full_date,
        c.day_num,
        c.wm_yr_wk,
        c.event_name_1,
        s.sales as target_sales
    from unpivoted_sales s
    join calendar c on s.day_column = c.day_id
),

-- Filter for only the good IDs before doing expensive operations
sales_filtered as (
    select sc.* from sales_with_cal sc
    join good_ids g on sc.item_id = g.item_id and sc.store_id = g.store_id
),

sales_with_prices as (
    select
        sf.*,
        p.sell_price
    from sales_filtered sf
    left join prices p
        on sf.item_id = p.item_id
        and sf.store_id = p.store_id
        and sf.wm_yr_wk = p.wm_yr_wk
),

forward_filled_prices as (
    select
        *,
        last_value(sell_price) ignore nulls over (partition by id order by full_date) as ff_sell_price
    from sales_with_prices
),

backward_filled_prices as (
    select
        *,
        first_value(ff_sell_price) ignore nulls over (partition by id order by full_date) as bff_sell_price
    from forward_filled_prices
)

select
    id,
    item_id,
    store_id,
    full_date,
    day_num,
    event_name_1,
    target_sales,
    bff_sell_price as sell_price
from backward_filled_prices