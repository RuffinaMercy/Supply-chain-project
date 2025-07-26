
    
    

select
    d as unique_field,
    count(*) as n_records

from M5_FORECASTING.RAW_DATA.CALENDAR
where d is not null
group by d
having count(*) > 1


