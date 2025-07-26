
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    d as unique_field,
    count(*) as n_records

from M5_FORECASTING.RAW_DATA.CALENDAR
where d is not null
group by d
having count(*) > 1



  
  
      
    ) dbt_internal_test