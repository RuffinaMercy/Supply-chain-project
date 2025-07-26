
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select d
from M5_FORECASTING.RAW_DATA.CALENDAR
where d is null



  
  
      
    ) dbt_internal_test