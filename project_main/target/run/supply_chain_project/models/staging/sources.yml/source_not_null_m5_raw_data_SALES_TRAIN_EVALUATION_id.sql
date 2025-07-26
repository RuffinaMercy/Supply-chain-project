
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from M5_FORECASTING.RAW_DATA.SALES_TRAIN_EVALUATION
where id is null



  
  
      
    ) dbt_internal_test