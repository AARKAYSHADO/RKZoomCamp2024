
-- Use the `ref` function to select from other models

{{ config(materialized='table') }}

with source_data as (

select *
from {{ ref('my_first_dbt_model') }}
where id = 1

)

select *
from source_data