{{ config(materialized='view') }}

select
    -- identifiers
    cast(wordle_id as integer) as wordle_id,
    
    -- timestamps
    cast(Date as date) as wordle_date,
    
    -- solution info
    cast(Answer as string) as solution,
from {{ source('staging','solutions_data') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}