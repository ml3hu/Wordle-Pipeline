{{ config(materialized='table') }}

with tweets_data as (
    select *
    from {{ ref('stg_tweets_data') }}
), 

solutions_data as (
    select *
    from {{ ref('stg_solutions_data') }}
)

select *
from solutions_data
inner join tweets_data
on solutions_data.wordle_id = tweets_data.wordle_id