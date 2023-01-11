{{ config(materialized='table') }}

with tweets_data as (
    select *
    from {{ ref('stg_tweets_data') }}
), 

solutions_data as (
    select *
    from {{ ref('stg_solutions_data') }}
)

select 
tweets_data.wordle_id,
wordle_date,
solution,
tweet_id,
tweet_username,
tweet_datetime,
attempt_count,
has_guess1,
has_guess2,
has_guess3,
has_guess4,
has_guess5,
has_guess6,
guess1,
guess1_incorrect,
guess1_wrong_spot,
guess1_correct,
guess2,
guess2_incorrect,
guess2_wrong_spot,
guess2_correct,
guess3,
guess3_incorrect,
guess3_wrong_spot,
guess3_correct,
guess4,
guess4_incorrect,
guess4_wrong_spot,
guess4_correct,
guess5,
guess5_incorrect,
guess5_wrong_spot,
guess5_correct,
guess6,
guess6_incorrect,
guess6_wrong_spot,
guess6_correct
from solutions_data
inner join tweets_data
on solutions_data.wordle_id = tweets_data.wordle_id