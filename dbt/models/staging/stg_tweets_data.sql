{{ config(materialized='view') }}

select
    -- identifiers
    cast(tweet_id as integer) as tweet_id,
    cast(wordle_id as integer) as wordle_id,
    cast(tweet_username as string) as tweet_username,
    
    -- timestamps
    cast(tweet_datetime as timestamp) as tweet_datetime,
    
    -- attempt info
    cast(attempt_count as integer) as attempt_count,
    cast(has_guess1 as bool) as has_guess1,
    cast(has_guess2 as bool) as has_guess2,
    cast(has_guess3 as bool) as has_guess3,
    cast(has_guess4 as bool) as has_guess4,
    cast(has_guess5 as bool) as has_guess5,
    cast(has_guess6 as bool) as has_guess6,
    
    -- guess info
    cast(guess1 as string) as guess1,
    cast(guess1_incorrect as integer) as guess1_incorrect,
    cast(guess1_wrong_spot as integer) as guess1_wrong_spot,
    cast(guess1_correct as integer) as guess1_correct,
    cast(guess2 as string) as guess2,
    cast(guess2_incorrect as integer) as guess2_incorrect,
    cast(guess2_wrong_spot as integer) as guess2_wrong_spot,
    cast(guess2_correct as integer) as guess2_correct,
    cast(guess3 as string) as guess3,
    cast(guess3_incorrect as integer) as guess3_incorrect,
    cast(guess3_wrong_spot as integer) as guess3_wrong_spot,
    cast(guess3_correct as integer) as guess3_correct,
    cast(guess4 as string) as guess4,
    cast(guess4_incorrect as integer) as guess4_incorrect,
    cast(guess4_wrong_spot as integer) as guess4_wrong_spot,
    cast(guess4_correct as integer) as guess4_correct,
    cast(guess5 as string) as guess5,
    cast(guess5_incorrect as integer) as guess5_incorrect,
    cast(guess5_wrong_spot as integer) as guess5_wrong_spot,
    cast(guess5_correct as integer) as guess5_correct,
    cast(guess6 as string) as guess6,
    cast(guess6_incorrect as integer) as guess6_incorrect,
    cast(guess6_wrong_spot as integer) as guess6_wrong_spot,
    cast(guess6_correct as integer) as guess6_correct
from {{ source('staging','tweets_data') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}