import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, to_date, month
from pyspark.sql import types

import pandas as pd

parser = argparse.ArgumentParser()

parser.add_argument('--input_solutions', required=True)
parser.add_argument('--input_tweets', required=True)
parser.add_argument('--output_solutions', required=True)
parser.add_argument('--output_tweets', required=True)

args = parser.parse_args()

input_solutions = args.input_solutions
input_tweets = args.input_tweets
output_solutions = args.output_solutions
output_tweets = args.output_tweets

spark = SparkSession.builder \
    .appName('wordle-pipeline') \
    .getOrCreate()

df_solution = spark.read.parquet(input_solutions)

df_solution = df_solution \
    .withColumn('full_date', concat(col("Date"), lit(" "), col("Year"))) \
    .withColumn('final_date', to_date(col('full_date'), "MMM. dd yyyy")) \
    .withColumn("wordle_id", col("wordle_id").cast(types.IntegerType())) \
    .withColumn("month", month("final_date")) \
    .select(col("final_date").alias("Date"), "wordle_id", "Answer") \
    
df_tweets = spark.read.parquet(input_tweets)

tweets = df_tweets.toPandas()

tweets['tweet_datetime'] = pd.to_datetime(tweets['tweet_date'])
tweets = tweets.drop(columns=['tweet_date'])
tweets['tweet_date'] = tweets['tweet_datetime'].dt.date
tweets['attempt_count'] = tweets['tweet_text'].str[11].astype('int')

tweets['tweet_text'] = tweets['tweet_text'].str.replace('⬜','X')
tweets['tweet_text'] = tweets['tweet_text'].str.replace('⬛','X')
tweets['tweet_text'] = tweets['tweet_text'].str.replace('🟨','W')
tweets['tweet_text'] = tweets['tweet_text'].str.replace('🟩','O')

for n in range(1,7):
    tweets[f'has_guess{n}'] = tweets['tweet_text'].str.split('\n').str[n+1] \
        .str.contains('|'.join(['X', 'W', 'O'])) \
        .fillna(False)
    
    tweets.loc[tweets[f'has_guess{n}'], f'guess{n}'] = tweets['tweet_text'].str.split('\n').str[n+1].str[:5]
    tweets[f'guess{n}'] = tweets[f'guess{n}'].fillna('No Guess')
    
    tweets.loc[tweets[f'has_guess{n}'], f'guess{n}_incorrect'] = tweets[f'guess{n}'].str.count('X')
    tweets.loc[tweets[f'has_guess{n}'], f'guess{n}_wrong_spot'] = tweets[f'guess{n}'].str.count('W')
    tweets.loc[tweets[f'has_guess{n}'], f'guess{n}_correct'] = tweets[f'guess{n}'].str.count('O')
    tweets[[f'guess{n}_incorrect', f'guess{n}_wrong_spot', f'guess{n}_correct']] = \
    tweets[[f'guess{n}_incorrect', f'guess{n}_wrong_spot', f'guess{n}_correct']].fillna('0')
    
    values = {f'guess{n}': 'No Guess', f'guess{n}_incorrect': 0, f'guess{n}_wrong_spot': 0, f'guess{n}_correct': 0}
    tweets.fillna(value=values)

tweets = tweets.drop(columns=['tweet_text'])

for n in range(2,7):
    tweets = tweets.astype({f'guess{n}_incorrect': int, f'guess{n}_wrong_spot': int, f'guess{n}_correct': int})

tweets_schema = types.StructType([
    types.StructField('wordle_id', types.IntegerType(), True), 
    types.StructField('tweet_id', types.LongType(), True), 
    types.StructField('tweet_username', types.StringType(), True), 
    types.StructField('tweet_datetime', types.TimestampType(), True), 
    types.StructField('tweet_date', types.DateType(), True), 
    types.StructField('attempt_count', types.IntegerType(), True),
    types.StructField('has_guess1', types.BooleanType(), True), 
    types.StructField('guess1', types.StringType(), True), 
    types.StructField('guess1_incorrect', types.IntegerType(), True),
    types.StructField('guess1_wrong_spot', types.IntegerType(), True),
    types.StructField('guess1_correct', types.IntegerType(), True),
    types.StructField('has_guess2', types.BooleanType(), True), 
    types.StructField('guess2', types.StringType(), True), 
    types.StructField('guess2_incorrect', types.IntegerType(), True),
    types.StructField('guess2_wrong_spot', types.IntegerType(), True),
    types.StructField('guess2_correct', types.IntegerType(), True),
    types.StructField('has_guess3', types.BooleanType(), True), 
    types.StructField('guess3', types.StringType(), True), 
    types.StructField('guess3_incorrect', types.IntegerType(), True),
    types.StructField('guess3_wrong_spot', types.IntegerType(), True),
    types.StructField('guess3_correct', types.IntegerType(), True),
    types.StructField('has_guess4', types.BooleanType(), True), 
    types.StructField('guess4', types.StringType(), True), 
    types.StructField('guess4_incorrect', types.IntegerType(), True),
    types.StructField('guess4_wrong_spot', types.IntegerType(), True),
    types.StructField('guess4_correct', types.IntegerType(), True),
    types.StructField('has_guess5', types.BooleanType(), True), 
    types.StructField('guess5', types.StringType(), True), 
    types.StructField('guess5_incorrect', types.IntegerType(), True),
    types.StructField('guess5_wrong_spot', types.IntegerType(), True),
    types.StructField('guess5_correct', types.IntegerType(), True),
    types.StructField('has_guess6', types.BooleanType(), True), 
    types.StructField('guess6', types.StringType(), True), 
    types.StructField('guess6_incorrect', types.IntegerType(), True),
    types.StructField('guess6_wrong_spot', types.IntegerType(), True),
    types.StructField('guess6_correct', types.IntegerType(), True),
    ])

df_tweets = spark.createDataFrame(tweets, schema=tweets_schema)

df_solution.write.parquet(output_solutions)
df_tweets.write.parquet(output_tweets)