import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window


def max_min_reviews():
    df = reviews_df_clean.groupby('asin') \
        .count()

    df1 = df.join(meta_df_clean, 'asin', 'left')

    df1.createOrReplaceTempView('df1')
    max_review_count = spark.sql(
        'select asin, title, count as review_count from df1 where count = (select max(count) from df1)')
    max_review_count.show()
    # max_review_count.write.parquet('max_review_count.parquet')

    min_review_count = spark.sql(
        'select asin, title, count as review_count from df1 where count = (select min(count) from df1)')
    min_review_count.show()
    # min_review_count.write.parquet('min_review_count.parquet')


def longest_review():
    df = reviews_df_clean.withColumn('review_length', length('reviewText'))
    df1 = df.join(meta_df_clean, 'asin', 'left')
    df1.createOrReplaceTempView('df1')

    max_review_length = spark.sql('select asin, title, review_length from df1 order by review_length desc limit 1')
    max_review_length.show()
    # max_review_length.write.parquet('max_review_length.parquet')


def convert_date():
    df = reviews_df_clean.withColumn('review_date', date_format('review_timestamp', 'MM-dd-yyyy')) \
        .drop('review_timestamp')
    df.show()


def multiple_reviews():
    df = reviews_df.withColumn('row_num', row_number().over(Window.partitionBy('asin', 'reviewerID').orderBy('asin')))
    df1 = df.filter(df.row_num > 1)
    df1.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("tensoriot").getOrCreate()
    meta_df = spark.read.json('files/meta_Grocery_and_Gourmet_Food.json.gz')
    reviews_df = spark.read.json('files/reviews_Grocery_and_Gourmet_Food.json.gz')
    # reviews_df.show()
    meta_df_clean = meta_df.drop('brand') \
        .drop('categories') \
        .drop('description') \
        .drop('imUrl') \
        .drop('price') \
        .drop('related') \
        .drop('salesRank') \
        .dropDuplicates() \
        .dropna(how='any').cache()

    reviews_df_clean = reviews_df.withColumn('review_timestamp', from_unixtime(col('unixReviewTime'))) \
        .drop('unixReviewTime') \
        .drop('reviewTime') \
        .drop('helpful') \
        .drop('reviewerID') \
        .drop('reviewerName') \
        .drop('summary') \
        .dropDuplicates().cache()

    max_min_reviews()
    longest_review()
    convert_date()
    multiple_reviews()
