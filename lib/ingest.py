import datetime
import os
import time

from functools import partial
from pathlib import Path

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, explode

from pyspark.sql.types import IntegerType, \
    StructType, \
    StringType, \
    LongType, \
    StructField, \
    ArrayType, \
    FloatType

from lib.api import RedditApiWrapper

from requests import HTTPError

COMMENT_SCHEMA_COLUMNS = ['body',
                          'id',
                          'score',
                          'author',
                          'author_fullname',
                          'parent_id',
                          'created_utc']

COMMENT_SPARK_STRUCT = StructType([
    StructField("body", StringType(), True),
    StructField("id", StringType(), False),
    StructField("score", IntegerType(), True),
    StructField("author", StringType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("created_utc", LongType(), False),
    StructField("posted_date", LongType(), False)
])

SUBMISSION_SCHEMA_COLUMNS = ['title',
                             'selftext',
                             'id',
                             'upvote_ratio',
                             'num_comments',
                             'link_flair_text',
                             'score',
                             'created_utc',
                             'author',
                             'author_fullname',
                             'retrieved_on']

SUBMISSION_SPARK_STRUCT = StructType([
    StructField('title', StringType(), True),
    StructField('selftext', StringType(), True),
    StructField('id', StringType(), False),
    StructField('upvote_ratio', FloatType(), False),
    StructField('num_comments', IntegerType(), True),
    StructField('link_flair_text',  StringType(), True),
    StructField('score', IntegerType(), True),
    StructField('created_utc', LongType(), False),
    StructField('author', StringType(), True),
    StructField('author_fullname', StringType(), True),
    StructField('retrieved_on', LongType(), False),
    StructField("posted_date", LongType(), False)
])


def filter_item_for_schema(item, schema):
    new_item = {}

    for key in schema:
        val = item.get(key, None)

        new_item[key] = val

    created_datetime = datetime.datetime.fromtimestamp(item["created_utc"])
    posted_date = created_datetime.replace(hour=0,
                                           minute=0,
                                           second=0,
                                           microsecond=0).timestamp()
    new_item["posted_date"] = int(posted_date)

    return new_item


def pull_items_for_timespan(hour_span, submissions=False):
    api_wrapper = RedditApiWrapper("TIdaL")

    span_end = hour_span
    span_start = int((datetime.datetime.fromtimestamp(hour_span) - datetime.timedelta(hours=1)).timestamp())

    current_items = []

    try:
        if submissions:
            current_items.extend(api_wrapper.pull_submissions(span_end, span_start))
        else:
            current_items.extend(api_wrapper.pull_comments(span_end, span_start))
    except HTTPError as e:
        print(e)

        return []

    view_func = None

    if submissions:
        view_func = partial(filter_item_for_schema, schema=SUBMISSION_SCHEMA_COLUMNS)
    else:
        view_func = partial(filter_item_for_schema, schema=COMMENT_SCHEMA_COLUMNS)

    current_items = list(map(view_func, current_items))

    return current_items


def ingest_comments(spans_df):
    comments_path = os.environ.get("TIDAL_COMMENTS_PATH", "/tmp/tidal/comments")
    process_func = udf(pull_items_for_timespan, ArrayType(COMMENT_SPARK_STRUCT, True))

    comments_df = spans_df.select(process_func("value").alias("comments")).select(explode("comments").alias("comment"))
    exploded_columns = list(map(lambda k: f"comment.{k}", COMMENT_SCHEMA_COLUMNS))
    comments_df = comments_df.select("comment.posted_date", *exploded_columns)

    comments_df.write.mode("overwrite").partitionBy("posted_date").parquet(comments_path)


def ingest_submissions(spans_df):
    submissions_path = os.environ.get("TIDAL_SUBMISSIONS_PATH", "/tmp/tidal/submissions")
    process_func = udf(partial(pull_items_for_timespan,
                               submissions=True), ArrayType(SUBMISSION_SPARK_STRUCT, True))

    submissions_df = spans_df.select(process_func("value").alias("submissions")).select(explode("submissions").alias("submission"))
    exploded_columns = list(map(lambda k: f"submission.{k}", SUBMISSION_SCHEMA_COLUMNS))
    submissions_df = submissions_df.select(*exploded_columns, "submission.posted_date")

    submissions_df.write.mode("overwrite").partitionBy("posted_date").parquet(submissions_path)


def ingest(date):
    spark = SparkContext()
    spark_sql = SQLContext(spark)

    hour_spans = [int(date.timestamp())]

    for i in range(1, 24):
        hour_span = date - datetime.timedelta(hours=i)
        hour_spans.append(int(hour_span.timestamp()))

    spans_df = spark_sql.createDataFrame(hour_spans, IntegerType())

    try:
        ingest_comments(spans_df)
    except Exception as e:
        print("Ingesting Comments Failed:", e)

    try:
        ingest_submissions(spans_df)
    except Exception as e:
        print("Ingesting Submissions Failed:", e)

