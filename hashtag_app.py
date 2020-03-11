from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from collections import namedtuple
import sys
import os
import requests

HOST = "localhost"
PORT = 9001

def aggregate_cnt(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def process_rdd(time, rdd):
    print("------------- %s -------------" % str(time))
    schema = StructType([
        StructField("hashtag", StringType(), True),
        StructField("cnt", IntegerType(), True),
    ])
    try:
        sql_context = SQLContext(rdd.context)
        row = rdd.map(lambda w: Row(hashtag=w[0], cnt=w[1]))
        sql_context.createDataFrame(row, schema=schema).registerTempTable("hashtags")
        df_hashtag = sql_context.sql(
            "select hashtag, cnt from hashtags order by cnt desc limit 10")
        # df = rdd.toDF().limit(10).registerTempTable("hashtags")
        send_to_dashboard(df_hashtag)
        df_hashtag.show()
    except:
        print("Error: ", sys.exc_info())

def send_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [t.cnt for t in df.select("cnt").collect()]
    # send the data through REST API
    url = 'http://localhost:9002/update'
    post_data = {'label': str(tags), 'data': str(tags_count)}
    response = requests.post(url, data=post_data)

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("tweetStreaming")
    sc = SparkContext(conf=conf)
    # sc.setLogLevel("ERROR")
    # streaming data will be divided into batches every 5s
    ssc = StreamingContext(sc, 5)
    # compute data in a 10min window for every 5s
    dataStream = ssc.socketTextStream(HOST, PORT).window(windowDuration=600, slideDuration=5)

    # fields = ("hashtag", "cnt")
    # Tweet = namedtuple('Tweet', fields)
    (dataStream.flatMap(lambda line: line.split(" "))  # Splits to a list
     .filter(lambda word: word.startswith("#") and len(word) > 1)  # Checks for hashtag calls
     .map(lambda word: (word.lower(), 1))  # Lower cases the word
     .reduceByKey(lambda a, b: a + b)  # Reduces
     # .updateStateByKey(aggregate_cnt)
     # .map(lambda rec: Tweet(rec[0], rec[1]))  # Stores in a Tweet Object
     .foreachRDD(process_rdd))
     # .foreachRDD(lambda rdd: rdd.toDF().limit(10).registerTempTable("tweets")))

    ssc.checkpoint("checkpoints_hashtag")
    ssc.start()
    ssc.awaitTermination()

