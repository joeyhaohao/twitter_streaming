import sys
import os
import requests
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext
from collections import namedtuple

# HOST = "localhost"
# PORT = 9001
ZOOKEEPER = 'localhost:2181'

# def aggregate_cnt(new_values, total_sum):
#     return sum(new_values) + (total_sum or 0)

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
    # arguments for spark-submit
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("tweetStreaming")
    sc = SparkContext(conf=conf)
    # sc.setLogLevel("ERROR")
    # streaming data will be divided into batches every 5s
    ssc = StreamingContext(sc, 5)
    # compute data in a 10min window for every 5s
    # dataStream = ssc.socketTextStream(HOST, PORT).window(windowDuration=600, slideDuration=5)
    dataStream = KafkaUtils.createStream(ssc, ZOOKEEPER, 'spark-streaming', {'china': 1}) \
        .window(windowDuration=600, slideDuration=5)

    # sqlContext = SQLContext(sc)
    # spark = sqlContext.sparkSession
    # df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "china") \
    #     .load()
    # df.show().start()

    # fields = ("hashtag", "cnt")
    # Hashtag = namedtuple('Hashtag', fields)
    (dataStream.flatMap(lambda line: line[1].split(" "))  #split to a list
     .filter(lambda word: word.startswith("#") and len(word) > 1)  # filter hashtag
     .map(lambda word: (word.lower(), 1))  # lower case, map
     .reduceByKey(lambda a, b: a + b)  # reduce
     # .updateStateByKey(aggregate_cnt)
     # .map(lambda rec: Hashtag(rec[0], rec[1]))  # store in a Hashtag Object
     # .foreachRDD(lambda rdd: rdd.toDF().limit(10).registerTempTable("tweets")))
     .foreachRDD(process_rdd))

    # dataStream.pprint()

    ssc.checkpoint("checkpoints_hashtag")
    ssc.start()
    ssc.awaitTermination()

