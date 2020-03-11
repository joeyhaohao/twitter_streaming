from pyspark import SparkContext, SparkConf
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import sys
import os
import requests

HOST = "localhost"
PORT = 9001

def predict(time, rdd):
    print("------------- %s -------------" % str(time))
    schema = StructType([
        StructField("text", StringType(), True)
    ])
    try:
        df = rdd.toDF(schema=schema)
        model = PipelineModel.read().load('naive_bayes')
        pred = model.transform(df)
        pred.show()
        send_to_dashboard(pred)
    except:
        print("Error: ", sys.exc_info())

def send_to_dashboard(df):
    tweets = [str(t.text) for t in df.select("text").collect()]
    probs = [str(t.probability) for t in df.select("probability").collect()]
    url = 'http://localhost:9002/pred'
    post_data = {'tweets': str(tweets), 'probs': str(probs)}
    response = requests.post(url, data=post_data)

if __name__ == "__main__":
    # os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("TwitterSentiment")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    # sc.setLogLevel("ERROR")
    # streaming data will be divided into batches every 10s
    ssc = StreamingContext(sc, 10)
    dataStream = ssc.socketTextStream(HOST, PORT).window(windowDuration=10, slideDuration=10)
    # map to a tuple (word,)
    dataStream.flatMap(lambda line: line.split('|')) \
        .filter(lambda word: len(word) > 0) \
        .map(lambda word: (word.lower(),)) \
        .foreachRDD(predict)

    ssc.checkpoint("checkpoints_twitter")
    ssc.start()
    ssc.awaitTermination()

