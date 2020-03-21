from pyspark import SparkContext, SparkConf
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import sys
import os
import requests

# HOST = "localhost"
# PORT = 9001
ZOOKEEPER = 'localhost:2181'
MODEL = 'ngrams'

def predict(time, rdd):
    print("------------- %s -------------" % str(time))
    schema = StructType([
        StructField("text", StringType(), True)
    ])
    try:
        df = rdd.toDF(schema=schema)
        model = PipelineModel.read().load('model/'+MODEL)
        pred = model.transform(df)
        pred.show()
        send_to_dashboard(pred)
    except:
        print("Error: ", sys.exc_info())

def format_prob(probs):
    lis = [round(p, 5) for p in eval(str(probs))]
    return str(lis)

def send_to_dashboard(df):
    tweets = [str(t.text) for t in df.select("text").collect()]
    probs = [format_prob(p.probability) for p in df.select("probability").collect()]
    url = 'http://localhost:9002/pred'
    post_data = {'tweets': str(tweets), 'probs': str(probs)}
    response = requests.post(url, data=post_data)

if __name__ == "__main__":
    # os.environ["PYSPARK_PYTHON"] = "python3"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("TwitterSentiment")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    # sc.setLogLevel("ERROR")
    # streaming data will be divided into batches every 10s
    ssc = StreamingContext(sc, 10)
    # dataStream = ssc.socketTextStream(HOST, PORT).window(windowDuration=10, slideDuration=10)
    dataStream = KafkaUtils.createStream(ssc, ZOOKEEPER, 'spark-streaming', {'china': 1}) \
        .window(windowDuration=10, slideDuration=10)

    dataStream.pprint()
    (dataStream.map(lambda line: line[1].lower())
        .filter(lambda word: len(word) > 0)
        .map(lambda word: (word,))  # map to a tuple (word,)
        .foreachRDD(predict))

    ssc.checkpoint("checkpoints_sentiment")
    ssc.start()
    ssc.awaitTermination()

