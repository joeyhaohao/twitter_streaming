from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes, LogisticRegression
import os
import sys

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

    # create SparkSession
    spark = SparkSession.builder.config("spark.driver.memory",  "2g").appName('sentiment').getOrCreate()

    # import the raw data from the dataset
    data = spark.read.csv('data/training.1600000.processed.noemoticon.csv', inferSchema=True)
    data = data.select(['_c0', '_c5']).withColumnRenamed('_c0', 'class').withColumnRenamed('_c5', 'text')

    # build the preprocessing pipeline
    # change label value from 0, 4 to 0, 1
    stringIndexer = StringIndexer(inputCol='class', outputCol='label')
    tokenizer = Tokenizer(inputCol='text', outputCol='tokens')
    stopwordsRemover = StopWordsRemover(inputCol='tokens', outputCol='tokens_nonstop')
    countVectorizer = CountVectorizer(inputCol='tokens_nonstop', outputCol='count_vec')
    idf = IDF(inputCol='count_vec', outputCol='features')
    # lr = LogisticRegression()
    nb = NaiveBayes()
    pipeline = Pipeline(stages=[stringIndexer, tokenizer, stopwordsRemover, countVectorizer, idf, nb])

    data_train, data_val = data.randomSplit([0.8, 0.2])
    model = pipeline.fit(data_train)

    train_pred = model.transform(data_train)
    val_pred = model.transform(data_val)
    data_val.show()
    val_pred.show()

    # the evaluation tool
    eval = MulticlassClassificationEvaluator()
    train_acc = eval.evaluate(train_pred)
    acc_val = eval.evaluate(val_pred)
    print("train_acc:", train_acc, "val acc:", acc_val)

    model.write().overwrite().save('model/naive_bayes')

