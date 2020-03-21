from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, HashingTF, IDF, NGram, StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes, LogisticRegression
from typing import Iterable
import os
import sys

class CustomFilter(Transformer):
    """
    A custom Transformer which removes stopwords in tweets
    """

    def __init__(self):
        super(CustomFilter, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:
        df.select('tokens').show(truncate=False)
        # df.select('tokens').filter(lambda word: '@' not in word)
        df = df.filter(
            udf(lambda tokens: 'http:' not in tokens, BooleanType())(df.tokens)
        )
        # df.select('tokens').show(truncate=False)
        return df

def build_ngrams(n=3):
    tokenizer = [Tokenizer(inputCol="text", outputCol="tokens")]
    stopwordsRemover = [StopWordsRemover(inputCol='tokens', outputCol='tokens_filtered')]
    ngrams = [
        NGram(n=i, inputCol="tokens", outputCol="{0}_grams".format(i))
        for i in range(1, n+1)
    ]
    cv = [
        CountVectorizer(vocabSize=5460, inputCol="{0}_grams".format(i), outputCol="{0}_cv".format(i))
        for i in range(1, n+1)
    ]
    idf = [
        IDF(inputCol="{0}_cv".format(i), outputCol="{0}_idf".format(i), minDocFreq=5)
        for i in range(1, n + 1)
    ]
    assembler = [
        VectorAssembler(inputCols=["{0}_idf".format(i) for i in range(1, n + 1)], outputCol="features")
    ]

    stringIndexer = [StringIndexer(inputCol="class", outputCol="label")]
    lr = [LogisticRegression(maxIter=100)]

    return Pipeline(stages=tokenizer + ngrams + cv + idf + assembler + stringIndexer + lr)

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
    stopwordsRemover = StopWordsRemover(inputCol='tokens', outputCol='tokens_filtered')
    countVectorizer = CountVectorizer(inputCol='tokens_filtered', outputCol='count_vec')
    # hashTF = HashingTF(numFeatures=2**16, inputCol="tokens_nonstop", outputCol='tf')
    idf = IDF(inputCol='count_vec', outputCol='features', minDocFreq=5) # minDocFreq: remove sparse terms
    nb = NaiveBayes()
    lr = LogisticRegression(maxIter=100)

    customFilter = CustomFilter()
    # pipeline = Pipeline(stages=[stringIndexer, tokenizer, stopwordsRemover, hashTF, idf, nb])
    # pipeline = Pipeline(stages=[stringIndexer, tokenizer, stopwordsRemover, countVectorizer, idf, nb])
    pipeline = build_ngrams(3)

    data_train, data_val = data.randomSplit([0.8, 0.2])
    model = pipeline.fit(data_train)
    train_pred = model.transform(data_train)
    val_pred = model.transform(data_val)
    # val_acc = val_pred.filter(val_pred.label==val_pred.prediction).count() / float(data_val.count())

    val_pred.show()

    eval = MulticlassClassificationEvaluator(metricName='accuracy')
    train_acc = eval.evaluate(train_pred)
    val_acc = eval.evaluate(val_pred)
    print("train acc:", train_acc, "val acc:", val_acc)

    model.write().overwrite().save('model/ngrams')
    ## lr: val_acc=0.768
    ## naive bayes: val_acc=0.756
    ## ngram: val_acc=0.802
