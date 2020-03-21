# Twitter Streaming

Real-time twitter hashtag trending and sentiment analysis

## Setup
* Install [Zookeeper](https://zookeeper.apache.org/releases.html) for Kafka
* Install [Kafka](https://kafka.apache.org/downloads) on all servers
* Install Spark `pip install pyspark==2.2.3`
* Install Twitter API `pip install tweepy`
* Get [Twitter](https://developer.twitter.com/en/apps) API keys

## Run in local machine
* Run Zookeeper in Standalone mode 

    `bin/zkServer.sh start`
    
* Start Kafka in server

    `kafka-server-start /usr/local/etc/kafka/server.properties`

* Create a Kafka topic

    `kafka-topics --create --zookeeper localhost:9092 --replication-factor 2 --partitions 4 --topic china`
    
* Put your API key in `twitter_app.py`
* Fetch real-time Twitter feeds

    `python twitter_app.py`

* Get top trending hashtags

    `python hashtag_app.py`

* Run real-time sentiment analysis

    `python sentiment_analysis/classifier.py`

* Run dashboard

    `python dashboard/dashboard_app.py`

## Demo
![](https://github.com/joeyhaohao/twitter_streaming/blob/master/demo.png)
