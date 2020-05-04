This is an extension of my previous work
https://github.com/rajasinghr/Twitter_Analysis_Using_Kafka_Spark_S3_ElasticSearch

Instead of processing through pySpark, I used the Real-Time Kafka Streams to process the messages from the topic.

In this project, I pulled tweets from "twitter-tweets" topic which I created in my previous project. I get the count of user's tweet based on the screen_name and pushed back into a new topic.
