from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials

## Import Kafka producer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import kafka_configuration

# # # # KAFKA PRODUCER # # # #
class TwitterKafkaProducer():
    """
    Class produce live tweets to kafka
    """
    def produce_Kafka(self, data):
        producer = KafkaProducer(bootstrap_servers=[kafka_configuration.BROKER1])

        # Asynchronous by default
        future = producer.send(kafka_configuration.TOPIC, data)

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass

        # Successful result returns assigned partition and offset
        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)

        def on_send_success(record_metadata):
            print(record_metadata.topic)
            print(record_metadata.partition)
            print(record_metadata.offset)

        def on_send_error(excp):
            log.error('I am an errback', exc_info=excp)
            # handle exception

        # block until all async messages are sent
        producer.flush()

        # configure multiple retries
        producer = KafkaProducer(retries=5)


# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            twitter_kafka_producer = TwitterKafkaProducer()
            twitter_kafka_producer.produce_Kafka(str(data))
            # with open(self.fetched_tweets_filename, 'a') as tf:
            #    tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True


    def on_error(self, status):
        print(status)



if __name__ == '__main__':

    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = ["donal trump", "hillary clinton", "barack obama", "bernie sanders"]
    fetched_tweets_filename = "tweets.txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)

