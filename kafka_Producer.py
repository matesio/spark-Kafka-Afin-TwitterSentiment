import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka

class TweetsListeneer (StreamListener):
	def __init__(self, kafkaProducer):
		print ("Tweets producer initialized")
		self.producer = kafkaProducer
	def on_data (self, data):
		try:
			json_data = json.loads(data)
			words = json_data["text"].split()
			hashtaglist = list(filter(lambda x: x.lower().startswith('#'),words))
			if (len(hashtaglist) !=0):
				for hashtag in hashtaglist:
					print(hashtag)
					self.producer.produce(bytes(hashtag))
		except KeyError as e:
			print ("Error on_data: %s" % str (e))

		return True


	def on_error(self, status):
		print (status)
		return True
def connect_to_twitter(kafkaProducer, tracks):
	api_key = ''
	api_secret = ''
	access_token = ''
	access_token_secret = ''
	auth = OAuthHandler(api_key,api_secret)
	auth.set_access_token (access_token, access_token_secret)

	twitter_stream = Stream(auth, TweetsListeneer(kafkaProducer))
	twitter_stream.filter(track=tracks, languages=['en'])

if __name__ == '__main__':
	if len(sys.argv) < 5:
		print ("Usage: python kafka_producer.py <host> <port> <topic_name> <tracks>")
		exit(-1)

	host = sys.argv[1]
	port = sys.argv[2]
	topic= sys.argv[3]
	tracks = sys.argv[4:]
	kafkaClient = pykafka.KafkaClient(host+ ":" +port)
	kafkaProducer = kafkaClient.topics[bytes("test")].get_producer()
	connect_to_twitter(kafkaProducer,tracks)
