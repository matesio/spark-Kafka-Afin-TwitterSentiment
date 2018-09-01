import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#allows us to perform sentiment analysis
from afinn import afinn


if __name__ == '__main__':
	if len(sys.argv) != 4:
		print ("Usage: spark-submit spark_stream_kafka.py <hostname> <port> <topic>")

	host = sys.argv[1]
	port = sys.argv[2]
	topic = sys.argv[3]

	spark = SparkSession\
			.builder\
			.appName("TwitterSentimentAnalysis")\
			.getOrCreateI()
	spark.sparkContext.setLogLevel("ERROR")
	tweetsDFROW = spark.readStream\
				  .format("kafka")\
				  .option("kafka.bootstrap.servers", host+": +port")\
				  .option("subscribe",topic)
				  .load()	 
	tweetsDF = tweetsDFROW.selectExpr("CAST(value AS STRING) as tweet")
	afinn = Afinn()

	def add_sentiment_score(text):
		sentiment_score = afinn.score(text) #-ve or +ve
		return sentiment_score
	add_sentiment_score_udf = udf(
								add_sentiment_score,
								FloatType()
								)
	tweetsDF = tweetsDF.withColumn(
									"sentiment_score",
									add_sentiment_score_udf(tweetsDF.tweetsDF.tweet)
									)
	query = tweetsDF.writeStream\
								.outputMode("append")\
								.format("console")\
								.option("truncate","false")\
								.trigger(processingTime="5 seconds")
								.start()\
								.awaitTermination()

