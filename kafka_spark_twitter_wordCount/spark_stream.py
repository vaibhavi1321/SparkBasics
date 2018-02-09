#This program will collect the data coming from kafka using twitterAPI.
# from pyspark import SparkContext  
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.streaming.kafka import KafkaUtils
#Initialize spark session to create an global instance
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

#By using tweet_clean,it will clean log data by using Regular expressions
def tweet_clean(tweets):
    user_clean = re.sub('@[^\s]+', ' ', tweets)
    link_clean  = re.sub('((www\.[\s]+)|(https?://[^\s]+))', '', user_clean)
    letters_only = re.sub("[^a-zA-Z]", " ", link_clean) 
    # words = letters_only.lower().split()
    words= " ".join(letters_only.split())
    # words.show()

    return (" ".join(words))

#Main program where streaming batch will count the words every 10 seconds
if __name__ == "__main__":
    
    # host, port = sys.argv[1:]
    sc = SparkContext(appName="PythonSqlNetworkWordCount")
    sc.setLogLevel("WARN")
    # sc=SparkContext()
    spark =SparkSession(sc)
    ssc = StreamingContext(sc, 10)


    kvs = KafkaUtils.createDirectStream(ssc, ['twitterstream'], {"metadata.broker.list": '127.0.0.1:9092'})
    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    # lines = ssc.socketTextStream(host, int(port))
    lines = kvs.map(lambda x: x[1])

    # words = lines.flatMap(lambda line: line.split(" "))
    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())
            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = spark.createDataFrame(rowRdd) 
            edu_level=udf(tweet_clean)
           
            wordsDataFrame=wordsDataFrame.withColumn("clean_text",edu_level(wordsDataFrame["word"]))
            wordsDataFrame=wordsDataFrame.createOrReplaceTempView("words")
            


            # Do word count on table using SQL and print it
            wordsDataFrame.show(5)

            # wordCountsDataFrame = \
            #     spark.sql("select word, count(*) as total from words group by word order by total")
            # wordCountsDataFrame.show(5)
        except:
            pass
    lines.foreachRDD(lambda x:spark.read.json(x).createOrReplaceTempView("sent"))
    wordCountsDataFrame = spark.sql("select * from sent")

    wordCountsDataFrame.show(5)

    ssc.start()
    ssc.awaitTermination()
    


#     udfScoreToCategory=udf(scoreToCategory, StringType())
# df.withColumn("category", udfScoreToCategory("score")).show(10)
