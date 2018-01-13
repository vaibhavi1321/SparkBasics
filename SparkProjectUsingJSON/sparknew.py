#This program to perform few metrics on given data
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext,Row
#pyspark.sql.functions import explode
sprk1=SparkSession \
       .builder \
       .config("spark.some.config.option","some-value") \
       .getOrCreate()
sc=sprk1.sparkContext
sqlContext=SQLContext(sc)
rd1=sqlContext.read.json("flattened.json")
rd2=sqlContext.read.json("flattened_ux.json")

#Register Table for running queries on both of the files
rd1.registerTempTable("userEventPlan")
rd2.registerTempTable("uxEvents")

#This query for counting errors in particular events
error=sqlContext.sql("select Count(*) AS Number,UXEvent.eventName[0] As EventName from uxEvents where UXEvent.eventType[0]='ERROR' group by UXEvent.eventName[0]").show()
#This query for counting number of age group who joined a particular plan
data=sqlContext.sql("select Count(*) AS Number ,personId.age[0] AS age from userEventPlan group by personId.age[0]").show()
#This query for counting which gender likely buy what kind of plan types
per=sqlContext.sql("select Count(*) AS Number, personId.gender[0] AS Gender,personId.plan[0].planType[0] AS plantype from userEventPlan group by personId.gender[0],personId.plan[0].planType[0]").show()
#To find average age group who will likely buy a particluar plan
age=sqlContext.sql("select Avg(int(personId.age[0])) AS averageAGE,personId.plan[0].planType[0] AS PlanType from userEventPlan group by PlanType").show()


