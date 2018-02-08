#Given application for reading data from csv and do analytics on it by running pyspark application and use SparkSQL 
#This MatchMaker application is for using data from CSV file take random row from given file and create list of matches depends on the choices entered by users
import random
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
#Create SparkSession and sparkContext
spark=SparkSession \
       .builder \
      .appName("python 2nd prog for sqlcontext") \
       .config("spark.some.config.option","some-value") \
       .getOrCreate()
#Above statement giving warning for accessing port
sc=spark.sparkContext
sqlContext=SQLContext(sc)
#------------------------------------------------------------------------------------------------------------------------------------#
#EXTRA:
df=sqlContext.read.format("com.databricks.spark.csv").options(header= "true", inferSchema= "true", delimiter=";").load("./FolksInATown.csv")
#sc.textFile("./FolksInATown.csv").map(lambda line: line.spli#t(";").filter(lambda line:len(line>1).map(lambda line:line[0#],line[1])).collect()
df.registerTempTable("Folks") #register csv file as table to perform queries
#df1=spark.sql("SELECT age,marital FROM Folks where NOT marital ='married'")
#df1.show()             It will shows only people with single and divorced status
#------------------------------------------------------------------------------------------------------------------------------------#
#will give random record as a list from dataframe

#ask user to enter choices from 1,2,3
def Single(c1):
        if c1==1:
                df1=spark.sql("select * from Folks where marital='single'")
                data=df1.collect()      #this statement will collect df1 as dataframe and convert it to list which will help in using random                                  function as list only can be used to extract random record
                row=random.sample(data,1)
                age=row[0]["age"]
                print(row)
#parts=data.map(lambda l: l.split(","))
                print("age marital education balance")
                for p in data:       
                        if((abs(int(p["age"])-int(age))<=5) and int(p["balance"])>=350):
                                
                                print(p["age"]+"  "+p["marital"]+"  "+p["education"]+"  "+str(p["balance"]))
#                        print((i["age"]+" "+i["marital"]+" "+i["education"]+" "+str(i["balance"])))
'''                        myrdd=sc.parallelize(p)
                        schema=StructType([StructField("age",IntegerType(),True),StructField("marital",StringType(),True),StructField("balance",IntegerType(),True)])
                        DF=sqlContext.createDataFrame(p,schema)
                        DF.show()
'''
def Divorced(c1):
        df1=spark.sql("select * from Folks where marital='divorced'")
        data=df1.collect()
        row=random.sample(data,1)
        age=row[0]["age"]
        print(row)
        print("age marital education balance")
        for p in data:
                if((abs(int(p["age"])-int(age))<=5) and int(p["balance"])>=350):
                        print(p["age"]+" "+p["marital"]+" "+p["education"]+" "+str(p["balance"]))
def SingleDivorced(c1):
        df1=spark.sql("select * from Folks where marital<>'married'")
        data=df1.collect()
        row=random.sample(data,1)
        age=row[0]["age"]
        print(row)
        print("age marital education balance")
        for p in data:
                if((abs(int(p["age"])-int(age))<=5) and int(p["balance"])>=350):
                        print(p["age"]+"  "+p["marital"]+"  "+p["education"]+"  "+str(p["balance"]))
#Main()
#This matches happen with random rows from given file to the given choices. If Random raw is single and person is married then they can find their person's match
c1=int(input("enter your choice from 1.single-single 2. divorced-divorced 3.single-divorced:"))
print("\n")
if c1==1:
        Single(c1)
elif c1==2:
        Divorced(c1)
elif c1==3:
        SingleDivorced(c1)
else:
        print("not correct selection to find your match")
