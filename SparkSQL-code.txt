#Use "auto-miles-per-gallon.csv" and "auto-data.csv" file and
#Create pyspark application to make data frames by running sparkSQL queries on given file
#import SparkSession and SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext
def WorstMPG():
    sqlDF1=spark.sql("select * from AutoData ORDER BY MPGCITY,MPGHWY")   #find Wost MPG 
    sqlDF1.show(5)
def BestMPG():
    sqlDF3=spark.sql("select * from AutoData Order By MPGCITY DESC, MPGHWY DESC") # find best MPG
    sqlDF3.show(5)
def CheapCars():
    sqlDF5=spark.sql("select * from AutoData order by PRICE") #find cheap cars
    sqlDF5.show(5)
def ExpensiveCars():
    sqlDF6=spark.sql("select * from AutoData Order by PRICE DESC") #find Expensive Cars
    sqlDF6.show(5)
def MaxDisp():
    sqlDF2=spark.sql("select * FROM AutoMilesPerGallon ORDER BY DISPLACEMENT DESC") #find max displacement
    sqlDF2.show(5)
def LeastDisp():
    sqlDF4=spark.sql("select * from AutoMilesPerGallon order by DISPLACEMENT") #find least displacement
    sqlDF4.show(5)
def Gallons():
    sqlDF7=spark.sql("select * from AutoGallons where MPG>24") # add column gallon
    sqlDF7.show(5)
#Function 1 for reading "auto-data.csv"
def AutoCSV1(index):
    df1=spark.read.csv("./auto-data.csv",header="true",inferSchema="true")
    df1=df1.withColumnRenamed('MPG-CITY','MPGCITY')
    df1=df1.withColumnRenamed('MPG-HWY','MPGHWY')
    df1.createOrReplaceTempView("AutoData") #Create a view to run SQL queries on this view
    
   # df1.printSchema()
    if index==1:
        print type(df1)
        WorstMPG()
    elif index==3:
        BestMPG()
    elif index==5:
        CheapCars()
    elif index==6:
        ExpensiveCars()
#Function 2 for reading "auto-miles-per-gallon.csv"
def AutoCSV2(index):
    df2=spark.read.csv("./auto-miles-per-gallon.csv",header="true",inferSchema="true")
    df2.createOrReplaceTempView("AutoMilesPerGallon")
    if index==2:
        print type(df2)
        MaxDisp()
    elif index==4:
        LeastDisp()
    elif index==7:
        print type(df2)
        df2=df2.withColumn("Gallon",lit(1)) #add columns to existing df2 dataframe
        df2.createOrReplaceTempView("AutoGallons") # to see new dataframe we have to create a view again
        Gallons()
#Main code for creating spark Sessioin and enter choices to get insights from csv data
spark=SparkSession \
       .builder \
       .appName("python program to find fuelEfficiency") \
       .config("spark.some.config.option","some-value") \
       .getOrCreate()
print("1.To find worst highway or city MPG press 1 ")
print("2.To find largest displacement press 2 ")
print("3.To find best highway or city MPG press 3")
print("4.To find least displacement press 4")
print("5.To find cheap cars press 5")
print("6.To find high price cars press 6")
print("7.To add gallons column press 7")
choice=input("enter your choice=")
if choice==1 or choice==3 or choice==5 or choice==6:
    AutoCSV1(choice) #Go to Function1
elif choice==2 or choice==4 or choice==7:
    AutoCSV2(choice)  #Go to Function2
else:
    print("incorrect choice")

#Extra-NOTE:
#df1.select(df1['MAKE'],df1['FUELTYPE'],df1['CYLINDERS'],df1['HP'],df1['MPG-CITY'],df1['MPG-HWY'],df1['PRICE']).show(5)
#Above statement is to fetch particular columns from the given csv file
