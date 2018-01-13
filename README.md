NOTE: I have used Python 2.7 for running spark 2.2.0 version applications
Platform: Linux VirtualBox
# SparkBasics
#I have stored this as .txt file but it's a python scripts
  Steps:
  1. For SparkSQL-code.py file please refer "FolksInATown.csv" file and for SparkSQL2nd.py file, I have used "auto-data.csv" and "auto-        miles-per-gallon.csv" files. You can use any CSV files
  2. Used python API for creating Spark Application
  3. Created RDD and SparkSQL queries by registering table to run SQL queries on CSV file
  4. Created Data frame to see insights from it.
#SparkBasics/SparkProjectUsingJSON
#In this folder, There are two JSON files which i have to use to run SparkSQL queries
  Steps:
  1. See user_event.json and ux_event.json this both files i have to flattened it
  2. See code for FlatFlie.py, It will flatten my original JSON file to give new flattened JSON files
  3. Using new Flattened file, I have created SparkNew.py file. I am running SparkSQL queries and created DataFrame by fetching key and        values of the Flattened JSON Data
  
