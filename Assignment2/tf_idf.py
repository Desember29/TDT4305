"""
Try to run the file with command: spark-submit tf_idf.py <arguments>.
For more info, visit: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
"""

from pyspark import SparkContext, SQLContext
import sys
import re
from operator import add

#Needed in order to remove an error message when trying to print results. Something about utf-8/ascii encoding error or the like.
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

#Path for datasets.
listingsFileLocation = sys.argv[1] + "listings_us.csv"
listingsWithNeighbourhoodsFileLocation = sys.argv[1] + "listings_ids_with_neighborhoods.tsv"

"""									 
#Relative path for datasets.
listingsFileLocation = "../airbnb_datasets/listings_us.csv"
listingsWithNeighbourhoodsFileLocation = "../airbnb_datasets/listings_ids_with_neighborhoods.tsv"
"""

#Read the datasets as dataframes that are tab seperated and with headers.
listingsDF = sqlContext.read.csv(listingsFileLocation, sep="\t", header=True)
listingsWithNeighbourhoodsDF = sqlContext.read.csv(listingsWithNeighbourhoodsFileLocation, sep="\t", header=True)

#Drop listings with null/None value as description and create a new data frame with neighbourhood added as a field.
listingsDF = listingsDF.na.drop(subset=["description"]).join(listingsWithNeighbourhoodsDF, "id")

#To print schemas
"""
listingsDF.printSchema()
listingsWithNeighbourhoodsDF.printSchema()
"""

def listingTF_IDF(listingID):
	#IDF for all terms for all listings in listingsDF.
	#Select relevant columns
	termsIDF_DF = listingsDF.select("id", "description")
	#Get total number of documents
	totalNumberOfDocuments = float(termsIDF_DF.count())
	#Clean the dataset, flatMapValues and distinct it so you get a proper count of number of documents with term t in it. Then simply calculate IDF for all terms.
	termsIDF_RDD = termsIDF_DF.rdd.map(lambda x: (x.id, re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x.description.lower())).strip())).flatMapValues(lambda x: x.split(" ")).distinct().map(lambda x: (x[1], 1)).reduceByKey(add).map(lambda x: (x[0], totalNumberOfDocuments / x[1]))
	#Create a dataframe from the RDD so you can join it on term.
	termsIDF_DF = sqlContext.createDataFrame(termsIDF_RDD, ("term", "idf"))
	
	#Select relevant listing, and clean it's description. Flatmap it so you can use it to get count.
	listingTermsTF_RDD = listingsDF.where(listingsDF.id == listingID).select("description").rdd.map(lambda x: re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x.description.lower())).strip()).flatMap(lambda x: x.split(" ")).map(lambda word: (word, 1))
	#Get total number of terms in listing.
	totalNumberOfTerms = float(listingTermsTF_RDD.count())
	#Get count of occurances for each term, then calcualte TF for all terms in listing description.
	listingTF_RDD = listingTermsTF_RDD.reduceByKey(add).map(lambda x: (x[0], x[1] / totalNumberOfTerms))
	#Create a dataframe from the RDD so you can join it on term. When you've joined it calculate the weight of the term, and take top 100 terms from the TF-IDF.
	listingTF_IDFList = sqlContext.createDataFrame(listingTF_RDD, ("term", "tf")).join(termsIDF_DF, "term").rdd.map(lambda x: (x[0], x[1] * x[2])).takeOrdered(100, key = lambda x: -x[1])
	#Create a file of the top 100 terms.
	sc.parallelize(listingTF_IDFList).map(lambda x: (x[0], str(x[1]))).toDF(["term", "weight"]).coalesce(1).write.options(delimiter="\t").csv("tf_idf_results", header=True)


def neighbourhoodTF_IDF(neighbourhood):
	#IDF for all terms for all neighbourhoods in listingsDF.
	#Select relevant columns and combine the descriptions based on neighbourhood.
	termsIDF_RDD = listingsDF.select("neighbourhood", "description").rdd.reduceByKey(add)
	#Get total number of documents
	totalNumberOfDocuments = float(termsIDF_RDD.distinct().count())
	#Clean the dataset, flatMapValues and distinct it so you get a proper count of number of documents with term t in it. Then simply calculate IDF for all terms.
	termsIDF_RDD = termsIDF_RDD.map(lambda x: (x[0], re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x[1].lower())).strip())).flatMapValues(lambda x: x.split(" ")).distinct().map(lambda x: (x[1], 1)).reduceByKey(add).map(lambda x: (x[0], totalNumberOfDocuments / x[1]))
	#Create a dataframe from the RDD so you can join it on term.
	termsIDF_DF = sqlContext.createDataFrame(termsIDF_RDD, ("term", "idf"))
	
	#Select relevant neighbourhood and combine all of it's listings descriptions. Clean it's description. Flatmap it so you can use it to get count.
	neighbourhoodTermsTF_RDD = listingsDF.where(listingsDF.neighbourhood == neighbourhood).select("neighbourhood", "description").rdd.reduceByKey(add).map(lambda x: re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x[1].lower())).strip()).flatMap(lambda x: x.split(" ")).map(lambda word: (word, 1))
	#Get total number of terms in listing.
	totalNumberOfTerms = float(neighbourhoodTermsTF_RDD.count())
	#Get count of occurances for each term, then calcualte TF for all terms in listing description.
	neighbourhoodTermsTF_RDD = neighbourhoodTermsTF_RDD.reduceByKey(add).map(lambda x: (x[0], x[1] / totalNumberOfTerms))
	#Create a dataframe from the RDD so you can join it on term. When you've joined it calculate the weight of the term, and take top 100 terms from the TF-IDF.
	neighbourhoodTermsTF_IDFList = sqlContext.createDataFrame(neighbourhoodTermsTF_RDD, ("term", "tf")).join(termsIDF_DF, "term").rdd.map(lambda x: (x[0], x[1] * x[2])).takeOrdered(100, key = lambda x: -x[1])
	#Create a file of the top 100 terms.
	sc.parallelize(neighbourhoodTermsTF_IDFList).map(lambda x: (x[0], str(x[1]))).toDF(["term", "weight"]).coalesce(1).write.options(delimiter="\t").csv("tf_idf_results", header=True)

#Check what operation the user is asking for and call the relevant functions.
if (sys.argv[2] == "-l"):
	listingID = sys.argv[3]
	if (listingID.isdigit()):
		listingTF_IDF(listingID)
	else:
		print "Listing_id has to be an integer!"

elif (sys.argv[2] == "-n"):
	neighbourhoodTF_IDF(sys.argv[3])

sc.stop()