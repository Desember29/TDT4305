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

#Relative path for datasets.
#calendarFileLocation = "../airbnb_datasets/calendar_us.csv"
listingsFileLocation = "../airbnb_datasets/listings_us.csv"
listingsWithNeighbourhoodsFileLocation = "../airbnb_datasets/listings_ids_with_neighborhoods.tsv"
#neighbourhoodTestFileLocation = "../airbnb_datasets/neighborhood_test.csv"
#neighhoodsFileLocaction = "../airbnb_datasets/neighbourhoods.geojson"
#reviewsFileLocation = "../airbnb_datasets/reviews_us.csv"


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

def listingTF(listingID):
	listingTermsRDD = listingsDF.where(listingsDF.id == listingID).select("description").rdd.map(lambda x: re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x.description.lower())).strip()).flatMap(lambda x: x.split(" ")).map(lambda word: (word, 1))
	totalNumberOfTerms = float(listingTermsRDD.count())
	listingTFList = listingTermsRDD.reduceByKey(add).map(lambda x: (x[0], x[1] / totalNumberOfTerms)).takeOrdered(100, key = lambda x: -x[1])
	sc.parallelize(listingTFList).map(lambda x: (x[0] + "\t" + str(x[1]))).saveAsTextFile(listingID + " TF")

if (sys.argv[1] == "-l"):
	listingID = sys.argv[2]
	if (listingID.isdigit()):
		listingTF(listingID)
	else:
		print "Listing_id has to be an integer!"

elif (sys.argv[1] == "-n"):
	print "Tried to run with neighbourhood! Not yet finished!"

#.where(listingsDF.id == "12567614")
#.where(listingsDF.id == "8342998")
#listingsDescriptionDF = .select("id", "description").rdd.map(lambda x: (x.id, re.sub("\s+", " ", re.sub("[^0-9a-z'\-&]", " ", x.description.lower())).strip())).flatMapValues(lambda x: x.split(" ")).map(lambda x: ((x[0], x[1]), 1)).foldByKey(0, add).map(lambda x: (x[0][0], (x[0][1], x[1]))).reduceByKey(lambda x, y: x + y).take(5)
#print listingsDescriptionDF


print("TF-IDF Assignment")
file = sc.textFile("data.txt").cache()
print("File has " + str(file.count()) + " lines.")
print("Passed arguments " + str(sys.argv))

sc.stop()
