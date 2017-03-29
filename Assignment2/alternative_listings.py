from pyspark import SparkContext, SQLContext
from math import sin, cos, asin, sqrt, radians
import sys
import re

#Needed in order to remove an error message when trying to print results. Something about utf-8/ascii encoding error or the like.
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

#Relative path for datasets.
calendarFileLocation = "../airbnb_datasets/calendar_us.csv"
listingsFileLocation = "../airbnb_datasets/listings_us.csv"

#Read the datasets as dataframes that are tab seperated and with headers.
calendarDF = sqlContext.read.csv(calendarFileLocation, sep="\t", header=True)
listingsDF = sqlContext.read.csv(listingsFileLocation, sep="\t", header=True)

#Drop listings with null/None value as amenities.
listingsDF = listingsDF.na.drop(subset=["amenities"])

#To print schemas
"""
calendarDF.printSchema()
listingsDF.printSchema()
"""

def haversine(lat1, lon1, lat2, lon2):
	lat1, lon1, lat2, lon2 = map(radians, [lon1, lat1, lon2, lat2])
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	c = 2 * asin(sqrt(a))
	r = 6371
	return c * r
	

listingID = sys.argv[1]

chosenListing = listingsDF.where(listingsDF.id == listingID).select("amenities", "latitude", "longitude", "price", "room_type").rdd.map(lambda x: ((re.sub("[^0-9a-z,'/ \-]", "", x.amenities.lower())).split(","), float(x.latitude), float(x.longitude), float(re.sub("[^0-9.]", "", x.price)), x.room_type)).collect()
maxPrice = chosenListing[0][3] * (1 + (float(sys.argv[3]) / float(100)))

availableOnDateDF = calendarDF.where((calendarDF.available == "t") & (calendarDF.date == sys.argv[2])).select("listing_id")

relevantAlternativeListingsRDD = listingsDF.where((listingsDF.id != listingID) & (listingsDF.room_type == chosenListing[0][4])).select("amenities", "id", "latitude", "longitude", "price").rdd.map(lambda x: (x[1], (re.sub("[^0-9a-z,'/ \-]", "", x[0].lower())).split(","), x[2], x[3], float(re.sub("[^0-9.]", "", x[4])))).filter(lambda x: x[4] <= (maxPrice)).map(lambda x: (x[0], x[2], x[3], x[1]))
relevantAlternativeListingsDF = sqlContext.createDataFrame(relevantAlternativeListingsRDD, ("listing_id", "latitude", "longitude", "amenities"))

relevantAlternativeListingsDF = availableOnDateDF.join(relevantAlternativeListingsDF, "listing_id").withColumn("")
print relevantAlternativeListingsDF

print("Passed arguments " + str(sys.argv))