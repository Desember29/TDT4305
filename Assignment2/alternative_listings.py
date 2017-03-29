from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from math import radians, cos, sin, asin, sqrt
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
calendarDF.printSchema()
listingsDF.printSchema()

listingID = sys.argv[1]

chosenListing = listingsDF.where(listingsDF.id == listingID).select("latitude", "longitude", "price", "room_type").rdd.map(lambda x: (float(x.latitude), float(x.longitude), float(re.sub("[^0-9.]", "", x.price)), x.room_type)).collect()
maxPrice = chosenListing[0][2] * (1 + (float(sys.argv[3]) / float(100)))
print chosenListing

chosenListingAmenitiesRDD = listingsDF.where(listingsDF.id == listingID).select("amenities").rdd.map(lambda x: (re.sub("[^0-9a-z,'/ \-]", "", x.amenities.lower()))).flatMap(lambda x: x.split(",")).collect()
print chosenListingAmenitiesRDD

def haversine(lat1, lon1, lat2 = chosenListing[0][1], lon2 = chosenListing[0][0]):
	#Note: something is wrong with this code, as I had to switch latitude with longitude and vice versa. It gives correct distance, but uses the variables reversed.
	lat1, lon1, lat2, lon2 = map(radians, [lon1, lat1, lon2, lat2])
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	c = 2 * asin(sqrt(a))
	r = 6371
	return c * r

haversineUDF = udf(haversine, FloatType())

availableOnDateDF = calendarDF.where((calendarDF.available == "t") & (calendarDF.date == sys.argv[2])).select("listing_id")

relevantAlternativeListingsRDD = listingsDF.where((listingsDF.id != listingID) & (listingsDF.room_type == chosenListing[0][3])).select("amenities", "id", "latitude", "longitude", "name", "price").rdd.map(lambda x: (x[1], x[4], (re.sub("[^0-9a-z,'/ \-]", "", x[0].lower())).split(","), float(x[2]), float(x[3]), float(re.sub("[^0-9.]", "", x[5])))).filter(lambda x: x[5] <= (maxPrice)).map(lambda x: (x[0], x[1], x[3], x[4], x[2], x[5]))
relevantAlternativeListingsDF = sqlContext.createDataFrame(relevantAlternativeListingsRDD, ("listing_id", "name", "latitude", "longitude", "amenities", "price"))

relevantAlternativeListingsTempDF = availableOnDateDF.join(relevantAlternativeListingsDF, "listing_id")
relevantAlternativeListingsDF = relevantAlternativeListingsTempDF.withColumn("distance", haversineUDF(relevantAlternativeListingsTempDF.longitude, relevantAlternativeListingsTempDF.latitude))
relevantAlternativeListingsDF = relevantAlternativeListingsDF.where(relevantAlternativeListingsDF.distance < sys.argv[4]).rdd.map(lambda x: ((x[0], x[1], x[6], x[5]), x[4])).flatMapValues(lambda x: x).take(10)

print relevantAlternativeListingsDF

print("Passed arguments " + str(sys.argv))