from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType
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
"""
calendarDF.printSchema()
listingsDF.printSchema()
"""

#Save listingsID for later use.
listingID = sys.argv[1]

#Get specified listing values for the different fields so we can filter based on these values at a later point.
chosenListing = listingsDF.where(listingsDF.id == listingID).select("amenities", "latitude", "longitude", "price", "room_type").rdd.map(lambda x: (float(x.latitude), float(x.longitude), float(re.sub("[^0-9.]", "", x.price)), x.room_type, (re.sub("[^0-9a-z,'/ \-]", "", x.amenities.lower())).split(","))).collect()
#Calculate the highest price any other listing can have compared to the specified listing.
maxPrice = chosenListing[0][2] * (1 + (float(sys.argv[3]) / float(100)))

#Default haversine specified in the assignment document.
def haversine(lat1, lon1, lat2 = chosenListing[0][1], lon2 = chosenListing[0][0]):
	#Note: something is wrong with this code, as I had to switch latitude with longitude and vice versa. It gives correct distance, but uses the variables reversed.
	lat1, lon1, lat2, lon2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	c = 2 * asin(sqrt(a))
	r = 6371
	return c * r

#Create a user defined function for spark to use.
haversineUDF = udf(haversine, FloatType())

#Function to count common amenities between the chosen listing and it's relevant listings.
def countCommonAmenities(comparedAmenitiesList, comparisonAmenitiesList = chosenListing[0][4]):
	count = 0
	for amenity in comparedAmenitiesList:
		if amenity in comparisonAmenitiesList:
			count += 1
	return count

#Create a user defined function for spark to use.
countCommonAmenitiesUDF = udf(countCommonAmenities, IntegerType())

#Get all listings that are available on the date specified. So we can join at a later date and filter out unavailable listings.
availableOnDateDF = calendarDF.where((calendarDF.available == "t") & (calendarDF.date == sys.argv[2])).select("listing_id")
"""
#Select all listings that aren't the specified listing, and remove all that aren't the same room_type. Then clean up the dataset. Filter out listings that exceed max price. Then remap fields position and remove unecessary fields.
relevantAlternativeListingsRDD = listingsDF.where((listingsDF.id != listingID) & (listingsDF.room_type == chosenListing[0][3])).select("amenities", "id", "latitude", "longitude", "name", "price").rdd.map(lambda x: (x[1], x[4], re.sub("[^0-9a-z,'/ \-]", "", x[0].lower()).split(","), float(x[2]), float(x[3]), float(re.sub("[^0-9.]", "", x[5])))).filter(lambda x: x[5] <= (maxPrice)).map(lambda x: (x[0], x[1], x[3], x[4], x[2], x[5]))
#Create a dataframe for relevant listings so we can join on it later in order to filter out listings that are not available.
relevantAlternativeListingsDF = sqlContext.createDataFrame(relevantAlternativeListingsRDD, ("listing_id", "name", "latitude", "longitude", "amenities", "price"))

#Filter out listings that are not available on date, by joining it with the dataframe created earlier for this use.
relevantAlternativeListingsTempDF = availableOnDateDF.join(relevantAlternativeListingsDF, "listing_id")
#Generate new fields with distance from specified listing and how many common ammenities the listing has with the specified listing.
relevantAlternativeListingsDF = relevantAlternativeListingsTempDF.withColumn("distance", haversineUDF(relevantAlternativeListingsTempDF.longitude, relevantAlternativeListingsTempDF.latitude)).withColumn("common_amenities", countCommonAmenitiesUDF(relevantAlternativeListingsTempDF.amenities))
#Filter out listings that are out of the range specified. Then map the listings according to the way the assignment specified. Then take top n listings ordered by descending value of common amenities.
relevantAlternativeListingsList = relevantAlternativeListingsDF.where(relevantAlternativeListingsDF.distance < sys.argv[4]).rdd.map(lambda x: (x[0], x[1], x[7], x[6], x[5])).takeOrdered(int(sys.argv[5]), key = lambda x: -x[2])
#Create a dataframe so we can save the results as a file.
sc.parallelize(relevantAlternativeListingsList).map(lambda x: (str(x[0]) + "\t" + x[1] + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]))).saveAsTextFile("alternatives")
"""

#Just used to create the file we used to visualize this task.
def visualization():
	relevantAlternativeListingsRDD = listingsDF.where((listingsDF.id != listingID) & (listingsDF.room_type == chosenListing[0][3])).select("amenities", "id", "latitude", "longitude", "name", "price", "host_name", "review_scores_rating").rdd.map(lambda x: (x[1], x[4], re.sub("[^0-9a-z,'/ \-]", "", x[0].lower()).split(","), float(x[2]), float(x[3]), float(re.sub("[^0-9.]", "", x[5])), x[6], x[7])).filter(lambda x: x[5] <= (maxPrice)).map(lambda x: (x[0], x[1], x[3], x[4], x[2], x[5], x[6], x[7]))
	relevantAlternativeListingsDF = sqlContext.createDataFrame(relevantAlternativeListingsRDD, ("listing_id", "name", "latitude", "longitude", "amenities", "price", "host_name", "review_scores_rating"))
	relevantAlternativeListingsTempDF = availableOnDateDF.join(relevantAlternativeListingsDF, "listing_id")
	relevantAlternativeListingsDF = relevantAlternativeListingsTempDF.withColumn("distance", haversineUDF(relevantAlternativeListingsTempDF.longitude, relevantAlternativeListingsTempDF.latitude)).withColumn("common_amenities", countCommonAmenitiesUDF(relevantAlternativeListingsTempDF.amenities))
	relevantAlternativeListingsList = relevantAlternativeListingsDF.where(relevantAlternativeListingsDF.distance < sys.argv[4]).rdd.map(lambda x: (x[0], x[1], x[9], x[8], x[5], x[2], x[3], x[6], x[7])).takeOrdered(int(sys.argv[5]), key = lambda x: -x[2])
	sc.parallelize(relevantAlternativeListingsList).map(lambda x: (str(x[0]) + "\t" + x[1] + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]) + "\t" + str(x[5]) + "\t" + str(x[6]) + "\t" + str(x[7]) + "\t" + str(x[8]))).saveAsTextFile("Visualization")

visualization()