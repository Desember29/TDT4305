from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import StringType
from collections import OrderedDict
from operator import add
from shapely.geometry import Polygon, Point
import sys

#Needed in order to remove an error message when trying to print results. Something about utf-8/ascii encoding error or the like.
reload(sys)
sys.setdefaultencoding('utf-8')

#Set up configuration for a single machine, and get the different contexts for both Spark and SQL.
conf = (SparkConf().setMaster("local[*]").setAppName("AirBnB Application"))
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Relative path for datasets.
calLoc = "airbnb_datasets/calendar_us.csv"
lisLoc = "airbnb_datasets/listings_us.csv"
neighTestLoc = "airbnb_datasets/neighborhood_test.csv"
neighLoc = "airbnb_datasets/neighbourhoods.geojson"
revLoc = "airbnb_datasets/reviews_us.csv"

#Read all the datasets as dataframes, as tab seperated and with headers.
calendarDF = sqlContext.read.csv(calLoc, sep="\t", header=True)
listingsDF = sqlContext.read.csv(lisLoc, sep="\t", header=True)
neighbourhoodTestDF = sqlContext.read.csv(neighTestLoc, sep="\t", header=True)
neighbourhoodsDF = sqlContext.read.json(neighLoc)
reviewsDF = sqlContext.read.csv(revLoc, sep="\t", header=True)


#To print schemas
"""
calendarDF.printSchema()
listingsDF.printSchema()
neighbourhoodsDF.printSchema()
reviewsDF.printSchema()
"""

#To count distinct values we first drop all null/None values, convert the value to uppercase if possible (removes case sensitive duplicates), then we loop through each column and count the distinct values. We then store it in a dictionary and print the output to a tab seperated file.
def task2b():
	distinctValuesPerColumn = OrderedDict()
	for column in listingsDF.schema.names:
		distinctValuesPerColumn[column] = listingsDF.select(column).na.drop().rdd.map(lambda x: str(x).upper()).distinct().count()
	print "Distinct values per field:"
	print distinctValuesPerColumn

#Here we simply select the city column and collect the distinct values from each city.
def task2c():	
	cities = listingsDF.select("city").distinct()
	cities.rdd.map(lambda p: unicode(p[0])).coalesce(1).saveAsTextFile("citiesResults")
	
	#On the AirBnB old dataset below was our old solution, as the cities were not normalized and were usually spelled wrongly. So we thought to extract state from the listings since state had less wrong entries and required little cleaning, then convert state (was on short form "NY") to the biggest city in the State. Then use this as the listings city. We could've also done this or something similar in the other tasks, but when we found the new normalized datasets we instead chose to simplify our work amount by using the new datasets.
	'''
	listingsDF = sqlContext.read.csv("../airbnb_datasets_old/listings_us.csv", sep="\t", header=True)
	cities = listingsDF.select("state").na.drop().rdd.map(lambda x: "NY" if str(x.state).upper()=="NEW YORK" else str(x.state).upper()).distinct().collect()
	print cities
	'''



#In this task we select city and price, and map them as a key-value pair. We then aggregate by key and get city column as key, with total price per city and total count of listings per city as value. The price we first remove $ and , signs from then convert to float. We then just map the values by dividing the total price with the total count of listings, for each city. Finally we write it to a file.
def task3a():
	priceAveragePerCityRDD = listingsDF.select("city", "price").rdd.map(lambda listing: (listing.city, float("".join(c for c in listing.price if c not in "$,"))))
	priceAveragePerCityRDD = priceAveragePerCityRDD.aggregateByKey((0, 0), lambda city, price: (city[0] + price, city[1] + 1), lambda city, price: (city[0] + price[0], city[1] + price[1]))
	priceAveragePerCityRDD = priceAveragePerCityRDD.mapValues(lambda row: row[0] / row[1])
	priceAveragePerCityRDD.toDF().coalesce(1).write.csv('task3a.csv')
	
	#Verification function to find average price in New York, only for testing/debugging of code above
	'''
	pricesForNewYork = listingsDF.where(listingsDF.city == "New York").select("price").rdd.map(lambda listing: float("".join(c for c in listing.price if c not in "$,"))).collect()
	counter = 0
	totalPrice = 0
	for item in pricesForNewYork:
		counter += 1
		totalPrice += item
	
	averageForNewYork = totalPrice/counter
	print "Average price for New York is " + str(averageForNewYork)
	'''

#Something similar is done in this task, except we have two fields as the key, which is city and room_type. Other than that we count the count of listings and total price for each key and aggregate by it. Then we map the values by dividing total price with the total count of listings for each key.
def task3b():
	priceAveragePerRoomInCityRDD = listingsDF.select("city", "room_type", "price").rdd.map(lambda listing: ((listing.city, listing.room_type), float("".join(c for c in listing.price if c not in "$,"))))
	priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.aggregateByKey((0,0),lambda a,b: (a[0]+b,a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1]))
	priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.mapValues(lambda row: row[0]/row[1])
	priceAveragePerRoomInCityRDD.map(lambda x: (x[0][0],x[0][1],x[1])).toDF().coalesce(1).write.csv('task3b.csv')

#For every city, we sum reviews per month and divide on amount of listings in that city.
def task3c():
	reviewAveragePerCityRDD = listingsDF.select("city", "reviews_per_month").rdd.map(lambda listing: (listing.city, float(0 if listing.reviews_per_month==None else listing.reviews_per_month)))
	reviewAveragePerCityRDD = reviewAveragePerCityRDD.aggregateByKey((0, 0), lambda city, revPerMonth: (city[0] + revPerMonth, city[1] + 1), lambda city, revPerMonth: (city[0] + revPerMonth[0], city[1] + revPerMonth[1]))
	reviewAveragePerCityRDD = reviewAveragePerCityRDD.mapValues(lambda row: row[0]/row[1])
	reviewAveragePerCityRDD.toDF().coalesce(1).write.csv('task3c.csv')

#Here we set reviews per month to 0 if the value is null/None, and figure out the estimated bookings per year by dividing by 0.7 and multiplying by average number of nights which is 3, then multiplying with 12 as we need to go from month to year. Then we simply do the aggregation similarly as we have done above, and calculate the average.
def task3d():
	numberOfNightsBookedPerYearRDD = listingsDF.select("city", "reviews_per_month").rdd.map(lambda listing: (listing.city, (float(0 if listing.reviews_per_month == None else listing.reviews_per_month) / 0.7) * 3 * 12))
	numberOfNightsBookedPerYearRDD = numberOfNightsBookedPerYearRDD.aggregateByKey((0, 0), lambda city, booking: (city[0] + booking, city[1] + 1), lambda city, booking: (city[0] + booking[0], city[1] + booking[1]))
	numberOfNightsBookedPerYearRDD = numberOfNightsBookedPerYearRDD.mapValues(lambda row: row[0]/row[1])
	numberOfNightsBookedPerYearRDD.toDF().coalesce(1).write.csv('task3d.csv')

#Similarly as above we do almost the same calculation, but also take the price into consideration. Which means we have to filter out $ and , signs from that field. We then find the total price spent per listing per city per year. Then we simply gather together the total price and reduce by the city key.
def task3e():
	totalPricePerYearRDD = listingsDF.select("city", "reviews_per_month", "price").rdd.map(lambda listing: (listing.city, (float(0 if listing.reviews_per_month == None else listing.reviews_per_month) / 0.7) * 3 * 12 * float("".join(c for c in listing.price if c not in "$,"))))
	totalPricePerYearRDD = totalPricePerYearRDD.reduceByKey(lambda x, y: x + y)
	totalPricePerYearRDD.toDF().coalesce(1).write.csv('task3e.csv')



#Count amount of host and amount of listings, then divided amount of listings on amount of hosts.
def task4a():
	totalListings = float(listingsDF.select("id").distinct().count())
	totalHosts = float(listingsDF.select("host_id").distinct().count())
	averageListingsPerHost = float(totalListings/totalHosts)
	print averageListingsPerHost

#Stores a RDD in a variable and uses that twice. First time all hosts with one or less listings and secondly counting every host. Then dividing the first with the second.
def task4b():
	listForPercentage = listingsDF.select("host_id","host_listings_count").rdd
	percentageOfHostsWithMultipleListings = float(listForPercentage.map(lambda x: (x.host_id,0) if x.host_listings_count==None else (x.host_id,x.host_listings_count)).filter(lambda x: float(x[1]) >= 2).count())/float(listForPercentage.distinct().count())
	print percentageOfHostsWithMultipleListings

#Joining two DataFrames together and reducing the key whilst calculating and creating a RDD where for every key, we would see which three host had the most income based on when their listings were not available.
def task4c():
	listingsTable = listingsDF.select("id","city","host_id","price")
	calendarTable = calendarDF.select("listing_id","date","available")
	topHostIncome = listingsTable.join(calendarTable, listingsTable.id == calendarTable.listing_id).rdd
	topHostIncome = topHostIncome.map(lambda x: ((x.city,x.host_id,x.id,x.price),1 if x.available=="f" else 0)).reduceByKey(add)
	topHostIncome = topHostIncome.map(lambda x: ((x[0][0],x[0][1],x[0][2]),float("".join(c for c in x[0][3] if c not in "$,"))*float(x[1])))
	topHostIncome = topHostIncome.map(lambda x: ((x[0][0],x[0][1]),x[1])).reduceByKey(add)
	topHostIncome = topHostIncome.map(lambda x: (x[0][0],x[0][1],x[1])).toDF(['city','host_id','income'])
	topHostIncome = topHostIncome.orderBy('income',ascending=False)
        topHostIncome = topHostIncome.rdd.map(lambda x: (x.city,(x.host_id,x.income))).groupByKey().mapValues(lambda x: [r for r, _ in sorted(x, key=lambda a: -a[1])[:3]]).collect()


        print topHostIncome
	
        
"""
	topHostIncome = topHostIncome.rdd.groupBy(lambda x: x[0]).map(lambda x: (x[0], list(x[1]))).collect()
	
	topHostPerCity = OrderedDict()
	
	for city in topHostIncome:
		for n in range(3):
			if city[0] in topHostPerCity:
				topHostPerCity[city[0]].append(city[1][n])
			else:
				topHostPerCity[city[0]] = [city[1][n]]
	
	print topHostPerCity
"""


#Here we first join the listingsDF and reviewsDF by their listing_ids. We then create a mapping with city and reviewer_id as key and a integer 1 as a count. We then reduce by key and get the count of bookings per reviewer per city. We then map the values so we have city as the only key, with reviewer_id and count as the value. We sort by the count value in descending order. Then we group by the city key and collect the results. We then loop through each city and gather the first 3 entries, which is the top 3 reviewers per city. Then we write the results from that operation to file.
def task5a():	
	#topGuestsRDD = reviewsDF.join(listingsDF, reviewsDF.listing_id == listingsDF.id).select("city", "reviewer_id").rdd.map(lambda row: ((row.city, int(row.reviewer_id)), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1]))).sortBy(lambda x: -x[1][1]).groupByKey().mapValues(list).collect()
        topGuestsRDD = reviewsDF.join(listingsDF, reviewsDF.listing_id == listingsDF.id).select("city", "reviewer_id").rdd.map(lambda row: ((row.city, int(row.reviewer_id)), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1]))).sortBy(lambda x: -x[1][1]).groupByKey().mapValues(lambda x: [r for r, _ in sorted(x, key=lambda a: -a[1])[:3]]).collect()
        print topGuestsRDD
	"""
	topGuestsByCity = OrderedDict()
	
	for city in topGuestsRDD:
		for n in range(3):
			if city[0] in topGuestsByCity:
				topGuestsByCity[city[0]].append(city[1][n])
			else:
				topGuestsByCity[city[0]] = [city[1][n]]
	
	#Test reviewer_id used to test results.
	#.where(reviewsDF.reviewer_id == "7107853")
	"""
	"""
	#The following code is for writing to file
	import csv
	
	keys, values = [], []
	
	for key, value in topGuestsByCity.items():
	    keys.append(key)
	    values.append(value)       
	
	with open("task5a.csv", "w") as outfile:
	    csvwriter = csv.writer(outfile)
	    for n in range(len(keys)):
	        csvwriter.writerow([keys[n],values[n][0],values[n][1],values[n][2],])
"""
#We do almost the same as the previous task except we use listing_id instead of city and we also include price to the operations. We then sum up all the prices for each reviewer_id and listing _id key pair. After that we remove the listing id from the RDD, and sum up the total money spent per reviewer on booking and select the highest spender.
def task5b():
	biggestSpender = reviewsDF.join(listingsDF, reviewsDF.listing_id == listingsDF.id).select("reviewer_id", "listing_id", "price").rdd.map(lambda row: ((int(row.reviewer_id), int(row.listing_id)), float("".join(c for c in row.price if c not in "$,")) * 3)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x, y: x + y).top(1, key = lambda x: x[1])
	print biggestSpender



#Some assistance variables and functions for the next two tasks
#Dictionary that's going to contain all the neighbourhood as polygons
neighbourhoodPolygons = OrderedDict()

#Function that generates the neighbourhood polygons based on the coordinate pairs list. It stores the neighbourhood polygons in the neighbourhoodPolygons dictionary, with the name of the neighbourhood as key and the polygon as value.
def createNeighbourhoodPolygons(neighbourhoodsDF = neighbourhoodsDF, neighbourhoodPolygons = neighbourhoodPolygons):
	neighbourhoodsDF = neighbourhoodsDF.select(explode("features")).rdd.map(lambda row: ((str(row[0][1][0]), str(row[0][1][1])), row[0][0][0][0][0])).collect()
	for neighbourhood in neighbourhoodsDF:
		neighbourhoodPolygons[neighbourhood[0]] = Polygon(neighbourhood[1])

#Function that assigns neighbourhoods for the different listings based on their longitude and latitude, from the neighbourhoodPolygons.
def assignNeighbourhoodForListing(longitude, latitude, neighbourhoodPolygons = neighbourhoodPolygons):
	listingPoint = Point(float(longitude), float(latitude))
	for neighbourhood in neighbourhoodPolygons:
		if neighbourhoodPolygons[neighbourhood].contains(listingPoint):
			return str(neighbourhood[0])
	return ""

#Initialises the neighbourhoodPolygons list with the correct neighbourhoods.
createNeighbourhoodPolygons()

#User defined function required to assign neighbourhoods to a new field in the data frame.
assignNeighbourhoodForListingUDF = udf(assignNeighbourhoodForListing, StringType())

#First we take id, city latitude and longitude columns from the listingsDF. Then we add neighbourhood field to the dataframe by using the user defined function with latitude and longitude. After that is done, we simply gather together the rows that are different and join them together into a new RDD. Through that we can see which differences there are between listings and cities. In order to figure out the percentage of entries in the test set that agree with our neighbourhood assignment, we simply take the row count of the total set and subtract by the count of rows that did not agree with our assignment. Then we divide that by the total row count of the set and multiply by 100.
def task6a():
	listingsDFTemp = listingsDF.select("id", "city", listingsDF.latitude.cast("float").alias("latitude"), listingsDF.longitude.cast("float").alias("longitude"))
	neighbourhoodListingsDF = listingsDFTemp.withColumn("neighbourhood", assignNeighbourhoodForListingUDF(listingsDFTemp.longitude, listingsDFTemp.latitude)).select("id", "neighbourhood", "city")
		
	myNeighbourhoodRowsDifferenceDF = neighbourhoodListingsDF.subtract(neighbourhoodTestDF).selectExpr("id as my_id", "neighbourhood as my_neighbourhood", "city as my_city")
	testNeighbourhoodRowsDifferenceDF = neighbourhoodTestDF.subtract(neighbourhoodListingsDF).selectExpr("id as test_id", "neighbourhood as test_neighbourhood", "city as test_city")
	differentRowsDF = myNeighbourhoodRowsDifferenceDF.join(testNeighbourhoodRowsDifferenceDF, myNeighbourhoodRowsDifferenceDF.my_id == testNeighbourhoodRowsDifferenceDF.test_id).select("my_id", "my_neighbourhood", "test_neighbourhood")
	print differentRowsDF.collect()
	print str(((float(neighbourhoodListingsDF.count()) - float(differentRowsDF.count())) / float(neighbourhoodListingsDF.count())) * 100) + "% of our assignments of neighbourhood agree with the test set"

#TODO: Thomas fyll inn og ordne
def task6b():
	listingsDFTemp = listingsDF.where(listingsDF.city == "Seattle").select("id", "amenities", listingsDF.latitude.cast("float").alias("latitude"), listingsDF.longitude.cast("float").alias("longitude"))
	neighbourhoodListingsDF = listingsDFTemp.withColumn("neighbourhood", assignNeighbourhoodForListingUDF(listingsDFTemp.longitude, listingsDFTemp.latitude)).select("neighbourhood", "amenities").collect()
	print neighbourhoodListingsDF


#This is a sample function to write a csv used to visualize price of a sample of listings, as explained in the description
def task1_3_1():
        priceOfListing = listingsDF.select("latitude","longitude","price", "name").rdd.toDF(["lat","lon","price","name"]).coalesce(1).write.csv('task1_3_1.csv', header=True)

task5a()
