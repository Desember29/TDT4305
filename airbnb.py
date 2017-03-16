from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from collections import OrderedDict
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

conf = (SparkConf().setMaster("local[*]").setAppName("AirBnB Application"))
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

calLoc = "airbnb_datasets/calendar_us.csv"
lisLoc = "airbnb_datasets/listings_us.csv"
neighLoc = "airbnb_datasets/neighbourhoods.geojson"
revLoc = "airbnb_datasets/reviews_us.csv"

calendarDF = sqlContext.read.csv(calLoc, sep="\t", header=True)
listingsDF = sqlContext.read.csv(lisLoc, sep="\t", header=True)
neighbourhoodsDF = sqlContext.read.json(neighLoc)
reviewsDF = sqlContext.read.csv(revLoc, sep="\t", header=True)


#To print schemas
"""
calendarDF.printSchema()
listingsDF.printSchema()
neighbourhoodsDF.printSchema()
reviewsDF.printSchema()
"""


#2. b)
"""
distinctValuesPerColumn = OrderedDict()
for column in listingsDF.schema.names:
	distinctValuesPerColumn[column] = listingsDF.select(column).na.drop().rdd.map(lambda x: str(x).upper()).distinct().count()
print "Distinct values per field:"
print distinctValuesPerColumn
#To write a tab seperated field-value pair
output = open("output.txt", "w")
for key in distinctValuesPerColumn:
	output.write(str(key) + "\t" + str(distinctValuesPerColumn[key]) + "\n")
output.close()
"""

#2. c)
"""
cities = listingsDF.select("city").distinct()
cities.rdd.map(lambda p: unicode(p[0])).coalesce(1).saveAsTextFile("citiesResults")

#On the AirBnB old dataset this was our solution, as the cities were not normalized and were usually spelled wrongly. So we thought to extract state from the listings since state had less wrong entries and required little cleaning, then convert state (was on short form "NY") to the biggest city in the State. Then use this as the listings city. We could've also done this or something similar in the other tasks, but when we found the new normalized datasets we instead chose to simplify our work amount by using the new datasets.
'''
#This import needs to be added as we (intended) to use it to convert the strings to lower/upper.
import org.apache.spark.sql.functions.{lower, upper}
listingsDF = sqlContext.read.csv("../airbnb_datasets_old/listings_us.csv", sep="\t", header=True)
cities = listingsDF.select("state").na.drop().rdd.map(lambda x: "NY" if str(x.state).upper()=="NEW YORK" else str(x.state).upper()).distinct().collect()
print cities
'''
"""


#3. a)
"""
priceAveragePerCityRDD = listingsDF.select("city", "price").rdd.map(lambda listing: (listing.city, float("".join(c for c in listing.price if c not in "$,"))))
priceAveragePerCityRDD = priceAveragePerCityRDD.aggregateByKey((0, 0), lambda city, price: (city[0] + price, city[1] + 1), lambda city, price: (city[0] + price[0], city[1] + price[1]))
priceAveragePerCityRDD = priceAveragePerCityRDD.mapValues(lambda row: row[0] / row[1]).collect()
print priceAveragePerCityRDD

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
"""

#3. b)
"""
priceAveragePerRoomInCityRDD = listingsDF.select("city", "room_type", "price").rdd.map(lambda listing: ((listing.city, listing.room_type), float("".join(c for c in listing.price if c not in "$,"))))
priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.aggregateByKey((0,0),lambda a,b: (a[0]+b,a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1]))
priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.mapValues(lambda row: row[0]/row[1]).collect()
print priceAveragePerRoomInCityRDD
"""

#3. c)
"""
reviewAveragePerCityRDD = listingsDF.select("city", "reviews_per_month").rdd.map(lambda listing: (listing.city, 0 if listing.reviews_per_month==None else float("".join(c for c in listing.reviews_per_month if c not in "$,"))))
reviewAveragePerCityRDD = reviewAveragePerCityRDD.aggregateByKey((0, 0), lambda city, revPerMonth: (city[0] + revPerMonth, city[1] + 1), lambda city, revPerMonth: (city[0] + revPerMonth[0], city[1] + revPerMonth[1]))
reviewAveragePerCityRDD = reviewAveragePerCityRDD.mapValues(lambda row: row[0]/row[1]).collect()
print reviewAveragePerCityRDD
"""

#3. d)
"""
numberOfNightsBookedPerYearRDD = listingsDF.select("city", "reviews_per_month").rdd.map(lambda listing: (listing.city, (float(0 if listing.reviews_per_month == None else listing.reviews_per_month) / 0.7) * 3 * 12))
numberOfNightsBookedPerYearRDD = numberOfNightsBookedPerYearRDD.aggregateByKey((0, 0), lambda city, booking: (city[0] + booking, city[1] + 1), lambda city, booking: (city[0] + booking[0], city[1] + booking[1]))
numberOfNightsBookedPerYearRDD = numberOfNightsBookedPerYearRDD.mapValues(lambda row: row[0]/row[1]).collect()
print numberOfNightsBookedPerYearRDD
"""

#3. e)
"""
totalPricePerYearRDD = listingsDF.select("city", "reviews_per_month", "price").rdd.map(lambda listing: (listing.city, (float(0 if listing.reviews_per_month == None else listing.reviews_per_month) / 0.7) * 3 * 12 * float("".join(c for c in listing.price if c not in "$,"))))
totalPricePerYearRDD = totalPricePerYearRDD.reduceByKey(lambda x, y: x + y).collect()
print totalPricePerYearRDD
"""


#4. a)
"""
totalListings = float(listingsDF.select("id").distinct().count())
totalHosts = float(listingsDF.select("host_id").distinct().count())
averageListingsPerHost = float(totalListings/totalHosts)
print averageListingsPerHost
"""

#4. b)
"""
listForPercentage = listingsDF.select("host_id","host_listings_count").rdd
percentageOfHostsWithMultipleListings = float(listForPercentage.map(lambda x: (x.host_id,0) if x.host_listings_count==None else (x.host_id,x.host_listings_count)).filter(lambda x: float(x[1]) >= 2).count())/float(listForPercentage.distinct().count())
print percentageOfHostsWithMultipleListings
"""

#4. c)
"""
listingsTable = listingsDF.select("id","city","host_id","price")
calendarTable = calendarDF.select("listing_id","date","available").rdd.filter(lambda x: x.available=="f").map(lambda x: (x[0],x[1])).countByKey()
print calendarTable
#.rdd.map(lambda x: (x.listing_id, x.date, 1 if x.available=="t" else 0)).filter(lambda x: int(x[2]) == 1).map(lambda x: (x[0],(x[1],x[2]))).aggregateByKey((0,0),lambda a,b: (a[0]+b[1],a[1]+1),lambda a,b: (a[0]+b[1][0],a[1]+b[1][1])).collect()
#print calendarTable
topHostIncome = listingsTable.join(calendarTable, listingsTable.id == calendarTable.listing_id)
#.rdd.filter(lambda x: x.available=="f").map(lambda x: ((x[0],x[1],x[2],x[3],x[4]),x[5])).countByKey()
#.aggregateByKey((0,0),lambda a,b: (a[0]+b,a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1]))

#print topHostIncome.take(10)

#calendarDF.printSchema()
#print calendarDF.select("date","available").take(20)
"""


#5. a)
#Test reviewer_id used to test results.
#.where(reviewsDF.reviewer_id == "7107853")
"""
topGuestsRDD = reviewsDF.join(listingsDF, reviewsDF.listing_id == listingsDF.id).select("city", "reviewer_id").rdd.map(lambda row: ((row.city, int(row.reviewer_id)), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1]))).sortBy(lambda x: -x[1][1]).groupByKey().mapValues(list).collect()

topGuestsByCity = OrderedDict()

for city in topGuestsRDD:
	for n in range(3):
		if city[0] in topGuestsByCity:
			topGuestsByCity[city[0]].append(city[1][n])
		else:
			topGuestsByCity[city[0]] = [city[1][n]]

print topGuestsByCity
"""

#5. b)
"""
biggestSpender = reviewsDF.join(listingsDF, reviewsDF.listing_id == listingsDF.id).select("reviewer_id", "listing_id", "price").rdd.map(lambda row: ((int(row.reviewer_id), int(row.listing_id)), float("".join(c for c in row.price if c not in "$,")))).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x, y: x + y).top(1, key = lambda x: x[1])
print biggestSpender
"""