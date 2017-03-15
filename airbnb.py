from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from collections import OrderedDict
from pyspark.sql import Column
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType
import sys
#import org.apache.spark.sql.functions.{lower, upper}

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
#listings = sc.textFile(lisLoc)

"""
#To print schemas
calendarDF.printSchema()
listingsDF.printSchema()
neighbourhoodsDF.printSchema()
reviewsDF.printSchema()
"""

#2. b)
#distinctValueCount = listingsDF.rdd.map(lambda listing: ((column for column in listingsDF.schema.names), (listing.Column(column for column in listingsDF.schema.names))))

#distinctValueCount = listingsDF.rdd.map(lambda listing: ((column for column in listingsDF.schema.names), ((column for column in listingsDF.schema.names).distinct().count()))).take(5)
#for column in listingsDF.schema.names:
#    counts.union(listingsDF.select(column).na.drop().rdd.map(lambda x: (column,str(x).upper())).distinct().count())

#for column in listingsDF.schema.names:
#	result = listingsDF.rdd.map(lambda row: (column, (row.Column(column).distinct().count())))
#	result.take(5)
	

#counts.saveAsTextFile("distinctCounts")


#Temporary fix to problem, attempting to figure out way to 
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
cities = listingsDF.select("city").distinct()
cities.rdd.map(lambda p: unicode(p[0])).coalesce(1).saveAsTextFile("citiesResults")
"""

"""
#3. a)
priceAveragePerCityRDD = listingsDF.select("city", "price").rdd.map(lambda listing: (listing.city, float("".join(c for c in listing.price if c not in "$,"))))
priceAveragePerCityRDD = priceAveragePerCityRDD.aggregateByKey((0, 0), lambda city, price: (city[0] + price, city[1] + 1), lambda city, price: (city[0] + price[0], city[1] + price[1]))
priceAveragePerCityRDD = priceAveragePerCityRDD.mapValues(lambda row: row[0]/row[1]).collect()
print priceAveragePerCityRDD
"""
"""
#Verification function to find average price in New York, only for testing/debugging of code above
pricesForNewYork = listingsDF.where(listingsDF.city == "New York").select("price").rdd.map(lambda listing: float("".join(c for c in listing.price if c not in "$,"))).collect()
counter = 0
totalPrice = 0
for item in pricesForNewYork:
	counter += 1
	totalPrice += item

averageForNewYork = totalPrice/counter
print "Average price for New York is " + str(averageForNewYork)
"""

"""
#3. b)
priceAveragePerRoomInCityRDD = listingsDF.select("city", "room_type", "price").rdd.map(lambda listing: (listing.city, listing.room_type, float("".join(c for c in listing.price if c not in "$,"))))
priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.map(lambda listing: ((listing[0],listing[1]), listing[2])).aggregateByKey((0,0),lambda a,b: (a[0]+b,a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1]))
priceAveragePerRoomInCityRDD = priceAveragePerRoomInCityRDD.mapValues(lambda row: row[0]/row[1]).collect()
print priceAveragePerRoomInCityRDD
"""



"""
cities = listingsDF.select("city","region_name","smart_location","state","street").distinct().collect()
cities = listingsDF.select("state").rdd.flatMap(lambda x: x).distinct().collect()
cities = listingsDF.select("smart_location").rdd.flatMap(lambda x: x).distinct().collect()

cities = listingsDF.select("state").rdd.map(lambda x: str(x.state).upper()).distinct().collect()
cities = listingsDF.na.drop(subset=["state"]).select("state").rdd.map(lambda x: str(x.state).upper()).distinct().collect()

cities = listingsDF.select("state").na.drop().rdd.map(lambda x: str(x.state).upper()).distinct().collect()

cities = listingsDF.select("state").na.drop().rdd.map(lambda x: "NY" if str(x.state).upper()=="NEW YORK" else str(x.state).upper()).distinct().collect()
cities = cities.withColumn("state", functions.when(cities["state"]=="NEW YORK", "NY").otherwise(cities["state"])).distinct().collect()

for c in cities:
    print c
pricePerCity = listingsDF.rdd.map(lambda c: for 
"""
