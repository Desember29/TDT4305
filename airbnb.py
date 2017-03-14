from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from pyspark.sql import functions
from collections import OrderedDict
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
listings = sc.textFile(lisLoc)

listingsDF.printSchema()

"""
#To print schemas
calendarDF.printSchema()
listingsDF.printSchema()
neighbourhoodsDF.printSchema()
reviewsDF.printSchema()
"""

#2. b)
"""counts = None
for column in listingsDF.schema.names:
    counts.union(listingsDF.select(column).na.drop().rdd.map(lambda x: (column,str(x).upper())).distinct().count())
    
counts.saveAsTextFile("distinctCounts")"""

distinctValuesPerColumn = OrderedDict()
for column in listingsDF.schema.names:
	distinctValuesPerColumn[column] = listingsDF.select(column).na.drop().rdd.map(lambda x: str(x).upper()).distinct().count()

print "Distinct values per field:"
print distinctValuesPerColumn
"""
"""
#To write a tab seperated field-value pair
output = open("output.txt", "w")
for key in distinctValuesPerColumn:
	output.write(str(key) + "\t" + str(distinctValuesPerColumn[key]) + "\n")
output.close()


#2. c)
#cities = listingsDF.select("city").distinct()
#cities.rdd.map(lambda p: unicode(p[0])).coalesce(1).saveAsTextFile("citiesResults")


#listingsDF.select("market").distinct().show()

#3. a)
#cities = listingsDF.select("city","region_name","smart_location","state","street").distinct().collect()
#cities = listingsDF.select("state").rdd.flatMap(lambda x: x).distinct().collect()
#cities = listingsDF.select("smart_location").rdd.flatMap(lambda x: x).distinct().collect()

#cities = listingsDF.select("state").rdd.map(lambda x: str(x.state).upper()).distinct().collect()
#cities = listingsDF.na.drop(subset=["state"]).select("state").rdd.map(lambda x: str(x.state).upper()).distinct().collect()

#cities = listingsDF.select("state").na.drop().rdd.map(lambda x: str(x.state).upper()).distinct().collect()

#cities = listingsDF.select("state").na.drop().rdd.map(lambda x: "NY" if str(x.state).upper()=="NEW YORK" else str(x.state).upper()).distinct().collect()
#cities = cities.withColumn("state", functions.when(cities["state"]=="NEW YORK", "NY").otherwise(cities["state"])).distinct().collect()

#for c in cities:
#    print c
#pricePerCity = listingsDF.rdd.map(lambda c: for 


