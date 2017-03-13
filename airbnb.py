from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from collections import OrderedDict
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

conf = (SparkConf().setMaster("local[*]").setAppName("AirBnB Application"))
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

calendarDF = sqlContext.read.csv("C:/Users/Thomas/Desktop/airbnb_datasets_old/calendar_us.csv", sep="\t", header=True)
listingsDF = sqlContext.read.csv("C:/Users/Thomas/Desktop/airbnb_datasets_old/listings_us.csv", sep="\t", header=True)
neighbourhoodsDF = sqlContext.read.json("C:/Users/Thomas/Desktop/airbnb_datasets_old/neighbourhoods.geojson")
reviewsDF = sqlContext.read.csv("C:/Users/Thomas/Desktop/airbnb_datasets_old/reviews_us.csv", sep="\t", header=True)

listingsDF.printSchema()

"""
#To print schemas
calendarDF.printSchema()
listingsDF.printSchema()
neighbourhoodsDF.printSchema()
reviewsDF.printSchema()
"""
"""
#2. b)
distinctValuesPerColumn = OrderedDict()
for column in listingsDF.schema.names:
	distinctValuesPerColumn[column] = listingsDF.select(column).distinct().count()

print "Distinct values per field:"
print distinctValuesPerColumn
"""
"""
#To write a tab seperated field-value pair
output = open("output.txt", "w")
for key in distinctValuesPerColumn:
	output.write(str(key) + "\t" + str(distinctValuesPerColumn[key]) + "\n")
output.close()
"""

#2. c)
cities = listingsDF.select("city").distinct()
"""
output = open("output.txt", "w")
for city in cities:
	output.write(str(city) + ", ")
output.close()
"""

#listingsDF.where(listingsDF.id == "12607303").select("city", "country", "country_code", "host_location", "is_location_exact", "latitude", "longitude", "neighborhood_overview", "region_id", "region_name", "region_parent_id", "state", "street", "zipcode").show()