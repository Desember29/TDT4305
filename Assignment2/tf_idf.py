"""
Try to run the file with command: spark-submit tf_idf.py <arguments>.
For more info, visit: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
"""

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StringType, StructField
import sys

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

#Generate new listings dataframe with neighbourhoods added.
listingsDF = listingsDF.join(listingsWithNeighbourhoodsDF, "id")

#To print schemas
"""
listingsDF.printSchema()
listingsWithNeighbourhoodsDF.printSchema()
"""

print("TF-IDF Assignment")
file = sc.textFile("data.txt").cache()
print("File has " + str(file.count()) + " lines.")
print("Passed arguments " + str(sys.argv))

sc.stop()
