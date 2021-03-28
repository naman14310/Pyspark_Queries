from pyspark.sql import SparkSession
import sys

#------------------------------------------------------------------------------------------------------------#

def execute(spark):
    table = spark.read.option("header", True).csv("airports.csv")
    table.createGlobalTempView("airport")
    print("\nCountry having highest number of airports: \n")
    spark.sql("SELECT COUNTRY FROM (SELECT COUNTRY, count(*) as NO_OF_AIRPORTS FROM global_temp.airport GROUP BY COUNTRY ORDER BY NO_OF_AIRPORTS DESC) LIMIT 1").show()

#------------------------------------------------------------------------------------------------------------#

def init(no_of_cpu):
    spark = SparkSession \
        .builder \
        .master(no_of_cpu) \
        .appName("Data Systems Assignment-5") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    execute(spark)

#------------------------------------------------------------------------------------------------------------#
    
def main():
    n = len(sys.argv)
    if(n!=2):
        print("Please enter no. of CPU's")
        return
    
    cpu = sys.argv[1]
    noc = "local[" + str(cpu) + "]"
    init(noc)

#------------------------------------------------------------------------------------------------------------#

main()