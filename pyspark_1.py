from pyspark.sql import SparkSession
import sys

#------------------------------------------------------------------------------------------------------------#

def execute(spark):
    table = spark.read.option("header", True).csv("airports.csv")
    '''
    print()
    print("Table Contents : \n\n")
    table.show()
    '''
    print("\nNumber of Airports by Country : \n")
    table.groupBy("COUNTRY").count().show()

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