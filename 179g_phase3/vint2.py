from __future__ import print_function
import glob
import sys
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import * 
from pyspark_cassandra import CassandraSparkContext 
from pyspark.sql import SQLContext
from string import ascii_lowercase

class Done(Exception): pass

#from pyspark.sql import HiveContext
conf = SparkConf().setAppName("vincent test")
sc = CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)
#hiveContext = HiveContext(sqlContext)
log = open("log.txt", "w")
#filenames = glob.glob("/home/cs179g/179_data/Occupation-Wage/partitionstest/partitions/*")
filenames = ['/home/cs179g/179_data/Occupation-Wage/oe.series']
for filename in filenames:
	test = sqlContext.read.format('com.databricks.spark.csv') \
		.options(delimiter="\t") \
		.options(header='true') \
		.load("/home/cs179g/179_data/Occupation-Wage/oe.series")
	
	test1 = test.filter("datatype_code != '01'")
	test2 = test1.filter("datatype_code != '04'")
	test3 = test2.filter("datatype_code != '12'")
	test4 = test3.filter("datatype_code != '13'")
	test5 = test4.filter("datatype_code != '14'")

	test.show()
	
	shorted_test = test.map(lambda row: {'series_id' : row.series_id, \
		'areatype_code' : row.areatype_code, \
		'industry_code' : row.industry_code, \
		'occupation_code' : row.occupation_code, \
		'datatype_code' : row.datatype_code, \
		'state_code' : row.state_code, \
		'area_code' : row.area_code, \
		'sector_code' : row.sector_code, \
		'series_title' : row.series_title, \
		'begin_period' : row.begin_period, \
		'begin_year' : row.begin_year,\
		'end_year' : row.end_year,\
		'end_period' : row.end_period})

	shorted_test.saveToCassandra(keyspace="occuwage", table="series")
