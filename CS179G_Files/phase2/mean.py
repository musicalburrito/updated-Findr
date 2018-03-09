from __future__ import print_function

import sys

from operator import add
from pyspark.sql.functions import col, exp, lit
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext

def average(*cols):
    print(cols[0])



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mean.py <fiile>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName("Calculates Mean")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)


    customSchema = StructType([\
                StructField("RegionID",StringType(), False), \
                StructField("RegionName",StringType(), True), \
                StructField("State", StringType(), True), \
                StructField("Metro", StringType(), True), \
                StructField("CountyName", StringType(),True), \
                StructField("SizeRank", FloatType(), False), \
                StructField("199604", FloatType(), True), \
                StructField("199605", FloatType(), True), \
                StructField("199607", FloatType(), True), \
                StructField("199608", FloatType(), True), \
                StructField("199609", FloatType(), True), \
                StructField("199610", FloatType(), True), \
                StructField("199611", FloatType(), True), \
                StructField("199612", FloatType(), True), \

                StructField("199701", FloatType(), True), \
                StructField("199702", FloatType(), True), \
                StructField("199703", FloatType(), True), \
                StructField("199704", FloatType(), True), \
                StructField("199705", FloatType(), True), \
                StructField("199706", FloatType(), True), \
                StructField("199707", FloatType(), True), \
                StructField("199708", FloatType(), True), \
                StructField("199709", FloatType(), True), \
                StructField("199710", FloatType(), True), \
                StructField("199711", FloatType(), True), \
                StructField("199712", FloatType(), True), \


                StructField("199801", FloatType(), True), \
                StructField("199802", FloatType(), True), \
                StructField("199803", FloatType(), True), \
                StructField("199804", FloatType(), True), \
                StructField("199805", FloatType(), True), \
                StructField("199806", FloatType(), True), \
                StructField("199807", FloatType(), True), \
                StructField("199808", FloatType(), True), \
                StructField("199809", FloatType(), True), \
                StructField("199810", FloatType(), True), \
                StructField("199811", FloatType(), True), \
                StructField("199812", FloatType(), True), \


                StructField("199901", FloatType(), True), \
                StructField("199902", FloatType(), True), \
                StructField("199903", FloatType(), True), \
                StructField("199904", FloatType(), True), \
                StructField("199905", FloatType(), True), \
                StructField("199906", FloatType(), True), \
                StructField("199907", FloatType(), True), \
                StructField("199908", FloatType(), True), \
                StructField("199909", FloatType(), True), \
                StructField("199910", FloatType(), True), \
                StructField("199911", FloatType(), True), \
                StructField("199912", FloatType(), True), \

                StructField("200001", FloatType(), True), \
                StructField("200002", FloatType(), True), \
                StructField("200003", FloatType(), True), \
                StructField("200004", FloatType(), True), \
                StructField("200005", FloatType(), True), \
                StructField("200006", FloatType(), True), \
                StructField("200007", FloatType(), True), \
                StructField("200008", FloatType(), True), \
                StructField("200009", FloatType(), True), \
                StructField("200010", FloatType(), True), \
                StructField("200011", FloatType(), True), \
                StructField("200012", FloatType(), True), \


                StructField("200101", FloatType(), True), \
                StructField("200102", FloatType(), True), \
                StructField("200103", FloatType(), True), \
                StructField("200104", FloatType(), True), \
                StructField("200105", FloatType(), True), \
                StructField("200106", FloatType(), True), \
                StructField("200107", FloatType(), True), \
                StructField("200108", FloatType(), True), \
                StructField("200109", FloatType(), True), \
                StructField("200110", FloatType(), True), \
                StructField("200111", FloatType(), True), \
                StructField("200112", FloatType(), True), \


                StructField("200201", FloatType(), True), \
                StructField("200202", FloatType(), True), \
                StructField("200203", FloatType(), True), \
                StructField("200204", FloatType(), True), \
                StructField("200205", FloatType(), True), \
                StructField("200206", FloatType(), True), \
 		StructField("200207", FloatType(), True), \
                StructField("200208", FloatType(), True), \
                StructField("200209", FloatType(), True), \
                StructField("200210", FloatType(), True), \
                StructField("200211", FloatType(), True), \
                StructField("200212", FloatType(), True), \


                StructField("200301", FloatType(), True), \
                StructField("200302", FloatType(), True), \
                StructField("200303", FloatType(), True), \
                StructField("200304", FloatType(), True), \
                StructField("200305", FloatType(), True), \
                StructField("200306", FloatType(), True), \
                StructField("200307", FloatType(), True), \
                StructField("200308", FloatType(), True), \
                StructField("200309", FloatType(), True), \
                StructField("200310", FloatType(), True), \
                StructField("200311", FloatType(), True), \
                StructField("200312", FloatType(), True), \


                StructField("200401", FloatType(), True), \
                StructField("200402", FloatType(), True), \
                StructField("200403", FloatType(), True), \
                StructField("200404", FloatType(), True), \
 		StructField("200405", FloatType(), True), \
                StructField("200406", FloatType(), True), \
                StructField("200407", FloatType(), True), \
                StructField("200408", FloatType(), True), \
                StructField("200409", FloatType(), True), \
                StructField("200410", FloatType(), True), \
                StructField("200411", FloatType(), True), \
                StructField("200412", FloatType(), True), \


                StructField("200501", FloatType(), True), \
                StructField("200502", FloatType(), True), \
                StructField("200503", FloatType(), True), \
                StructField("200504", FloatType(), True), \
                StructField("200505", FloatType(), True), \
                StructField("200506", FloatType(), True), \
                StructField("200507", FloatType(), True), \
                StructField("200508", FloatType(), True), \
                StructField("200509", FloatType(), True), \
                StructField("200510", FloatType(), True), \
                StructField("200511", FloatType(), True), \
                StructField("200512", FloatType(), True), \


                StructField("200601", FloatType(), True), \
                StructField("200602", FloatType(), True), \
                StructField("200603", FloatType(), True), \
                StructField("200604", FloatType(), True), \
                StructField("200605", FloatType(), True), \
                StructField("200606", FloatType(), True), \
 		StructField("200607", FloatType(), True), \
                StructField("200608", FloatType(), True), \
                StructField("200609", FloatType(), True), \
                StructField("200610", FloatType(), True), \
                StructField("200611", FloatType(), True), \
                StructField("200612", FloatType(), True), \


                StructField("200701", FloatType(), True), \
                StructField("200702", FloatType(), True), \
                StructField("200703", FloatType(), True), \
                StructField("200704", FloatType(), True), \
                StructField("200705", FloatType(), True), \
                StructField("200706", FloatType(), True), \
                StructField("200707", FloatType(), True), \
                StructField("200708", FloatType(), True), \
                StructField("200709", FloatType(), True), \
                StructField("200710", FloatType(), True), \
                StructField("200711", FloatType(), True), \
                StructField("200712", FloatType(), True), \


                StructField("200801", FloatType(), True), \
                StructField("200802", FloatType(), True), \
                StructField("200803", FloatType(), True), \
                StructField("200804", FloatType(), True), \
                StructField("200805", FloatType(), True), \
                StructField("200806", FloatType(), True), \
                StructField("200807", FloatType(), True), \
                StructField("200808", FloatType(), True), \
                StructField("200809", FloatType(), True), \
                StructField("200810", FloatType(), True), \
                StructField("200811", FloatType(), True), \
 		StructField("200812", FloatType(), True), \


                StructField("200901", FloatType(), True), \
                StructField("200902", FloatType(), True), \
                StructField("200903", FloatType(), True), \
                StructField("200904", FloatType(), True), \
                StructField("200905", FloatType(), True), \
                StructField("200906", FloatType(), True), \
                StructField("200907", FloatType(), True), \
                StructField("200908", FloatType(), True), \
                StructField("200909", FloatType(), True), \
                StructField("200910", FloatType(), True), \
                StructField("200911", FloatType(), True), \
                StructField("200912", FloatType(), True), \

                StructField("201001", FloatType(), True), \
                StructField("201002", FloatType(), True), \
                StructField("201003", FloatType(), True), \
                StructField("201004", FloatType(), True), \
                StructField("201005", FloatType(), True), \
                StructField("201006", FloatType(), True), \
                StructField("201007", FloatType(), True), \
                StructField("201008", FloatType(), True), \
                StructField("201009", FloatType(), True), \
                StructField("201010", FloatType(), True), \
                StructField("201011", FloatType(), True), \
                StructField("201012", FloatType(), True), \


                StructField("201101", FloatType(), True), \
                StructField("201102", FloatType(), True), \
                StructField("201103", FloatType(), True), \
 		StructField("201104", FloatType(), True), \
                StructField("201105", FloatType(), True), \
                StructField("201106", FloatType(), True), \
                StructField("201107", FloatType(), True), \
                StructField("201108", FloatType(), True), \
                StructField("201109", FloatType(), True), \
                StructField("201110", FloatType(), True), \
                StructField("201111", FloatType(), True), \
                StructField("201112", FloatType(), True), \


                StructField("201201", FloatType(), True), \
                StructField("201202", FloatType(), True), \
                StructField("201203", FloatType(), True), \
                StructField("201204", FloatType(), True), \
                StructField("201205", FloatType(), True), \
                StructField("201206", FloatType(), True), \
                StructField("201207", FloatType(), True), \
                StructField("201208", FloatType(), True), \
                StructField("201209", FloatType(), True), \
                StructField("201210", FloatType(), True), \
                StructField("201211", FloatType(), True), \
                StructField("201212", FloatType(), True), \


                StructField("201301", FloatType(), True), \
                StructField("201302", FloatType(), True), \
                StructField("201303", FloatType(), True), \

 		StructField("201304", FloatType(), True), \
                StructField("201305", FloatType(), True), \
                StructField("201306", FloatType(), True), \
                StructField("201307", FloatType(), True), \
                StructField("201308", FloatType(), True), \
                StructField("201309", FloatType(), True), \
                StructField("201310", FloatType(), True), \
                StructField("201311", FloatType(), True), \
                StructField("201312", FloatType(), True), \


                StructField("201401", FloatType(), True), \
                StructField("201402", FloatType(), True), \
                StructField("201403", FloatType(), True), \
                StructField("201404", FloatType(), True), \
                StructField("201405", FloatType(), True), \
                StructField("201406", FloatType(), True), \
                StructField("201407", FloatType(), True), \
                StructField("201408", FloatType(), True), \
                StructField("201409", FloatType(), True), \
                StructField("201410", FloatType(), True), \
                StructField("201411", FloatType(), True), \
                StructField("201412", FloatType(), True), \


                StructField("201501", FloatType(), True), \
                StructField("201502", FloatType(), True), \
                StructField("201503", FloatType(), True), \
                StructField("201504", FloatType(), True), \
                StructField("201505", FloatType(), True), \
                StructField("201506", FloatType(), True), \
                StructField("201507", FloatType(), True), \
		  StructField("201508", FloatType(), True), \
                StructField("201509", FloatType(), True), \
                StructField("201510", FloatType(), True), \
                StructField("201511", FloatType(), True), \
                StructField("201512", FloatType(), True), \


                StructField("201601", FloatType(), True), \
                StructField("201602", FloatType(), True), \
                StructField("201603", FloatType(), True), \
                StructField("201604", FloatType(), True), \
                StructField("201605", FloatType(), True), \
                StructField("201606", FloatType(), True), \
                StructField("201607", FloatType(), True), \
                StructField("201608", FloatType(), True), \
                StructField("201609", FloatType(), True), \
                StructField("201610", FloatType(), True), \
                StructField("201611", FloatType(), True), \
                StructField("201612", FloatType(), True)])
#calc is a dataframe used to find columns for now.
    test = sqlContext.read.format('com.databricks.spark.csv') \
                .options(header = 'true') \
                .options(delimiter=",") \
                .load(sys.argv[1], schema = customSchema)
  #test.printSchema()
   # test.show()
    test_filter = test.filter(test.RegionID == '6181')

    print('# of whatever'.format(test_filter.count()))
    temp = test_filter.map(lambda row: {'regionid': row.RegionID,
                                        'mean': average(row),
                                        'regionname': row.RegionName,
                                        'state': row.State,
                                        'metro': row.Metro,
                                        'countyname': row.CountyName,
                                        'sizerank': row.SizeRank})

  rowMean  = (sum(col(x) for x in test.columns[7:]) / 249).alias("mean1")
    new_df = test.withColumn('Mean',rowMean)
    new_df.show(3)
    test_filter1 = new_df.filter(new_df.RegionID == '6181')


    newest_temp = test_filter1.map(lambda row: { 'regionid': row.RegionID,
                                           'mean': row.Mean,
                                           'regionname': row.RegionName,
                                           'state': row.State,
                                           'metro': row.Metro,
                                           'countyname': row.CountyName,
                                           'sizerank': row.SizeRank})


   # sqlContext.registerDataFrameAsTable(test,"table1")
   # print(test.where(test.RegionID.isNull()).count())
  ##  test_filter.select("RegionID").show()
   # print(test_filter)
  #  test2 = test.groupBy().mean('199604').collect()
   # print(test2)
    #rowMean  = (sum(col(x) for x in test.columns[7:]) / 12).alias("mean")
    #test5 = test.select(rowMean)
#    print(test.columns[7:].row[1:])
    temp.saveToCassandra(keyspace='mean', table = 'test')
  #  print(temp)




                                                                                                                    1,8           Top

