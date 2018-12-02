

import findspark
from operator import add
import re
findspark.init('/opt/spark-2.2.0-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.streaming import StreamingContext


sc = SparkContext('local[2]','Shweta')
ssc= StreamingContext(sc,10)
sqlContext = HiveContext(sc)
lines= ssc.socketTextStream('localhost',9999)

def process(rdd):
    df=sqlContext.createDataFrame(rdd,"string")
    df.createOrReplaceTempView("test1")
    sqlContext.sql("insert into test select * from test1")
    sqlContext.sql("select * from test").show()
    

lines.foreachRDD(process)
ssc.start()



