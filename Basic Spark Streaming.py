

import pyspark
from pyspark.streaming import StreamingContext



sc=pyspark.SparkContext('local[1]','streaming')
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream('localhost',9999)


words=lines.flatMap(lambda x:x.split()).countByValue()
words.pprint()
ssc.start()


