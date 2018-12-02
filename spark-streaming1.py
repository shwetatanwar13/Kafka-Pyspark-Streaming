from __future__ import print_function

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))