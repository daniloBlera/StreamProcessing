# UTILIZA PYTHON3
# -*- coding: utf-8 -*-
# Execução:
# spark-submit --jars <arquivo.jar> script.py
#
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils


def pretty_print(time, rdd):
    events = rdd.collect()
    print("PRIMERO PRINT: {}".format(str(events)))

    if len(events) != 0:
        print("EVENTS: " + str(events))
    else:
        print("EMPTY")

sc = SparkContext(appName="EventStreamReader")
ssc = StreamingContext(sc, 10)

ssc0 = StreamingContext(sc, 10)
ssc1 = StreamingContext(sc, 10)
ssc2 = StreamingContext(sc, 10)
ssc3 = StreamingContext(sc, 10)

brokerUrl = 'tcp://172.16.206.18:1883'
# brokerUrl = 'tcp://192.168.25.7:1883'

mqttStream = MQTTUtils.createStream(ssc, brokerUrl, "#")

friendships = MQTTUtils.createStream(ssc0, brokerUrl, "event.friendships")
comments = MQTTUtils.createStream(ssc1, brokerUrl, "event.friendships")
likes = MQTTUtils.createStream(ssc2, brokerUrl, "event.friendships")
posts = MQTTUtils.createStream(ssc3, brokerUrl, "event.friendships")

posts_list = []
update_list = []
sc.parallelize(posts_list)
sc.parallelize(update_list)

mqttStream.pprint()

friendships.foreachRDD(pretty_print)
comments.foreachRDD(pretty_print)
likes.foreachRDD(pretty_print)
posts.foreachRDD(pretty_print)

try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    print("\nLEITURA INTERROMPIDA")
finally:
    ssc.stop()
