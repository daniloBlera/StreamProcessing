# UTILIZA PYTHON3
# -*- coding: utf-8 -*-
# Execução:
# spark-submit --jars <arquivo.jar> script.py
#
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

from event_structures import Post, Comment


sc = SparkContext(appName="EventStreamReader")
ssc = StreamingContext(sc, 10)

posts_list = []
update_list = []
sc.parallelize(posts_list)
sc.parallelize(update_list)


def store_posts(time, rdd):
    events = rdd.collect()

    for element in events:
        post = Post(element)
        timestamp = post.timestamp

        posts_list.append(post)
        print("\n--POST INSERTED INTO LIST--")
        print("POSTS' NUMBER: %d" % len(posts_list))
        update_list.append((timestamp, post, post))
        print("ELEMENTS TO UPDATE: %d\n" % len(update_list))


def store_comments(time, rdd):
    events = rdd.collect()

    for element in events:
        comment = Comment(element)

        post = get_commented_post_from(comment)

        if post is not None:
            post.insert_comment(comment)
            print("--COMMENT INSERTED--")
            update_list.append(comment)
        else:
            print("--COMMENT IGNORED--")

        print("ELEMENTS TO UPDATE: %d\n" % len(update_list))


def get_commented_post_from(comment):
    for post in posts_list:
        if post.is_parent_of(comment):
            return post

    return None


def pretty_print(time, rdd):
    events = rdd.collect()

    for item in events:
        print("ITEM: {}".format(str(item)))


# brokerUrl = 'tcp://172.16.206.18:1883'
brokerUrl = 'tcp://192.168.25.7:1883'

# mqttStream = MQTTUtils.createStream(ssc, brokerUrl, "#")

friendships = MQTTUtils.createStream(ssc, brokerUrl, "friendships")
comments = MQTTUtils.createStream(ssc, brokerUrl, "comments")
likes = MQTTUtils.createStream(ssc, brokerUrl, "likes")
posts = MQTTUtils.createStream(ssc, brokerUrl, "posts")

# mqttStream.pprint()

# mqttStream.foreachRDD(pretty_print)
# friendships.foreachRDD(pretty_print)
# likes.foreachRDD(pretty_print)
posts.foreachRDD(store_posts)
comments.foreachRDD(store_comments)

ssc.start()
ssc.awaitTermination()
ssc.stop()
