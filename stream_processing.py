# UTILIZA PYTHON3
# -*- coding: utf-8 -*-
# Execução:
# spark-submit --jars <arquivo.jar> --py-files <modulo.py> script.py
#
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

from event_structures import Post, Comment


sc = SparkContext(appName="EventStreamReader")
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/tmp/spark-streaming-checkpoints")

posts_list = []
event_update_list = []

inicio_simulado = '2010-01-25T05:12:32.921'


def update_posts():
    pass


def store_posts(time, rdd):
    global event_update_list
    events = rdd.collect()

    for element in events:
        parameters = element.split('|')
        timestamp = parameters[0][:23]
        post_id = parameters[1]
        score = 10
        ttl = 10

        event_update_list.append((timestamp, post_id, score, ttl))

    print("ELEMENTS: {}".format(len(event_update_list)))


# def store_posts(time, rdd):
#     events = rdd.collect()
#
#     for element in events:
#         post = Post(element)
#         timestamp = post.timestamp
#
#         posts_list.append(post)
#         print("\n--POST INSERTED INTO LIST--")
#         print("POSTS' NUMBER: %d" % len(posts_list))
#         update_list.append((timestamp, post))
#         print("ELEMENTS TO UPDATE: %d\n" % len(update_list))


def store_comments(time, rdd):
    events = rdd.collect()

    for element in events:
        comment = Comment(element)
        timestamp = comment.timestamp

        post = get_commented_post_from(comment)

        if post is not None:
            # TODO: suporte paralelismo?
            post.insert_comment(comment)
            print("--COMMENT INSERTED--")
            print("POST SCORE: {}".format(post.total_score))
            event_update_list.append((timestamp, comment))
        else:
            print("--COMMENT IGNORED--")

        print("ELEMENTS TO UPDATE: %d\n" % len(event_update_list))


def get_commented_post_from(comment):
    for post in posts_list:
        if post.is_parent_of(comment):
            return post

    return None


def update_function(new_values, last_value):
    event = last_value

    if new_values:
        event = new_values

    return event


if __name__ == "__main__":
    brokerUrl = 'tcp://localhost:1883'

    # mqttStream = MQTTUtils.createStream(ssc, brokerUrl, "#")

    friendships = MQTTUtils.createStream(ssc, brokerUrl, "friendships")
    comments = MQTTUtils.createStream(ssc, brokerUrl, "comments")
    likes = MQTTUtils.createStream(ssc, brokerUrl, "likes")
    posts = MQTTUtils.createStream(ssc, brokerUrl, "posts")

    # mqttStream.pprint()
    # posts.foreachRDD(store_posts)

    posts_updated = posts.map(
        lambda event: (event.split('|')[1], event.strip('\n'))
    ).updateStateByKey(update_function)
    
    posts_updated.pprint()
    # post_count = posts_updated.map(lambda tuple: (tuple, 1)).reduceByKey(lambda x, y: x + y)
    # post_count.pprint()
    
    # comments.foreachRDD(store_comments)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
