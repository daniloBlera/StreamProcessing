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


def update_function(new_values, last_value):
    event = last_value

    if new_values:
        event = new_values

        if last_value:
            for element in last_value:
                event.append(element)

    return event

def relational_table(new_values, last_value):
    event = last_value

    if new_values:
        event = new_values[0]

    return event


def teste_print(rdd):
    events = rdd.collect()

    for event in events:
        print(event)


if __name__ == "__main__":
    brokerUrl = 'tcp://localhost:1883'

    friendships = MQTTUtils.createStream(ssc, brokerUrl, "friendships")
    comments = MQTTUtils.createStream(ssc, brokerUrl, "comments")
    likes = MQTTUtils.createStream(ssc, brokerUrl, "likes")
    posts = MQTTUtils.createStream(ssc, brokerUrl, "posts")

    # DStreams
    posts_updated = posts.map(
            lambda event: (event.split('|')[1], event.strip('\n'))
    ).updateStateByKey(update_function)

    comments_reply_post = comments.map(
        lambda event: (event.split('|')[6].strip('\n'), event.strip('\n'))
    ).filter(lambda pair: pair[0] != '').updateStateByKey(update_function)

    comment_post_table = comments.filter(
        lambda comment: comment.split('|')[6].strip('\n') != ''
    ).map(
        lambda event: (event.split('|')[1].strip('\n'), event.split('|')[6].strip('\n'))
    ).updateStateByKey(relational_table)

    # comment_reply_comment = comments.filter(
    #     lambda comment: comment.split('|')[6].strip('\n') == '').map(
    #     lambda event: (event.split('|')[5].strip('\n'), event.strip('\n'))
    # ).updateStateByKey(update_function)

    comment_reply_comment = comments.filter(
        lambda comment: comment.split('|')[6].strip('\n') == '').map(
        lambda event: (event.split('|')[5].strip('\n'), event.strip('\n'))
        ).join(comment_post_table).map(
        lambda pair: (pair[1][1], pair[1][0])).updateStateByKey(update_function)

    # aux = comments.filter(
    #     lambda comment: comment.split('|')[6].strip('\n') == '').map(
    #     lambda event: (event.split('|')[5].strip('\n'), event.strip('\n'))
    #     ).join(comment_post_table).map(
    #     lambda pair: (pair[1][1], pair[1][0]))

    # comment_reply_comment = aux.updateStateByKey(update_function)

    comment_post_table = comments.filter(
        lambda comment: comment.split('|')[6].strip('\n') == '').map(
        lambda event: (event.split('|')[5].strip('\n'), event.strip('\n'))
        ).join(comment_post_table).map(
        lambda pair: (pair[1][0].split('|')[1], pair[1][1])).union(
        comment_post_table).updateStateByKey(relational_table)

    # comment_post_table = aux.map(lambda pair: (pair[0], pair[1][1])).union(comment_post_table).updateStateByKey(relational_table)
    #posts_updated.pprint()

    unified_structure = posts_updated.union(comments_reply_post).union(comment_reply_comment)

    unified_structure.pprint(50)
    #comments_reply_post.pprint(50)
    comment_post_table.pprint(50)
    comment_reply_comment.pprint(50)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
