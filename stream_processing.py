# -*- coding: utf-8 -*-
# Execução:
# spark-submit --jars <arquivo.jar> script.py
#
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils


sc = SparkContext(appName="EventStreamReader")
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 4)
ssc.checkpoint("/tmp/spark-streaming-checkpoints")

brokerUrl = 'tcp://localhost:1883'
friendships = MQTTUtils.createStream(ssc, brokerUrl, "friendships")
comments = MQTTUtils.createStream(ssc, brokerUrl, "comments")
likes = MQTTUtils.createStream(ssc, brokerUrl, "likes")
posts = MQTTUtils.createStream(ssc, brokerUrl, "posts")


posts_list = []
event_update_list = []

def update_function(new_value, last_value):
    event = last_value

    if new_value:
        event = new_value

        if last_value:
            for element in last_value:
                event.append(element)

    return event

# TODO: pontuações
def update_event(new_event, last_event):
    if new_event:
        return new_event

    return last_event

def update_relational_table(new_value, last_value):
    event = last_value

    if new_value:
        event = new_value[0]

    return event


if __name__ == "__main__":
    print("Leitura de eventos iniciada...")

    # DStreams
    posts_updated = posts.map(
        lambda event: (event.split('|')[1], event.strip('\n'))
        ).updateStateByKey(update_event)

    comments_updated = comments.map(
        lambda evt: (evt.split('|')[1], tuple(evt.split('|')[5:7]))
        ).updateStateByKey(update_event)

    post_replies = comments.map(
        lambda event: (event.split('|')[6].strip('\n'), event.strip('\n'))
        ).filter(lambda pair: pair[0] != '').updateStateByKey(update_event)

    # comment_post_table = comments.map(
    #     lambda event: (event.split('|')[1], event.split('|')[6].strip('\n'))
    #     ).filter(lambda pair: pair[1] != ''
    #     ).updateStateByKey(update_relational_table)

    # comment_replies = comments.map(
    #     lambda event: (event.split('|')[5], event.strip('\n'))).filter(
    #     lambda pair: pair[0] != '').updateStateByKey(update_function)

    # comment_replies = comments.map(
    #     lambda evt: (evt.split('|')[5], evt.strip('\n')) ).filter(
    #     lambda pair: pair[0] != '').join(
    #     comment_post_table).map(
    #     lambda pair: (pair[1][1], pair[1][0])).updateStateByKey(
    #     update_function)

    cp_table = comments.map(
    lambda evt: (evt.split('|')[1], evt.split('|')[6].strip('\n')) ).filter(
    lambda pair: pair[1] != '').updateStateByKey(
    update_relational_table)

    cc_table = comments.map(
        lambda evt: (evt.split('|')[5], evt.split('|')[1]) ).filter(
        lambda pair: pair[0] != '')

    ccp_table = cc_table.join(
        cp_table).map(
        lambda pair: (pair[1][0], pair[1][1]) ).updateStateByKey(
        update_relational_table)

    comment_replies = comments.map(
        lambda evt: (evt.split('|')[5], evt.strip('\n'))).filter(
        lambda pair: pair[0] != '').join(
        cp_table).map(
        lambda pair: (pair[1][0].split('|')[1], pair[1][1])).updateStateByKey(
        update_function)

    # comment_post_table = comments.filter(
    #     lambda comment: comment.split('|')[6].strip('\n') == '').map(
    #     lambda event: (event.split('|')[5].strip('\n'), event.strip('\n'))
    #     ).join(comment_post_table).map(
    #     lambda pair: (pair[1][0].split('|')[1], pair[1][1])).union(
    #     comment_post_table).updateStateByKey(update_relational_table)

    cp_table = comments.map(
        lambda evt: (evt.split('|')[5], evt.strip('\n')) ).filter(
        lambda pair: pair[0] != '').join(
        cp_table).map(
        lambda pair: (pair[1][0].split('|')[1], pair[1][1])).union(
        cp_table).updateStateByKey(update_relational_table)

    # unified_structure = posts_updated.union(post_replies).union(comment_replies)

    # Prints
    # posts_updated.pprint(50)
    # post_replies.pprint(50)
    comments_updated.pprint(50)
    cp_table.pprint(50)
    ccp_table.pprint(50)
    # comment_post_table.pprint(50)
    # comment_replies.pprint(50)
    # unified_structure.pprint(50)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
