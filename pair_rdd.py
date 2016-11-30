# *-* coding: utf-8 *-*
from datetime import datetime

from pyspark import SparkContext
import pika


sc = SparkContext("local[5]", "jesus_christ_that's_jason_bourne")

partitions = 4

posts = sc.emptyRDD()
comments = sc.emptyRDD()
events = sc.emptyRDD()
scores = sc.emptyRDD()

current_sim_time = datetime(2000, 1, 1, 0, 0)
current_top3 = None
change_timestamp = None

parameters = pika.ConnectionParameters(host='localhost', port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
exchange_name = "amq.topic"
queue_name = "SOCIAL_NETWORK_EVENTS"

channel.exchange_declare(exchange=exchange_name, type='topic', durable=True)
# channel.queue_declare(queue=queue_name)
# channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key="#")

def consume_handler(ch, method, properties, body):
    """
    Função de callback que implementa o consumo de mensagens do serviço de
    filas.
    """
    print("RECIEVED EVENT: {}".format(method.routing_key))

    if method.routing_key == "posts":
        insert_post(body)
        # # LINHAS ABAIXO PARA PROPÓSITO DE DEPURAÇÃO
        # print("\n--POSTS--")
        # posts.foreach(print_ln)
        # print('\n')

    elif method.routing_key == "comments":
        insert_comment(body)
        # LINHAS ABAIXO PARA PROPÓSITO DE DEPURAÇÃO
        # print("\n--COMMENTS--")
        # comments.foreach(print_ln)
        # print('\n')

    print("\n--WAITING FOR NEW EVENTS--")

def print_ln(obj):
    """Wrapper para função 'print' ser usada em foreach"""
    print(obj)

def is_post_reply(comment):
    """
    Verifica se o comentário é uma resposta a um post ou a um comentário.

    :param comment: String contendo o comentário
    :return: True caso o comentário for uma resposta a um post, False caso
             contrário
    """
    parent_id = comment.split('|')[5:7]

    if (parent_id[1] == '' or
        posts.keys().filter(lambda id: id == parent_id[1]).isEmpty()):
        return False

    return True

def get_root_id_from(comment):
    """
    Recupera o ID do post raiz em que o comentário está ligado direta (resposta
    a um post) ou indiretamente (resposta a um comentário).

    :param comment: String contendo o evento do tipo `comentário`
    :return: id do post raiz ao qual o comentário está relacionado
    """
    reply_id = comment.split('|')[5]

    parent_comment = comments.filter(lambda pair: pair[1][1] == reply_id)

    if parent_comment.isEmpty():
        return

    # print("PARENT_COMMENT: {}".format(parent_comment.collect()))
    return parent_comment.take(1)[0][0]

def insert_comment(comment):
    """
    Insere o comentário no RDD de comentários.

    :paramm comment: String contendo o comentário a ser inserido no RDD
    :return: None
    """
    global comments
    parameters = tuple(comment.split('|'))

    if is_post_reply(comment):
        root_id = parameters[6]
    else:
        root_id = get_root_id_from(comment)

        if not root_id:
            return

    comments = comments.union(sc.parallelize([(root_id, parameters)], partitions))
    update_scores(parameters[0][:23])

def insert_post(post):
    """
    Insere o post recebido no RDD de posts

    :param post: String contendo o post a ser inserido no RDD
    :return: None
    """
    global posts
    parameters = tuple(post.split('|'))
    post_id = parameters[1]
    posts = posts.union(sc.parallelize([(post_id, parameters)], partitions))
    update_scores(parameters[0][:23])

def update_scores(event_timestamp):
    global posts
    global comments
    global current_sim_time
    global current_top3
    global change_timestamp

    current_sim_time = get_datetime_from(event_timestamp)
    post_structures = posts.union(comments)
    # print("\nUNION - {}".format(current_sim_time))
    # post_structures.foreach(print_ln)

    active_elements = post_structures.filter(
        lambda pair: (
            current_sim_time - get_datetime_from(pair[1][0][:23])
        ).days < 10
    )

    # TODO: Remover posts inativos
    scores = active_elements.mapValues(
        lambda pair: (
            10 - (current_sim_time - get_datetime_from(pair[0][:23])).days
        )
    ).reduceByKey(lambda x, y: x+y)

    top3 = scores.takeOrdered(3, key = lambda pair: -pair[1])

    if top3 != current_top3:
        current_top3 = top3
        change_timestamp = current_sim_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        print("\nMUDOU - {}".format(current_sim_time))

        for e in current_top3:
            print("ELEMENT: {}".format(e))

        print("\n")

        # active_elements.map(lambda pair: (pair[1]))


    # print("\nACTIVE ELEMENTS - {}".format(current_sim_time))
    # active_elements.foreach(print_ln)

    # print("\nSCORES: - {}".format(current_sim_time))
    # scores.foreach(print_ln)

    # print("\nTOP-3 SCORES: {}".format(current_sim_time))

    # for element in top_3:
    #     print("ID: {}\t, SCORE: {}".format(element[0], element[1]))
    # for e in scores:
        # print("ELEMENT: {}".format(e))

    # print"\n"

def get_datetime_from(timestamp):
    ts_format = "%Y-%m-%dT%H:%M:%S.%f"
    return datetime.strptime(timestamp, ts_format)

def signal_new_top3():
    pass

if __name__ == "__main__":
    print("\nLeitura de eventos iniciada...")
    channel.basic_consume(consume_handler, queue=queue_name, no_ack=True)
    channel.start_consuming()
