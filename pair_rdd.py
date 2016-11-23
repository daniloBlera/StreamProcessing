# *-* coding: utf-8 *-*
from pyspark import SparkContext

sc = SparkContext("local", "jesus_christ_it's_jason_bourne")

posts = sc.emptyRDD()
comments = sc.emptyRDD()

def is_post_reply(comment):
    parent_id = comment.split('|')[5:7]
    
    if (parent_id[1] == '' or
        posts.keys().filter(lambda id: id == parent_id[1]).isEmpty()):
        return False
    
    return True
    
def get_root_id_from(comment):
    reply_id = comment.split('|')[5]

    parent_comment = comments.filter(
        lambda pair: pair[1].split('|')[1] == reply_id)

    if parent_comment.isEmpty():
        return
    
    return parent_comment.take(1)[0][0]

def insert_comment(comment):
    global comments
    
    if is_post_reply(comment):
        root_id = comment.split('|')[6]
    else:
        root_id = get_root_id_from(comment)

        if not root_id:
            return

    comments = comments.union(sc.parallelize([(root_id, comment)]))

def insert_post(post):
    global posts
    posts = posts.union(sc.parallelize([(post.split('|')[1], post)]))


if __name__ == "__main__":
    p1 = ("p_id1", "ts|p_id1|user_id|post1|user")
    p2 = ("p_id2", "ts|p_id2|user_id|post2|user")
    p3 = ("p_id3", "ts|p_id3|user_id|post3|user")
    
    insert_post(p1[1])
    insert_post(p2[1])
    insert_post(p3[1])

    c1 = "ts|c_id1|user_id|comment|user||p_id2"
    c2 = "ts|c_id2|user_id|comment|user||p_id1"
    c3 = "ts|c_id3|user_id|comment|user|c_id1|"
    c4 = "ts|c_id4|user_id|comment|user|c_id3|"

    c5 = "ts|c_id4|user_id|comment|user|this_comment_doesnt_exists|"
    c6 = "ts|c_id4|user_id|comment|user||this_post_doesnt_exists"

    print("POSTS: {}".format(posts.collect()))

    print("\nINSERINDO {}".format(c1))
    insert_comment(c1) 
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

    print("\nINSERINDO {}".format(c2))
    insert_comment(c2)
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

    print("\nINSERINDO {}".format(c3))
    insert_comment(c3)
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

    print("\nINSERINDO {}".format(c4))
    insert_comment(c4)
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

    print("\nINSERINDO {}".format(c5))
    insert_comment(c5)
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

    print("\nINSERINDO {}".format(c6))
    insert_comment(c6)
    print("COMENTARIOS ATÉ O MOMENTO:")
    for i in comments.collect():
        print(i)

