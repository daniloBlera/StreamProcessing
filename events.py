# -*- coding: utf-8 -*-
class Post:
    structure_points = 0
    points = 0
    timestamp = None
    post_id = None
    user_id = None
    message = None
    user_name = None
    comments_num = 0
    comments = []

    def __init__(self, event):
        """

        :param event:
        """
        segments = event.split('|')

        self.timestamp = segments[0]
        self.post_id = segments[1]
        self.user_id = segments[2]
        self.message = segments[3]
        self.user_name = segments[4]


class Friendship:
    # is the friendship’s establishment timestamp
    timestamp = None

    # is the id of one of the users
    user_id_1 = None

    # is the id of the other user
    user_id_2 = None

    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.user_id_1 = segments[1]
        self.user_id_2 = segments[2]


class Comment:
    points = 0
    # is the comment’s timestamp
    timestamp = None

    # is the unique id of the comment
    comment_id = None

    # is the unique id of the user
    user_id = None

    # is a string containing the actual comment
    message = None

    # is a string containing the actual user name
    user_name = None

    # is the id of the comment being replied to (-1 if the tuple is a reply to
    # a post)
    # replied_comment_id = None

    # is the id of the post being commented (-1 if the tuple is a reply to a
    # comment)
    commented_post_id = None

    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.comment_id = segments[1]
        self.user_id = segments[2]
        self.message = segments[3]
        self.user_name = segments[4]
        # self.replied_comment_id = segments[5]
        self.commented_post_id = segments[6]


class Like:
    # is the like’s timestamp
    timestamp = None

    # is the id of the user liking the comment
    user_id = None

    # is the id of the comment
    comment_id = None

    def __init__(self, event):
        segments = event.split('|')

        self.timestamp = segments[0]
        self.user_id = segments[1]
        self.comment_id = segments[2]
