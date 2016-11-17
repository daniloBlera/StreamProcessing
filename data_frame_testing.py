# -*- coding: utf-8 -*-
from pyspark import SparkContext, SQLContext
from graphframes import GraphFrame

if __name__ == "__main__":
    sc = SparkContext(master="local[5]", appName="DataFrameTesting")
    sqlContext = SQLContext(sc)

    vertices = sqlContext.createDataFrame(
        [(1, 'post', 'p1'),
         (2, 'post', 'p2'),
         (3, 'comment', 'c1'),
         (4, 'comment', 'c2'),
         (5, 'comment', 'c3'),
         (6, 'comment', 'c4')],
        ['id', 'type', 'eventId']
    )

    edges = sqlContext.createDataFrame(
        [(3, 1, 'comm'),
         (4, 2, 'comm'),
         (5, 2, 'comm'),
         (6, 1, 'comm')],
        ['src', 'dst', 'relationship']
    )

    graph = GraphFrame(vertices, edges)
