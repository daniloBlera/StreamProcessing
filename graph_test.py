# UTILIZA PYTHON3
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SQLContext
from graphframes import *

sc = SparkContext(appName="GraphTesting")
sqlContext = SQLContext(sc)

v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])
# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)
g.inDegrees.show()
print("G: {}".format(str(g)))
