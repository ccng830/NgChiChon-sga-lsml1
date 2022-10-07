from pyspark import SparkContext, SparkConf
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def err_mask(line):
    if 'error' in str(line[2]).lower():
        return [line[0], line[1], line[2], 'err', line[4]]
    return [line[0], line[1], line[2], line[3], line[4]]

def construct_route(line):
    temp = str(line[1]).split('-')
    if temp[0] == 'err':
        ans = ''
    ans = temp[0]
    for i in range(len(temp)-1):
        if temp[i+1] == 'err':
            break
        if temp[i+1] != temp[i]:
            ans += '-'
            ans += temp[i+1]
    return [line[0], ans]


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path = '/data/lsml/sga/clickstream.csv'
df = spark.read.option("delimiter", "\t").option("header", "true").csv(path)

df2 = df.rdd.map(lambda line: err_mask(line))
df3 = df2.map(lambda line: (line[0] + ', ' + line[1], line[3])).reduceByKey(lambda a, b: a + '-' + b)
df4 = df3.map(lambda line: construct_route(line))

res = df4.map(lambda line: (line[1], 1))
sort_res = res.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)
ans = ''
for i in sort_res.take(30):
    ans += '%s\t%s\n' % (i[0], i[1])

with open('w6_rdd.tsv', 'w') as f:
    f.write(ans[:-1])