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
from pyspark.sql.functions import lower, col, concat, lit
from pyspark.sql import functions as F
from pyspark.sql.functions import udf


def construct_route(s):
    temp = str(s).split('-')
    if temp[0] == 'err':
        return ''
    ans = temp[0]
    for i in range(len(temp)-1):
        if temp[i+1] == 'err':
            return ans
        if temp[i+1] != temp[i]:
            ans += '-'
            ans += temp[i+1]
    return ans

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path = '/data/lsml/sga/clickstream.csv'
df = spark.read.option("delimiter", "\t").option("header", "true").csv(path)

df = df.withColumn('event_type', lower(col('event_type')))

df_with_err_mask = df.withColumn('err_mask', 
								 F.when(df["event_type"].contains("error"), 
								 lit('err')).otherwise(df['event_page'])
								 )

df2 = (df_with_err_mask
 .groupBy(["user_id", "session_id"])
 .agg(F.expr("concat_ws('-', sort_array(collect_list(struct(Timestamp, err_mask))).err_mask) as Values"))
)

construct = udf(construct_route)
df3 = df2.select('user_id', 'session_id', construct('Values'))
df4 = df3.groupby('construct_route(Values)').agg(F.count('construct_route(Values)'))
res = df4.sort('count(construct_route(Values))', ascending=False)

ans = ''
for val in res.take(30):
    ans += '%s\t%s\n' % (val[0], val[1])

with open('w6_dataframe.tsv', 'w') as f:
    f.write(ans[:-1])
