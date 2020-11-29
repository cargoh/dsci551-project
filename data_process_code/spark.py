# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 20:30:35 2020

@author: 17917
"""
import json
import requests
import findspark
findspark.init()
#import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row



sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

NYChome = spark.read.json('NYC_home.json')
NYChost = spark.read.json('NYC_host.json')
Bohome = spark.read.json('Bo_home.json')
Bohost = spark.read.json('Bo_host.json')

#Hosts both have houses in Boston and in NYC
host_intersect = NYChost.join(Bohost, ["host_id"], "leftsemi")
host_intersect['host_id','host_name'].show()
results = host_intersect.toJSON().map(lambda j: json.loads(j)).collect()

#requests.delete(url = 'https://final-review-ca45f.firebaseio.com/Spark/HostinBoNYC.json',headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"})
requests.put(url = 'https://final-review-ca45f.firebaseio.com/Spark/HostinBoNYC.json',data = json.dumps(results))
file = open('spark/Hosts_Bo_NYC.json','w',encoding='utf-8')
json.dump(results,file)
file.close()

#Price rank group by neighbourhood location in NYC(avg)
NYCPriceRank = NYChome.groupBy("neighbourhood").agg({'Price': 'avg'}).orderBy('avg(Price)', ascending=False).na.drop(subset=["avg(Price)"])
NYCPriceRank = NYCPriceRank.withColumnRenamed( "avg(Price)","avgPrice")
results = NYCPriceRank.toJSON().map(lambda j: json.loads(j)).collect()
requests.put(url = 'https://final-review-ca45f.firebaseio.com/Spark/PricenbNYC.json',data = json.dumps(results))
file = open('spark/Price_rank_NYC_nb.json','w',encoding='utf-8')
json.dump(results,file)
file.close()

#Price rank group by neighbourhood location in Boston(avg)
BoPriceRank = Bohome.groupBy("neighbourhood").agg({'Price': 'avg'}).orderBy('avg(Price)', ascending=False).na.drop(subset=["avg(Price)"])
BoPriceRank = BoPriceRank.withColumnRenamed( "avg(Price)","avgPrice")
results = BoPriceRank.toJSON().map(lambda j: json.loads(j)).collect()
requests.put(url = 'https://final-review-ca45f.firebaseio.com/Spark/PricenbBo.json',data = json.dumps(results))
file = open('spark/Price_rank_Bo_nb.json','w',encoding='utf-8')
json.dump(results,file)
file.close()


def findcomment(keyid):
    getstr = 'https://final-review-ca45f.firebaseio.com/NYCreview/.json?orderBy="listing_id"&equalTo=' + keyid
    test = requests.get(getstr).text
    test = json.loads(test)
    test_list = list(test.values())
    test1 = spark.createDataFrame(Row(**x) for x in test_list)
    test1.show(10)
    return test1

comment_5178 = findcomment('5178')



"""
Try Failed


NYCPriceRankjson = NYCPriceRank.groupby('neighbourhood').agg(collect_list(NYCPriceRank.avgPrice).alias("value"),collect_list(NYCPriceRank.neighbourhood).alias("key"))

NYCPriceRankjson = NYCPriceRankjson.withColumn("map", map_from_arrays(NYCPriceRankjson.key,NYCPriceRankjson.value))

NYCPriceRankjson.show(10,False)
NYCPriceRankjson.printSchema()

NYCPriceRankjson = NYCPriceRankjson.withColumn("map", to_json("map"))

NYCPriceRankjson.select('map').show(10,False).map(lambda j: json.loads(j)).collect()

"""

