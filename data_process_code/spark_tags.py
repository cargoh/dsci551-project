from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
import json
import requests

spark = SparkSession.builder.appName("readJSON").getOrCreate()

NYC = spark.read.json('json/NYC_home.json')
Bo = spark.read.json('json/Bo_home.json')


tags_NYC = []
tags_Bo  = []


NYC_count = NYC.agg(fc.count("*")).collect()[0][0]

Bo_count = Bo.agg(fc.count("*")).collect()[0][0]


NYC_wifi = NYC.filter("amenities like '%Wifi%'").groupBy('room_type').agg(fc.count("*"))
NYC_dryer = NYC.filter("amenities like '%Dryer%'").groupBy('room_type').agg(fc.count("*"))
NYC_heating = NYC.filter("amenities like '%Heating%'").groupBy('room_type').agg(fc.count("*"))
NYC_ac = NYC.filter("amenities like '%Air conditioning%'").groupBy('room_type').agg(fc.count("*"))
NYC_TV = NYC.filter("amenities like '%TV%'").groupBy('room_type').agg(fc.count("*"))
Bo_wifi = Bo.filter("amenities like '%Wifi%'").groupBy('room_type').agg(fc.count("*"))
Bo_dryer = Bo.filter("amenities like '%Dryer%'").groupBy('room_type').agg(fc.count("*"))
Bo_heating = Bo.filter("amenities like '%Heating%'").groupBy('room_type').agg(fc.count("*"))
Bo_ac = Bo.filter("amenities like '%Air conditioning%'").groupBy('room_type').agg(fc.count("*"))
Bo_TV = Bo.filter("amenities like '%TV%'").groupBy('room_type').agg(fc.count("*"))

transit = NYC_wifi.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'wifi'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/NYC_count
tags_NYC.append(temple)

transit = NYC_dryer.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'dryer'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/NYC_count
tags_NYC.append(temple)

transit = NYC_heating.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'heating'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/NYC_count
tags_NYC.append(temple)

transit = NYC_ac.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'ac'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/NYC_count
tags_NYC.append(temple)

transit = NYC_TV.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'TV'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/NYC_count
tags_NYC.append(temple)


transit = Bo_wifi.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'wifi'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/Bo_count
tags_Bo.append(temple)

transit = Bo_dryer.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'dryer'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/Bo_count
tags_Bo.append(temple)

transit = Bo_heating.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'heating'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/Bo_count
tags_Bo.append(temple)

transit = Bo_ac.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'ac'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/Bo_count
tags_Bo.append(temple)

transit = Bo_TV.toJSON().map(lambda j: json.loads(j)).collect()
temple = {'tag': 'TV'}
for i in range(len(transit)):
    temple[transit[i]['room_type']] = transit[i]['count(1)']/Bo_count
tags_Bo.append(temple)



file = open('json/tags_NYU.json','w',encoding='utf-8')
json.dump(tags_NYC,file)
file.close()

file = open('json/tags_Bo.json','w',encoding='utf-8')
json.dump(tags_Bo,file)
file.close()

requests.put(url = 'https://final-review-ca45f.firebaseio.com/Spark/tags/NYU.json',data = json.dumps(tags_NYC))

requests.put(url = 'https://final-review-ca45f.firebaseio.com/Spark/tags/Bo.json',data = json.dumps(tags_Bo))
