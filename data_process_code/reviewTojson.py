# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 17:02:44 2020

@author: 17917
"""
import pandas as pd
import json
import requests

review = pd.read_csv('reviews.csv') 

review_sample = review.sample(frac=0.5)

def review_tojson(review):
    jsonid = list(set(review['listing_id']))
    
    reviewjson = dict.fromkeys(jsonid,None)
    
    for index, row in review.iterrows():
        #print(index)
        inner = {}
        #inner['id'] = row['id']
        inner['reviewer_id'] = row['reviewer_id']
        inner['date'] = row['date']
        inner['reviewer_name'] = row['reviewer_name']
        inner['comments'] = row['comments']
        if reviewjson[row['listing_id']] == None:
            outer = {}
            outer[row['id']] = inner
            reviewjson[row['listing_id']] = outer
        else:
            outer = reviewjson[row['listing_id']]
            outer[row['id']] = inner
            reviewjson[row['listing_id']] = outer
    return reviewjson

def review_tojson_ver2(review):
    jsonid = list(set(review['listing_id']))
    
    reviewjson = dict.fromkeys(jsonid,[])
    
    for index, row in review.iterrows():
#        print(index)
        inner = {}
        inner['id'] = row['id']
        inner['reviewer_id'] = row['reviewer_id']
        inner['date'] = row['date']
        inner['reviewer_name'] = row['reviewer_name']
        inner['comments'] = row['comments']
        if reviewjson[row['listing_id']] == []:
            reviewjson[row['listing_id']] = [inner]
        else:
            reviewjson[row['listing_id']].append(inner)
    return reviewjson

reviewjson = review_tojson(review_sample)
test = review_sample.head(100)
review_sample = review_sample.set_index('id')
test.comments.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
test.reviewer_name.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
reviewjsontest = review_tojson_ver2(test)

reviewj = review_sample.to_json(orient='index',force_ascii=False)
reviewj = json.loads(reviewj)
file = open('dtJson/reviews.json','w',encoding='utf-8')
json.dump(reviewj,file)
file.close()

json.dumps(reviewjsontest, indent = 4)   

file = open('NYCreview.json','w',encoding='utf-8')
json.dump(reviewjsontest,file,ensure_ascii=False)
file.close()

with open("NYCreviewtest.json","w") as f:
    json.dump(reviewjsontest,f)

data = json.dumps(reviewjsontest, ensure_ascii=False)
requests.delete(url = 'https://final-review-ca45f.firebaseio.com/.json',headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"})
requests.put(url = 'https://final-review-ca45f.firebaseio.com/NYCreviewer.json',data = json.dumps(reviewj))

jsonid = list(set(review_sample['listing_id']))[0:100]

file = open('NYCreviewtest.json','w',encoding='utf-8')
json.dump(reviewjsontest,file,ensure_ascii=False)
file.close()


def fixjson(badjson):
    s = badjson
    idx = 0
    while True:
        try:
            start = s.index( '": "', idx) + 4
            end1  = s.index( '",\n',idx)
            end2  = s.index( '"\n', idx)
            if end1 < end2:
                end = end1
            else:
                end = end2
            content = s[start:end]
            content = content.replace('"', '\\"')
            s = s[:start] + content + s[end:]
            idx = start + len(content) + 6
        except:
            return s
        
fixedjson = fixjson(json.dumps(reviewjsontest))
fixedjson_read = json.loads(fixedjson)

with open('NYCreviewtest.json','r') as s:
    while True:
        try:
            result = json.loads(s)   # try to parse...
            break                    # parsing worked -> exit loop
        except Exception as e:
            # "Expecting , delimiter: line 34 column 54 (char 1158)"
            # position of unexpected character after '"'
            unexp = int(e.findall(r'\(char (\d+)\)', str(e))[0])
            # position of unescaped '"' before that
            unesc = s.rfind(r'"', 0, unexp)
            s = s[:unesc] + r'\"' + s[unesc+1:]
            # position of correspondig closing '"' (+2 for inserted '\')
            closg = s.find(r'"', unesc + 2)
            s = s[:closg] + r'\"' + s[closg+1:]
    print(result)


with open('NYCreviewtest.json','r') as s:
    result = json.loads(s)
