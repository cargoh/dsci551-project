from flask import Flask,render_template, request
import requests
import pymysql
import cryptography
app = Flask(__name__)
def db(table,page=1, keyword=None,case='all'):
   cnx = pymysql.connect(host='localhost', user='root', password='123456', database='project', charset="utf8")

   cursor = cnx.cursor()
   query = 'select * from '+table+' '
   if keyword:
      if case == '0':
         query = query + "where concat(id, name, description, host_id, neighbourhood, room_type, amenities) like concat('%', '"+keyword+"', '%') "
      elif case == '1':
         query = query + "where id = '"+keyword+"' "
      elif case == '2':
         query = query + "where name like '%" + keyword + "%' "
      elif case == '3':
         query = query + "where neighbourhood like '%" + keyword + "%' "
      elif case == '4':
         query = query + "where host_id = '"+keyword+"' "
   start = (int(page) - 1) * 13
   sql = query + "limit " + str(start) + ",13"
   print(sql)
   cursor.execute(sql)
   result = cursor.fetchall()
   cnx.close
   return result

def review_search(keyid):
   query = 'https://final-review-ca45f.firebaseio.com/NYCreview/.json?orderBy="listing_id"&equalTo=' + keyid
   response = requests.get(query)
   result = response.json()
   response.close()
   return result

def db_host(table,page=1, keyword=None,case='all'):
   cnx = pymysql.connect(host='localhost', user='root', password='123456', database='project', charset="utf8")
   cursor = cnx.cursor()
   query = 'select * from '+table+' '
   if keyword:
      if case == '0':
         query = query + "where concat(host_id, host_name, host_location, host_about, host_verifications) like concat('%', '"+keyword+"', '%') "
      elif case == '1':
         query = query + "where host_id = '"+keyword+"' "
      elif case == '2':
         query = query + "where host_name like '%" + keyword + "%' "
      elif case == '3':
         query = query + "where host_location like '%" + keyword + "%' "

   start = (int(page) - 1) * 13
   sql = query + "limit " + str(start) + ",13"
   print(sql)
   cursor.execute(sql)
   result = cursor.fetchall()
   cnx.close
   return result

def price_NYC(area):
   query = 'https://final-review-ca45f.firebaseio.com/Spark/PricenbNYC/.json?orderBy="neighbourhood"&startAt="' +area+'"&endAt="'+area+'\uf8ff"'
   response = requests.get(query)
   result = response.json()
   response.close()
   return result

def tag(area):
   query = 'https://final-review-ca45f.firebaseio.com/Spark/tags/'+area+'/.json?'
   response = requests.get(query)
   result = response.json()
   response.close()
   return result

def joinSearch(table1,table2,column1,column2,keyword1,keyword2,page=1):
   cnx = pymysql.connect(host='localhost', user='root', password='123456', database='project', charset="utf8")
   cursor = cnx.cursor()
   query = 'SELECT * FROM '+table1+' t1 JOIN '+table2+' t2 USING(host_id) '

   if column1 == 'price':
      print(keyword1.split(','))
      min,max = keyword1.split(',')
      if min!='' and max!='':
         query = query + 'where t1.' + column1 + '>=' + min +' and t1.' + column1 + '<=' +max
      elif min!='':
         query = query + 'where t1.' + column1 + '>=' + min
      elif max!='':
         query = query + 'where t1.' + column1 + '<=' + max
   else:
      query = query + 'where t1.'+column1+' like "%'+keyword1+'%"'
   query = query + ' and t2.'+column2+' like "%'+keyword2+'%"'
   start = (int(page) - 1) * 13
   sql = query + " limit " + str(start) + ",13"
   print(sql)
   cursor.execute(sql)
   result = cursor.fetchall()
   cnx.close
   return result

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/NYChome')
def NYChome():
    page = request.args.get('page')
    keyword = request.args.get('keyword')
    case = request.args.get('case')
    if not page or int(page) == 0:
        page = 1
    result = db('nyc_home', page, keyword, case)
    return render_template('NYChome.html', items=result, page=int(page), case=case, keyword=keyword)

@app.route("/Bostonhome")
def Bohome():
   page = request.args.get('page')
   keyword = request.args.get('keyword')
   case = request.args.get('case')
   if not page or int(page) == 0:
      page = 1
   result = db('bo_home',page,keyword,case)
   return render_template('Bostonhome.html', items=result, page=int(page),case = case, keyword = keyword)

@app.route("/hostsearch")
def HostinBN_home():
   page = request.args.get('page')
   host_id = request.args.get('host_id')
   if not page or int(page) == 0:
      page = 1
   print(host_id)
   cnx = pymysql.connect(host='localhost', user='root', password='123456', database='project', charset="utf8")
   cursor = cnx.cursor()
   query = 'select * from bo_home where host_id='+host_id+' union all select * from nyc_home where host_id='+host_id+' '
   page=int(page)
   start = (int(page) - 1) * 13
   sql = query + "limit " + str(start) + ",13"
   print(sql)
   cursor.execute(sql)
   result = cursor.fetchall()
   cnx.close
   return render_template('host.html', items=result, host_id=host_id,page=page)

@app.route("/NYChost")
def NYChost():
   page = request.args.get('page')
   keyword = request.args.get('keyword')
   print(keyword)
   case = request.args.get('case')
   if not page or int(page) == 0:
      page = 1
   result = db_host('nyc_host',page,keyword,case)
   return render_template('NYChost.html', items=result, page=int(page),case=case, keyword=keyword)


@app.route("/Bostonhost")
def Bohost():
   page = request.args.get('page')
   keyword = request.args.get('keyword')
   print(keyword)
   case = request.args.get('case')
   if not page or int(page) == 0:
      page = 1
   result = db_host('bo_host',page,keyword,case)
   return render_template('Bohost.html', items=result, page=int(page),case=case, keyword=keyword)


@app.route("/bostonprice")
def bo_price():
   query = 'https://final-review-ca45f.firebaseio.com/Spark/PricenbBo/.json'
   response = requests.get(query)
   result = response.json()
   response.close()
   return render_template('boprice.html', items=result)

@app.route("/nycprice")
def area():
   area = request.args.get('area')
   if area == None:
      result = price_NYC('Staten Island')
   else:
      result = price_NYC(area)
   return render_template('nycprice.html', items=result,area=area)

@app.route("/tag")
def taghtml():
   area = request.args.get('area')
   result = tag(area)
   return render_template('tag.html', items=result,area=area)


@app.route("/review")
def review():
   keyid = request.args.get('keyid')
   result = review_search(keyid)
   return render_template('review.html', items=result,houseid=keyid)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/search')
def search():
    return render_template('search.html')

@app.route('/joinresult')
def joinresult():
    table1 = request.args.get('table1')
    column1 = request.args.get('column1')
    keyword1 = request.args.get('keyword1')

    table2 = request.args.get('table2')
    column2 = request.args.get('column2')
    keyword2 = request.args.get('keyword2')

    page = request.args.get('page')
    if not page or int(page) == 0:
       page = 1

    result = joinSearch(table1,table2,column1,column2,keyword1,keyword2,page)
    return render_template('joinresult.html',items=result,page=int(page),table1=table1,table2=table2,column1=column1,column2=column2,keyword1=keyword1,keyword2=keyword2)


@app.route("/hostinBN")
def hostinBN():
   query = 'https://final-review-ca45f.firebaseio.com/Spark/HostinBN/.json'
   response = requests.get(query)
   result = response.json()
   response.close()
   return render_template('hostinBN.html', items=result)

if __name__ == '__main__':
    app.run()
