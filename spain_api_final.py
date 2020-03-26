#!/usr/bin/env python
# coding: utf-8

# In[3]:


try:
    import pandas as pd
    import json
    import urllib.parse

    import requests as rq
    import base64

    def get_oauth_token():
        url = "https://api.idealista.com/oauth/token"    
        auth = 'aHVvOWlzczJvaXlnd2YzNzVvYzg2ang0cGhkdnlqbWM6MENZdXZsOWN2aWVo'
        headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' ,'Authorization' : 'Basic ' + auth}
        params = urllib.parse.urlencode({'grant_type':'client_credentials'})
        response = rq.post(url,headers = headers, params=params)
        response = response.json()
        access_token= response['access_token']
        return access_token

    def search_api(token, url):  
        headers = {'Content-Type': 'Content-Type: multipart/form-data;', 'Authorization' : 'Bearer ' + token}
        response = rq.post(url, headers = headers)
        response = response.json()
        return response

    locale = 'es' #es means SPAIN
    language = 'es' #es means SPAIN
    country = 'es' #es means SPAIN
    max_items = '50' #i we can't go more than 50 it's in the documentation
    operation = 'sale' 
    center = '40.4167,3.7492' #coordinates for spain 
    distance = '60000'
    propertyType= 'homes'

    #you can also add new parameters here like the above ones



    df_tot = pd.DataFrame()
    limit = 2

    for i in range(1,limit):
        url = ('https://api.idealista.com/3.5/'+country+'/search?operation='+operation+#"&locale="+locale+
               '&maxItems='+max_items+
               '&center='+center+
               '&distance='+distance+
               '&propertyType='+propertyType+
               #add new parameters here liike examples
               '&language='+language)  
        a = search_api(get_oauth_token(), url)
        df = pd.DataFrame.from_dict(a['elementList'])
        df_tot = pd.concat([df_tot,df])

    df_tot = df_tot.reset_index()

    from mysql.connector import (connection)
    from datetime import datetime

    conn = connection.MySQLConnection(user='GxZgyPCemh', password='KsjiYgg7ZJ',
                                     host='remotemysql.com',
                                     database='GxZgyPCemh')
    cur = conn.cursor()
    # cur.execute('SHOW TABLES')
    # print(cur.fetchall())
    # cur.execute('DROP TAB
    cur.execute('CREATE TABLE IF NOT EXISTS SPAIN (id int NOT NULL AUTO_INCREMENT, datetime TEXT, PRIMARY KEY (id),               index1 TEXT, propertyCode TEXT, thumbnail TEXT, externalReference TEXT, numPhotos TEXT,               floor TEXT, price TEXT, propertyType TEXT, operation TEXT, size TEXT, exterior TEXT,               rooms TEXT, bathrooms TEXT, address TEXT, province TEXT, municipality TEXT, district TEXT,               country TEXT, latitude TEXT, longitude TEXT, showAddress TEXT, url TEXT, distance TEXT,               hasVideo TEXT, status TEXT, newDevelopment TEXT, hasLift TEXT, priceByArea TEXT,               detailedType TEXT, suggestedTexts TEXT, hasPlan TEXT, has3DTour TEXT, has360 TEXT,parkingSpace TEXT)')
    for index, row in df_tot.iterrows():

        cur.execute('INSERT INTO SPAIN VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,                                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',                    (None,datetime.now(),                    str(row['index']),str(row['propertyCode']),str(row['thumbnail']),str(row['externalReference']),str(row['numPhotos']),str(row['floor']),                     str(row['price']),str(row['propertyType']),str(row['operation']),str(row['size']),str(row['exterior']),str(row['rooms']),str(row['bathrooms']),                     str(row['address']),str(row['province']),str(row['municipality']),str(row['district']),str(row['country']),str(row['latitude']),str(row['longitude']),                     str(row['showAddress']),str(row['url']),str(row['distance']),str(row['hasVideo']),str(row['status']),str(row['newDevelopment']),                     str(row['hasLift']),str(row['priceByArea']),str(row['detailedType']),str(row['suggestedTexts']),str(row['hasPlan']),str(row['has3DTour']),                     str(row['has360']),str(row['parkingSpace'])))
        conn.commit()

    cur.execute('SELECT * FROM SPAIN')
    conn.close()
    print('success')
except Exception as e:
    print('Exception ', e)

