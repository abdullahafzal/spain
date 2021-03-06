while True:
    try:
        import requests
        import json
        import time
        headers = {
            'Content-Type': 'application/json',
            'X-Api-Key': 'i1aIuZ5mV6HT24piafq1RBGAt8ckxkpcpkruvWir',
        }

        raw_data={
        "latitude": 40.3973892,
        "longitude": -3.6421778,
        "operation": 1,
        "typology": 1,
        "subtypology":"",
        "area":100,
        "areamargin":"1",
        "agesince":1970,
        "ageuntil":"",
        "roomnumber":3,
        "bathnumber":"",
        "withcadastralreferenceonly":"",
        "distance":1000
        }
        response = requests.post("https://www.idealista.com/data/ws/witnesses", headers=headers, data=json.dumps(raw_data))
        list = json.loads(response.text)



        hostname = 'localhost'
        username = 'my_user'
        password = '123789456'
        database = 'spain_db'

        # Simple routine to run a query on a database and print the results:
        def doQuery( conn ) :
            cur = conn.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS spain_data(unique_key serial not null primary key, id TEXT, ad_id TEXT, ad_state TEXT, ad_operation TEXT, \
                        ad_typology TEXT, ad_propertytype TEXT, ad_price TEXT, ad_unitprice TEXT, ad_activationdate TEXT, ad_deactivationdate TEXT,\
                        ad_modificationdate TEXT, ad_latitude TEXT, ad_longitude TEXT, ad_layerlocation TEXT, ad_province TEXT, ad_town TEXT,\
                        ad_postalcode TEXT, ad_streettype TEXT, ad_streetname TEXT, ad_streetnumber TEXT, ad_floornumber TEXT,\
                        ad_addressvisible TEXT, ad_subtypology TEXT, ad_area TEXT, ad_builttype TEXT, ad_roomnumber TEXT, ad_bathnumber TEXT,\
                        ad_flatlocation TEXT, ad_haslift TEXT, ad_hasparkingspace TEXT, ad_hasboxroom TEXT, ad_hasswimmingpool TEXT, \
                        ad_urlactive TEXT, ad_urlinactive TEXT, newdevelopment_commercialname TEXT, newdevelopment_fromprice TEXT, \
                        newdevelopment_averageprice TEXT, adstats_visits TEXT, adstats_sendtofriend TEXT, adstats_savedasfavorite TEXT,\
                        adstats_daysonmarket TEXT, adstats_contactsbyemail TEXT, estate_cadastralroot TEXT, property_cadastralreference TEXT,\
                        property_builtdate TEXT, property_area TEXT, property_use TEXT, censustract_id TEXT, censustract_town_id TEXT,\
                        layerlocation_area TEXT, layerlocation_population TEXT, address_certainty TEXT, cadastral_certainty TEXT, distance TEXT)')
            conn.commit()
            
            for l in list:
                key=cur.execute('SELECT unique_key FROM spain_data ORDER BY unique_key DESC LIMIT 1') # Last key 
                key = cur.fetchall()
                print('Getting the last key from last_key: ',key,type(key))
                if key ==[]:
                    key=[(0,'')]
                    print('first time ',e)
                key = key[0][0]
                key+=1
                cur.execute("INSERT INTO spain_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",\
                            (key,l['id'], l['ad_id'], l['ad_state'], l['ad_operation'], l['ad_typology'], l['ad_propertytype'], l['ad_price'], l['ad_unitprice'], l['ad_activationdate'], 
                             l['ad_deactivationdate'], l['ad_modificationdate'], l['ad_latitude'], l['ad_longitude'], l['ad_layerlocation'], l['ad_province'], l['ad_town'], 
                             l['ad_postalcode'], l['ad_streettype'], l['ad_streetname'], l['ad_streetnumber'], l['ad_floornumber'], l['ad_addressvisible'], 
                             l['ad_subtypology'], l['ad_area'], l['ad_builttype'], l['ad_roomnumber'], l['ad_bathnumber'], l['ad_flatlocation'], l['ad_haslift'], 
                             l['ad_hasparkingspace'], l['ad_hasboxroom'], l['ad_hasswimmingpool'], l['ad_urlactive'], l['ad_urlinactive'],
                             l['newdevelopment_commercialname'], l['newdevelopment_fromprice'], l['newdevelopment_averageprice'], l['adstats_visits'], 
                             l['adstats_sendtofriend'], l['adstats_savedasfavorite'], l['adstats_daysonmarket'], l['adstats_contactsbyemail'], l['estate_cadastralroot'],
                             l['property_cadastralreference'], l['property_builtdate'], l['property_area'], l['property_use'], l['censustract_id'], l['censustract_town_id'],
                             l['layerlocation_area'], l['layerlocation_population'], l['address_certainty'], l['cadastral_certainty'], l['distance']))
                conn.commit()
                print('commited')

        import psycopg2
        myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
        print('connection sucessfull')
        doQuery( myConnection )
        myConnection.close()
    except Exception as e:
        print('exception ',e)

    time.sleep(43200)
