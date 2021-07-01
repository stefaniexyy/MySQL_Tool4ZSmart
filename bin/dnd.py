#!/usr/bin/py3
import pymysql
import datetime
import json
import re

file_out=open('../data/DND_'+datetime.datetime.now().strftime('%Y%m%d')+'.csv','w')

with open('../config/conn.json') as db_conn_cfg_file:
    db_conn_json=json.load(db_conn_cfg_file)

set_prod_attr=set()
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^prod_attr_value\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select prod_id from '+table_name[0]+' where attr_id in (400056,400057,400058) and value=\'N\'')
            for prod_attr_value_info in cursor:
                set_prod_attr.add(prod_attr_value_info[0])
    cursor.close()
    conn.close()

set_prod=set()
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^prod\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select prod_id from '+table_name[0]+' where prod_state=\'A\' and indep_prod_id is null')
            for prod_info in cursor:
                if prod_info[0] in set_prod_attr:
                    set_prod.add(prod_info[0])
    cursor.close()
    conn.close()
del set_prod_attr

num=0
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^SUBS\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select SUBS_ID,PREFIX,ACC_NBR from '+table_name[0])     
            for subs_info in cursor:
                if subs_info[0] in set_prod:
                    num+=1
                    file_out.write(str(subs_info[1])+str(subs_info[2])+'\n')  
    cursor.close()
    conn.close()
del set_prod

print('Get record '+str(num))          
file_out.close()