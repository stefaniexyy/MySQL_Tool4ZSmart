#!/usr/bin/py3
import re
import pymysql
from multiprocessing  import Queue,Process,JoinableQueue
import json
import os
import sys


def split_file(file_path,split_file_num):
    file_num=0
    file=open(file_path,'r')
    print('File is:'+file_path)
    #file_name=re.search(r'\/([^\/]+)$',file,re.M|re.I).group(1)
    hash_output={}
    for i in range(0,split_file_num):
        hash_output[i]=open(file_path+'.tmp.'+str(i),'w')
    while 1:
        line=file.readline()
        if not line:
            break
        line=line.strip()
        file_num=file_num+1
        hash_output[file_num%split_file_num].write(line+'\n')
    for j in hash_output:
        hash_output[j].close()

    return file_num

def check_file_num(file_path):
    num=0
    file=open(file_path,'r')
    for i in file:
        line=file.readline()
        if not line:
            break
        line+=1
    return line

def check_table_num(table_name,db_conn_obj):
    cursor=db_conn_obj.cursor()
    cursor.execute('select count(1) from '+table_name)
    result=cursor.fetchone()[0]
    cursor.close()
    return result        
        
def load_file_mysql(Delimiter,table_name,file_path_a,db_info):
    if os.path.getsize(file_path_a)>0:
        try:
            mysql_conn=pymysql.connect(host=db_info[2],user=db_info[0],passwd=db_info[1],database=db_info[4],port=db_info[3],local_infile=1)
        except pymysql.err.OperationalError as e:
            print(e)
            return_code=0
        else:
            cursor=mysql_conn.cursor()
            sql="load data LOCAL infile \'"+file_path_a+"\' into table "+table_name+" fields terminated by \'"+Delimiter+"\' LINES TERMINATED BY '\n';"
            sql_feedback=cursor.execute(sql)
            cursor.execute('commit')
            cursor.close()
            return_code=1
    else:
        sql_feedback=-1
    return sql_feedback

def load_file_mysql_cdr(table_name,file_path_a,db_info):
    if os.path.getsize(file_path_a)>0:
        try:
            mysql_conn=pymysql.connect(host=db_info[2],user=db_info[0],passwd=db_info[1],database=db_info[4],port=db_info[3],local_infile=1)
        except pymysql.err.OperationalError as e:
            print(e)
            return_code=0
        else:
            cursor=mysql_conn.cursor()
            sql="load data LOCAL infile \'"+file_path_a+"\' into table "+table_name+" fields terminated by ';' LINES TERMINATED BY '\n';"
            sql_feedback=cursor.execute(sql)
            cursor.execute('commit')
            cursor.close()
            return_code=1
    else:
        sql_feedback=-1
    return sql_feedback


#print(split_file('/soft/rec/rec/data/input/cas_info/V9E_SUBS.unl',8))
with open ('../config/conn.json') as db_conn_file:
    conn_info=json.load(db_conn_file)


def load_mysql_sharding(delimiter,routing_id,routing_location,sharding_value,sharding_location,table_name,file_path,dbtype):
    f=open(file_path,'r')
    hash_f_out={}
    for f_routing_num in range(0,routing_id+1):
        hash_f_out[f_routing_num]={}
        for f_sharding_id in range(0,sharding_value):
            hash_f_out[f_routing_num][f_sharding_id]=[open(file_path+'.'+str(f_routing_num)+'.'+str(f_sharding_id),'w'),file_path+'.'+str(f_routing_num)+'.'+str(f_sharding_id)]

    while 1:
        f_line=f.readline()
        if not f_line:
            break
        arr_f=f_line.split('|')
        sharding_id=int(arr_f[sharding_location])%sharding_value
        hash_f_out[int(arr_f[routing_location])][sharding_id][0].write(f_line)

    for i in hash_f_out:
        for j in hash_f_out[i]:
            hash_f_out[i][j][0].close()


    for f_routing_num in range(0,routing_id):
       db_conn=conn_info[dbtype][dbtype+str(f_routing_num+1)]
       for f_sharding_id in range(0,sharding_value):
           print('Loading :'+str(f_routing_num)+','+str(f_sharding_id))
           load_file_mysql(delimiter ,table_name+str(f_sharding_id),file_path+'.'+str(f_routing_num)+'.'+str(f_sharding_id),db_conn)
    for f_routing_num in range(0,routing_id+1):
        for f_sharding_id in range(0,sharding_value):
            os.remove(file_path+'.'+str(f_routing_num)+'.'+str(f_sharding_id))
    return 1
        
#load_mysql_sharding(4,27,32,28,'SPC_BAL_SHARE','/soft/rec/xyy/data/FIX_SPC_BAL_SHARE.unl','CC')

with open ('../config/load.json') as load_info_json:
    load_info_obj=json.load(load_info_json)

for db in load_info_obj:
    print(db)
    for arr in load_info_obj[db]:
        if arr[0]=='normal':
            my_conn_info=conn_info[db][arr[1]]
            load_file_mysql(arr[2],arr[3],arr[4],my_conn_info)
        elif arr[0]=='sharding':
            load_mysql_sharding(arr[1],arr[2],arr[3],arr[4],arr[5],arr[6],arr[7],db)


#
#my_sql_conn=conn_info['SPC']['SPC1']
#load_file_mysql('BILL_ACCT','/soft/rec/xyy/data/FIX_BILL_ACCT.unl',my_sql_conn)
#
#

#
#my_sql_conn=conn_info['SIC']['SIC1']
#load_file_mysql('IC_ACC_NBR_IDENTIFY','/soft/rec/xyy/data/FIX_IC_ACC_NBR_IDENTIFY_1.unl',my_sql_conn)
#
#my_sql_conn=conn_info['SIC']['SIC2']
#load_file_mysql('IC_ACC_NBR_IDENTIFY','/soft/rec/xyy/data/FIX_IC_ACC_NBR_IDENTIFY_2.unl',my_sql_conn)
#
#my_sql_conn=conn_info['SIC']['SIC3']
#load_file_mysql('IC_ACC_NBR_IDENTIFY','/soft/rec/xyy/data/FIX_IC_ACC_NBR_IDENTIFY_3.unl',my_sql_conn)
#
#my_sql_conn=conn_info['RBC']['RBC4']
#load_file_mysql_cdr('EVENT_USAGE_C_633','/soft/rec/xyy/data/RBC4.EVENT_USAGE_C_632.unl',my_sql_conn)

#
#

