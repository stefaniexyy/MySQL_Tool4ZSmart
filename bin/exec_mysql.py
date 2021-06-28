#!/usr/bin/py3
import json
import os
import time 
from multiprocessing import Process,Pipe
import pymysql
import re
import sys

with open ('../config/conn.json') as db_conn_file:
    conn_info=json.load(db_conn_file)

def transfer_list(old_list):
    new_list=[]
    for j in old_list:
        if str(j)=='None':
            new_list.append('')
        else:
            new_list.append(str(j))
    return new_list

def proc_mysql(p_no,db_type,db_name,db_connect_info_obj,file_path):
    username=db_connect_info_obj[db_type][db_name][0]
    password=db_connect_info_obj[db_type][db_name][1]
    host=db_connect_info_obj[db_type][db_name][2]
    port=int(db_connect_info_obj[db_type][db_name][3])
    database=db_connect_info_obj[db_type][db_name][4]
    try:
        conn_mysql=pymysql.connect(host=host,user=username,passwd=password,database=database,port=port)
    except pymysql.err.OperationalError as e:
        print('connect fail:'+e)
    else:
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        exec_sql=open(file_path,'r')
        exec_num=0
        while 1:
            line=exec_sql.readline()
            if not line:
                break
            line=line.strip()
            arr=line.split('|')
            if arr[1].upper()==db_name.upper():
                try:
                    sql_return=cursor1.execute(arr[0])
                except pymysql.err.DataError:
                    print(arr[0]+" execute fail,DataError.")
                except pymysql.err.ProgrammingError:
                   print(arr[0]+" execute fail,ProgrammingError.") 
                else:
                    exec_num=1+exec_num
            if exec_num%5000==0 and exec_num!=0:
                print('Process '+str(p_no)+' excute '+str(exec_num))
                cursor1.execute('commit')
        print('Process '+str(p_no)+' exec finind,success num:'+str(exec_num)) 
        cursor1.execute('commit')             
        conn_mysql.close()
    return 1

print(os.getpid())
p_no=0
for db in conn_info[sys.argv[1]]:
    print(db)
    p=Process(target=proc_mysql,args=(p_no,sys.argv[1],db,conn_info,sys.argv[2],))
    p.start()
    p_no+=1

