#!/usr/bin/py3
import json
import os
import time 
from multiprocessing import Process,Pipe
import pymysql
import re

with open ('../config/conn.json') as db_conn_file:
    conn_info=json.load(db_conn_file)

def transfer_list(old_list):
    new_list=[]
    for j in old_list:
        if str(j)=='None' or str(j)=='' or str(j)=='0000-00-00 00:00:00':
            new_list.append('\\N')
        else:
            new_list.append(str(j))
    return new_list

def transfer_list_oracle(old_list):
    new_list=[]
    for j in old_list:
        if str(j)=='None' or str(j)=='' or str(j)=='0000-00-00 00:00:00':
            new_list.append('')
        else:
            new_list.append(str(j))
    return new_list


def proc_mysql(p_no,db_type,db_name,db_connect_info_obj,field_list,table_name):
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
        OUTFILE=open('../data/'+db_name+'.'+table_name+'.unl','w')
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        cursor2=conn_mysql.cursor()
        cursor1.execute('show tables')
        re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '
        else:
            sql_text='select * from ' 
        for table_name in cursor1:
                #print('\r'+'Now downing '+db_name+':'+table_name[0],end="",flush=True)
                if re_tablename.match(table_name[0]):
                    print('Now downing '+db_name+':'+table_name[0],0,p_no)
                    try:
                        cursor2.execute(sql_text+table_name[0])
                    except pymysql.err.OperationalError:
                        print(sql_text+table_name[0])
                    else:
                        for row in cursor2:
                            OUTFILE.write(db_name+'|'+table_name[0]+'|'+'|'.join(transfer_list(row))+'\n')

        OUTFILE.close()
        conn_mysql.close()
    return 1

def proc_mysql4oracle(p_no,db_type,db_name,db_connect_info_obj,field_list,table_name):
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
        OUTFILE=open('../data/'+db_name+'.'+table_name+'.unl','w')
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        cursor2=conn_mysql.cursor()
        cursor1.execute('show tables')
        re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '
        else:
            sql_text='select * from ' 
        for table_name in cursor1:
                #print('\r'+'Now downing '+db_name+':'+table_name[0],end="",flush=True)
                if re_tablename.match(table_name[0]):
                    print('Now downing '+db_name+':'+table_name[0],0,p_no)
                    try:
                        cursor2.execute(sql_text+table_name[0])
                    except pymysql.err.OperationalError:
                        print(sql_text+table_name[0])
                    else:
                        for row in cursor2:
                            OUTFILE.write(db_name+'|'+table_name[0]+'|'+'|'.join(transfer_list_oracle(row))+'|\n')

        OUTFILE.close()
        conn_mysql.close()
    return 1


def proc_mysql_conditin(p_no,db_type,db_name,db_connect_info_obj,field_list,table_name,where_conition):
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
        OUTFILE=open('../data/'+db_name+'.'+table_name+'.unl','w')
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        cursor2=conn_mysql.cursor()
        cursor1.execute('show tables')
        re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '
        else:
            sql_text='select * from ' 
        for table_name in cursor1:
                #print('\r'+'Now downing '+db_name+':'+table_name[0],end="",flush=True)
                if re_tablename.match(table_name[0]):
                    print('Now downing '+db_name+':'+table_name[0],0,p_no)
                    try:
                        cursor2.execute(sql_text+table_name[0]+' '+where_conition)
                    except pymysql.err.OperationalError:
                        print(sql_text+table_name[0]+' '+where_conition)
                    else:
                        for row in cursor2:
                            OUTFILE.write(db_name+'|'+table_name[0]+'|'+'|'.join(transfer_list(row))+'\n')

        OUTFILE.close()
        conn_mysql.close()
    return 1

def proc_mysql4oracle_conditin(split_type,p_no,db_type,db_name,db_connect_info_obj,field_list,table_name,where_conition):
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
        OUTFILE=open('../data/'+db_name+'.'+table_name+'.unl','w')
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        cursor2=conn_mysql.cursor()
        cursor1.execute('show tables')
        re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '
        else:
            sql_text='select * from ' 
        for table_name in cursor1:
                #print('\r'+'Now downing '+db_name+':'+table_name[0],end="",flush=True)
                if re_tablename.match(table_name[0]):
                    print('Now downing '+db_name+':'+table_name[0],0,p_no)
                    try:
                        cursor2.execute(sql_text+table_name[0]+' '+where_conition)
                    except pymysql.err.OperationalError:
                        print(sql_text+table_name[0]+' '+where_conition)
                    except pymysql.err.ProgrammingError:
                        print(sql_text+table_name[0]+' '+where_conition)
                    else:
                        for row in cursor2:
                            OUTFILE.write(db_name+split_type+table_name[0]+split_type+split_type.join(transfer_list_oracle(row))+'|\n')

        OUTFILE.close()
        conn_mysql.close()
    return 1

def proc_my4ora_cdr_conditin(p_no,db_type,db_name,db_connect_info_obj,field_list,table_name,where_conition):
    split_type=';'
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
        OUTFILE=open('../data/'+db_name+'.'+table_name+'.unl','w')
        print(db_name+' connect succ')
        cursor1=conn_mysql.cursor()
        cursor2=conn_mysql.cursor()
        cursor1.execute('show tables')
        re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '
        else:
            sql_text='select * from ' 
        for table_name in cursor1:
                #print('\r'+'Now downing '+db_name+':'+table_name[0],end="",flush=True)
                if re_tablename.match(table_name[0]):
                    print('Now downing '+db_name+':'+table_name[0],0,p_no)
                    try:
                        cursor2.execute(sql_text+table_name[0]+' '+where_conition)
                        print(sql_text+table_name[0]+' '+where_conition)
                    except pymysql.err.OperationalError:
                        print(sql_text+table_name[0]+' '+where_conition)
                    except pymysql.err.ProgrammingError:
                        print(sql_text+table_name[0]+' '+where_conition)
                    else:
                        for row in cursor2:
                            OUTFILE.write(';'.join(transfer_list(row))+'\n')

        OUTFILE.close()
        conn_mysql.close()
    return 1





with open ('../config/download.json') as download_file:
    download_info=json.load(download_file)

for db in download_info:
    p_no=0
    for arr in download_info[db]:
        for db_node in conn_info[db]:
           print(db_node)
           p=Process(target=proc_mysql4oracle_conditin,args=(arr[0],p_no,db,db_node,conn_info,arr[1],arr[2].upper(),arr[3]))
           p.start()
           p_no+=1   