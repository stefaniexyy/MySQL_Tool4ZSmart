#!/usr/bin/python
import re
import pymysql
import cx_Oracle

conn_oracle=cx_Oracle.connect('mig_transfer/smart@10.159.0.35:1521/mtncc')
cur_ora=conn_oracle.cursor()
conn_mysql=pymysql.connect(host='10.159.0.112',port=3308,user='inv',passwd='1jian8Shu)',db='inv_0001',charset="utf8")
cur_mysql=conn_mysql.cursor()
cur_ora.execute('select * from FIX_REPLACE_ACCT_NAME where ACCT_ID=413317')
for i in cur_ora:
    acct_id=i[0]
    replaced_name=i[2]
    cur_mysql.execute('select XML_FILE from bill_data_632 where ACCT_ID='+str(acct_id)+' and XML_FILE is not null')
    xml_content=cur_mysql.fetchone()[0]
    replaced_name=re.sub('&','&amp;',replaced_name)
    xml_content=re.sub('<CUST_NAME>.*\<\/CUST_NAME>','<CUST_NAME>'+replaced_name+'</CUST_NAME>',xml_content)
    sql="update bill_data_632 set XML_FILE=%s where ACCT_ID=%s"
    cur_mysql.execute(sql,[xml_content,acct_id])
    cur_mysql.execute('commit')
    print(xml_content)
conn_mysql.close()
cur_mysql.close()
    #sql="select * from userinfo where name=%s and pwd=%s" #！！！注意%s需要去掉引号，因为pymysql会自动为我们加上
    #result=cursor.execute(sql,[user,pwd])