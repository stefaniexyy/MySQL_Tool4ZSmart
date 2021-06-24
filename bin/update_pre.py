#!/usr/bin/py3

import pymysql
import cx_Oracle

con=pymysql.connect(host='10.159.0.17',port=3307,user='abc',passwd='1jian8Shu)',db='abc_0001',charset="utf8")
cur=con.cursor()

con_oracle=cx_Oracle.connect('mig_transfer/smart@10.159.0.35:1521/mtncc')
cur_oracle=con_oracle.cursor()
hash_acct_p={}
cur_oracle.execute('select acct_id from MIG_ACCT_OK_3')
for i in cur_oracle:
     hash_acct_p[i[0]]=1

cur_oracle.close()
con_oracle.close()

sql="select distinct acct_id from rm_txn a"
cur.execute(sql)
acct_id_list=cur.fetchall()
for one_acct_id in acct_id_list:
    #if one_acct_id[0] in hash_acct_p:
    print('Now Process ACCT:'+str(one_acct_id))
    sql="select txn_id,amount,pre_balance,balance from rm_txn a where acct_id = %s and txn_type_id not in (5,4)  and (ref_attr<>\'isDisplay=N\' or ref_attr is null) order by entry_date" % one_acct_id
    cur.execute(sql)
    table_data_list=cur.fetchall()
    prebalance=0
    for one_data in table_data_list:
        txn_id,amount,_x,_y=one_data
        balance=prebalance+amount
        upsql="update rm_txn set pre_balance=%s,balance=%s where txn_id=%s" % (prebalance,balance,txn_id)
        cur.execute(upsql)
        con.commit()
        prebalance=balance
    #sql='select sum(ORI_CHARGE+SETT_CHARGE) from RM_DOC where acct_id=%s and doc_type_id<>2' %(one_acct_id)
    #cur.execute(sql)
    #oustanding_amount=cur.fetchone()[0]
    #if str(oustanding_amount)=='None':
    #    oustanding_amount=0
    #sql='update rm_main_bal set amount='+str(oustanding_amount)+' where acct_id='+str(one_acct_id)
    #try:
    #    cur.execute(sql)
    #except pymysql.err.OperationalError:
    #    print(sql)
    #con.commit()


con.commit()
cur.close()
con.close()
