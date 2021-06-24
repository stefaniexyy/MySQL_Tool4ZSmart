#!/usr/bin/python
import pymysql

con=pymysql.connect(host='10.159.0.17',port=3307,user='abc',passwd='1jian8Shu)',db='abc_0001',charset="utf8")
cur=con.cursor()
cur_2=con.cursor()
cur.execute('select pre_balance+recv_charge+adjust_charge+due,acct_id from BILL where BILL_TYPE=42 and BILLING_CYCLE_ID=632' )
arr_result_set=cur.fetchall()
for result in arr_result_set:
    cur.execute('select pre_balance from bill where BILL_TYPE=42 and BILLING_CYCLE_ID=633 and acct_id='+str(result[1]))
    try:
        if result[0]!=cur.fetchone()[0]:
            cur_2.execute('UPDATE BILL set PRE_BALANCE='+str(result[0])+' where acct_id='+str(result[1])+' and billing_cycle_id=633 and bill_type=42')
            cur_2.execute('commit')
    except TypeError:
        continue

cur.close()
cur_2.close()
con.close()