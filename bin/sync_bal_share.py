#!/usr/bin/python
import json
import pymysql
import datetime
import re

with open('../config/conn.json') as db_conn_cfg_file:
    db_conn_json=json.load(db_conn_cfg_file)


log=open('../log/sync_bal_share.log','w')
del_sql_file=open('../data/spc/CELAN_SPC_BILL_BAL_SHARE.unl','w')

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_BAL_SHARE')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_BAL_SHARE\n')
log.close()
log=open('../log/sync_bal_share.log','a')
set_bill_bal_share=set()
hash_bill_bal_dupliate={}
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_BAL_SHARE\d{0,2}$',table_name[0],re.M|re.I):
            #                       0            1      2             3     4           5               6           7           8       9   
            cursor.execute('select BAL_SHARE_ID,SEQ,PROD_INST_ID,PRIORITY,ACCT_ID,ACCT_ITEM_GROUP_ID,CREATE_DATE,UPDATE_DATE,EFF_DATE,EXP_DATE from '+table_name[0]+ ' where UPDATE_DATE is null and EXP_DATE is null')
            for bill_bal_share_info in cursor:
                if (bill_bal_share_info[2],bill_bal_share_info[5],bill_bal_share_info[4]) in set_bill_bal_share:
                    del_sql_file.write('delete from BILL_BAL_SHARE where BAL_SHARE_ID='+str(bill_bal_share_info[0])+'|SPC1|\n')
                else:
                    set_bill_bal_share.add((bill_bal_share_info[2],bill_bal_share_info[5],bill_bal_share_info[4]))
                    hash_bill_bal_dupliate[(bill_bal_share_info[2],bill_bal_share_info[5],bill_bal_share_info[4])]=bill_bal_share_info[0]
    cursor.close()
    conn.close()
        

hash_bal_share={}
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table CC.BAL_SHARE')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table CC.BAL_SHARE\n')
log.close()
log=open('../log/sync_bal_share.log','a')
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BAL_SHARE\d{0,2}$',table_name[0],re.M|re.I):
            #                       0            1        
            cursor.execute('select BAL_SHARE_ID,ACCT_ID from '+table_name[0])
            for bal_share_info in cursor:
                hash_bal_share[bal_share_info[0]]=[bal_share_info[1],table_name[0]]

    cursor.close()
    conn.close()


hash_cc_bal_share_detail={}
del_sql_file_cc=open('../data/spc/CELAN_CC_BAL_SHARE.unl','w')
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table CC.BAL_SHARE_DETAIL')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table CC.BAL_SHARE_DETAIL\n')
log.close()
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BAL_SHARE_DETAIL\d{0,2}$',table_name[0],re.M|re.I):
            #                       0                       1            2        3     4           5      6           7              8             
            cursor.execute('select BAL_SHARE_DETAIL_ID,BAL_SHARE_ID,SUBS_ID,EFF_DATE,EXP_DATE,PRIORITY,CUST_ID,ACCT_ITEM_GROUP_ID,ROUTING_ID from '+table_name[0]+ ' where EXP_DATE is null')
            for bal_share_detail_info in cursor:
                if not (bal_share_detail_info[2],bal_share_detail_info[7],hash_bal_share[bal_share_detail_info[1]][0]) in set_bill_bal_share:
                    if not bal_share_detail_info[1] in hash_cc_bal_share_detail:
                        hash_cc_bal_share_detail[bal_share_detail_info[1]]=[bal_share_detail_info[2],bal_share_detail_info[3],bal_share_detail_info[4],bal_share_detail_info[5],bal_share_detail_info[6],bal_share_detail_info[7],bal_share_detail_info[8],hash_bal_share[bal_share_detail_info[1]][0]]
                    else:
                       del_sql_file_cc.write('delete from BAL_SHARE where BAL_SHARE_ID='+str(bal_share_detail_info[1])+'|CC1|\n')
                       del_sql_file_cc.write('delete from BAL_SHARE_DETAIL where BAL_SHARE_DETAIL_ID='+str(bal_share_detail_info[0])+'|CC1|\n')                                  
                    set_bill_bal_share.add((bal_share_detail_info[2],bal_share_detail_info[7],hash_bal_share[bal_share_detail_info[1]][0]))
                else:
                    if hash_bill_bal_dupliate[(bal_share_detail_info[2],bal_share_detail_info[7],hash_bal_share[bal_share_detail_info[1]][0])] !=bal_share_detail_info[1]:
                        if  hash_bill_bal_dupliate[(bal_share_detail_info[2],bal_share_detail_info[7],hash_bal_share[bal_share_detail_info[1]][0])] in hash_bal_share:  
                            del_sql_file_cc.write('delete from '+hash_bal_share[bal_share_detail_info[1]][1]+' where BAL_SHARE_ID='+str(bal_share_detail_info[1])+'|CC1|\n')
                            del_sql_file_cc.write('delete from '+table_name[0]+' where BAL_SHARE_DETAIL_ID='+str(bal_share_detail_info[0])+'|CC1|\n')
                        else:
                            del_sql_file.write('update BILL_BAL_SHARE set BAL_SHARE_ID='+str(bal_share_detail_info[1])+' where BAL_SHARE_ID='+str(hash_bill_bal_dupliate[(bal_share_detail_info[2],bal_share_detail_info[7],hash_bal_share[bal_share_detail_info[1]][0])])+'|SPC1|\n')

    cursor.close()
    conn.close()
del_sql_file_cc.close()
del_sql_file.close()


"""
   hash_cc_bal_share_detail 
    key:BAL_SHARE_ID
    0:SUBS_ID
    1:EFF_DATE
    2:EXP_DATE
    3:PRIORITY
    4:CUST_ID
    5:ACCT_ITEM_GROUP_ID
    6:ROUTING_ID
    7:ACCT_ID
"""

BILL_BAL_SHARE=open('../data/spc/BILL_BAL_SHARE.unl','w')
for bal_share_id in hash_cc_bal_share_detail:
    if str(hash_cc_bal_share_detail[bal_share_id][2])=='None':
        hash_cc_bal_share_detail[bal_share_id][2]='\\N'
    BILL_BAL_SHARE.write(str(bal_share_id)+'|1|'+str(hash_cc_bal_share_detail[bal_share_id][0])+'|\\N|'+str(hash_cc_bal_share_detail[bal_share_id][3])+'|'+str(hash_cc_bal_share_detail[bal_share_id][7])+'|'+str(hash_cc_bal_share_detail[bal_share_id][5])+'|0|\\N|\\N|\\N|1200|\\N|\\N|0|1|-1|1|\\N|\\N|1|'+str(hash_cc_bal_share_detail[bal_share_id][1])+'|'+str(hash_cc_bal_share_detail[bal_share_id][2])+'|'+str(hash_cc_bal_share_detail[bal_share_id][1])+'|'+str(hash_cc_bal_share_detail[bal_share_id][2])+'|0|'+str(hash_cc_bal_share_detail[bal_share_id][4])+'|'+str(hash_cc_bal_share_detail[bal_share_id][6])+'|\\N\n')

BILL_BAL_SHARE.close()