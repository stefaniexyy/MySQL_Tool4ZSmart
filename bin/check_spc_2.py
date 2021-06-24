#!/usr/bin/py3
import json
import pymysql
import re
import sys
import openpyxl
import datetime

with open('../config/conn.json') as db_conn_cfg_file:
    db_conn_json=json.load(db_conn_cfg_file)

##########################################
log=open('../log/check_spc.log','w')

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for un-terminated subscribers')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for un-terminated subscribers\n')
log.close()
log=open('../log/check_spc.log','a')
set_subs_not_terminated=set()
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^PROD\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select prod_id from '+table_name[0]+' where indep_prod_id is null and prod_state<>\'B\'')
            for subs_id in cursor:
                set_subs_not_terminated.add(subs_id[0])
    cursor.close()
    conn.close()

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table BC_MEMEBR/SUBS_RELA/ACCT')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table BC_MEMEBR/SUBS_RELA/ACCT\n')
log.close()
log=open('../log/check_spc.log','a')

set_subs_need_sync=set()
set_acct_post_paid=set()
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BC_MEMBER\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select SUBS_ID from '+table_name[0])
            for subs_id in cursor:
                if subs_id[0] in set_subs_not_terminated:
                    set_subs_need_sync.add(subs_id[0])
        if re.search(r'^SUBS_RELA\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select subs_id from '+table_name[0]+' where STATE=\'A\'')
            for subs_id in cursor:
                if subs_id[0] in set_subs_not_terminated:
                    set_subs_need_sync.add(subs_id[0])
        if re.search(r'^ACCT\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select acct_id from '+table_name[0]+' where POSTPAID=\'Y\'')
            for acct_id in cursor:
                set_acct_post_paid.add(acct_id[0])
    conn.close()

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SUBS')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SUBS\n')
log.close()
log=open('../log/check_spc.log','a')

hash_subs_info={}#0:SUBS_ID,1:ACCT_ID,2:OFFER_ID,3:SUBS_PLAN_ID,4:UPDATE_DATE,5:CUST_ID,6:ROUTING_ID,7[SUBS_UPP_INST]
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^SUBS\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select subs_id,acct_id,acc_nbr from '+table_name[0])
            for subs_id in cursor:
                if subs_id[1] in set_acct_post_paid and subs_id[0] in set_subs_not_terminated:
                    hash_subs_info[subs_id[0]]=[subs_id[2],subs_id[1]]
                if subs_id[0] in set_subs_need_sync and subs_id[0] in set_subs_not_terminated:
                    hash_subs_info[subs_id[0]]=[subs_id[2],subs_id[1]]
        
del set_subs_need_sync
del set_acct_post_paid
del set_subs_not_terminated


print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for subscriber info')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for subscriber info\n')
log.close()
log=open('../log/check_spc.log','a')

for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^PROD\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select PROD_ID,OFFER_ID,SUBS_PLAN_ID,UPDATE_DATE,CUST_ID,ROUTING_ID,PROD_STATE from '+table_name[0]+' where indep_prod_id is null and prod_state<>\'B\'')
            for prod_info in cursor:
                if prod_info[0] in hash_subs_info:
                    hash_subs_info[prod_info[0]].append(prod_info[1])#2 OFFER_ID
                    hash_subs_info[prod_info[0]].append(prod_info[2])#3 SUBS_PLAN_ID
                    hash_subs_info[prod_info[0]].append(prod_info[3])#4 UPDATE_DATE
                    hash_subs_info[prod_info[0]].append(prod_info[4])#5 CUST_ID
                    hash_subs_info[prod_info[0]].append(prod_info[5])#6 ROUTINF_ID
    cursor.close()
    conn.close()

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for supplyment offer')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table PROD for supplyment offer\n')
log.close()
log=open('../log/check_spc.log','a')




print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_PROD_INST_FUNC')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_PROD_INST_FUNC')
log.close()
log=open('../log/check_spc.log','a')

set_spc_sub_info_func=set()
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_PROD_INST_FUNC\d{0,2}$',table_name[0],re.M|re.I):
            cursor.execute('select PROD_INST_ID,PROD_ID from '+table_name[0])
            for bill_prod_inst_info in cursor:
                set_spc_sub_info_func.add((bill_prod_inst_info[0],bill_prod_inst_info[1]))

    cursor.close()
    conn.close()


BILL_PROD_INST_FUNC=open('../data/spc/BILL_PROD_INST_FUNC.unl','w')
set_supplyment_inst=set()
num_bill_prod_inst_func_lost=0
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^PROD\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select PROD_ID,OFFER_ID,UPDATE_DATE,CUST_ID,ROUTING_ID,INDEP_PROD_ID from '+table_name[0]+' where indep_prod_id is not null and state=\'A\'')
            for prod_info in cursor:
                if prod_info[5] in hash_subs_info :
                    #hash_prod_supplyment_inst[prod_info[0]]=[prod_info[1],prod_info[2],prod_info[3],prod_info[4],prod_info[5]]
                    set_supplyment_inst.add(prod_info[0])
                    if not (prod_info[0],prod_info[1]) in set_spc_sub_info_func:
                        num_bill_prod_inst_func_lost+=1
                        BILL_PROD_INST_FUNC.write(str(prod_info[0])+'|1|'+str(prod_info[1])+'|'+str(prod_info[5])+'|'+str(prod_info[2])+'|100000|'+str(prod_info[2])+'|N|'+str(prod_info[2])+'|\\N|'+str(prod_info[2])+'|\\N|0|'+str(prod_info[3])+'|'+str(prod_info[4])+'\n')
    cursor.close()
    conn.close()
del set_spc_sub_info_func
BILL_PROD_INST_FUNC.close()


print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Find '+str(num_bill_prod_inst_func_lost)+' records lost')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Find '+str(num_bill_prod_inst_func_lost)+' records lost')
log.close()
log=open('../log/check_spc.log','a')

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from SPC.BILL_PROD_INST_ATTR')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from SPC.BILL_PROD_INST_ATTR\n')
log.close()
log=open('../log/check_spc.log','a')

set_spc_bill_prod_inst_attr=set()
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_PROD_INST_ATTR\d{0,2}$',table_name[0],re.M|re.I):
            cursor.execute('select PROD_INST_ID,ATTR_ID from '+table_name[0])
            for prod_attr_id in cursor:
                set_spc_bill_prod_inst_attr.add((prod_attr_id[0],prod_attr_id[1]))
    cursor.close()
    conn.close()

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'] TEST')
BILL_PROD_INST_ATTR=open('../data/spc/bill_prod_inst_attr.csv','w')
num_bill_prod_inst_attr=0
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^PROD_ATTR_VALUE\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select prod_id,attr_id,value,eff_date,routing_id,cust_id from '+table_name[0])
            for prod_attr_info in cursor:
                if prod_attr_info[0] in hash_subs_info:
                    if not (prod_attr_info[0],prod_attr_info[1]) in set_spc_bill_prod_inst_attr:
                        num_bill_prod_inst_attr+=1
                        BILL_PROD_INST_ATTR.write(str(prod_attr_info[0])+';'+str(prod_attr_info[1])+';1;'+str(prod_attr_info[2])+';'+str(prod_attr_info[3])+';\\N;'+str(prod_attr_info[3])+';\\N;0;'+str(prod_attr_info[5])+';'+str(prod_attr_info[4])+'\n')

    cursor.close()
    conn.close()
BILL_PROD_INST_ATTR.close()
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Find '+str(num_bill_prod_inst_attr)+' records lost')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Find '+str(num_bill_prod_inst_attr)+' records lost')
log.close()
log=open('../log/check_spc.log','a')
###########################################################################################################################################
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from SPC.BILL_OFFER_INST')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from SPC.BILL_OFFER_INST\n')
log.close()
log=open('../log/check_spc.log','a')

set_spc_bill_offer_inst=set()
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_OFFER_INST\d{0,2}$',table_name[0],re.M|re.I):
            cursor.execute('select OFFER_INST_ID from '+table_name[0])
            for offer_inst in cursor:
                set_spc_bill_offer_inst.add(offer_inst[0])
    cursor.close()
    conn.close()


num_bill_offer_inst_lost=0
hash_subs_upp_inst_available={}
BILL_OFFER_INST=open('../data/spc/BILL_OFFER_INST.unl','w')
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^SUBS_UPP_INST\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select SUBS_UPP_INST_ID,SUBS_ID,PRICE_PLAN_ID,EFF_DATE,ROUTING_ID,CUST_ID,SPC_UPP_INST_RELA_ID from '+table_name[0]+' where STATE=\'A\'')
            for subs_upp_inst_info in cursor:
                if subs_upp_inst_info[1] in hash_subs_info:
                    hash_subs_upp_inst_available[subs_upp_inst_info[0]]=subs_upp_inst_info[1]
                    if not subs_upp_inst_info[0] in set_spc_bill_offer_inst:
                        num_bill_offer_inst_lost+=1
                        BILL_OFFER_INST.write(str(subs_upp_inst_info[0])+'|1|'+str(subs_upp_inst_info[1])+'|'+str(subs_upp_inst_info[2])+'|100000|'+str(subs_upp_inst_info[3])+'|N|1000|'+str(subs_upp_inst_info[3])+'|\\N|'+str(subs_upp_inst_info[3])+'|'+str(subs_upp_inst_info[3])+'|\\N|0|'+str(subs_upp_inst_info[5])+'|'+str(subs_upp_inst_info[4])+'\n')
    cursor.close()
    conn.close()


del set_spc_bill_offer_inst
BILL_OFFER_INST.close()

print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SUBS_UPP_INST_VALUE')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SUBS_UPP_INST_VALUE\n')
log.close()
log=open('../log/check_spc.log','a')


print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_PROD_INST')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_PROD_INST\n')
log.close()
log=open('../log/check_spc.log','a')

set_spc_sub_info=set()
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_PROD_INST\d{0,2}$',table_name[0],re.M|re.I):
            cursor.execute('select PROD_INST_ID,PROD_ID,ACC_NUM from '+table_name[0])
            for bill_prod_inst_info in cursor:
                set_spc_sub_info.add(bill_prod_inst_info[0])

    cursor.close()
    conn.close()


print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now Compare table SPC.BILL_PROD_INST')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now Compare table SPC.BILL_PROD_INST\n')
log.close()
log=open('../log/check_spc.log','a')

BILL_PROD_INST=open('../data/spc/BILL_PROD_INST.unl','w')
num_bill_prod_inst_lost=0
for subs_id in hash_subs_info:
    if not subs_id in set_spc_sub_info:
        num_bill_prod_inst_lost+=1
        BILL_PROD_INST.write(str(subs_id)+'|1|'+str(hash_subs_info[subs_id][2])+'|250|'+str(hash_subs_info[subs_id][1])+'|1200|'+str(hash_subs_info[subs_id][5])+'|'+str(hash_subs_info[subs_id][4])+'|'+str(hash_subs_info[subs_id][4])+'|'+str(hash_subs_info[subs_id][4])+'|\\N|\\N|100000|'+str(hash_subs_info[subs_id][4])+'|'+str(hash_subs_info[subs_id][4])+'|\\N|'+str(hash_subs_info[subs_id][4])+'|\\N|0|\\N|'+str(hash_subs_info[subs_id][6])+'|\\N|'+str(hash_subs_info[subs_id][3])+'\n')
del set_spc_sub_info
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Compare finish ,find '+str(num_bill_prod_inst_lost)+' record lost')
BILL_PROD_INST.close()



print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_OFFER_INST_ATTR')
log.write('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Now collect info from table SPC.BILL_OFFER_INST_ATTR\n')
log.close()
log=open('../log/check_spc.log','a')
set_spc_bill_offer_inst_attr=set()
for sharding_db in db_conn_json['SPC']:
    conn=pymysql.connect(host=db_conn_json['SPC'][sharding_db][2],user=db_conn_json['SPC'][sharding_db][0],passwd=db_conn_json['SPC'][sharding_db][1],database=db_conn_json['SPC'][sharding_db][4],port=db_conn_json['SPC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^BILL_OFFER_INST_ATTR\d{0,2}$',table_name[0],re.M|re.I):
            cursor.execute('select OFFER_INST_ID,ATTR_ID from '+table_name[0])
            for bill_offer_inst_attr_info in cursor:
                set_spc_bill_offer_inst_attr.add((bill_offer_inst_attr_info[0],bill_offer_inst_attr_info[1]))

    cursor.close()
BILL_OFFER_INST_ATTR=open('../data/spc/BILL_OFFER_INST_ATTR.csv','w')
sum_bill_offer_inst_attr_lost=0
for sharding_db in db_conn_json['CC']:
    conn=pymysql.connect(host=db_conn_json['CC'][sharding_db][2],user=db_conn_json['CC'][sharding_db][0],passwd=db_conn_json['CC'][sharding_db][1],database=db_conn_json['CC'][sharding_db][4],port=db_conn_json['CC'][sharding_db][3])
    cursor=conn.cursor()
    cursor.execute('show tables')
    table_list=cursor.fetchall()
    for table_name in table_list:
        if re.search(r'^SUBS_UPP_INST_VALUE\d{1,2}$',table_name[0],re.M|re.I):
            cursor.execute('select SUBS_UPP_INST_ID,ATTR_ID,VALUE,EFF_DATE,ROUTING_ID,CUST_ID from '+table_name[0]+' where STATE=\'A\'')
            for subs_upp_inst_value_info in cursor:
                if subs_upp_inst_value_info[0] in hash_subs_upp_inst_available  and not (subs_upp_inst_value_info[0],subs_upp_inst_value_info[1]) in set_spc_bill_offer_inst_attr:
                    sum_bill_offer_inst_attr_lost+=1
                    BILL_OFFER_INST_ATTR.write(str(subs_upp_inst_value_info[0])+';'+str(subs_upp_inst_value_info[1])+';1;'+str(hash_subs_upp_inst_available[subs_upp_inst_value_info[0]])+';'+str(subs_upp_inst_value_info[2])+';100000;'+str(subs_upp_inst_value_info[3])+';\\N;'+str(subs_upp_inst_value_info[3])+';\\N;0;'+str(subs_upp_inst_value_info[5])+';'+str(subs_upp_inst_value_info[4])+'\n')
    cursor.close()
    conn.close()
BILL_OFFER_INST_ATTR.close()
print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']Compare finish ,find '+str(sum_bill_offer_inst_attr_lost)+' record lost')
BILL_PROD_INST.close()




