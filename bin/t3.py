#!/usr/bin/py3
import json
import pymysql
import re

table_name='BILL_OFFER_INST'
test_value='BILL_OFFER_INST0'
regexp_value=re.compile(r'^'+table_name+'\d{0,2}$',re.I)

if regexp_value.search(test_value):
    print('OK')
else:
    print('Not OK')

