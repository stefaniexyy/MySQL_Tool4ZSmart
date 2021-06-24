#!/usr/bin/py3
import sys
import json

with open ('../data/test.json') as test_json:
    test_obj=json.load(test_json)


for i in test_obj['prodInstAcctRelHis']:
    print(i['pk'])



    mysql -uspc -p1jian8Shu\) -h 10.159.0.110 -P 3308