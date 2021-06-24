#!/usr/bin/python

class mysql_tool:

    def __int__(self,cfg_obj,dbname):
        self.cfg_obj=cfg_obj
        self.re_tablename=re.compile(r"^"+table_name+'\d*$',re.M|re.I)
        self.max_process=4

        return 1
        
    def transfer_list_oracle(self,old_list):
        new_list=[]
        for j in old_list:
            if str(j)=='None' or str(j)=='' or str(j)=='0000-00-00 00:00:00':
                new_list.append('')
            else:
                new_list.append(str(j))
    return new_list

    def transfer_list_mysql(self,old_list):
        new_list=[]
        for j in old_list:
            if str(j)=='None' or str(j)=='' or str(j)=='0000-00-00 00:00:00':
                new_list.append('\N')
            else:
                new_list.append(str(j))
        return new_list

    def connect_mysql(self)
        hash_db_connect={}
        for i in self.cfg_obj[self.dbname]:
            try:
                hash_db_connect[i]=pymysql.connect(username=i[0],password=i[1],host=i[2].port=int(i[3]),database=i[4])
            except pymysql.err.OperationalError as e:
                print('connect fail:'+e)
                sys.exit()
            else:
                print('connect succ '+i)
        return hash_db_connect
    
    def close_mysql(self,connect_obj):
        for cursor in connect_obj:
            connect_obj.close()
        return 1

    def find_download_table(self,field_list,table_name,where_conition):
        hash_download_sql={}
        hash_connect_obj=connect_mysql()
        hash_cursor_obj={}
        for i in hash_connect_obj:
            hash_cursor_obj[i]=hash_connect_obj[i].cursor()
            hash_download_sql[i]={}
        if len(field_list)>0:
            sql_text='select '+','.join(field_list)+' from '+
        else:
            sql_text='select * from ' 
        for i in hash_cursor_obj:
            hash_cursor_obj.execute('show tables')
            for tablename in hash_cursor_obj:
                if self.re_tablename.match(table_name[0]):
                    hash_download_sql[i][tablename]=sql_text+table_name+where_conition

            hash_connect_obj[i].close()        
        return hash_download_sql

    def dwonload_mysql(self,field_list,table_name,where_conition,sql_collect,connect_obj):
        
        



    
    def downlaod_mysql(self,filed_list,tablename,condition):
        



