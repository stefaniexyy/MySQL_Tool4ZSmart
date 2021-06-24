免责申明
=================
任何使用本工具包造成的现网数据问题和数据库问题概不负责  
本工具包使用python3 perl hash编写，建议由一定python基础的人执行  
需求环境安装zlib 
需求环境安装python3.9及以下类库  
1) cx_Oracle
2) pymysql
3) json
4) pdfminer(选装)  

需求环境安装perl 5.010及其以上版本

推荐使用VS CODE编辑json配置文件

使用说明
=================
基本流程
-----------------
考虑导是现网业务
1) 所有的删除操作都必须转换为单条sql，删除条件必须只对应唯一的一条数据
2) 所有更新操作必须转化为单挑sql，更新条件必须只对应唯一的一条数据
3) 批量插入采用mysql loadfile的形式，插入的数据保存在"|"为分隔符的文本文件

基本逻辑
-----------------
1) 使用donwload_mysql.py 导出需要修改的表和关联的表
2) 把导出的文件使用load.sh导入到oracle
3) 生成更新，删除，插入的数据
4) 使用unload.sh导出生成的数据
5) 使用工具更新mysql

脚本说明
=================
|--sqluldr2_linux64_10204.bin  *用于下载oracle数据二进制文件，不要动*   
|--Unload.sh  **用于下载oracle数据的shell脚本**    
|--__pycache__  
|--ora2mysql.pl  **把oracle格式的文本文件转换为mysql入库格式的perl脚本**  
|--test.py  
|--lib  
|--Load.sh  **把文本文件导入oracle数据库**  
~~|--Analysis_pdf.py  **把pdf转换为文本文件**~~  
|--upd_bill_pre.py     
~~|--update_xml_mysql.py   **修改出帐账单下xml**~~    
|--donwload_mysql.py   **修改出帐账单下xml**  
|--update_pre.py   **更新rm_txn表的pre_balance**  
|--check_spc_2.py   **检查用户是否同步到spc**  
|--sync_bal_share.py  **检查代付关系是否同步到spc**  
|--exec_mysql.py  **在mysql批量执行sql语句**  
|--load_mysql.py   **把文本文件导入mysql**  

donwload_mysql.py
=================
执行命令:  
  py3 donwload_mysql.py

配置文件格式   
1) conn.json   
2) download.json

__download.json__
```json
{
    "CRMHOT":[
        ["|",[],"order_item"," "],
        ["|",[],"cust_order"," "],
        ["|",[],"cust_contact"," "]

    ],
    "CC":[
        ["|",["SUBS_ID","ACC_NBR"],"SUBS"," where ACC_NBR=1"]  
    ]
}
```
外层key对应conn.json中配置mysql逻辑数据库名字
内存是一个二维数组  
其中["|",[],"order_item"," "]
* 域1 导出文件的分隔符
* 域2 []内写需要导出的字段名字，如果需要导出全表，则不需要写
* 域3 导出的表名
* 域4 where条件 注意where前面加一个空格 __不支持多表关联__

导出的文件在../data目录下
文件命规则分片库名字.表名.unl  
例如SUBS表，存在四个分片导出的文件在data目录下存在4个文件  
* CC1.SUBS.unl
* CC2.SUBS.unl
* CC3.SUBS.unl
* CC4.SUBS.unl

#### 注意!!
无论是否下载全表，下载的文件总是会在开头包含2个字段  
* 分片库名字
* 表名
例如    CRMHOT1.CUST_CONTACT.unl  
CRMHOT1|cust_contact|113803788|10323275||A|5|2021-05-25 09:57:50|A|F|685244||||2105000113105487||0||257|0|  
文件结尾也是有分隔符的  
***

Load.sh
=================
把文本文件导入oracle  
__执行命令__
__./Load.cfg load.cfg /__  
命令包含两个参数  
* 参数1   配置文件名字
* 参数2  入库文件位置 /表示在../data目录下 
   
需要配置文件load.cfg 位于../config/目录下 配置文件可以叫其他名字 取决于参数1 

```cfg
#the file for load.

#three DB: memdb oracle informix
[dbtype]
oracle

#if memdb ,write memdb
[dbname]
username/passwd@IP:port/tns

# max process
[max_process_count]
5

#the number of cpu
[cpuno]
5

#expdb mode
[date only mode]
N

[tablelist]
src_order_item|CRMHOT1.ORDER_ITEM.unl|
src_cust_order|CRMHOT1.CUST_ORDER.unl|
src_cust_contact|CRMHOT1.CUST_CONTACT.unl|
[END]

```
入库失败日志在 ../log/目录下  
入库入库失败日志文件格式名字load_入库文件名.log  
如果出现入库失败请检查入库日志

Unload.sh
=================
导出oracle数据库的数据
__执行命令__  
__./Unload.sh__  

需要配置文件unload.cfg
```cfg
#support 3 kind of db:1:memdb 2:oracle 3:informix
[dbtype]
oracle

#If it is memdb, just write memdb
[dbname]
mig_transfer/smart@10.159.0.35:1521/mtncc

[tablelist]
FIX_cust_contact2|select *from FIX_cust_contact|
[END]

```

如果是全表导出 select *from *要和from写在一起
如果是选择导出部分数据 可以写 select column1,column2,column3 from table1 where column=xxx  

导出文件在../data目录下  
.unl结尾，文件名取决于[tablelist]下配置的第一个域  

ora2mysql.pl
=================
把oracle格式的入库文件转换为mysql格式的入库文件  
__执行命令__  
__perl ora2mysql.pl xxx.tmp__  

**需要传参 参数必须是xxx.tmp 会在../data/目录下生成同名的xxx.unl文件**
**使用Unload.sh下载的数据，需要先在../data/目录下把下载的文件没改成tmp,然后使用这个oracle2mysql.pl转换格式** 

#### 注意！！
**如果使用load_mysql.py脚本导入数据，必须检查文本文件是否符合mysql格式**  
**即对于空字段为\N，结尾没有分隔符**  
**如果强行把oracle格式的文本文件导入mysql会造成在代理上查不到同时在物理库存在的情况**  
**这个时候就需要调用exec_mysql.py去清理掉这些异常数据**

load_mysql.py
=================
把mysql格式的文本导入mysql数据库
__执行命令__  
__py3 load_mysql.py__  
导入数据之前先确定是单片表还是分片表  
去代理商查一下zddas_sharding_rule表  
如果是单片表 生成的文本文件需要是每个库一个文件  
如果是分片表，只需要一个表生成一个文件  

举例：      
SUBS_IDENTIFY表，在每个库上是单片表，那么需要根据zdaas_sharding_rule里面的规则，abs(mod(hash_code(identify_value),4)) 生成4个文件，然后用load_mysql.py导入数据库  
SUBS表，是分片表，那么只需要生成一个文件，在zdaas_sharding_rule上查到routing_id和sharding_id字段位置 用load_mysql.py导入数据  

__需要配置文件load.json__
```json
{
    "CRMHOT":[
        ["normal","CRMHOT1","|","cust_contact","/soft/rec/xyy/data/FIX_cust_contact2.unl"]
    ],
    "CC":[
        ["sharding","|",4,27,32,28,"SPC_BAL_SHARE","/soft/rec/xyy/data/FIX_SPC_BAL_SHARE.unl"]
    ]
}
```

json格式  
如果是单片表  
["normal","CRMHOT1","|","cust_contact","/soft/rec/xyy/data/FIX_cust_contact2.unl"]
* 第一个域 normal表示单片表 
* 第二个域 分隔符，如果是普通表写"|", 如果是attr表“;"
* 第三个域 导入的表名
* 第四个域 文件位置
  如果是分片表
["sharding","|",4,27,32,28,"SPC_BAL_SHARE","/soft/rec/xyy/data/FIX_SPC_BAL_SHARE.unl"]  
* 第一个域 sharding表示分片表
* 第二个域 "|" 文件的分隔符
* 第三个域 routing对于的分库数量
* 第四个域 routing字段对应的位置，从0开始
* 第五个域 sharding分片数据量，一般是32
* 第六个域 sharding字段所在的位置 从0开始
* 第七个域 对应逻辑表的表名
* 第八个域 入库文件所在域

__警告!!__
导入数据库的时候需要先确定1
1) 文本文件是MySQL格式的
2) pk没用冲突
3) 导入后需要检查是否能在代理上查到数据


exc_mysql.py
=================
批量执行sql语句  
__执行命令__  
__py3 exc_mysql.py CC ../data/FIX_SCP_SQL.unl__ 
* 参数一 执行SQL的逻辑库名
* 参数二 SQL文件路径
  
其中SQL文件格式如下:  
sql语句|对应物理库名字|
例如
delete from bal_share_detail where BAL_SHARE_DETAIL_ID=58396|CC1|
sql语句不需要";"结尾  
表名需要是真实的物理表名  
download_mysql.py之所以回导出物理库名字和物理表名，就是在这边使用的  

需要配置文件conn.cfg  


常见操作
=================
## 批量补数据
* 如果确认漏数据，确定需要补数据的范围和数据来源的表
* 使用download_mysql.py下载需要的表，为了节约时间和磁盘空间可以只下载部分字段和部分需要数据
* 在oracle根据mysql的表结构建立表，一般表名字为SRC_mysql逻辑表名 注意表结构需要在头部增加DB VARCHAr2(64) 和 TAB varchar2(64) 两列  例如
```sql
create table SRC_ACCT
(
  db                         VARCHAR2(32),
  tab                        VARCHAR2(32),
  acct_id                    NUMBER(12) not null,
  std_addr_id                NUMBER(15),
  payment_method_id          NUMBER(3),
  cust_bill_delivery_info_id NUMBER(12),
  parent_acct_id             NUMBER(12),
  cust_id                    NUMBER(12),
  bill_address               VARCHAR2(255),
  acct_nbr                   VARCHAR2(60) not null,
  acct_name                  VARCHAR2(120),
  billing_cycle_type_id      NUMBER(6) not null,
  payment_type               CHAR(1) not null,
  bank_id                    NUMBER(6),
  bank_acct_nbr              VARCHAR2(60),
  bank_acct_name             VARCHAR2(120),
  bank_acct_exp_date         DATE,
  payment_comments           VARCHAR2(255),
  bank_card_type             NUMBER(6),
  created_date               DATE not null,
  update_date                DATE not null,
  state                      CHAR(1) not null,
  state_date                 DATE not null,
  postpaid                   CHAR(1),
  routing_id                 NUMBER(6),
  bill_format_id             NUMBER(6),
  default_flag               CHAR(1),
  is_lock                    CHAR(1),
  party_type                 CHAR(1),
  party_code                 VARCHAR2(60),
  need_upload                CHAR(1),
  bill_flag                  CHAR(1),
  bill_currency              NUMBER(9),
  sp_id                      NUMBER(6),
  bank_acct_issue_date       DATE,
  def_lang_id                NUMBER(6),
  allow_mod_state_date       DATE,
  last_order_item_id         NUMBER(16),
  seq                        NUMBER(6),
  previous_seq               VARCHAR2(120),
  upload_date                DATE
)
```
* 建立对应的需要补数据的表的结构,一般以FIX_表名，表结构必须和mysql逻辑表一致  
* 配置load.cfg 把从MySQL下载的数据导入Oracle
* 根据逻辑往FIX_XXX表插入需要补充的数据
* 配置unload.cfg 使用Unload.sh下载数据
* 修改下载数据的后缀为tmp
* 使用ora2mysql.pl把下载的文件转换为mysql格式
* 使用load_mysql脚本导入mysql
  
## 批量修改数据
原理同上，只是需要生成批量执行的SQL文件在把sql文件下载下来就行了




