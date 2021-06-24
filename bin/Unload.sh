#!/bin/sh
#sqlunloader2 www.anysql.com


#./sqluldr2_linux64_10204.bin  USER=ADMIN/Admin_01@ QUERY="select *from BP_DATA_ROUTER" head=yes FILE=/home/oracle/CBS_TEST/sqluldr2/BP_DATA_ROUTER.unl  field='|'
#2017-10-26|������sql���޷�ʹ���ַ������ӷ���BUG
SPLIT=\|
CPUNO=4

HOME=`pwd`
DATA="${HOME}/../data/"
CONFIG="${HOME}/../config/unload.cfg"
BEGIN=`sed -n "/\[tablelist\]/=" "${CONFIG}"`
END=`sed -n "/\[END\]/=" "${CONFIG}"`
((BEGIN++))
NUM2=`sed -n "/\[dbname\]/=" "${CONFIG}"`
((NUM2++))
ORACLE_CONNCECT=`sed -n ""$NUM2"p" "$CONFIG"`

j=0
for((i=$BEGIN;i<$END;i++))
 	do
 	SQL0=`sed -n ""$i"p" "${CONFIG}"|cut -d \| -f 2-`
 	#ȥ�������һ������
 	SQL1=${SQL0%|*}
if [ -n "$SQL1" ];then
	SQL0=`sed -n ""$i"p" "${CONFIG}"|cut -d \| -f 2-`
	SQL2=`sed -n ""$i"p" "${CONFIG}"|cut -d \| -f 1`
	SQL1=${SQL0%|*}
	SQL[$j]=$SQL2
    SQL1=\""$SQL1 "\"" head =yes FILE=""$DATA""/""$SQL2"".unl"" record=0x7c0x0a field='""$SPLIT""'"
	echo $SQL1
	EXEC[$j]="./sqluldr2_linux64_10204.bin  USER=$ORACLE_CONNCECT  QUERY=$SQL1"
	 ((j++))
else

	SQL1=`sed -n ""$i"p" "${CONFIG}"|cut -d \| -f 1`
	
	SQL[$j]=$SQL1
	SQL1="\"SELECT *FROM ""$SQL1\""" head =yes FILE=""$DATA""/""$SQL1"".unl"" record=0x7c0x0a field='""$SPLIT""'"
	echo $SQL1
	EXEC[$j]="./sqluldr2_linux64_10204.bin  USER=$ORACLE_CONNCECT  QUERY=$SQL1"
	((j++))

fi
 done


trap "exec 1000>&-;exec 1000<&-;exit 0" 2
mkfifo testfifo
exec 1000<>testfifo
rm -fr testfifo
for((n=1;n<=4;n++))
do
	echo>&1000
done

echo [`date +%Y-%m-%d\ %H:%M:%S`] Process Begin....
start=`date "+%s"`
for((i=0;i<$j;i++))
do
	read -u 1000
	{	
		echo  [`date +%Y-%m-%d\ %H:%M:%S`] ${SQL[$i]} Unload Begin....
		echo ${EXEC[$i]}
	 	eval ${EXEC[$i]}
	 	echo  [`date +%Y-%m-%d\ %H:%M:%S`] ${SQL[$i]} Unload End....
	 	sleep 1
	 	echo>&1000
	}&
done
wait
echo [`date +%Y-%m-%d\ %H:%M:%S`] All Complete!
exec 1000>&-
exec 1000<&-
