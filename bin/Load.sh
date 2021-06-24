#!/bin/ksh

#########################################################################################
# Function: load data into corresponding database according to configuration.
# For oracle database: sqlldr
# For informix database: dbload
# For memdb: load of mdsql
# For gmdb: gmloader or gmdcp
# Configuration file: ../config/env.cfg
# Data path: ../data
# Usage:./load.sh
##########################################################################################
# Modify Record:
# 20140127: add for loading gmdb data
##########################################################################################

##################Config Begin###################
gmdcpflag=0           #0:use gmloader to load,not use gmdcp to load;
                      #1:use gmdcp once to load all table;
                      #2:loop to use gmdcp to load table one by one
commitnum=10000       #dbload put in line number
charmaxlength=1000000 #clob length
##################Config End#####################

###############################################################################
# Function name: logwrite
# Function: print infomation to standard output and log files
###############################################################################
logwrite()
{
  curtime=`date '+%Y-%m-%d %H:%M:%S'`
  print "[ $curtime ] $*" |tee -a $logfile
}
###############################################################################
# Function name: add_one_process
# Function: load data into ORACLE by sqlldr,or into INFORMIX by dbload,or MEMDB
#           by load of mdsql
# Temp variable: MDSQLPARAM/EXITSTAT
###############################################################################
add_one_process()
{
  logwrite "Load ${tablename}|${dataname}"
  cd $datadir
  if [ "${DBTYPE}" = "informix" ];then
    load_informix ${DBNAME} ${tablename} ${dataname} 0 >> $logfile 2>&1
    EXITSTAT=$?
    
  elif [ "${DBTYPE}" = "oracle" ];then
    load_oracle ${tablename} ${DBNAME} ${dataname} >> $logfile 2>&1
    EXITSTAT=$?
    
  elif [ "${DBTYPE}" = "memdb" ];then
    echo "quit"|mdsql -d >/dev/null 2>&1
    if [ "$?" = "0" ];then
      MDSQLPARAM='-d'
    fi
    # check status( mdsql always return 0 when sql fail )
    logtmp="$logdir/load_${dataname}.log"
    echo "load from '${dataname}' insert into ${tablename};"|mdsql $MDSQLPARAM > $logtmp
    cat $logtmp >> $logfile
    grep "succeeded" $logtmp >/dev/null
    EXITSTAT=$?
    rm -f $logtmp
    
  elif [ "${DBTYPE}" = "gmdb" ];then
    if [ "${gmdcpflag}" != "2" ];then
      echo "gmloader -c ${DBNAME} -i ${tablename} $dataname -dateformat "YYYYMMDDHH24MISS"" >> $logfile
      gmloader -c ${DBNAME} -i ${tablename} $dataname -dateformat "YYYYMMDDHH24MISS" >> $logfile
    elif [ "${gmdcpflag}" = "2" ];then
      echo "gmdcp -c ${DBNAME} -i -t ${tablename} -r -dateformat "YYYYMMDDHH24MISS" -p ./" >> $logfile
      gmdcp -c ${DBNAME} -i -t ${tablename} -r -dateformat "YYYYMMDDHH24MISS" -p ./ >> $logfile
    fi
    EXITSTAT=$?
  fi
  
  if [ "$EXITSTAT" = "0" ];then
    logwrite "Load ${tablename}|${dataname} successfully!"
  else
    logwrite "Load ${tablename}|${dataname} failed!"
    echo "load ${tablename}|${dataname} failed" >> ${load_exit_statfile}
  fi
  
  echo >& 8
}

###############################################################################
# Function name: load_informix
# Function: load data into into INFORMIX by dbload
# Parameters: dbname tablename datafile and errnum
# Usage: load_informix $dbname $tablename $datafile $errnum
# Temp variable: EXITSTAT
###############################################################################
load_informix()
{
  dbname=$1
  tablename=$2
  datafile=$3
  errnum=$4
  
  if [ ! -s $datafile ];then
    echo "There is no data in file $datafile"
    return
  fi
  
  logfilename="${refdir}/log/${datafile}.log"
  cfilename="${tmpfile}/${tablename}$$.fmt"
  fieldnum=`awk -F'|' '{print NF-1;exit}' $datafile`
  
  cat >$cfilename<<!
file '${datafile}' delimiter '|' ${fieldnum} ;
insert into ${tablename};
!
  dbload -d ${dbname} -c $cfilename -n $commitnum -l $logfilename -e $errnum -r
  EXITSTAT=$?
  if [ ! -s $logfilename ];then
    rm -f $logfilename
  fi
  
  return $EXITSTAT
}

###############################################################################
# Function name: load_oracle
# Function: load data into into INFORMIX by dbload
# Parameters: table dbname and data
# Usage: load_oracle $table $dbname $data
###############################################################################
load_oracle()
{
  table=$1
  dbname=$2
  data=$3
  lv_rows=10000
  lv_bindsize=8192000
  lv_readsize=8192000
  lv_errors=50
  lv_direct=false
  lv_parallel=false
  lv_skip_index_maintenance=false
  lv_log="$logdir/load_${data%\.*}.log"
  
  f_generate_control_file ${table} ${dbname} ${data}
  sqlldr ${dbname} \
  control=${lv_control} \
  rows=${lv_rows} \
  bindsize=${lv_bindsize} \
  readsize=${lv_readsize} \
  log=${lv_log} \
  bad=$data.err \
  direct=${lv_direct} \
  parallel=${lv_parallel} \
  skip_index_maintenance=${lv_skip_index_maintenance} \
  errors=${lv_errors}
}

###############################################################################
# Function name: f_generate_control_file
# Function: create control file of oracle's sqlldr 
# Parameters: table dbname data
# Usage: f_generate_control_file $table $dbname $data
###############################################################################
f_generate_control_file()
{
  lv_table=$1
  dataname=$3
  tablefile="${3}"
  dbname=$2
  filenamepre=${tablefile%\.*}
  lv_temp="wk_${filenamepre}.test"
  lv_temp1="wk_${filenamepre}.test1"
  lv_temp2="wk_${filenamepre}.test2"
  lv_temp3="wk_${filenamepre}.test3"
  lv_control="${filenamepre}.ctl"
  dataformat="'YYYY-MM-DD HH24:MI:SS'"
  
  sqlplus ${dbname} >/dev/null <<!
set sqlprompt "SQL>"
spool ${lv_temp};
select column_name||' '||data_type||' '||data_length from user_tab_columns where table_name=upper('${lv_table}') order by column_id;
spool off;
exit
!
  if [ "$?" != "0" ];then
    echo "Error: sqlplus ${dbname} error in generate control file for table ${lv_table} !"
    echo "please check userid and passwd or oracle_sid."
    exit
  fi

  if [ -f ${lv_temp} ];then
    cat ${lv_temp}|grep -v "^SQL>" |grep -v "COLUMN_NAME" |grep -v "^-" |grep -v " rows selected" |awk '{print $1,$2,$3}' > ${lv_temp1}
    sed -e '/^[[:space:]]*$/d' ${lv_temp1} >${lv_temp2}
    lv_line_num=`cat ${lv_temp2} | wc -l`
    lv_index=0
    while read colname coldatatype datalength
    do
      if [ "$coldatatype" = "DATE" ];then
        coldatatype='"'"to_date(:$colname,$dataformat)"'"'
      # [2012-5-10]add function when length of [datatype=char] > 255
      elif [ `echo $coldatatype|grep -c CHAR` -gt 0 -a $datalength -gt 255 ]; then
        coldatatype=" CHAR($datalength)"
      # [2013-10-25]add function for CLOB data type
      elif [ "$coldatatype" = "CLOB" ];then
        coldatatype=" CHAR($charmaxlength)"
      else
        coldatatype=""
      fi
      lv_index=`expr ${lv_index} + 1`
      if [ ${lv_index} -eq ${lv_line_num} ];then
        echo $colname " " "$coldatatype" >>$lv_temp3
      else
        echo $colname " " "$coldatatype," >>$lv_temp3
      fi
    done < ${lv_temp2}
  else
    echo "Error: not find ${lv_temp} file."
    exit
  fi
  
  lv_str="LOAD DATA INFILE '${tablefile}' BADFILE '${filenamepre}.err' APPEND INTO TABLE ${lv_table} FIELDS TERMINATED BY \"|\" optionally enclosed by '\"'"
  if [ "${lv_direct}" = "true" ];then
    echo "unrecoverable" > ${lv_control}
  fi
  echo ${lv_str} > ${lv_control}
  echo "(" >> ${lv_control}
  cat ${lv_temp3} >> ${lv_control}
  echo ")" >> ${lv_control}
  
  rm -f ${lv_temp}
  rm -f ${lv_temp1}
  rm -f ${lv_temp2}
  rm -f ${lv_temp3}
}

#################################################################################
# Function name: get_db_lines
# Function: get the number of sql table structure by desc in ORACLE when spooled.
# Parameter: filename(include path) which this function output information to.
# Usage: get_db_lines $OUTFILENAME
# use global variable: tmpfile/DBTYPE/DBNAME
# Temp variable: outfile/tablename/MDSTAT/count_db/
#################################################################################
get_db_lines()
{
  outfile=$1
  rm -f $outfile
  touch $outfile

  logwrite "Begin to count(*) to file < $outfile >"
  
  while read tablename
  do
    if [ "${DBTYPE}" = "informix" ];then
      rm -f ${tmpfile}/table_rows.tmp
      dbaccess $DBNAME > /dev/null 2>&1 <<!
      #delete from $tablename;
      unload to ${tmpfile}/table_rows.tmp select count(*) from $tablename;
!
      #echo $tablename
      #ls ${tmpfile}
      count_db=`awk -F"." '{ printf("%d",$1)}' ${tmpfile}/table_rows.tmp`
      echo "$tablename|$count_db" >> $outfile
    
    elif [ "${DBTYPE}" = "oracle" ];then
      rm -f ${tmpfile}/table_rows.tmp
      sqlplus -s ${DBNAME} > /dev/null <<!
      set serveroutput on
      set feedback off
      set trims on
      set pagesize 0
      set timing off
      set lin 5000
      spool ${tmpfile}/table_rows.tmp app
      select count(*) from $tablename;
      spool off
      COMMIT;
      set serveroutput off
!
      
      count_db=`awk '{ printf("%d",$1)}' ${tmpfile}/table_rows.tmp`
      echo "$tablename|$count_db" >> $outfile
      rm -f ${tmpfile}/table_rows.tmp
    
    elif [ "${DBTYPE}" = "memdb" ];then
      mdstat -v >/dev/null 2>&1
      if [ "$?" = "0" ];then
        MDSTAT=mdstat
      else
        MDSTAT=mdstatus
      fi
      count_db=`$MDSTAT -t ${tablename}|grep "Record Num"|awk '{print $3}'`
      
      if [ -z "$count_db" ];then
        echo "get ${tablename} failed!"
        count_db=0
      fi
      echo "$tablename|$count_db" >> $outfile
    
    elif [ "${DBTYPE}" = "gmdb" ];then
      count_db=`gmstat -t ${tablename}|grep "record number"|awk '{print $4}'`

      if [ -z "$count_db" ];then
#       echo "get ${tablename} failed!"
        count_db=0
      fi
      echo "$tablename|$count_db" >> $outfile
    fi
    
    logwrite "$tablename|$count_db"
    
  done < ${tmptablist}
  
  logwrite "End to count(*) to file < $outfile >"
}

###############################################################################
# Function name: get_unl_lines
# Function: get the number of record in corresponding UNL files of every table.
# Parameter: filename(include path) which this function output information to.
# Usage: get_unl_lines $OUTFILENAME
# use global variable: datapath/tablenamelistfile
# temp variable: outfile/table/unlfile/count_unl
###############################################################################
get_unl_lines()
{
  outfile=$1
  rm -f $outfile
  touch $outfile
  
  logwrite "Begin to print the newline counts of unl files to file < $outfile >"
  
  while read table unlfile
  do
    #command_outfile=${datadir}/${table}
    count_unl=`wc -l ${datadir}/$unlfile|tail -1|awk '{print $1}'`
    echo "$table|${count_unl}" >> get_unl_lines.tmp
    
    logwrite "$table|${count_unl}"
    
  done < ${tmptab_filelist}
  awk 'BEGIN{FS="|";OFS="|"}{ARR[$1]+=$2}END{for(idx in ARR){printf("%s|%.0f\n",idx,ARR[idx])}}' get_unl_lines.tmp >> $outfile
  rm -rf get_unl_lines.tmp
  logwrite "Begin to print the newline counts of unl files to file < $outfile >"
}

###############################################################################
# Function name: CheckConfig
# Function: check configuration in corresponding configuration file.
# Usage: CheckConfig
# use global variable: tmpfile
# Temp variable: tablename/dataname/MDSTAT
###############################################################################
CheckConfig()
{
  #--20140127: add for max_process_count
  if [ "${max_process_count}" -lt 1 ];then
    echo "Error: max_process_count is wrong in ${env_cfgfile}" && exit 1
  fi
  
  # judge whether database is OK or not.
  if [ "${DBTYPE}" = "informix" ]; then
    # judge whether informix database is OK #
    echo "" |dbaccess "${DBNAME}" >/dev/null 2>&1
    [ "$?" != "0" ] && echo "Error: informix database ( ${DBNAME} ) has something wrong" && exit 1
  elif [ "${DBTYPE}" = "oracle" ]; then
    # judge whether oracle database is OK #
    # signal is always 0 whenever use sqlplus for oracle db which db maybe has something wrong.
    echo "" |sqlplus -s "${DBNAME}" |grep "^ORA-" >/dev/null
    [ "$?" = "0" ] && echo "Error: oracle database ( ${DBNAME} ) has something wrong" && exit 1
  elif [ "${DBTYPE}" = "memdb" ]; then
    # judge whether memdb is OK #
    echo "" |mdsql |grep "dbsql>" >/dev/null 2>&1
    [ "$?" != "0" ] && echo "Error: memdb database has something wrong" && exit 1
  elif [ "${DBTYPE}" = "gmdb" ];then
    # judge whether gmdb is OK #
    echo "" |gmsql "${DBNAME}" |grep "Successfully CONNECT to DB" >/dev/null
    [ "$?" != "0" ] && echo "Error: memdb database has something wrong" && exit 1
  else
    echo "Error: dbtype is configed wrong,which must be memdb or oracle or informix or gmdb" && exit 1
  fi
  
  while read tablename dataname
  do
    # judge whether data file exists or not.
    [ ! -f "${datadir}/${dataname}" ] && echo "Error: ${dataname} not exists!" && exit 1
    
    # judge whether table exists or not.
    if [ "${DBTYPE}" = "informix" ]; then
      echo "select first 1 0 from ${tablename};"|dbaccess "${DBNAME}" >/dev/null 2>&1
      [ "$?" != "0" ] && echo "Error: the table or view ${tablename} not exists in informix!" && exit 1
    elif [ "${DBTYPE}" = "memdb" ]; then
      mdstat -v >/dev/null 2>&1
      if [ "$?" = "0" ];then
        MDSTAT=mdstat
      else
        MDSTAT=mdstatus
      fi
      $MDSTAT -t "${tablename}" >/dev/null 2>&1
      [ "$?" != "0" ] && echo "Error: the table or view ${tablename} not exists in memdb!" && exit 1
    elif [ "${DBTYPE}" = "oracle" ]; then
      echo "select 1 from ${tablename} where rownum=1;"|sqlplus -s "${DBNAME}" |grep "^ORA-" > /dev/null
      [ "$?" = "0" ] && echo "Error: the table or view ${tablename} not exists in oralce!" && exit 1
    elif [ "${DBTYPE}" = "gmdb" ]; then
      gmstat -t "${tablename}" | grep "table information" >/dev/null
      [ "$?" != "0" ] && echo "Error: the table or view ${tablename} not exists in gmdb!" && exit 1
    fi
  done < ${tmptab_filelist}
}
######################### main #############################
# define global directory or file variable
workdir=`pwd`
refdir=`dirname $workdir`
datadir="${refdir}/data/"$2
echo $datadir
tmpfile="${refdir}/temp"
logdir="${refdir}/log"
logfile="${refdir}/log/load.log"
env_cfgfile="${refdir}/config/"$1
tmptablist="${tmpfile}/tablelist.tmp"
tmptab_filelist="${tmpfile}/tablefilelist.tmp"
load_exit_statfile="${tmpfile}/load_exit_stat.tmp" #status file

echo $datadir
echo $env_cfgfile
mkdir -p $tmpfile $bakdir $logdir
touch $logfile
cat /dev/null > ${load_exit_statfile} #initialize status file

logwrite "Start..."

DBTYPE=`awk '/^\[dbtype\]/{getline;print tolower($1);exit}' ${env_cfgfile}`
DBNAME=`awk '/^\[dbname\]/{getline;print $1;exit}' ${env_cfgfile}`

#-- 20140127: add for inputing max_process_count in env.cfg
#the most number or processes which this script can use
max_process_count=`awk '/^\[max_process_count\]/{getline var;printf("%d",var);exit}' ${env_cfgfile}`
#-- 20140127:end

awk -F"|" '{if($0~/^\[tablelist\]/){match_flag=1;next}if(match_flag==1){if($0~/^\[END\]/){exit}print toupper($1),$2}}' ${env_cfgfile} > ${tmptab_filelist}
awk '{if(TABLE[$1]!=1){print $1;TABLE[$1]=1}}' ${tmptab_filelist} > ${tmptablist}

#check configuration in corresponding configuration file.
CheckConfig

# get table record number before loading
get_db_lines ${datadir}/pdb_lines_beforeload.txt

if [ "${DBTYPE}" = "gmdb" -a "${gmdcpflag}" = "1" ]; then
  TABLEALL=`awk '{printf $0 " "}' ${tmptablist}`
  # GMDB V200R005C00 gmdcp's most process must be 10,so need to judge and limit it.
  if [ "${max_process_count}" -gt 10 ];then
    max_process_count=10
  fi
  logwrite "Begin to load table"
  logwrite "gmdcp -c ${DBNAME} -i -t ${TABLEALL} -r -p $datadir -n ${max_process_count}"
  gmdcp -c ${DBNAME} -i -t ${TABLEALL} -r -dateformat "YYYYMMDDHH24MISS" -p $datadir -n ${max_process_count} | tee -a $logfile
  logwrite "Load table end!"
else
  # multiprocess
  rm -f ${tmpfile}/processfifo
  mkfifo ${tmpfile}/processfifo
  exec 8<>${tmpfile}/processfifo
  rm -f ${tmpfile}/processfifo
  
  process_num=${max_process_count}
  while [ "$process_num" -gt 0 ]
  do
    echo
    ((process_num=process_num-1))
  done >& 8
  
  # only need to get unique table list,when DBTYPE is gmdb and use gmdcp to load.
  if [ "${DBTYPE}" = "gmdb" -a "${gmdcpflag}" = "2" ];then
    loadtablelist=${tmptablist}
  else
    loadtablelist=${tmptab_filelist}
  fi
  
  while read tablename dataname
  do
    read -u8
    sleep 1
    
    # check exit status of last child process for [ideploy tool]
    if [ -s "${load_exit_statfile}" ];then
      exit 1
    fi
    
    add_one_process &
  done < ${loadtablelist}
  
  wait
  exec 8>&-
fi

logwrite ""
logwrite "...check data..."
get_db_lines ${datadir}/pdb_lines_afterload.txt
get_unl_lines ${datadir}/getpdb_unl_lines.txt


awk -F"|" '{
  if(FILENAME ~ "pdb_lines_beforeload.txt")
  {
    array1[$1]=$2
  }
  else if(FILENAME ~ "getpdb_unl_lines.txt")
  {
    array2[$1]=$2
  }
  else
  {
    array3[$1]=$2
  }
}END{
  for(table in array1)
  {
    diff=array1[table]+array2[table]-array3[table]
    if(diff == 0 )
    {
      printf("table:%25s,before load lines:%10d,unl lines:%10d,after load lines:%10d,OK \n",table,array1[table],array2[table],array3[table])
    }
    else
    {
      printf("table:%25s,before load lines:%10d,unl lines:%10d,after load lines:%10d,NOT EQUAL(diff:%d) ! \n",table,array1[table],array2[table],array3[table],diff)
    }
  }
}' ${datadir}/pdb_lines_beforeload.txt ${datadir}/getpdb_unl_lines.txt ${datadir}/pdb_lines_afterload.txt |tee -a $logfile

logwrite "End..."

rm -f ${tmpfile}/*.tmp
rm -f ${datadir}/*.ctl
exit
