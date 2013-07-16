#!/usr/bin/ksh
set -x

cd $HOME
. .profile

# Server running this script
RQ_SERVER=`hostname -s`
WK_DIR=$HOME
WK_LOG="$WK_DIR/log_`date +%Y%m`"
cd $WK_DIR

#Check to see if right number of arguments were passed
if [ $# -ne 1 ]
then
  echo "ERROR $0 called with wrong number of arguments RVA Laterals." > $WK_LOG
  echo "Usage: $0 [host]" >> $WK_LOG
  exit 1
fi

remote_svr=$1

echo "starting rva laterals file transfer from user to pdm..." > $WK_LOG

#Check status of Oracle database 

#ca_status=`caa_stat -t oracle|grep oracle|awk '{print $4}'`
ca_status=ONLINE

#Server email and paging
server_addr=user@domain.com
server_oncall=user@domain.com

if [[ "$ca_status" != "ONLINE" ]]
then 
  echo "**Oracle off-line RVA **" `date` > oncallpage1
  cat oncallpage1 | /usr/bin/mailx -s "ORACLE OFF LINE" $server_oncall  
  echo "Oracle off-line notification from RVA RT Laterals" 
  exit 0       
fi

#Select from Oracle table oncall person

sqlplus -s $sqlplus << EOF > pager_rva.prt 2>&1
  set head off verify off pagesize 0 linesize 82 tab off
  select email,fullpager from oncall where position = (select MIN(position) from oncall);
  exit
EOF

echo "selected oncall person"

sed '/^[ ]*$/d'  pager_rva.prt > pgr_rva.prt
oncall_email=`cut -c 1-41 pgr_rva.prt`
oncall_pager=`cut -c 42-82 pgr_rva.prt`

echo $oncall_pager >> $WK_LOG
echo $oncall_email >> $WK_LOG

#Clean up
rm pgr_rva.prt
rm pager_rva.prt

#Check if the privous process is still running
rva_flag=$WK_DIR/rva.flag
mm=`date +%m`
dd=`date +%d`
yy=`date +%y`
tt=`date +%T`

if [ -s $rva_flag ]
then 
  echo  "$mm/$dd/$yy $tt $RQ_SERVER :Error: $0 already running RVA Laterals, exit" >> $WK_LOG
  exit 1
fi

touch rva.flag
echo "Start RVA Laterals Request at " `date` >> $WK_LOG

#Check if the request.verify.ok flag exists on PDM

username=s
index_id=`date +%Y%m%d%H%M%S`
finqual=$HOME/fin
remote_svr_dir=/neutralfile

remote_cnet_dir=$remote_svr_dir/cnet_rvout
local_arch_dir=$WK_DIR/rva_archived

# define rva_log file variable
rva_log=$finqual/rvafin$index_id

ck_flag=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify.ok|wc -l`
ck_file=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify|wc -l` 

# IT isnt always right on time every time. Lets give them a few extra minute -Jason
try=1
while [ $try -ne 3 ] && [ $ck_flag -eq 1 ] && [ $ck_file -eq 1 ]
do
  sleep 60
  ck_flag=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify.ok|wc -l`
  ck_file=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify|wc -l` 
  try=$((try + 1))
done

#If the request.verify.ok exists on PDM, remove flag. 
if [ $ck_flag -eq 1 ] && [ $ck_file -eq 1 ] 
then 
  # notify file request status flag
  # email the right party

  echo "IT has not picked up RVA Laterals request file from last cron." `date` >> $WK_LOG
  echo "IT has not picked up RVA Laterals request file from last cron." `date`

  echo "Contact IT RVA Laterals request file not picked up from pdm" >> $rva_log
 
  cat $rva_log | /usr/bin/mailx -s "IT didn't pick up RVA Laterals file " $oncall_email
  cat $rva_log | /usr/bin/mailx -s "IT didn't pick up RVA Laterals file" $oncall_pager
  echo "EMAIL file sent" >> $rva_log
  echo "Pager sent" >> $rva_log
  echo "Finished" >> $rva_log
        
  exit 1
fi  

# remove prior data files from prior run
cd $WK_DIR
rm -f rvalist.prt
rm -f rvalist.final
 
# If the request.verify.ok not exists on PDM
# means IT pick up the lastest request
# move on to update the database control tables 
  
echo "IT pick up RVA Laterals requested file from last cron" 'date' >> $WK_LOG

sqlplus $sqlplus << EOF > rvalist.prt 2>&1
  set heading off verify off linesize 500 tab off
  select unique ltrim(rtrim(a.event_idx)),ltrim(rtrim(a.request_type)),
         ltrim(rtrim(a.device_type)),ltrim(rtrim(a.device_id)),
         ltrim(rtrim(a.utility_id)),
         ltrim(rtrim(a.util_extra1)), ltrim(rtrim(a.util_extra2)),
         ltrim(rtrim(to_char(decode(a.util_extra3, null, '0000000000000000000000000', a.util_extra3)))),
         ltrim(rtrim(to_char(a.restore_time, 'MM/DD/YY HH24:MI:SS')))
    from cnet_rva_req_t a, cnet_rva_rt_ctrl b
   where a.event_idx = b.order_number
     and trim(a.util_extra1) = trim(b.util_extra_1)
     and a.event_idx like 'R%'
     and b.order_number like 'R%';
  exit;
EOF

grep 'cluster' rvalist.prt  > rvalist.final
cut -c1-25,27-29,31,33-52,54-78,80-104,106-130,132-156,158-174 rvalist.final > rva.data
cat rva.data | mailx -s "RVA Laterals Outage List" user@domain.com

sqlplus $sqlplus << EOF
  set heading off feedback off verify off tab off
  update cnet_rva_rt_ctrl
     set flag_rva = 'V', restore_time = sysdate
   where flag_rva = 'U';
  commit;
  exit;
EOF

echo "RVA Laterals database control tables updated with flags" >> $WK_LOG
cp rva.data request.verify
rm -f rva.data 
scp request.verify $username@$remote_svr:$remote_cnet_dir
ssh -xl $username $remote_svr chmod 666 $remote_cnet_dir/request.verify
ssh -xl $username $remote_svr touch $remote_cnet_dir/request.verify.ok
ssh -xl $username $remote_svr chmod 666 $remote_cnet_dir/request.verify.ok
        
# update control table from verified records to processed
# and delete records from cnet_rva_req_t
        
cd $WK_DIR
       
# work with query to delete before moving to production 
sqlplus $sqlplus << EOF
  set heading off feedback off verify off tab off
  update cnet_rva_rt_ctrl
     set flag_rva = 'P', restore_time = sysdate
   where flag_rva = 'V';
  commit;
  exit;
EOF
 
echo "updated control table flag to processed from verified" >> $WK_LOG 
echo "RVA Laterals rva_request_db.sh finished at $mm$dd$yy $tt" >> $WK_LOG

rm -f rva.flag  rm -f req.tmp
echo 'rva.flag and req.tmp flag are removed' `date` >> $WK_LOG 
 
exit 0
