#!/usr/bin/ksh
set -x

cd $HOME
. .profile

sleep 30
ONCALLSQL=$HOME/sql/oncall.sql
ONCALLOUT=$HOME/reports/oncall.spool

oncall_email=user@domain.com
oncall_pager=pager@domain.com
RQ_SERVER=`hostname -s`
WK_DIR=$HOME
WK_LOG="$WK_DIR/log_`date +%Y%m`"
cd $WK_DIR

# Pick up on call person
# Connect to database and run script to query oncall table and spool results.
sqlplus -s $sqlplus @$ONCALLSQL $ONCALLOUT

if [[ $? -ne 0 ]]
then
  echo "ERROR $0 could not pick up on-call person RVA Singles." >> $WK_LOG

  cat $WK_LOG | mailx -s "ERROR $0 Unable to get on-call RVA Singles" $oncall_email
  cat $WK_LOG | tail -1 | mailx $oncall_pager

  exit 1
else
  oncall_email=`sed '/^$/d' $ONCALLOUT | awk '{print $1}'`
  oncall_pager=`sed '/^$/d' $ONCALLOUT | awk '{print $2}'`

  echo "On-call Email: $oncall_email" >> $WK_LOG
  echo "On-call Pager: $oncall_pager" >> $WK_LOG
fi

# Check to see if right number of arguments were passed
if [[ $# -ne 1 ]]
then
  echo "ERROR $0 called with wrong number of arguments RVA Singles." >> $WK_LOG
  echo "Usage: $0 [host]" >> $WK_LOG

  cat $WK_LOG | mailx -s "ERROR $0 Bad arguments RVA Singles" $oncall_email
  cat $WK_LOG | head -1 | mailx $oncall_pager

  exit 1
else
  remote_svr=$1
fi

echo "starting rva singles file transfer from user to pdm..." > $WK_LOG

#Check if the privous process is still running
rva_flag=$WK_DIR/rva.flag
mm=`date +%m`
dd=`date +%d`
yy=`date +%y`
tt=`date +%T`

if [ -s $rva_flag ]
then 
  echo  "$mm/$dd/$yy $tt $RQ_SERVER :Error: $0 already running rva singles, exit" >> $WK_LOG
  exit 1
fi

touch rva.flag
echo "Start RVA Singles Request at " `date` >> $WK_LOG

#Check if the request.verify.ok flag exists on PDM

username=s

remote_svr_dir=/neutralfile

remote_cnet_dir=$remote_svr_dir/cnet_rvout
local_arch_dir=$WK_DIR/rva_archived

# ck_flag=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify.ok|wc -l`
ck_file=`ssh -xl $username $remote_svr ls -l $remote_cnet_dir/request.verify|wc -l` 

#If the request.verify.ok exists on PDM, remove flag. 
if [  $ck_file -eq 1 ]
# && [  $ck_flag -eq 1 ] 
then 
  # notify file request status flag
  echo " IT has not pickup RVA Singles request file from last cron." `date` >> $WK_LOG
  echo "IT has not pick up RVA Singles request file from last cron" `date` | mailx $oncall_pager
  cat $WK_LOG | mailx -s "IT has not pick up RVA Singles request file from last cron" $oncall_email

  echo "IT did not pick up previous RVA Singles request file." `date` >> $WK_LOG
  echo "EXIT RVA Singles process and e-mail on-call pager." `date` >> $WK_LOG

  #ssh -xl $username $remote_svr rm -f $remote_cnet_dir/request.verify*
  #cd $WK_DIR
  #collect_resend 1
  #echo "IT did not pick up RVA Singles last batch, resending data with batch" >> $WK_LOG
  #cp singles.data request.verify
  #rm -f singles.data
  #scp request.verify $username@$remote_svr:$remote_cnet_dir
  #ssh -xl $username $remote_svr chmod 666   $remote_cnet_dir/request.verify
  #ssh -xl $username $remote_svr touch $remote_cnet_dir/request.verify.ok
  #ssh -xl $username $remote_svr chmod 666   $remote_cnet_dir/request.verify.ok
  echo "rva_request.sh finished at $mm$dd$yy $tt" >> $WK_LOG

else
  # remove prior data files from prior run
  cd $WK_DIR
  rm -f singleslist.prt
  rm -f singleslist.final

  #If the request.verify.ok not exists on PDM
  # means IT pick up the lastest request
  # move on to update the database control tables 

  echo "IT pick up RVA Singles request file from last cron" 'date' >> $WK_LOG
  #cnet_singles_load.sql

  sqlplus $sqlplus << ENDSQL > singleslist.prt 2>&1
    set heading off verify off linesize 500 tab off
    select ltrim(rtrim(a.event_idx)),ltrim(rtrim(a.request_type)),
           ltrim(rtrim(a.device_type)),ltrim(rtrim(a.device_id)),
           ltrim(rtrim(a.utility_id)),
           ltrim(rtrim(a.util_extra1)), ltrim(rtrim(a.util_extra2)),
           ltrim(rtrim(to_char(decode(a.util_extra3, null, '0000000000000000000000000', a.util_extra3)))),
           ltrim(rtrim(to_char(a.restore_time, 'MM/DD/YY HH24:MI:SS')))
      from cnet_singles_req_t a, cnet_rva_ctrl b
     where a.event_idx = b.order_number
       and to_char(a.restore_time, 'MM/DD/YY HH24:MI:SS') = to_char(b.restore_time, 'MM/DD/YY HH24:MI:SS')
       and a.restore_time between (sysdate -10/1440) and sysdate
       and a.flag_rva = 'N'
       and b.flag_rva='U';
    exit
ENDSQL

  grep 'cluster' singleslist.prt  > singleslist.final
  cut -c1-25,27-29,31,33-52,54-78,80-104,106-130,132-156,158-174 singleslist.final > singles.data
  cat singles.data | mailx -s "Singles Outage List" user@domain.com

  cp singles.data request.verify

  # check if request file has more than 10 records before processing
  ck_file_size=`wc -l < request.verify`
  echo "RVA Single file records included are $ck_file_size" >> $WK_LOG

  if [  $ck_file_size -lt 10 ]
  then
    echo "RVA Singles file records included are $ck_file_size not enough" >> $WK_LOG
    rm -f singles.data
    rm -f rva.flag  rm -f req.tmp
    echo 'rva.flag and req.tmp flag are removed' `date` >> $WK_LOG
    echo "RVA Singles rva_request_db.sh finished at $mm$dd$yy $tt" >> $WK_LOG
    exit 2
  fi

  cd $WK_DIR/sql
  sqlplus $sqlplus @rva_continue.sql
  echo "RVA Singles database control tables updated with flags" >> $WK_LOG

  cd $WK_DIR
  rm -f singles.data 
  scp request.verify $username@$remote_svr:$remote_cnet_dir
  ssh -xl $username $remote_svr chmod 666   $remote_cnet_dir/request.verify
  # ssh -xl $username $remote_svr touch $remote_cnet_dir/request.verify.ok
  # ssh -xl $username $remote_svr chmod 666   $remote_cnet_dir/request.verify.ok

  # update control table from verified records to processed
  # and delete records from cnet_singles_req_t

  cd $WK_DIR/sql

  # work with query to delete before moving to production 
  sqlplus $sqlplus @rva_outage_flag.sql

  echo "updated control table flag to processed from verified" >> $WK_LOG 
  echo "RVA Singles rva_request_db.sh finished at $mm$dd$yy $tt" >> $WK_LOG
fi
cd $WK_DIR
rm -f rva.flag  rm -f req.tmp
echo 'rva.flag and req.tmp flag are removed' `date` >> $WK_LOG 

exit 0
