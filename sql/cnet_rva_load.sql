#!/bin/ksh
set -x

cd $HOME
. .profile

ONCALLSQL=$HOME/sql/oncall.sql
ONCALLOUT=$HOME/reports/oncall.spool
ONCALLEMAIL=`sed '/^$/d' $ONCALLOUT | awk '{print $1}'`
ONCALLPAGER=`sed '/^$/d' $ONCALLOUT | awk '{print $2}'`
AP1=127.0.0.1
AP2=127.0.0.2
AP3=127.0.0.3
AP4=127.0.0.4
C_DIR=$HOME/works
S_DIR=$HOME/works

# Pick up on call person
# Connect to database and run script to query oncall table and spool results.
sqlplus -s $sqlplus @$ONCALLSQL $ONCALLOUT

if [[ $? -ne 0 ]]
then
  echo "ERROR $0 could not pick up on-call person." | mailx -s "ERROR $0 Unable to get on-call" $ONCALLEMAIL $ONCALLPAGER
  exit 1
else
  ONCALLEMAIL=`sed '/^$/d' $ONCALLOUT | awk '{print $1}'`
  ONCALLPAGER=`sed '/^$/d' $ONCALLOUT | awk '{print $2}'`

  echo "On-call Email: $ONCALLEMAIL"
  echo "On-call Pager: $ONCALLPAGER"
fi

# FOR TESTING ONLY
ONCALLEMAIL=user@domain.com
ONCALLPAGER=user@domain.com

# determine what sites are avaliable
set -A SITES `cmd sites | sed '1d' | awk '{print $1}' | grep -e $AP1 -e $AP2 -e $AP3 -e $AP4`

if [[ ${#SITES[*]} -lt 1 ]]
then
  echo "ERROR $0 could not find available app server" | mailx -s "ERROR $0 No app servers" $ONCALLEMAIL $ONCALLPAGER
  exit 1
fi

# determine load on each avaliable site
CURRENT=0

while [[ $CURRENT -lt ${#SITES[*]} ]]
do
  scp ${SITES[$CURRENT]}:$C_DIR/RVA_* $S_DIR
  ssh -x ${SITES[$CURRENT]} "rm $C_DIR/RVA_*"
  CURRENT=$((CURRENT + 1))
done

cd works
FILELIST=`ls RVA_*`
echo 'BEGIN' > cnet_rva_proc.sql
for FILE in $FILELIST
do
  sed /DOCTYPE/d $FILE > $FILE.tmp
  rm $FILE
  mv $FILE.tmp $FILE 
  echo "INSERT INTO RVA_XML VALUES (XMLType(BFILENAME('TESTXML', '$FILE'),NLS_CHARSET_ID('AL32UTF8')));" >> cnet_rva_proc.sql
  echo "INSERT INTO RVA_XML_COL (filename, xml_rva_document) VALUES('$FILE', XMLType(BFILENAME('TESTXML', '$FILE'),NLS_CHARSET_ID('AL32UTF8')));" >> cnet_rva_proc.sql
done
chmod 644 RVA_*
echo 'PCK_TEST_XML_RVA.PROC_TEST_XML_RVA;' >> cnet_rva_proc.sql
echo 'COMMIT;' >> cnet_rva_proc.sql
echo 'END;' >> cnet_rva_proc.sql
echo '/' >> cnet_rva_proc.sql
echo 'exit' >> cnet_rva_proc.sql

sqlplus $sqlplus @cnet_rva_proc.sql
rm -f RVA_*

exit;
