set heading off;
set feedback off;
set verify off; 
set tab off;
update cnet_rva_ctrl
set flag_rva = 'V', restore_time = sysdate
where flag_rva =  'U';
commit;
exit;

