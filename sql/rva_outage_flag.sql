set heading off; 
set feedback off; 
set verify off; 
set tab off; 
update cnet_rva_ctrl
set flag_rva = 'P', restore_time = sysdate 
where flag_rva = 'V';
commit;
exit;
