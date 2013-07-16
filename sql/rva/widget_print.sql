DROP DIRECTORY RVADIR;

CREATE OR REPLACE DIRECTORY
SQLDIR AS
'/home/user/sql';

declare
  res boolean;
begin
  res := dbms_xdb.createFolder('/home');
  commit;
end;
/

declare
  res boolean;
begin
  res := dbms_xdb.createFolder('/home/user');
  commit;
end;
/

declare
  res boolean;
begin
  res := dbms_xdb.createFolder('/home/user/sql');
  commit;
end;
/

declare
  res boolean;
begin
  res := dbms_xdb.createResource('/home/user/sql/widget_print.dtd',bfilename('SQLDIR','widget_print.dtd'));
  commit;
end;
/

exit;
