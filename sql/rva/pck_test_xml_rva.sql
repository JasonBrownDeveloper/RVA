CREATE OR REPLACE PACKAGE PCK_TEST_XML_RVA IS
  PROCEDURE  proc_test_xml_rva;
END PCK_TEST_XML_RVA;
/

CREATE OR REPLACE PACKAGE BODY Pck_Test_Xml_Rva IS
  PROCEDURE proc_test_xml_rva IS
    I NUMBER;
    CELL_EVENT VARCHAR2(25);
    CELL_DATE VARCHAR2(25);
    CELL_MTR VARCHAR(20);
    CELL_DVC VARCHAR(25); 
    SPID VARCHAR2(25);
    MTRID VARCHAR2(32);
    EVENT_TIME DATE; 

    CURSOR rva_xml IS 
      SELECT e.xml_rva_document AS qrecord_value
        FROM RVA_XML_COL e; 

    CURSOR rva_spid IS
      SELECT DISTINCT c.meter_id AS SPID
        FROM CU_METERS c
       WHERE meter_no = CELL_MTR;

    CURSOR rva_cust_data IS
      SELECT supply_idx
           , h_cls
           , h_idx
           , account_name
           , address_building
           , address, city_state
           , phone_number
           , priority
           , life_support
           , account_number
           , meter_id
           , meter_number
           , device_id
           , meter_psr
           , feeder_id 
        FROM CES_CUSTOMERS
       WHERE account_number = SPID
         AND meter_id IS NOT NULL
         AND meter_psr IS NOT NULL
         AND meter_number IS NOT NULL; 

    CURSOR rva_begin_time IS
      SELECT restore_time 
        FROM JOBS
       WHERE event_idx = CELL_EVENT; 

  BEGIN
    DELETE FROM CNET_RVA_REQ_T;
    COMMIT;

    I := 0;
    CELL_EVENT := NULL;
    CELL_DATE := NULL;
    CELL_MTR := NULL;
    CELL_DVC := NULL;
    SPID := NULL;
    EVENT_TIME := NULL;

    FOR rva_xml_rec IN rva_xml
    LOOP
      I := 1;
      IF rva_xml_rec.qrecord_value.existsNode('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[7]/CHILDREN/WIDGET[NAME=''tiCustRefNumText'']/VALUE') > 0
      THEN
        CELL_EVENT := rva_xml_rec.qrecord_value.extract('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[7]/CHILDREN/WIDGET[NAME=''tiCustRefNumText'']/VALUE/text()').getStringVal();
       -- DBMS_OUTPUT.PUT_LINE('Event ID: ' || CELL_EVENT);
       -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);

        LOOP 
          EXIT WHEN rva_xml_rec.qrecord_value.existsNode('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']') = 0;
          IF rva_xml_rec.qrecord_value.existsNode('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[5]/VALUE/text()') > 0
          THEN
            CELL_DATE := rva_xml_rec.qrecord_value.extract('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[5]/VALUE/text()').getStringVal();
           -- DBMS_OUTPUT.PUT(RPAD('Date: ' || CELL_DATE, 25, ' '));
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);
          END IF;

          /* logic works, open loop once a call has been created and generates an event..
           * change values as for the begin time selected comes from the jobs table */ 
          FOR rva_begin_time_rec IN rva_begin_time
          LOOP 
            IF I >= 1
            THEN
             -- DBMS_OUTPUT.PUT_LINE('Tell me, I am inside the begin time loop ');
              EVENT_TIME := SYSDATE;
             -- rva_begin_time_rec.restore_time;
              CELL_DATE := (TO_CHAR(EVENT_TIME,'mmddyyhh24miss'));
             -- DBMS_OUTPUT.PUT_LINE('Job begin time: ' || CELL_DATE);
            END IF;
          END LOOP; 

          IF rva_xml_rec.qrecord_value.existsNode('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[12]/VALUE/text()') > 0
          THEN
            CELL_MTR := rva_xml_rec.qrecord_value.extract('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[12]/VALUE/text()').getStringVal();
           -- DBMS_OUTPUT.PUT(RPAD('Meter: ' || CELL_MTR, 25, ' '));
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);
          END IF;

          FOR rva_spid_rec IN rva_spid
          LOOP
            IF I >= 1
            THEN
             -- DBMS_OUTPUT.PUT_LINE('SPID from cu_meter table: ' || rva_spid_rec.SPID);
             -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);
              SPID := rva_spid_rec.SPID;
            END IF; 
          END LOOP;  

          IF rva_xml_rec.qrecord_value.existsNode('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[15]/VALUE/text()') > 0
          THEN
            CELL_DVC := rva_xml_rec.qrecord_value.extract('/WIDGET_PRINT/WIDGET/CHILDREN/WIDGET[6]/CELL_VALUES/ROW['||I||']/CELL[15]/VALUE/text()').getStringVal();
           -- DBMS_OUTPUT.PUT_LINE('Device: ' || CELL_DVC);
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);
          END IF;

          I := I + 1;

         -- IF CELL_EVENT IS NOT NULL  THEN  -- test
           -- DBMS_OUTPUT.PUT_LINE(' RECAP --------------------------------------------- ');
           -- DBMS_OUTPUT.PUT_LINE('RECAP WHAT DO WE HAVE UP TO FIRST LOOP, counter moving to next record...');
           -- DBMS_OUTPUT.PUT_LINE('Event ID: ' || CELL_EVENT);
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I); DBMS_OUTPUT.PUT(RPAD('Cell Date: ' || CELL_DATE, 25, ' '));
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I); DBMS_OUTPUT.PUT(RPAD('Event Time: ' || EVENT_TIME, 25, ' '));
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I); DBMS_OUTPUT.PUT(RPAD('Meter: ' || CELL_MTR, 25, ' '));
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I); DBMS_OUTPUT.PUT_LINE('SPID from cu_meter table: ' || SPID);
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I); DBMS_OUTPUT.PUT_LINE('Device: ' || CELL_DVC);
           -- DBMS_OUTPUT.PUT_LINE('Counter: ' || I);
           -- DBMS_OUTPUT.PUT_LINE('END RECAP ---------------------------------------');
         -- END IF; 

          FOR rva_cust_data_rec IN rva_cust_data
          LOOP
            IF rva_cust_data_rec.meter_psr IS NOT NULL
            THEN
              IF CELL_EVENT IS NOT NULL --AND OUTAGE_TIME_t BETWEEN (SYSDATE - 10/1440 AND SYSDATE)
              THEN
                INSERT INTO CNET_RVA_REQ_T ( event_idx
                                           , request_type
                                           , device_type
                                           , device_id
                                           , utility_id
                                           , util_extra1
                                           , util_extra2
                                           , util_extra3
                                           , restore_time
                                           , flag_rva
                                           , process_time)
                                    VALUES ( 'R'||LTRIM(RTRIM(CELL_EVENT))||'cluster'
                                           , 'IND'
                                           , 'M'
                                           , LTRIM(RTRIM(CELL_MTR))
                                           , LPAD(LTRIM(RTRIM(SPID, ' ')), 13, '0')
                                          -- , LTRIM(RTRIM(TO_CHAR(CELL_DATE,'mmddyyhh24miss')))
                                           , LTRIM(RTRIM(CELL_DATE))
                                           , LTRIM(RTRIM(CELL_EVENT))
                                           , LTRIM(RTRIM(rva_cust_data_rec.meter_psr))
                                           , SYSDATE
                                           , 'N'
                                           , SYSDATE);
                COMMIT; 
              END IF;  
            END IF;

            IF CELL_MTR IS NOT NULL
            THEN
              INSERT INTO CNET_RVA_RT_CTRL ( order_number
                                           , request_type
                                           , device_type
                                           , device_id
                                           , utility_id -- service point id
                                           , util_extra_1 -- data holder must be outage time from supply log table
                                           , util_extra_2 -- data holder must be event idx
                                           , util_extra_3 -- data holder to be defined
                                           , restore_time
                                           , flag_rva
                                           , process_time)
                                    VALUES ( 'R'||LTRIM(RTRIM(CELL_EVENT))||'cluster'
                                          -- , ('S'||to_char(sysdate,'mmddyyhh24miss')||'cluster'
                                           , 'IND'
                                           , 'M'
                                           , LTRIM(RTRIM(CELL_MTR))
                                           , LPAD(LTRIM(RTRIM(SPID, ' ')), 13, '0') -- service point id
                                          -- , LTRIM(RTRIM(TO_CHAR(CELL_DATE,'mmddyyhh24miss')))
                                           , LTRIM(RTRIM(CELL_DATE))
                                           , LTRIM(RTRIM(CELL_EVENT)) -- data holder must be event idx
                                           , LTRIM(RTRIM(rva_cust_data_rec.meter_psr)) -- data holder to be defined
                                           , SYSDATE
                                           , 'U'
                                           , SYSDATE);
              COMMIT; 
            END IF; 
          END LOOP; 
        END LOOP;
      END IF;
    END LOOP; 
  END proc_test_xml_rva;
END Pck_Test_Xml_Rva;
/

