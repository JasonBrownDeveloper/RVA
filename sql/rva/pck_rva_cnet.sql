CREATE OR REPLACE PACKAGE Pck_Rva_Cnet IS
  PROCEDURE  proc_rva_cnet;
END Pck_Rva_Cnet;
/

CREATE OR REPLACE PACKAGE BODY Pck_Rva_Cnet IS
  PROCEDURE proc_rva_cnet IS
    DEVICE CHAR(13);
    CNET_TIME DATE;
    EVENT_TIME DATE;
    STATUS CHAR(3);
    EVENT_ID CHAR(25);

    CURSOR rva_cnet_rt IS
      SELECT DISTINCT RTRIM(a.utility_id) AS qDEVICEID
           , a.restore_time AS qRESTORE_TIME
           , a.device_power_status AS qSTATUS
           , LTRIM(RTRIM(a.util_extra_2)) AS qEVENT_IDX
           , b.begin_time AS qBEGIN_TIME 
        FROM RVA_RESPONSE_LOAD_T a, JOBS b
       WHERE a.device_type = 'M'
         AND a.order_number LIKE 'R%'
         AND a.UPDATE_FLAG = 'R'
         AND LTRIM(RTRIM(a.util_extra_2)) = TO_CHAR(LTRIM(RTRIM(b.event_idx)))
         AND b.alarm_state IN ('CMP') /* use 'NEW' when testing */
         AND b.num_cust_out > 1
         AND b.devcls_name IS NOT NULL
         AND b.restore_time IS NOT NULL;
      -- ORDER BY a.utility_id;

    CURSOR valid_rva_mtr_rt(p_DEVICEID CHAR, p_RESTORE_TIME DATE, p_STATUS CHAR, p_EVENT_IDX  CHAR, qBEGIN_TIME DATE) IS
      SELECT DISTINCT b.restore_time
           , b.util_extra_3
           , a.meter_number
           , c.meter_no
           , a.account_number
           , a.phone_number
           , a.phone_area
           , a.city_state
           , a.address
           , a.account_name
          -- , b.electrical_id
           , a.supply_idx
           , a.address_building
           , SUBSTR(a.address_building,1,1) AS svc_center
           , c.meter_user_def_1 AS meter_location
        FROM CES_CUSTOMERS a, RVA_RESPONSE_LOAD_T b, CU_METERS c
       WHERE a.account_number = p_DEVICEID  
         AND c.meter_id = p_DEVICEID 
         AND b.restore_time = p_RESTORE_TIME 
         AND b.device_power_status = p_STATUS
         AND TO_NUMBER(RTRIM(b.utility_id)) = a.account_number
         AND TO_NUMBER(RTRIM(b.utility_id)) = c.meter_id;
      /* replaced check below due to data formats */
        -- and rtrim(b.utility_id, ' ') = rtrim(a.account_number, ' ') 
        -- and RTRIM( b.utility_id, ' ') = RTRIM(c.meter_id, ' '); 
  BEGIN
    /* on entry update the rva load table singles records to flag as 'R' meaning response from cellnet  has been processed */
    UPDATE RVA_RESPONSE_LOAD_T
       SET UPDATE_FLAG = 'R'
     WHERE order_number LIKE 'R%';

    DBMS_OUTPUT.PUT_LINE('BEGIN PROC');

    FOR rva_cnet_rt_rec IN rva_cnet_rt
    LOOP
     -- DBMS_OUTPUT.PUT_LINE('INSIDE OF LOOP variable assignment');

      DEVICE := RTRIM(rva_cnet_rt_rec.qDEVICEID);
      CNET_TIME := rva_cnet_rt_rec.qRESTORE_TIME;
      EVENT_TIME := rva_cnet_rt_rec.qBEGIN_TIME;
      STATUS := rva_cnet_rt_rec.qSTATUS;
      EVENT_ID := TO_CHAR(LTRIM(RTRIM(rva_cnet_rt_rec.qEVENT_IDX)));

      /* make sure that the event idx - outage - is set to processed on the control table,
       * meaning, the RECORD was already selected ON a PREVIOUS RUN AND there IS no need TO send it
       * back TO process again. Once the records makes it into the cotrol table and the flag is 'V' it means that
       * IT didn't missed that record on the file transfer. The rva_continue_run.sql was processed. */

      UPDATE CNET_RVA_RT_CTRL
         SET flag_rva = 'R'
       WHERE flag_rva = 'P'
         AND LTRIM(RTRIM(order_number)) = 'R'||LTRIM(RTRIM(event_id))||'cluster';

      /* enter data into the reporting table ... these files were processed too late by cellnet */
      /* insert into clause against the REPORTING TABLE */

      FOR valid_rva_rt_rec IN valid_rva_mtr_rt(DEVICE,CNET_TIME, STATUS,EVENT_ID, EVENT_TIME)
      LOOP
       -- DBMS_OUTPUT.PUT_LINE('INSIDE OF LOOP records');

        /* for testing fake ON as OU behavior */
        IF STATUS = 'OU' AND valid_rva_rt_rec.util_extra_3 >= '85'
        THEN
          /* lights OUT, PSR  range is satisfied - add notification to dispatcher that cellnet has confirmed the outage tc 1005*/
          INSERT INTO CES_CALLS ( account_number
                                , phone_number
                                , phone_area
                                , city_state
                                , address
                                , account_name
                                , device_cls
                                , device_idx
                                , trouble_code
                                , date_time
                                , ces_status
                                , trs_status
                                , empl_no
                                , svc_cntr
                                , meter_number
                                , meter_location
                                , process_time)
                         VALUES ( LPAD(RTRIM(DEVICE), 13, '0')
                                , valid_rva_rt_rec.phone_number
                                , valid_rva_rt_rec.phone_area
                                , valid_rva_rt_rec.city_state
                                , valid_rva_rt_rec.address
                                , valid_rva_rt_rec.account_name
                                , 994
                                , valid_rva_rt_rec.supply_idx
                                , '1005'
                                , EVENT_TIME
                                , 'NEW'
                                , 'NEW'
                                , 'RVA'
                                , valid_rva_rt_rec.svc_center
                                , LPAD(RTRIM(valid_rva_rt_rec.meter_number, ' '), 13, '0')
                                , valid_rva_rt_rec.meter_location
                                , SYSDATE);

         -- DBMS_OUTPUT.PUT_LINE('CNET Device number: '||valid_rva_rt_rec.account_number);

          COMMIT;
        END IF;

        IF STATUS = 'RO' AND valid_rva_rt_rec.util_extra_3 >= '85'
        THEN
          /* lights OUT, PSR  range is satisfied - add notification to dispatcher that cellnet has confirmed the outage tc 1005*/
          INSERT INTO CES_CALLS ( account_number
                                , phone_number
                                , phone_area
                                , city_state
                                , address
                                , account_name
                                , device_cls
                                , device_idx
                                , trouble_code
                                , date_time
                                , ces_status
                                , trs_status
                                , empl_no
                                , svc_cntr
                                , meter_number
                                , meter_location
                                , process_time)
                         VALUES ( LPAD(RTRIM(DEVICE), 13, '0')
                                , valid_rva_rt_rec.phone_number
                                , valid_rva_rt_rec.phone_area
                                , valid_rva_rt_rec.city_state
                                , valid_rva_rt_rec.address
                                , valid_rva_rt_rec.account_name
                                , 994
                                , valid_rva_rt_rec.supply_idx
                                , '1005'
                                , EVENT_TIME
                                , 'NEW'
                                , 'NEW'
                                , 'RVA'
                                , valid_rva_rt_rec.svc_center
                                , LPAD(RTRIM(valid_rva_rt_rec.meter_number, ' '), 13, '0')
                                , valid_rva_rt_rec.meter_location
                                , SYSDATE);

         -- DBMS_OUTPUT.PUT_LINE('CNET Device number: '||valid_rva_rt_rec.account_number);

          COMMIT;
        END IF; 

        --- TO DO for IMPLEMENTATION
        -- third pass, if meter status is anything else besides ON or OU and the psr is less than 85, 
        -- then I need to dump the data into the report table. Write reports for operations. 
        -- Once this is done, work the four  hour run - same as this, only every four hrs instead. refer to notes. 

        /* DBMS_OUTPUT.PUT_LINE('CNET Device number: '||valid_rva_rec.account_number);
         * DBMS_OUTPUT.PUT_LINE('CES Meter number: '||valid_rva_rec.meter_number); */

        IF (STATUS != 'OU' OR status != 'RO') AND valid_rva_rt_rec.util_extra_3 < '85'
        THEN
          INSERT INTO CNET_SINGLES_RPT ( account_number
                                       , phone_number
                                       , phone_area
                                       , city_state
                                       , address
                                       , account_name
                                       , device_cls
                                       , device_idx
                                       , trouble_code
                                       , date_time
                                       , ces_status
                                       , trs_status
                                       , empl_no
                                       , svc_cntr
                                       , meter_number
                                       , meter_location
                                       , process_time)
                                VALUES ( LPAD(RTRIM(DEVICE), 13, '0')
                                       , valid_rva_rt_rec.phone_number
                                       , valid_rva_rt_rec.phone_area
                                       , valid_rva_rt_rec.city_state
                                       , valid_rva_rt_rec.address
                                       , valid_rva_rt_rec.account_name
                                       , 994
                                       , valid_rva_rt_rec.supply_idx
                                       , '1000'
                                       , CNET_TIME
                                       , 'NEW'
                                       , 'NEW'
                                       , 'RVA'
                                       , valid_rva_rt_rec.svc_center
                                       , LPAD(RTRIM(valid_rva_rt_rec.meter_number, ' '), 13, '0')
                                       , valid_rva_rt_rec.meter_location
                                       , SYSDATE);

          --- TO DO for IMPLEMENTATION
          -- third pass, if meter status is anything else besides ON or OU and the psr is less than 85, 
          -- then I need to dump the data into the report table. Write reports for operations. 

          COMMIT;
        END IF;
      END LOOP; 
    END LOOP;

    DELETE FROM RVA_RESPONSE_LOAD_T
    WHERE order_number LIKE 'R%';

  END proc_rva_cnet;
END Pck_Rva_Cnet;
/

quit
