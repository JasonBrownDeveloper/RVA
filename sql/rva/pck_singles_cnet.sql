CREATE OR REPLACE PACKAGE Pck_Singles_Cnet IS
  PROCEDURE  proc_singles_cnet;
END Pck_Singles_Cnet;
/

CREATE OR REPLACE PACKAGE BODY Pck_Singles_Cnet IS
  PROCEDURE proc_singles_cnet IS
    DEVICE CHAR(13);
    CNET_TIME DATE;
    STATUS CHAR(3);
    EVENT_ID CHAR(25);

    CURSOR rva_cnet IS
      SELECT DISTINCT RTRIM(a.utility_id) AS qDEVICEID
           , a.restore_time AS qRESTORE_TIME
           , a.device_power_status AS qSTATUS
           , LTRIM(RTRIM(a.util_extra_2)) AS qEVENT_IDX
        FROM RVA_RESPONSE_LOAD_T a, JOBS b
       WHERE a.device_type ='M'
         AND a.UPDATE_FLAG = 'R'
         AND a.order_number LIKE 'S%'
         AND LTRIM(RTRIM(a.util_extra_2)) = TO_CHAR(LTRIM(RTRIM(b.event_idx)))
         AND b.alarm_state IN ('NEW', 'UAS', 'ASN')
         AND b.num_cust_out = 1
         AND b.devcls_name IS NOT NULL
         AND b.restore_time IS NULL;
      -- ORDER BY a.utility_id;

    CURSOR valid_rva_mtr(p_DEVICEID CHAR, p_RESTORE_TIME DATE, p_STATUS CHAR, p_EVENT_IDX  CHAR) IS
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
           , SUBSTR(a.address_building,1,8) AS address_building
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
     WHERE order_number LIKE 'S%';

    FOR rva_cnet_rec IN rva_cnet
    LOOP
      DEVICE := RTRIM(rva_cnet_rec.qDEVICEID);
      CNET_TIME := rva_cnet_rec.qRESTORE_TIME;
      STATUS := rva_cnet_rec.qSTATUS;
      EVENT_ID := TO_CHAR(LTRIM(RTRIM(rva_cnet_rec.qEVENT_IDX)));

      /* make sure that the event idx - outage - is set to processed on the control table,
       * meaning, the RECORD was already selected ON a PREVIOUS RUN AND there IS no need TO send it
       * back TO process again. Once the records makes it into the cotrol table and the flag is 'V' it means that
       * IT didn't missed that record on the file transfer. The rva_continue_run.sql was processed. */

      UPDATE CNET_RVA_CTRL
         SET flag_rva = 'R'
       WHERE flag_rva = 'P'
         AND LTRIM(RTRIM(order_number)) = 'S'||LTRIM(RTRIM(event_id))||'cluster';

      /* enter data into the reporting table ... these files were processed too late by cellnet */
      /* insert into clause against the REPORTING TABLE */

      FOR valid_rva_rec IN valid_rva_mtr(DEVICE,CNET_TIME, STATUS,EVENT_ID)
      LOOP
        IF STATUS='ON' --AND valid_rva_rec.util_extra_3 >= '85'
        THEN
          /* Cancel Call when the single meter is ON tc 2003*/
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
                                , valid_rva_rec.phone_number
                                , valid_rva_rec.phone_area
                                , valid_rva_rec.city_state
                                , valid_rva_rec.address
                                , valid_rva_rec.account_name
                                , 994
                                , valid_rva_rec.supply_idx
                                , '1003'
                                , CNET_TIME
                                , 'NEW'
                                , 'NEW'
                                , 'RVA'
                                , valid_rva_rec.svc_center
                                , LPAD(RTRIM(valid_rva_rec.meter_number, ' '), 13, '0')
                                , valid_rva_rec.meter_location
                                , SYSDATE);
        END IF;

        IF STATUS='OU' AND valid_rva_rec.util_extra_3 >= '85'
        THEN
          /* lights OUT - per DSO send outage only to report table - changes from  original spec to create new outage 05/2008 */
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
                                       , valid_rva_rec.phone_number
                                       , valid_rva_rec.phone_area
                                       , valid_rva_rec.city_state
                                       , valid_rva_rec.address
                                       , valid_rva_rec.account_name
                                       , 994
                                       , valid_rva_rec.supply_idx
                                       , '1000'
                                       , CNET_TIME
                                       , 'NEW'
                                       , 'NEW'
                                       , 'RVA'
                                       , valid_rva_rec.svc_center
                                       , LPAD(RTRIM(valid_rva_rec.meter_number, ' '), 13, '0')
                                       , valid_rva_rec.meter_location
                                       , SYSDATE);
        END IF;

        IF STATUS='RO' AND valid_rva_rec.util_extra_3 >= '85'
        THEN
          /* RESTORED- OUT, PSR  range is satisfied - add notification to dispatcher that cellnet has confirmed the outage tc 1005*/
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
                                , valid_rva_rec.phone_number
                                , valid_rva_rec.phone_area
                                , valid_rva_rec.city_state
                                , valid_rva_rec.address
                                , valid_rva_rec.account_name
                                , 994
                                , valid_rva_rec.supply_idx
                                , '1005'
                                , CNET_TIME
                                , 'NEW'
                                , 'NEW'
                                , 'RVA'
                                , valid_rva_rec.svc_center
                                , LPAD(RTRIM(valid_rva_rec.meter_number, ' '), 13, '0')
                                , valid_rva_rec.meter_location
                                , SYSDATE);

          --- TO DO for IMPLEMENTATION
          -- third pass, if meter status is anything else besides ON or OU and the psr is less than 85, 
          -- then I need to dump the data into the report table. Write reports for operations. 
          -- Once this is done, work the four  hour run - same as this, only every four hrs instead. refer to notes. 

          /* DBMS_OUTPUT.PUT_LINE('CNET Device number: '||valid_rva_rec.account_number);
           * DBMS_OUTPUT.PUT_LINE('CES Meter number: '||valid_rva_rec.meter_number); */
        END IF;

        IF STATUS='NA' OR valid_rva_rec.util_extra_3 < '85'
        THEN
          /* lights OUT, PSR  range is satisfied - add notification to dispatcher that cellnet has confirmed the outage tc 1005*/
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
                                       , valid_rva_rec.phone_number
                                       , valid_rva_rec.phone_area
                                       , valid_rva_rec.city_state
                                       , valid_rva_rec.address
                                       , valid_rva_rec.account_name
                                       , 994
                                       , valid_rva_rec.supply_idx
                                       , '1000'
                                       , CNET_TIME
                                       , 'NEW'
                                       , 'NEW'
                                       , 'RVA'
                                       , valid_rva_rec.svc_center
                                       , LPAD(RTRIM(valid_rva_rec.meter_number, ' '), 13, '0')
                                       , valid_rva_rec.meter_location
                                       , SYSDATE);

          --- TO DO for IMPLEMENTATION
          -- third pass, if meter status is anything else besides ON or OU and the psr is less than 85, 
          -- then I need to dump the data into the report table. Write reports for operations. 
          -- Once this is done, work the four  hour run - same as this, only every four hrs instead. refer to notes. 

          /* DBMS_OUTPUT.PUT_LINE('CNET Device number: '||valid_rva_rec.account_number);
           * DBMS_OUTPUT.PUT_LINE('CES Meter number: '||valid_rva_rec.meter_number); */
        END IF;
      END LOOP; 
    END LOOP;

    DELETE FROM RVA_RESPONSE_LOAD_T
     WHERE order_number LIKE 'S%';

  END proc_singles_cnet;
END Pck_Singles_Cnet;
/

quit
