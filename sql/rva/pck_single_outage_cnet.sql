CREATE OR REPLACE PACKAGE Pck_Single_Outage_Cnet IS
	PROCEDURE  proc_single_outage_cnet;
END Pck_Single_Outage_Cnet;
/

CREATE OR REPLACE PACKAGE BODY Pck_Single_Outage_Cnet IS
  PROCEDURE proc_single_outage_cnet IS
    PICKUP_FLAG       VARCHAR2(1);
    EVENT_IDX_t       CHAR(25);
    EVENT_IDX_s       CHAR(25);
    DEVICE_ID_t       CHAR(20);
    UTILITY_ID_t      CHAR(25);
    OUTAGE_TIME_t     DATE;
    COLLECT_SINGLES_T DATE;

    CURSOR single_event_idx IS
      /* every ten minutes a cron job will be initiated on previously unchecked singles */
      SELECT begin_time, event_idx AS qEVENT_IDX
        FROM JOBS
       WHERE alarm_state IN ('NEW', 'UAS', 'ASN' )
         AND num_cust_out = 1
         AND devcls_name IS NOT NULL
         AND restore_time IS NULL;

    /* replace the sysdate -30 minutes with the following... */
    /* the event idx should not be on the control table, meaning, the record has not being requested before from cellnet */
    /* and the status of the control record must still a 'P' processsed by request, but not by response meaning exclude 'R' . */

/*
    (SELECT SUBSTR(order_number,2,6)
       FROM CNET_RVA_CTRL
      WHERE TO_CHAR(process_time, 'mm/dd/yy') >= '08/09/07'
        AND util_extra_3 >= 85)
*/

    CURSOR single_sp_idx (p_EVENT_IDX INTEGER) IS
      SELECT DISTINCT a.account_num AS qSP_ID /* select also outage time and enhance procedure to use it*/
        FROM INCIDENTS a
       WHERE a.event_idx = p_EVENT_IDX
         AND user_name <> 'CNET'
         AND NOT REGEXP_LIKE(user_name, '[0-9]{4}$')
         AND a.complaint LIKE '1%' /* take only lights out outages */
       ORDER BY a.account_num;
    /* enhance proc to pass golbal variable event_id */
    -- TODO add to the code:
    -- check for if the user_name != 'CNET'  for 10  minutes singles check
    -- if is an 4 hours report, then we need to check 'CNET'

    CURSOR single_sp_meter_id (p_SP_ID CHAR)IS
      SELECT supply_idx
           , h_cls
           , h_idx
           , account_name
           , address_building
           , address
           , city_state
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
       WHERE account_number = p_SP_ID
         AND meter_id IS NOT NULL
         AND meter_psr IS NOT NULL
         AND meter_number IS NOT NULL;

  BEGIN
    --- programatically control the next hit to the table in order to check when IT has gathered, or not, the previous record
    -- set sent from this table. If it has been gathered, delete contents and move on to instert into the control table and
    -- update flag_rva to 'V' as wel as process times.
    -- If last set of records has not been picked up by IT, then append the new hit of records to the existing table records 
    -- and create a new file to replace the one missed by IT  - must be less than 2000 records, waiting on queue at pdm. 
    -- Check new design flow diagram for more information. Also, note that the FLAG will be updated togehter with a process
    -- time date stamp. 
    --- Note that the flag can be U for unprocessed (initial value), V for Verified (when data file was transfered to cellnet by IT)
    -- P for Pending (IT didn't send the data file, and we had to recreate it) and R for Response received by Cellnet.
    -- When the record flag is R, the CES_CALLS table is then populated accordingle or any other process such as callback 
    -- emails are ready to execute. 

    COLLECT_SINGLES_T := (SYSDATE - 240/1440);

    --DELETE FROM CNET_SINGLES_REQ_T;
    --COMMIT;

    /* Four hours run recollects all of those outages left behind */
    /* IF COLLECT_SINGLES_T = (SYSDATE - 240/1440)  THEN */

    /* testing from MB - need data from production, so when this process runs every ten minutes I will aslo be collecting the 
     * new calls from production that are ten minutes old. REMOVE THIS CODE BEFORE IMPLEMENTATION ON PROD */
/*
    INSERT INTO CES_CALLS 
    SELECT ACCOUNT_NUMBER
         , PRIORITY
         , PHONE_NUMBER
         , PHONE_AREA
         , COMMENTS
         , CITY_STATE
         , ADDRESS
         , ACCOUNT_NAME
         , DEVICE_CLS
         , DEVICE_IDX
         , TROUBLE_CODE
         , CB_PHONE_NUMBER
         , CB_PHONE_AREA
         , ELEC_ADDR
         , DATE_TIME
         , CES_STATUS
         , TRS_STATUS
         , EMPL_NO
         , ACT_DATE_TIME
         , ACT_TYPE
         , SVC_CNTR
         , CALLBACK_SW
         , TOWN_CODE
         , METER_NUMBER
         , METER_LOCATION
         , EST_CALLBACK_SW
         , UPD_CALLBACK_SW
         , ESTIMATED_TIME
         , PREVIOUS_INCIDENT
         , RESTORATION_CALLBACK
         , PROCESS_TIME
         , FLAG_ST_LIGHTS
         , FLAG_TREES
      FROM CES_CALLS
     WHERE process_time BETWEEN (SYSDATE - 10/1440) AND SYSDATE;
    COMMIT; 
*/

    FOR single_event_idx_rec IN single_event_idx 
    LOOP
      /* there is nothing to process when the first cursor is empty; no single outages */
      EXIT WHEN single_event_idx%ROWCOUNT <= 0;

      EVENT_IDX_t:= single_event_idx_rec.qEVENT_IDX; 
      EVENT_IDX_s :=  'S'||LTRIM(RTRIM(event_idx_t))||'cluster';

      OUTAGE_TIME_t := single_event_idx_rec.begin_time;
      EXIT WHEN single_event_idx%ROWCOUNT >= 750;

      FOR single_sp_idx_rec IN single_sp_idx(single_event_idx_rec.qEVENT_IDX)
      LOOP
        UTILITY_ID_t:= single_sp_idx_rec.qSP_ID;

        FOR single_sp_meter_id_rec IN single_sp_meter_id(single_sp_idx_rec.qSP_ID)
        LOOP
          DEVICE_ID_t := single_sp_meter_id_rec.meter_number;

          IF device_id_t IS NOT NULL --AND (OUTAGE_TIME_t  BETWEEN (SYSDATE - 10/1440) AND SYSDATE)
          THEN 
            /* INSERT INTO the INTERIM TABLE */
            INSERT INTO CNET_SINGLES_REQ_T ( event_idx /* internally defined identifier  'S + event_id + machine name' */ 
                                           , request_type
                                           , device_type
                                           , device_id
                                           , utility_id /* service point id */
                                           , util_extra1 /* data holder must be outage time */
                                           , util_extra2 /* data holder must be event idx */
                                           , util_extra3 /* data holder to be defined */
                                           , restore_time
                                           , flag_rva
                                           , process_time)
                                    VALUES ( 'S'||LTRIM(RTRIM(event_idx_t))||'cluster'
                                           , 'IND'
                                           , 'M'
                                           , LTRIM(RTRIM(DEVICE_ID_t))
                                           , LPAD(LTRIM(RTRIM(UTILITY_ID_t, ' ')), 13, '0') /* service point id */
                                           , LTRIM(RTRIM(TO_CHAR(outage_time_t,'mmddyyhh24miss'))) /* data holder must must be time field from outage table */
                                           , LTRIM(RTRIM(event_idx_t)) /* data holder must be event idx */
                                           , LTRIM(RTRIM(single_sp_meter_id_rec.meter_psr)) /* data holder to be defined */
                                           , SYSDATE
                                           , 'N'
                                           , SYSDATE);

            COMMIT; 
          END IF; 

          IF device_id_t IS NOT NULL 
          THEN 
            INSERT INTO CNET_RVA_CTRL ( order_number
                                      , request_type
                                      , device_type
                                      , device_id
                                      , utility_id /* service point id */
                                      , util_extra_1 /* data holder must be outage time from supply log table */
                                      , util_extra_2 /* data holder must be event idx */
                                      , util_extra_3 /* data holder to be defined */
                                      , restore_time
                                      , flag_rva
                                      , process_time)
                               VALUES ( 'S'||LTRIM(RTRIM(event_idx_t))||'cluster'
                                      , 'IND'
                                      , 'M'
                                      , LTRIM(RTRIM(DEVICE_ID_t))
                                      , LPAD(LTRIM(RTRIM(UTILITY_ID_t, ' ')), 13, '0') /* service point id */
                                      , LTRIM(RTRIM(TO_CHAR(outage_time_t,'mmddyyhh24miss'))) /* data holder must must be time field from outage table */
                                      , LTRIM(RTRIM(event_idx_t)) /* data holder must be event idx */
                                      , LTRIM(RTRIM(single_sp_meter_id_rec.meter_psr)) /* data holder to be defined */
                                      , SYSDATE
                                      , 'U'
                                      , SYSDATE);

            COMMIT; 
          END IF; 

          /* test output */
          /*RDBMS_OUTPUT.PUT_LINE('S'||trim(event_idx_t)||'cluster'||','|| 'IND'||','||'M'||','||device_id_t||','||utility_id_t||','||TO_CHAR(outage_time_t,'mmddyyhh24miss')
           *                     ||','||event_idx_t||','||single_sp_meter_id_rec.meter_psr||','||TO_CHAR(SYSDATE,'MM/DD/YY HH24:MI:SS'));*/
        END LOOP;
      END LOOP;
    END LOOP; 
    COMMIT; 
  END proc_single_outage_cnet;
END Pck_Single_Outage_Cnet;
/

quit
