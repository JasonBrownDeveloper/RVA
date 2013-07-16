OPTIONS (ERRORS=3)
LOAD DATA
APPEND
INTO TABLE RVA_RESPONSE_LOAD_T
WHEN REQUEST_TYPE = 'IND' AND DEVICE_TYPE = 'M'
(order_number	position(01:25) char
, request_type	position(26:28) char
, device_type	position(29:29) char
, req_device_id  position(30:49) char  
, resp_device_id position(50:69) char
, utility_id 	position(70:94) char
, restore_time	position(95:111) date 'MM/DD/YY HH24:MI:SS' 
, util_extra_1 	position(112:136) char
, util_extra_2 	position(137:161) char
, util_extra_3 	position(162:186) char
, process_time  position(187:203) date 'MM/DD/YY HH24:MI:SS'
, last_packet_time position(204:220) date 'MM/DD/YY HH24:MI:SS'
, last_gasp_time position(221:237) date 'MM/DD/YY HH24:MI:SS'
, device_power_status position(238:239) char 
, error_msg position(240:319) char
, update_flag position(320:320) char 
)
