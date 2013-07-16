LOAD DATA
APPEND
INTO TABLE RVA_WORK
( event_idx	position(01:25) char
, meter_id 	position(70:94) char
, message_rx	position(01:320) char
, received      SYSDATE
, util_extra_1 	position(112:136) char
, util_extra_2 	position(137:161) char
, util_extra_3 	position(162:186) char
, last_packet_time position(204:220) date 'MM/DD/YY HH24:MI:SS'
, device_power_status position(238:239) char 
)
