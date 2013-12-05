nobots_weblogs = LOAD '/user/kevin_gwdong/input/pigtutorial/apache_nobots_tsv.txt/' AS (ip:chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

ordered_weblogs = ORDER nobots_weblogs BY timestamp;

STORE ordered_weblogs INTO 'pig/ordered_weblogs';