REGISTER sessionize.jar;
DEFINE Sessionize my.tutorial.lesson7.pig.sessionize.Sessionize('1800');

nobots_weblogs = LOAD '/user/kevin_gwdong/input/pigtutorial/apache_nobots_tsv.txt' AS (ip:chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

ip_groups = GROUP nobots_weblogs BY ip;

sessions = FOREACH ip_groups { 
	ordered_by_timestamp = ORDER nobots_weblogs BY timestamp;		
	GENERATE FLATTEN(Sessionize(ordered_by_timestamp));
}

STORE sessions INTO 'pig/sessionize';