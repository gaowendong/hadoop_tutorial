set mapred.cache.files '/user/kevin_gwdong/input/pigtutorial/blacklist.txt#blacklist';
set mapred.create.sysmlink 'yes';

REGISTER ./myudfjar.jar;

all_weblogs = LOAD '/user/kevin_gwdong/output/apache_clf/part*' AS (ip:chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

nobots_weblogs = FILTER all_weblogs BY NOT my.tutorial.lesson6.pigfilter.IsUseragentBot(useragent);

STORE nobots_weblogs INTO '/user/kevin_gwdong/pig/nobots';