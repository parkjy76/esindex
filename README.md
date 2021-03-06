# Bulk Indexing for Elasticsearch with ZeroMQ - PHP
* high performance bulk indexing for elasticsearch
* parallel pipeline processing with zeromq
 - push-pull pattern - ventilator, worker, sink
 - pub-sub pattern for handling signal
* worker transfer indexing data to elsaticsearch by round robin
* use PCNTL on PHP 
* river, mapper
* index toggling

## Requirements
+ PHP 5.5+
+ ZeroMQ 2.X
+ ZeroMQ PHP binding module

## Usage
Usage: bulk [OPTIONS]  
 -t <string> Task type  
 -w <number> Number of workers. Defaults to 4  
 -q <number> Size of queue. Defaults to 250  
 -r Release LOCK  
 -x No (re)create index for updating and deleting  

    [parkjy76@storm]$ bulk -t test
    [XXXX-XX-XX XX:XX:XX] 22807(22807) test::ventilator::Sending tasks to 4 workers
    [XXXX-XX-XX XX:XX:XX] 22814(22807) test::worker2::Tasks start
    [XXXX-XX-XX XX:XX:XX] 22811(22807) test::worker1::Tasks start
    [XXXX-XX-XX XX:XX:XX] 22817(22807) test::worker3::Tasks start
    [XXXX-XX-XX XX:XX:XX] 22820(22807) test::worker4::Tasks start
    [XXXX-XX-XX XX:XX:XX] 22807(22807) test::ventilator::Sent tasks(XXXXX)
    [XXXX-XX-XX XX:XX:XX] 22811(22807) test::worker1::Tasks end
    [XXXX-XX-XX XX:XX:XX] 22817(22807) test::worker3::Tasks end
    [XXXX-XX-XX XX:XX:XX] 22820(22807) test::worker4::Tasks end
    [XXXX-XX-XX XX:XX:XX] 22814(22807) test::worker2::Tasks end
    [XXXX-XX-XX XX:XX:XX] 22808(22807) test::sink::Total(XXXXX) elapsed time: X.XXXXXXXXX sec

## Settings
base directory is etc/. settings are classified by Task type
* es.ini - elasticsearch settings
 - hosts - multiple host(ip:port), seperator is ,
 - alias - index alias
 - baseindex - base index
 - schema - index schema
 - param(eters) - elasticsearch option parameters(GET)
* mapper.ini - schema mapper settings
 - name - mapper name, match .php in module/mapper/
* river.ini - river settings
 - driver - mysql, mongo etc. match .php in module/river/
 - host - river host of dsn
 - port - river port of dsn
 - dbname - database name etc. (optional]
 - user - username
 - passwd - password
 - charset - character set
 - query - query file. sql etc.
* zmq.ini - zeromq access point(include protocol) settings
 - ventilator - ventilator access point
 - worker - worker access point
 - sink - sink access point
 - controller - controller access point

## Test
Indexing Server(VM) : 8 core, 4GB  
Elasticsearch Server(VM) : 8 core, 12GB(Heap Mem:6G) * 4, RAID6  



## Index Toggling and Alias
if you use -x option, the bulk indexing is performed to defined index.  
unless you use -x option, it creates new index and the bulk indexing(full indexing) is performed to new one.  
full indexing mechanism :  
if new index created, it closes old one  
when creating index, index's suffix append a number(0 or 1)  
if current index's suffix is 0, it creates 1. - index toggling  
so you should use index-alias of elasticsearch because the index's name is changed every indexing times.  
request to index-alias.
<p align="center"><img src="http://blogfiles.naver.net/20140306_124/parkjy76_13940786232946Tk1H_GIF/alias1.gif" alt="index toggling"></p>
