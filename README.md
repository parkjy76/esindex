# Bulk Indexing for Elasticsearch with ZeroMQ - PHP
* high performance bulk indexing for elasticsearch
* parallel pipeline processing with zeromq
 - push-pull pattern - ventilator, worker, sink
 - pub-sub pattern for handling signal
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
 -x No (re)create index for updating and deleting  

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

## Index Toggling
-x オプションを使うと指定したindexに対してbulk処理を行う  
指定されていない場合は（基本動作）はindexをフル更新する。  
フル更新のメカニズムは新しいindexを作って既存のindexをcloseする。  
index名の後ろに0または1を付け0と1間のtoggleで処理を行うためaliasを使わなければいけない。  
<p align="center"><img src="http://blogfiles.naver.net/20140306_124/parkjy76_13940786232946Tk1H_GIF/alias1.gif" alt="index toggling"></p>
