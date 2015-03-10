# Bulk Indexing for Elasticsearch with ZeroMQ - PHP
* high performance bulk indexing for elasticsearch
* parallel pipeline processing
* push-pull pattern of zeromq (ventilator, worker, sink)
* pub-sub pattern of zeromq for handling signal
* river, mapper
* index toggling

## Index Toggling
-x オプションを使うと指定したindexに対してbulk処理を行う  
指定されていない場合は（基本動作）はindexをフール更新する。  
フール更新のメカニズムは新しいindexを作って既存のindexをcloseする。  
index名の後ろに0または1を付け0と1間のtoggleで処理を行うためaliasを使わなければいけない。  
<p align="center"><img src="http://blogfiles.naver.net/20140306_124/parkjy76_13940786232946Tk1H_GIF/alias1.gif" alt="index toggling"></p>

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
...
