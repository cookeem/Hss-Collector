HSS-Collector
========
基于Akka Cluster2.4.8和Akka Stream2.4.8以及Redis和Kafka-0.9.0.1的ftp日志文件采集和记录入库集群
支持节点自动发现以及roundRobin负载分配, 支持节点故障任务自动转移, 支持分布式采集
支持ftp目录为二级目录,日志文件为包含header的RFC.RFC4180的csv文件的kafka入库

## 支持无阻塞式的异步处理

## 支持服务异常关闭恢复文件处理

## 支持集群故障发现以及故障转移

## worker支持分布式处理


## redis记录格式
记录总共有多少目录:

``hss:dirs -> set(dir)``

记录该目录最后一个文件的名字,新添加的文件必须大于该文件名字:

``hss:dir:SZHHSS07/188.2.138.22:lastfile -> string(filename)``

记录该目录未处理的文件名,已处理的自动从集合删除:

``hss:dir:SZHHSS07/188.2.138.22:files -> set(filename)``

记录该目录正在处理的文件名(只要从kafka中读取了就表示进入处理中的队列),已处理的自动从集合删除:

``hss:processingfiles -> set(dir:filename)``


## 集群名称: hss-cluster
## CollectorMaster: (collector-master)
用于获取所有目录列表,把目录列表写入到redis,并且把目录列表分发到各个CollectorRouter
## CollectorRouter: (collector-router)
作为ListFileWorker的封装router,分发文件列表任务到ListFileWorker
## ListFileWorker: (listfile-router)
获取目录下的文件列表,并且把新的文件列表写入到redis以及kafka
## GetFileWorker: (getfile-worker)
从kafka读取文件列表,并把文件ftp下载下来,入库到kafka的记录topic中

## 运行说明:
2551 和 2552 端口为akka集群的seed nodes端口, 务必先启动

1. 启动master以及router。master用于获取ftp目录下的文件夹，router用于获取ftp目录下的文件列表清单。（注意，只能启动一个）

`` sbt "run-main cmgd.zenghj.hss.HssCollector -s master" ``

2. 启动worker,获取ftp文件,并且把文件进行记录入库kafka. worker可以开多个集群进程, 提升入库效率。

`` sbt "run-main cmgd.zenghj.hss.HssCollector -s worker" ``

## 异常恢复说明:
当worker节点出现异常的时候, 会导致出现异常的worker节点已经从kafka获取的文件列表的处理会终止, 导致部分文件未处理.

此时,请关闭所有master router worker节点, 重新按照运行说明启动所有节点即可恢复未处理文件的处理.

master启动的时候会检查是否有文件从kafka读取了但是还没有来得及处理,然后把这些文件重新写回到kafka中.

## 启动worker之后千万不要启动master,否则会出现文件重复处理



## 1. 运行
``sbt "run-main cmgd.zenghj.hss.HssRestful"``

## 2. 浏览器访问
``http://localhost:9870``

## 3. 重置
``
http DELETE localhost:9200/hss

kafka-topics.sh --zookeeper localhost:2181 --list
kafka-topics.sh --zookeeper localhost:2181 --delete --topic hss-files-topic

./create-cluster stop
./create-cluster clean

./create-cluster start
echo 'yes' | ./create-cluster create
``