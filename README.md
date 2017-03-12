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

``hss-neName:dirs -> set(dir)``

记录该目录最后一个文件的名字,新添加的文件必须大于该文件名字:

``hss-neName:dir:SZHHSS07/188.2.138.22:lastfile -> string(filename)``

记录该目录未处理的文件名,已处理的自动从集合删除:

``hss-neName:dir:SZHHSS07/188.2.138.22:files -> set(filename)``

记录该目录正在处理的文件名(只要从kafka中读取了就表示进入处理中的队列),已处理的自动从集合删除:

``hss-neName:processingfiles -> set(dir:filename)``


## 集群名称: hss-cluster
## CollectorMaster: (collector-master)
用于获取所有目录列表,并且把目录列表任务分发到各个ListFileWorker
## ListFileWorker: (listfile-worker)
获取目录下的文件列表,并且把新的文件列表写入到redis以及kafka
## GetFileWorker: (getfile-worker)
从kafka读取文件列表,并把文件ftp下载下来,入库到kafka的记录topic中

# 启动相关依赖服务，zookeeper，kafka，elasticsearch，redis
cd /Volumes/Share/Scala_program/HssCollector
redis-server &
zkServer.sh start
kafka-server-start.sh /Volumes/Share/hadoop/kafka-0.10.1.1/config/server-0.properties &
elasticsearch -d -Ecluster.name=es-cluster
sleep 10
curl localhost:9200

# 启动master。master用于获取ftp目录下的文件夹（注意，只能启动一个）

sbt "run-main cmgd.zenghj.hss.HssCollector -s master -d download"

# 启动listfile。listfile用于获取ftp目录下的文件清单，并且把新的文件清单写入redis和kafka（可以启动多个）

sbt "run-main cmgd.zenghj.hss.HssCollector -s listfile"

# 启动getfile。getfile用于从kafka读取文件清单，获取ftp文件，并且把文件写入到es（可以启动多个）

sbt "run-main cmgd.zenghj.hss.HssCollector -s getfile -d download-0"

## 异常恢复说明:
当getfile节点出现异常的时候, 会导致出现异常的getfile节点已经从kafka获取的文件列表的处理会终止, 导致部分文件未处理.

此时,请关闭所有master listfile getfile节点, 重新按照运行说明启动所有节点即可恢复未处理文件的处理.

master启动的时候会检查是否有文件从kafka读取了但是还没有来得及处理,然后把这些文件重新写回到kafka中.

## 启动getfile之后千万不要启动master,否则会出现文件重复处理



## 1. 运行
sbt "run-main cmgd.zenghj.hss.HssRestful"

## 2. 浏览器访问
``http://localhost:9870``

## 3. 重置

echo 'keys *' | redis-cli
kafka-console-producer.sh --topic hss-files-topic --broker-list localhost:9092
kafka-console-consumer.sh --from-beginning --topic hss-files-topic --bootstrap-server localhost:9092
http DELETE localhost:9200/hss
kafka-topics.sh --zookeeper localhost:2181 --list
kafka-topics.sh --zookeeper localhost:2181 --delete --topic hss-files-topic
echo 'flushall' | redis-cli 

# 关闭相关服务
# 启动相关依赖服务，zookeeper，kafka，elasticsearch，redis
cd /Volumes/Share/Scala_program/HssCollector
ps -ef | grep redis-server
jps
kafka-server-stop.sh
zkServer.sh stop
