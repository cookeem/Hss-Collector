HSS-Collector
========
- 基于Akka Cluster2.4.17和Akka Stream2.4.17以及Redis和Kafka-0.10.1.1、elasticsearch5.2.0的ftp日志文件采集和记录入库集群
- 支持节点自动发现以及roundRobin负载分配, 支持节点故障任务自动转移, 支持分布式采集
- 支持ftp目录为三级目录,日志文件为包含header的RFC.RFC4180的csv文件的kafka入库


### ListFileWorker
- 获取目录下的文件列表,并且把新的文件列表写入到redis以及kafka

### GetFileWorker: 支持同时多个运行
- 从kafka读取文件列表,并把文件ftp下载下来,入库到kafka的记录topic中

### 启动相关依赖服务，zookeeper，kafka，elasticsearch，redis
```sh
cd /Volumes/Share/Scala_program/HssCollector

#启动redis-server
redis-server &

#启动zookeeper
zkServer.sh start
sleep 5
zkServer.sh status
sleep 5

#启动kafka
kafka-server-start.sh /Volumes/Share/hadoop/kafka-0.10.1.1/config/server-0.properties &

#启动elasticsearch
elasticsearch -d -Ecluster.name=es-cluster
sleep 10
curl localhost:9200
```

### 启动listfile。listfile用于获取ftp目录下的文件清单，并且把新的文件清单写入redis和kafka（可以启动多个）
```sh
java -classpath "hss-lib/*" cmgd.zenghj.hss.AppListFile
```

### 启动getfile。getfile用于从kafka读取文件清单，获取ftp文件，并且把文件写入到es（可以启动多个）

```sh
java -classpath "hss-lib/*" cmgd.zenghj.hss.AppGetFile
```

### 启动restful服务。restful为日志查询界面

```sh
java -classpath "hss-lib/*" cmgd.zenghj.hss.HssRestful
```
- 浏览器访问
```sh
http://localhost:9870
```

### 异常恢复说明:
- 当getfile节点出现异常的时候, 会导致出现异常的getfile节点已经从kafka获取的文件列表的处理会终止, 导致部分文件未处理.
- 此时,请关闭所有master listfile getfile节点, 重新按照运行说明启动所有节点即可恢复未处理文件的处理.
- master启动的时候会检查是否有文件从kafka读取了但是还没有来得及处理,然后把这些文件重新写回到kafka中.
- 启动getfile之后千万不要启动master,否则会出现文件重复处理


### 删除并重置所有数据

```sh
# 清除redis数据
echo 'keys *' | redis-cli
echo 'flushall' | redis-cli 

# 清除kafka数据
kafka-topics.sh --zookeeper localhost:2181 --list
kafka-topics.sh --zookeeper localhost:2181 --delete --topic hss-files-topic

# 清除elasticsearch数据
http DELETE localhost:9200/hss
```
