# 在k8s-hss上停止所有服务
cd /data/hss-cluster/docker-compose
docker-compose up -d

# 在k8s-hss上启动服务
cd /data/hss-cluster/docker-compose
docker-compose up -d

# 在k8s-hss上查看服务输出的日志状态
docker logs -f --tail 100 dockercompose_hss-listfile-0_1
docker logs -f --tail 100 dockercompose_hss-getfile-0_1
docker logs -f --tail 100 dockercompose_hss-getfile-1_1

# 在k8s-hss上重置相关数据

# 关闭相关服务的容器
docker stop dockercompose_hss-listfile-0_1
docker stop dockercompose_hss-getfile-0_1
docker stop dockercompose_hss-getfile-1_1
docker rm dockercompose_hss-listfile-0_1
docker rm dockercompose_hss-getfile-0_1
docker rm dockercompose_hss-getfile-0_1

# 清除redis数据
docker exec -ti dockercompose_redis-0_1 bash
echo 'KEYS *' | redis-cli -p 7000 -a changeme
echo 'FLUSHALL' | redis-cli -p 7000 -a changeme
exit

# 清除kafka数据
docker exec -ti dockercompose_kafka-0_1 bash
/kafka-bin/bin/kafka-console-producer.sh --broker-list kafka-0:9092 --topic test
/kafka-bin/bin/kafka-console-consumer.sh --bootstrap-server kafka-0:9092 --topic test --from-beginning

/kafka-bin/bin/kafka-topics.sh --zookeeper zk-0:2181 --list
/kafka-bin/bin/kafka-topics.sh --zookeeper zk-0:2181 --delete --topic hss-files-topic
exit

# 清除elasticseach数据
docker exec -ti dockercompose_es-0_1 bash
curl -u elastic:changeme -XDELETE localhost:9200/hss/?pretty
exit

# 在k8s-hss上关闭相关服务
cd /data/hss-cluster/docker-compose
docker-compose stop && docker-compose rm -f

