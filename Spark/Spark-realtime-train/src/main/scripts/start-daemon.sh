
# Start HDFS
hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode

# Start YARN
yarn-daemon.sh start resourcemanager
yarn-daemon.sh start nodemanager

# Start MRHistoryServer
mr-jobhistory-daemon.sh start historyserver

# Start Spark HistoryServer
/export/server/spark/sbin/start-history-server.sh

# Start Zookeeper
zookeeper-daemon.sh start

# Start Kafka
kafka-daemon.sh start

# Start HBase
hbase-daemon.sh start master
hbase-daemon.sh start regionserver

# Start Elasticsearch
elasticsearch-daemon.sh start

# Start Redis
/export/server/redis/bin/redis-server /export/server/redis/conf/redis.conf

