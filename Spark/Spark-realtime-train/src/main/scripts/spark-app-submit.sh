# 提交Spark Application，设置启用动态资源管理
$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 5g \
--driver-cores 5 \
--executor-memory 10g \
--executor-cores 5 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=5 \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=15 \
--conf spark.dynamicAllocation.executorIdleTimeout=300s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=10s \
--class cn.roohom.spark.report.DailyRealTimeOrderReport \
order-apps.jar

