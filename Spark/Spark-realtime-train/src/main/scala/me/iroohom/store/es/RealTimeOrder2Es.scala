package me.iroohom.store.es

import me.iroohom.config.ApplicationConfig
import me.iroohom.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
 * StructuredStreaming 实时消费Kafka Topic中数据，存入到Elasticsearch索引中
 */
object RealTimeOrder2Es {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)

    import spark.implicits._


    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()


    // 从JSON字符串中提取各个字段值，采用from_json函数
    /*
      {
        "orderId": "20201128145020837000002",
        "userId": "400000843",
        "orderTime": "2020-11-28 14:50:20.837",
        "ip": "123.235.249.37",
        "orderMoney": "65.04",
        "orderStatus": 0,
        "province": "山东省",
        "city": "青岛市"
      }
     */

    val orderSchema: StructType = new StructType()
      .add("orderId", StringType, nullable = true)
      .add("userId", StringType, nullable = true)
      .add("orderTime", StringType, nullable = true)
      .add("ip", StringType, nullable = true)
      .add("orderMoney", StringType, nullable = true)
      .add("orderStatus", StringType, nullable = true)
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)

    //提取值过滤字段
    val orderStreamDF: DataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      //转换成Dataset
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      //def from_json(e: Column, schema: StructType): Column
      //as("order")写在外面报：Caused by: org.elasticsearch.hadoop.EsHadoopIllegalArgumentException: [DataFrameFieldExtractor for field [[orderId]]] cannot extract value from entity [class java.lang.String] | instance [([[20201128200144172000001,400000622,2020-11-28 20:01:44.172,106.84.121.115,422.04,0,重庆,重庆市]],StructType(StructField(jsontostructs(value),StructType(StructField(orderId,StringType,true), StructField(userId,StringType,true), StructField(orderTime,StringType,true), StructField(ip,StringType,true), StructField(orderMoney,StringType,true), StructField(orderStatus,StringType,true), StructField(province,StringType,true), StructField(city,StringType,true)),true)))]
      .select(from_json($"value", orderSchema).as("order"))
      .select($"order.*")


    val query = orderStreamDF
      .writeStream
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", ApplicationConfig.STREAMING_ES_CKPT)
      .format("es")
      .option("es.nodes", ApplicationConfig.ES_NODES)
      .option("es.port", ApplicationConfig.ES_PORT)
      .option("es.index.auto.create", ApplicationConfig.ES_INDEX_AUTO_CREATE)
      .option("es.write.operation", ApplicationConfig.ES_WRITE_OPERATION)
      .option("es.mapping.id", ApplicationConfig.ES_MAPPING_ID)
      .start(ApplicationConfig.ES_INDEX_NAME)


    //设置扫描文件来优雅的关闭流式程序，防止暴力关闭而带来的负面影响
    StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_ES_FILE)

  }
}
