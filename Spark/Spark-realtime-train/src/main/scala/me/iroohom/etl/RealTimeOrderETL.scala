package me.iroohom.etl

import me.iroohom.config.ApplicationConfig
import me.iroohom.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 订单数据实时ETL：实时从Kafka Topic 消费数据，进行过滤转换ETL，将其发送Kafka Topic，以便实时处理
 * TODO：基于StructuredStreaming实现，Kafka作为Source和Sink
 */
object RealTimeOrderETL {

  /**
   * 对流式数据进行 ETL
   *
   * @param dataFrame DF
   * @return
   */
  def streamingProcess(dataFrame: DataFrame): _root_.org.apache.spark.sql.DataFrame = {
    val session: SparkSession = dataFrame.sparkSession
    import dataFrame.sparkSession.implicits._


    val orderStreamDF: DataFrame = dataFrame
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(message => message != null && message.trim.length > 0)
      .select(
        get_json_object($"value", "$.orderId").as("orderId"),
        get_json_object($"value", "$.userId").as("userId"),
        get_json_object($"value", "$.orderTime").as("orderTime"),
        get_json_object($"value", "$.ip").as("ip"),
        get_json_object($"value", "$.orderMoney").as("orderMoney"),
        get_json_object($"value", "$.orderStatus").cast(IntegerType).as("orderStatus")
      )

    val filterStreamDF: Dataset[Row] = orderStreamDF.filter($"ip".isNotNull.and($"orderStatus" === 0))

    //自定义UDF函数：ip_to_location，调用第三方库ip2Region解析IP地址为省份和城市；
    //VITAL:分布式缓存
    session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    val ip_to_location: UserDefinedFunction = udf(
      (ip: String) => {

        //每查一个ip都需要创建一个DbSearcher 性能不好
        //        val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), ApplicationConfig.IPS_DATA_REGION_PATH)
        val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))

        val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)

        //获取解析的省份城市
        val region = dataBlock.getRegion

        val Array(_, _, province, city, _) = region.split("\\|")

        (province, city)
      }
    )
    val resultStreamDF: DataFrame = filterStreamDF
      .withColumn("region", ip_to_location($"ip"))
      //      .writeStream
      //      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
      //        if (!batchDF.isEmpty) {
      //          batchDF.show(10, truncate = false)
      //        }
      //      })
      //      .start()
      .withColumn("province", $"region._1")
      .withColumn("city", $"region._2")
      .select(
        //kafka中key
        $"orderId".as("key"),
        //将struct类型转换为json
        to_json(
          //先将列转换为Struct类型
          struct(
            $"orderId",
            $"userId",
            $"orderTime",
            $"ip",
            $"orderMoney",
            $"orderStatus",
            $"province",
            $"city"
          )
          //VITAL:字段名要写为value 如果不写或者写错位置会报 Required attribute 'value' not found 异常
        ).as("value")
      )
    resultStreamDF
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)

    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      //从orderTopic读取数据做ETL
      .option("subscribe", ApplicationConfig.KAFKA_SOURCE_TOPICS)
      .load()


    val etlStreamDF: DataFrame = streamingProcess(kafkaStreamDF.toDF())

    val query = etlStreamDF.writeStream
      //ETL数据需要追加
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      //做了ETL的结果数据写到kafka的orderEtlTopic
      .option("topic", ApplicationConfig.KAFKA_ETL_TOPIC)
      .option("checkpointLocation", ApplicationConfig.STREAMING_ETL_CKPT)
      .start()

    //    query.awaitTermination()
    //    query.stop()
    //配置设置文件自动停止，防止暴力停止程序而带来的不良影响
    StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_ETL_FILE)
  }

  //  1）、从Kafka获取消息Message，使用get_json_object函数提交订单数据字段；
  //  2）、过滤获取订单状态为打开（orderState == 0）订单数据；
  //  3）、自定义UDF函数：ip_to_location，调用第三方库ip2Region解析IP地址为省份和城市；
  //  4）、组合订单字段为struct类型，转换为json字符串；
}
