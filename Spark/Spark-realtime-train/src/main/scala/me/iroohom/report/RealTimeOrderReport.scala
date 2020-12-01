package me.iroohom.report

import me.iroohom.config.ApplicationConfig
import me.iroohom.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StringType}


/**
 * 实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，结果实时存储Redis数据库，维度如下：
 * - 第一、总销售额：sum
 * - 第二、各省份销售额：province
 * - 第三、重点城市销售额：city
 * "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
 */
object RealTimeOrderReport {
  /**
   * 实时统计销售总额
   *
   * @param orderStreamDF DF
   * @return
   */
  def reportAmtTotal(orderStreamDF: DataFrame): Unit = {
    val session: SparkSession = orderStreamDF.sparkSession
    import session.implicits._

    val resultStreamDF: DataFrame = orderStreamDF
      //对金额进行聚合累加统计并取别名为total_amt
      .agg(sum($"money").as("total_amt"))
      //VITAL： withColumn的第二个参数col必须调用其他的列计算得到，而是用lit()函数可以直接得到字面值
      .withColumn("total", lit("global"))

    //写出到redis并启动流式程序
    resultStreamDF
      .writeStream
      //更新模式，只要最新的销售总额
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      //输出到Redis
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .coalesce(1)
          //VITAL: 行转列
          .groupBy()
          .pivot($"total").sum("total_amt")
          .withColumn("type", lit("total"))
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("host", ApplicationConfig.REDIS_HOST)
          .option("port", ApplicationConfig.REDIS_PORT)
          .option("dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:money")
          .option("key.column", "type")
          .save()
      }
      .start()


  }


  /**
   * 实时统计每个省的总销售额
   *
   * @param orderStreamDF 订单流DF
   * @return
   */
  def reportAmtProvince(orderStreamDF: DataFrame): Unit = {
    val session = orderStreamDF.sparkSession
    import session.implicits._

    val resultStreamDF: DataFrame = orderStreamDF
      .groupBy($"province")
      .agg(sum($"money").as("total_amt"))

    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-province")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_PROVINCE_CKPT)
      //输出到Redis
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .coalesce(1)
          //VITAL: 行转列
          .groupBy()
          .pivot($"province").sum("total_amt")
          .withColumn("type", lit("province"))
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("host", ApplicationConfig.REDIS_HOST)
          .option("port", ApplicationConfig.REDIS_PORT)
          .option("dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:money")
          .option("key.column", "type")
          .save()
      }
      .start()


  }

  /**
   * 实时统计重点城市的总销售额
   *
   * @param orderStreamDF 订单流DF
   * @return
   */
  def reportAmtCity(orderStreamDF: DataFrame): Unit = {
    val session = orderStreamDF.sparkSession
    import session.implicits._

    val cities: Array[String] = Array(
      "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
    )

    //广播变量
    val citiesBroadcast: Broadcast[Array[String]] = session.sparkContext.broadcast(cities)

    //自定义UDF，用来判断城市是否是重点城市
    val city_is_contains: UserDefinedFunction = udf(
      (cityName: String) => citiesBroadcast.value.contains(cityName)
    )

    val resultStreamDF: DataFrame = orderStreamDF
      .filter(city_is_contains($"city"))
      .groupBy($"city")
      .agg(sum($"money").as("total_amt"))

    //输出Redis及启动流式程序
    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF
          .coalesce(1)
          //VITAL: 行转列
          .groupBy()
          .pivot($"city").sum("total_amt")
          .withColumn("type", lit("city"))
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("host", ApplicationConfig.REDIS_HOST)
          .option("port", ApplicationConfig.REDIS_PORT)
          .option("dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:money")
          .option("key.column", "type")
          .save()
      }
      .start()

  }


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //消费kafka
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    //提供数据字段
    val orderStreamDF: DataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(line => line != null && line.trim.split(",").length > 0)
      .select(
        get_json_object($"value", "$.orderMoney")
          //VITAL: 使用BigDecimal的方法，不是直接使用BigeDecimal() 为了防止精度丢失
          .cast(DataTypes.createDecimalType(10, 2)).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )

    orderStreamDF.printSchema()


    //实时报表统计：总销售额、各省份销售额及重点城市销售额
    reportAmtTotal(orderStreamDF)
    reportAmtProvince(orderStreamDF)
    reportAmtCity(orderStreamDF)


    spark.streams.active.foreach { query =>
      StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)
    }
  }
}
