package me.iroohom.report

import me.iroohom.config.ApplicationConfig
import me.iroohom.utils.{JedisUtils, SparkUtils, StreamingUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StringType}
import redis.clients.jedis.Jedis

object DailyRealTimeOrderReport {

  /**
   * 保存数据到Redis
   *
   * @param batchDF
   */
  def saveToRedis(batchDF: DataFrame): Unit = {
    batchDF
      .coalesce(1)
      //Operate on RDD
      .rdd
      .foreachPartition { iter =>
        val jedis: Jedis = JedisUtils.getJedisPoolInstance(
          ApplicationConfig.REDIS_HOST, ApplicationConfig.REDIS_PORT.toInt
        ).getResource

        jedis
          .select(ApplicationConfig.REDIS_DB.toInt)

        iter.foreach { row =>
          val redisKey = row.getAs[String]("key")
          val redisField = row.getAs[String]("field")
          val redisValue = row.getAs[String]("value")
          jedis.hset(redisKey, redisField, redisValue)
        }

        JedisUtils.release(jedis)

      }
  }

  /**
   * 实时统计总销售额
   * {"orderId":"20201128205257012000001","userId":"500000976","orderTime":"2020-11-28 20:52:57.012","ip":"106.89.162.33",
   * "orderMoney":"452.09","orderStatus":0,"province":"重庆","city":"重庆市"}
   *
   * @param orderStreamDF 订单流DF
   * @return
   */
  def reportAmtTotal(orderStreamDF: DataFrame): Unit = {
    val session: SparkSession = orderStreamDF.sparkSession
    import session.implicits._

    val resultStreamDF: Dataset[Row] = orderStreamDF
      //设置水位线，清楚过时状态数据
      .withWatermark("order_timestamp", "10 minutes")
      //每日实时统计，按照日期分组
      .groupBy($"order_date")
      .agg(sum($"money").as("total_amt"))
      //Redis中HASH结构Value字段
      .select($"total_amt".cast(StringType).as("value"), $"order_Date")
      //redis中Hash结构Field字段
      .withColumn("field", lit("global"))
      //redis中hash结构key值子弹
      .withColumn("prefix", lit("orders:money:total"))
      .withColumn("key", concat($"prefix", lit(":"), $"order_date"))


    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        saveToRedis(batchDF)
      }
      .start()
  }


  def reportAmtProvince(orderStreamDF: DataFrame) = {
    val session = orderStreamDF.sparkSession
    import session.implicits._


    val resultStreamDF: DataFrame = orderStreamDF
      //设置水位线过滤过时状态的数据
      .withWatermark("order_timestamp", "10 minutes")
      //以订单日期和省份分组
      .groupBy($"order_date", $"province")
      //聚合money字段，并别名为total_amt
      .agg(sum($"money").as("total_amt"))
      .select(
        $"total_amt".cast(StringType).as("value"),
        $"province".cast(StringType).as("field"),
        $"order_date"
      )
      .withColumn("prefix", lit("orders:money:province"))
      .withColumn("key", concat($"prefix", lit(":"), $"order_date"))

    //输出至Redis
    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt_province")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_PROVINCE_CKPT)
      .foreachBatch((batchDF: DataFrame, batchId: Long) =>
        saveToRedis(batchDF)
      )
      .start()

  }


  def reportAmtCity(orderStreamDF: DataFrame): Unit = {
    // a. 导入隐式转换
    val session = orderStreamDF.sparkSession
    import session.implicits._

    // 重点城市：9个城市
    val cities: Array[String] = Array(
      "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
    )

    //变量广播
    val citiesBroadcast: Broadcast[Array[String]] = session.sparkContext.broadcast(cities)
    val city_is_contains: UserDefinedFunction = udf(
      (cityName: String) => citiesBroadcast.value.contains(cityName)
    )

    val resultStreamDF: DataFrame = orderStreamDF
      .filter(city_is_contains($"city"))
      .withWatermark("order_timestamp", "10 minutes")
      .groupBy($"order_date", $"city")
      .agg(
        sum($"money").as("total_amt")
      )
      .select(
        $"total_amt".cast(StringType).as("value"),
        $"city".as("field"),
        $"order_date"
      )
      .withColumn("prefix", lit("orders:money:city"))
      .withColumn("key", concat($"prefix", lit(":"), $"order_date"))

    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      // 结果输出到Redis
      .foreachBatch { (batchDF: DataFrame, _: Long) => saveToRedis(batchDF) }
      .start() // 启动start流式应用

  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()


    val orderStreamDF: DataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(record => null != record && record.trim.length > 0)
      .select(
        //获取每个订单的日期用于分组
        to_date(get_json_object($"value", "$.orderTime")).as("order_date"),
        //获取每个订单日期时间，类型是Timestamp,设置水位线watermark
        to_timestamp(get_json_object($"value", "$.orderTime")).as("order_timestamp"),
        get_json_object($"value", "$.orderMoney").cast(DataTypes.createDecimalType(10, 2)).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )


    //报表统计
    reportAmtTotal(orderStreamDF)
    reportAmtProvince(orderStreamDF)
    reportAmtCity(orderStreamDF)


    spark.streams
      .active
      .foreach { query =>
        //设置不暴力关停程序，而是监听目录下的文件来优雅停止程序
        StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)
      }
  }
}
