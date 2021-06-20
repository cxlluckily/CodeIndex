package me.iroohom.spark.etl

import me.iroohom.spark.config.ApplicationConfig
import me.iroohom.spark.utils.{IpUtils, SparkUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import org.apache.spark.sql.functions._

/**
 * 广告数据进行ETL处理，具体步骤如下：
 * 第一步、加载json数据
 * 第二步、解析IP地址为省份和城市
 * 第三步、数据保存至Hive表
 * TODO: 基于SparkSQL中DataFrame数据结构，使用DSL编程方式
 */
object PmtEtlRunner {

  def processData(dataFrame: DataFrame): DataFrame = {
    val sparkSession = dataFrame.sparkSession

    /**
     * TODO: 优化 ：分布式缓存
     */
    sparkSession.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)

    //提取每条数据的中的IP调用工具类，进行解析转化成省份和城市
    //如果调用的是DataFrame中类似RDD转换函数，建议先将DF转换为RDD，再调用转换函数 TODO:DF属于弱类型，类型不安全

    val rowsRDD: RDD[Row] = dataFrame.rdd.mapPartitions { iter =>

      //TODO : 优化：针对每个进行IP地址操作，可以每个分区创建DbSearcher 此处必须使用文件名来获取
      val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))

      //val xx: Iterator[Row] = iter
      iter.map { row =>
        val ipValue = row.getAs[String]("ip")
        val region: Region = IpUtils.convertIpToRegion(ipValue, dbSearcher)
        //将省份和城市追加到原来的Row中
        //TODO: “.+” 方式
        val newSeq: Seq[Any] = row.toSeq :+ (region.province) :+ (region.city)
        //返回Row
        Row.fromSeq(newSeq)
      }
    }

    /**
     * 定义Schema信息 在原来的基础上加上province和city
     */
    val schema = dataFrame.schema
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)


    //    sparkSession.createDataFrame(rowsRDD, schema)

    val df = sparkSession.createDataFrame(rowsRDD, schema)
    //加入一个分区字段date_str，昨日的日期,保存Hive分区表时使用此字段
    df.withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
  }

  /**
   * 将DF中数据保存PARQUET文件中
   *
   * @param dataFrame DF
   */
  def saveAsParquet(dataFrame: DataFrame) = {
    //降低分区数
    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("date_str")
      .parquet("dataset/pmt-etl")
  }

  /**
   * 保存数据到Hive分区表
   *
   * @param dataFrame DF
   */
  def saveAsHiveTable(dataFrame: DataFrame) = {
    dataFrame
      //降低分区数，存储数据在1个文件中
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      //TODO: 一定要指定Hive数据源
      .format("hive")
      .partitionBy("date_str")
      .saveAsTable("roohom_ads.pmt_ads_info")
  }

  def main(args: Array[String]): Unit = {

    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //加载json数据
    val pmtDF: DataFrame = spark.read.json(ApplicationConfig.DATAS_PATH)
    //    pmtDF.printSchema()
    //    pmtDF.show(10, truncate = false)

    //加载
    val etlDF = processData(pmtDF)
    etlDF.printSchema()
    etlDF.show(10, truncate = false)
    //保存ETL数据到Hive分区表
    /**
     * 保存成parquet存入HDFS
     */
    saveAsParquet(etlDF)

    /**
     * 存到Hive表
     */
    saveAsHiveTable(etlDF)


    spark.stop()
  }
}
