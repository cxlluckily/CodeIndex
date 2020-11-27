package me.iroohom.spark.utils

import me.iroohom.spark.config.ApplicationConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * 构建SparkSession实例对象工具类，加载配置属性
 */
object SparkUtils {

  /**
   * 构建SparkSession实例对象
   *
   * @return SparkSession实例
   */
  def createSparkSession(clazz: Class[_]): SparkSession = {
    //构建SparkConf对象、设置通用相关属性
    val sparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))
      // 设置输出文件算法
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")


    //判断是否是本地模式，如果是就设置master
    if (ApplicationConfig.APP_LOCAL_MODE) {
      sparkConf.setMaster(ApplicationConfig.APP_SPARK_MASTER)
        .set("spark.sql.shuffle.partitions", "4")
    }

    //创建Builder对象传递SparkConf
    val builder: SparkSession.Builder = SparkSession.builder()
      .config(sparkConf)

    //
    if (ApplicationConfig.APP_IS_HIVE) {
      builder
        .enableHiveSupport()
        .config("hive.metastore.uris", ApplicationConfig.APP_HIVE_META_STORE_URLS)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    builder.getOrCreate()
  }

  /**
   * 验证程序正确性
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val session = createSparkSession(this.getClass)

    println(session.getClass.getSimpleName)

//    Thread.sleep(10000000)
    session.stop()
  }
}
