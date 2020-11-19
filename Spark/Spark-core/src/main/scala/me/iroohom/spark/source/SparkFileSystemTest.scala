package me.iroohom.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 从HDFS/LocalFS文件系统加载文件数据，封装为RDD集合, 可以设置分区数目
 * * - 从文件系统加载：sc.textFile("")
 * * - 保存文件系统： rdd.saveAsTextFile("")
 */

object SparkFileSystemTest {
  def main(args: Array[String]): Unit = {
    /**
     * 创建sparkconf对象，设置应用的配置信息
     */
    val sc: SparkContext = {

      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    /**
     * 从文件系统加载数据，创建RDD数据集
     */
    val inputRDD: RDD[String] = sc.textFile("datas\\wordcount.data", minPartitions = 2)
    println(s"Partitions Number :${inputRDD.getNumPartitions}")


    /**
     * 调用集合RDD中函数处理分析处理
     */
    val resultRDD: RDD[(String, Int)] = inputRDD.flatMap(_.split("\\s+"))
      .map(item => (item, 1))
      .reduceByKey((temp, item) => temp + item)

    resultRDD.foreach(println)

    sc.stop()
  }
}
