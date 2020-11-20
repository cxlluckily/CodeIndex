package me.iroohom.spark.operations.partition

import org.apache.spark.{SparkConf, SparkContext}


/**
 * RDD中的分区函数，调整RDD分区数目，可以增加分区和减少分区
 */
object SparkPartitionTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }

    /**
     * 读取本地文件系统文件创建RDD
     */
    val inputRDD = sc.textFile("datas\\wordcount.data", minPartitions = 2)

    /**
     * TODO：增加分区数
     */
    val etlRDD = inputRDD.repartition(3)
    println(s"ETLRDD 分区数目 = ${etlRDD.getNumPartitions}")

    /**
     * 词频统计
     */
    val resultRDD = inputRDD
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .mapPartitions {
        iter => iter.map(word => (word, 1))
      }
      .reduceByKey((a, b) => a + b)

    /**
     * 输出结果
     */
    resultRDD

      /**
       * 对结果降低分区数目 分区数目减一
       */
      .coalesce(1)
      .foreachPartition(iter => iter.foreach(println))
    println(s"ETLRDD 分区数目 = ${resultRDD.getNumPartitions}")

    sc.stop()

  }
}
