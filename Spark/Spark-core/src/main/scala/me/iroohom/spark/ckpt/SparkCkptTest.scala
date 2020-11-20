package me.iroohom.spark.ckpt

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD数据Checkpoint设置，案例演示
 */
object SparkCkptTest {
  def main(args: Array[String]): Unit = {

    val sc = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      new SparkContext(sparkConf)
    }

    /**
     * 设置检查点目录
     */
    sc.setCheckpointDir("datas/spark/chkp/")

    /**
     * 读取文件数据
     */
    val datasRDD = sc.textFile("datas\\wordcount.data")

    /**
     * 调用checkpoint函数，将RDD进行备份，需要RDD中Action函数触发
     */
    datasRDD.checkpoint()
    datasRDD.count()

    /**
     * 再次执行count 此时从checkpoint读取数据
     */
    datasRDD.count()

    sc.stop()
  }
}
