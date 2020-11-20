package me.iroohom.spark.source

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 采用并行化的方式构建Scala集合Seq中的数据为RDD
 * - 将Scala集合转换为RDD
 * sc.parallelize(seq)
 * - 将RDD转换为Scala中集合
 * rdd.collect()
 * rdd.collectAsMap()
 */

object SparkParallelizeTest {
  def main(args: Array[String]): Unit = {

    /**
     * 创建SparkConf对象，设置应用的配置信息
     */
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }


    /**
     * Scala中集合Seq序列存储数据
     */
    val seq: Seq[String] = Seq(
      "hadoop scala hive spark scala sql sql", //
      "hadoop scala spark hdfs hive spark", //
      "spark hdfs spark hdfs scala hive spark" //
    )

    /**
     * def parallelize[T: ClassTag](
     * seq: Seq[T],
     * numSlices: Int = defaultParallelism
     * ): RDD[T]
     */
    val inputRDD = sc.parallelize(seq, numSlices = 2)

    /**
     * 调用集合RDD中函数处理分析数据
     */
    val resultRDD = inputRDD
      .flatMap(_.split("\\s+"))
      .map(item => (item, 1))
      .reduceByKey((temp, item) => temp + item)

    resultRDD.foreach(println)

    sc.stop()
  }
}
