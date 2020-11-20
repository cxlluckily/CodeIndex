package me.iroohom.spark.operations.agg

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD聚合函数
 * 针对RDD中数据类型Key/Value对：
 * * groupByKey
 * * reduceByKey/foldByKey
 * * aggregateByKey
 * * combineByKey
 */
object SparkAggByKeyTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      new SparkContext(sparkConf)
    }

    val lineSeq = Seq(
      "hadoop scala hive spark scala sql sql", //
      "hadoop scala spark hdfs hive spark", //
      "spark hdfs spark hdfs scala hive spark" //
    )

    val inputRDD = sc.parallelize(lineSeq, numSlices = 2)

    val wordsRDD = inputRDD.flatMap(line => line.split("\\s+"))
      .map(word => word -> 1)

    /**
     * 先使用groupByKey函数分组，在使用map函数聚合
     */
    val wordsGroupRDD = wordsRDD.groupByKey()
    val resultRDD = wordsGroupRDD.map {
      case (word, values) =>
        val count = values.sum
        word -> count
    }
    println(resultRDD.collectAsMap())
    println(resultRDD)

    /**
     * 直接使用reduceByKey或者foldByKey分组聚合
     */
    val resultRDD2 = wordsRDD.reduceByKey((temp, item) => temp + item)
    println(resultRDD2.foreach(println))

    val resultRDD3 = wordsRDD.foldByKey(0)((temp, item) => item + temp)
    println(resultRDD3.foreach(println))

    /**
     * 使用aggregateByKey聚合
     * def aggregateByKey[U: ClassTag]
     * (zeroValue: U) // 聚合中间临时变量初始值，类似fold函数zeroValue
     * (
     * seqOp: (U, V) => U, // 各个分区内数据聚合操作函数
     * combOp: (U, U) => U // 分区间聚合结果的聚合操作函数
     * ): RDD[(K, U)]
     */
    val resultRDD4 = wordsRDD.aggregateByKey(0)(
      (temp, item) => {
        temp + item
      },
      (temp, result) => {
        temp + result
      }
    )

    resultRDD4.foreach(println)


    sc.stop()

  }
}
