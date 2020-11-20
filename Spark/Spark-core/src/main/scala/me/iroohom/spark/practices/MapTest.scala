package me.iroohom.spark.practices

import org.apache.spark.{SparkConf, SparkContext}

object MapTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new SparkContext(sparkConf)
    }

    val list = List(2, 3, 7, 8, 5, 3, 6, 7, 10)

    /**
     * map函数
     */
    val rdd1 = sc.parallelize(list)
    rdd1.map(_ * 2).collect.foreach(println)

    /**
     * filter函数
     */
    val rdd2 = sc.parallelize(list)
    rdd2.filter(_ >= 10).collect.foreach(println)

    /**
     * flatMap函数
     */
    val rdd3 = sc.parallelize(Array("a b c ", "d e f", "h i j"))
    rdd3.flatMap(_.split(" ")).collect.foreach(println)

    /**
     * 交集 并集 差集 笛卡尔积
     */
    val rdd4 = sc.parallelize(List(5, 6, 4, 3))
    val rdd5 = sc.parallelize(List(1, 2, 3, 4))
    val rdd6 = rdd4.union(rdd5)

    /**
     * 并集不去重
     */
    rdd6.collect.foreach(println)

    /**
     * 去重
     */
    rdd6.distinct.collect.foreach(println)


    /**
     * 交集
     */
    val rdd7 = rdd4.intersection(rdd5)
    rdd7.collect.foreach(println)

    /**
     * 差集
     */
    val rdd8 = rdd4.subtract(rdd5)
    rdd8.collect.foreach(println)

    /**
     * 笛卡尔积
     */
    val ardd = sc.parallelize(List("jack", "tom"))
    val brdd = sc.parallelize(List("java", "python", "scala"))
    val crdd = ardd.cartesian(brdd)
    crdd.collect.foreach(println)


    val rdd10 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4), 3)
    rdd10.distinct.collect.foreach(println)


    val drdd = sc.parallelize(list)

    /**
     * 取出最大的两个
     */
    println(drdd.top(2))

    /**
     * 取前N个
     */
    println(drdd.sortBy(x => x, true).take(2))

    /**
     * 取第一个
     */
    println(drdd.first)

    val kvRdd = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val kvrddres = kvRdd.map(x => (x.length, x))
    kvrddres.collect.foreach(println)
    kvrddres.keys.collect.foreach(println)
    kvrddres.values.collect.foreach(println)

    /**
     * mapValues函数 对value进行操作
     */
    val erdd = sc.parallelize(List((1, 10), (2, 20), (3, 30)))
    erdd.mapValues(_ * 2).collect.foreach(println)

    /**
     * collectAsMap函数
     */
    val frdd = sc.parallelize(List(("a", 1), ("b", 2)))
    frdd.collectAsMap().foreach(println)

    /**
     * mapPartitionsWithIndex函数
     */
    val grdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12), 3)
    val func = (
                 index: Int, iter: Iterator[Int]) => {
      iter.map(x => "[partID:" + index + ", val:" + x + "]")
    }
    grdd.mapPartitionsWithIndex(func).collect.foreach(println)


  }
}
