package me.iroohom.ClassDemo.ScalaWordCount

object WordCount {
  def main(args: Array[String]): Unit = {
    val list = List("Spark Flink Hadoop Hive Hue Hue", "Oozie Spark Mahout Flink Hadoop Hadoop")

    println(list.flatMap(_.split("\\s+")).map(_ -> 1))
    println()
    println(list.flatMap(_.split("\\s+")).map(_ -> 1).groupBy(_._1))

    println()
    println(list.flatMap(_.split("\\s+")).map(_ -> 1).groupBy(_._1))

    println("=========================================================")

    val words = list.flatMap(line => line.trim.split("\\s+"))
    println(words)
    println()
    val tuples = words.map(word => word -> 1)
    println(tuples)
    println()
    //按照单词进行分组
    val groups = tuples.groupBy(tuple => tuple._1)
    println(groups)
    println()

    /**
     * 偏函数方式
     */
    println(groups.map { case (word, values) =>
      //将List中的二元组的值累加
      val count = values.map(_._2).sum
      //返回二元组
      word -> count
    })

    println("============================================")

    println(list.flatMap(_.trim.split("\\s+")).map((_, 1)).groupBy(_._1).map(tuple => (tuple._1, tuple._2.map(_._2).sum)))


  }

}
