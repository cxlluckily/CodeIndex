package me.iroohom.ClassDemo.Operator

/**
 * FlatMap函数演示
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    /**
     * 词频统计WordCount
     * hadoop spark spark flink flink
     * spark flink spark
     */
    val lineList: List[String] = List("hadoop spark spark flink flink")
    // \\s 表示空白就匹配
    val list = lineList.map(line => line.split("\\s+"))

    val flatten = list.flatten

    println(flatten)


    //FlatMap

    val flatMap = lineList.flatMap(line => line.split("\\s+"))
    println(flatMap)


    //简化
    println(lineList.flatMap(_.split("\\s+")))


  }

}
