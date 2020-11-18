package me.iroohom.ClassDemo.Test

import scala.collection._

object Count {
  def main(args: Array[String]): Unit = {
    val list = List("Spark Flink Hadoop Hive Hue Hue", "Oozie Spark Mahout Flink Hadoop Hadoop")

    //FlatMap
    println(list.flatMap(_.split("\\s+")))
    println()

    //groupBy
    var map = list.flatMap(_.split("\\s+")).groupBy(item => item)
    println(map)

    //    for ((key,value) <- map) println(s"${key} -> ${value.size}")
    val map1 = mutable.Map()

    //map
    val map2 = map.map {
      case (key, value) => key -> value.map(item => 1)
    }
    println(map2)


    println("==========================================")

    /**
     * benv.readTextFile("/root/words.txt").flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()
     */
    println(list.flatMap(_.split("\\s+"))
      .groupBy(item => item)
      .map {
        case (key, value) => key -> value.map(_ => 1)

      }.map{
      case (key,value) => key -> value.reduce((tmp,item) => tmp + item)
    }
    )


  }
}
