package me.iroohom.ClassDemo.PartialFunc

/**
 * TODO:偏函数 是包含在“{}”内没有match的一组case语句
 */
object PartialFuncDemo {
  def main(args: Array[String]): Unit = {
    val list = (1 to 10).toList
    /**
     * 对集合中的元素进行平方 map内传入的是匿名函数
     */
    val result = list.map(item => item * item)
    println(result)
    println()

    /**
     * 使用偏函数 map内传入的是偏函数
     */
    println(list.map {
      case item => item * item
    })
    println()

    val words = Map("spark" -> 2,"flink"->3,"hive" -> 4)
    //对集合中的value值进行平方
    /**
     * 使用匿名函数
     */
    val map = words.map(tuple => {
      val value = tuple._2
      val sum = value * value
      tuple._1 -> sum
    })
    println(map)
    println()

    val map1 = words.map {
      case (key, value) => key -> value * value
    }
    println(map1)

  }

}
