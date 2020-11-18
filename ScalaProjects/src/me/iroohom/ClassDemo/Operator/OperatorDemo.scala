package me.iroohom.ClassDemo.Operator


/**
 * List常见高阶函数的使用
 * foreach的使用
 */
object OperatorDemo {
  def main(args: Array[String]): Unit = {
    val list = List(-23, 34, 65, 2, 655, 8, -22)
    val tupleList = List(11 -> "A", 22 -> "B", 33 -> "C")
    val list1 = List(1, 2, 3)

    //TODO:foreach 只作遍历
    list.foreach((item: Int) => print(item + " "))
    println()
    list.foreach(item => print(item + " "))
    println()
    list.foreach(print(_))
    list.foreach(item => print(Math.abs(item) + " "))


    //TODO:map函数 遍历集合中的元素进行操作
    println(list.map(item => Math.pow(item, 2)))

    //filter函数
    println(list.filter(item => item > 50))

    //TODO:sortBy函数
    println(list.sortBy(item => item))
    println(list.sortBy(item => -item))

    //按照第一个字段进行降序排序
    println(tupleList.sortBy(tuple => -tuple._1))
    //按照第二个字段进行升序排序
    println(tupleList.sortBy(tuple => tuple._2))

    //SortWith函数 用户比较集合中两个元素的大小 使用较少
    println(list.sortWith((x1, x2) => x1 < x2))


    //TODO:groupBy函数 分组
    val map = list.groupBy(item => Math.abs(item % 2))
    println(map)

    //reduce函数和fold函数
    println(list.reduce((tmp, item) => tmp + item))
    list.reduce((tmp, item) => {
      println(s"tmp = $tmp, item = $item")
      tmp + item
    })

    /**
     * Right：
     * 1、聚合中间变量在右边
     * 2、中间临时变量初始值，是聚合最右边的元素值
     * 3、从集合右边开始，一直向右边遍历元素
     */
    println("*********ReduceRight**********")
    list.reduceRight((item, tmp) => {
      println(s"tmp = $tmp, item = $item")
      tmp + item
    })

    println(list.reduce(_ + _))
    println(list.sum)

    /**
     * def fold[A](z: A)(op: (A, A) => A): A
     */
    println("************Fold**************")
    list.fold(0)((tmp, item) => {
      println(s"tmp = $tmp, item = $item")
      tmp + item
    })

    println("*******************************")
    println(list1.fold(0)((tmp, item) => tmp + item))

  }
}
