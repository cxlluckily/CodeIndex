package me.iroohom.ClassDemo.Tuple

object TupleDemo {
  def main(args: Array[String]): Unit = {
    val tuple = (1, "zhangsan", 2, "zhangdaqiang")
    println(tuple)

    //当元组中的元素个数为2时，称为二元组，就是key value对
    println(tuple._1)
    println(tuple._2)

    val t2 = 3 -> "dazhaung"
    println(t2)

    //二元组的方法，将键值对调换
    println(t2.swap)
    println(t2)
  }
}
