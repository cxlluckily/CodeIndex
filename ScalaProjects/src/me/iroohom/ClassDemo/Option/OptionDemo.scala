package me.iroohom.ClassDemo.Option

object OptionDemo {

  def div(a: Int, b: Int): Option[Double] = {
    if (b != 0) {
      val result = a / b.toDouble
      Some(result)
    }
    else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val result: Option[Double] = div(10, 0)
    //    println(s"result = ${result.get}")


    result match {
      case Some(x) => println(s"result = $x")
      case None => println("none")
    }

    //映射
    /**
     * def getOrElse[B1 >: B](key: A, default: => B1): B1 = get(key) match {
     * case Some(v) => v
     * case None => default
     * }
     */
    val map = Map("a" -> 1, "b" -> 2, "c" -> 3)
    println(map.getOrElse("a", "none"))
  }
}
