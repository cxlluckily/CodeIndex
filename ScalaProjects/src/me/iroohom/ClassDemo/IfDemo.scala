package me.iroohom.ClassDemo

object IfDemo {
  def main(args: Array[String]): Unit = {
    var sex: String = "male"
    //Scala代码中代码块中的最后一行就是返回值
    var result = if (sex == "male") 1 else 0
    println(result)

    val addValue = {
      val sum = 1 + 10
      println(s"1 + 10 = ${sum}")
      sum
    }
    println(addValue)
  }

}
