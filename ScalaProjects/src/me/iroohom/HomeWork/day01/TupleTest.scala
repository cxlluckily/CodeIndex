package me.iroohom.HomeWork.day01

object TupleTest {
  def main(args: Array[String]): Unit = {
    val t1 = ("Red", "male", 23)
    val t2 = "Red" -> 66
    val t3 = ("Red", 1.76)
    val bmi = t2._2.toDouble / (t3._2 * t3._2)
    println(bmi)


  }

}
