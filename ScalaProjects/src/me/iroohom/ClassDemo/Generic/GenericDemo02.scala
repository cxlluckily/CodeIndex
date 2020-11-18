package me.iroohom.ClassDemo.Generic

class Tuple[T](val _1: T, _2: T) {
  override def toString: String = s"(${_1},${_2})"
}

object GenericDemo02 {
  def main(args: Array[String]): Unit = {
    val tuple = new Tuple[String]("A", "a")
    println(tuple)

    val t2 = new Tuple[Int](100, 99)
    println(t2)
  }

}
