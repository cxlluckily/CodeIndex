package me.iroohom.HomeWork.day01

object ForDemo {
  def main(args: Array[String]): Unit = {
    for (item <- 1 to 10) if (item % 2 == 0) print(item + " ")

    println()

    for (item <- 1 to 10 if item % 2 == 0) print(item + " ")

  }

}
