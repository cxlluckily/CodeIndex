package me.iroohom.ClassDemo.Loop

object ForEnhanced {
  def main(args: Array[String]): Unit = {
    for (row <- 1 to 3) {
      for (star <- 1 to 5) {
        print("*")
      }
      println()
    }

    println()

    for (j <- 1 to 3; i <- 1 to 5) {
      print("*")
      if (i == 5) println()
    }
  }

}
