package me.iroohom.ClassDemo.Loop

object WhileDemo {
  def main(args: Array[String]): Unit = {
    //先判断再执行
    var i: Int = 0
    while (i <= 10) {
      println(i)
      i += 1
    }

    //Do ... While ... 先执行再判断
    var j = 0
    do {
      print(j + " ")
      j += 1
    } while (j <= 10)


  }

}
