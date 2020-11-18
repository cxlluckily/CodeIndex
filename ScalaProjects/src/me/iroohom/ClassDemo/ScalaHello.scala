package me.iroohom.ClassDemo

object ScalaHello {
  def main(args: Array[String]): Unit = {
    println("Hello Scala!")

    val name: String = "Big"
    println(name)

    val content = "Hello"

    println(s"${content},Scala")

    val  querySql =
      """
        |SELECT *
        |FROM DB_TABLE_NAME
        |""".stripMargin

    print(querySql)


    var number: Int = 1
    number.+=(1)
    println(number)


  }
}
