package me.iroohom.ClassDemo.FuncEnhanced

/**
 * 定义一个函数与不同的人打招呼，招呼不同
 */
object FuncEnhanced {

  /**
   * 定义打招呼的方法 高阶函数 原因在于sayHello参数类型是一个函数（仅有一个参数，类型是字符串，没有返回值）
   *
   * @param name     打招呼对象的名字
   * @param sayHello 打招呼的动作
   */
  def greeting(name: String, sayHello: (String) => Unit): Unit = {
    sayHello(name)
  }


  def main(args: Array[String]): Unit = {
    greeting("前台", (name: String) => println("Hi"))
    greeting("老乡", (name: String) => println(s"${name}早哇"))
    greeting("张总", (name) => println(s"${name}早上好"))

    /**
     * 较为常用写法
     */
    greeting("张总", name => println(s"${name}早上好"))

    greeting("张总", println(_))
    greeting("张总", println)


    val func = (name: String) => println(s"${name}早上好")
    greeting("张总", func)
  }

}
