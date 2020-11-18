package me.iroohom.ClassDemo.Advanced.demo01

/**
 * 普通人类
 *
 * @param name 姓名
 */
class Man(val name: String)


//object Man {
//  implicit def man2SuperMan(man: Man): SuperMan = {
//    new SuperMan(man.name)
//  }
//}

object Utils {
  implicit def man2SuperMan(man: Man): SuperMan = {
    new SuperMan(man.name)
  }
}

/**
 * 超人类
 *
 * @param name 姓名
 */
class SuperMan(val name: String) {
  def emitLaser(): Unit = {
    println("盖亚......")
  }
}

object ImplicitDemo {

  //  /**
  //   * 因式转换函数 默认会在当前作用域找 其次会在原类型的伴生对象中找
  //   *
  //   * @param man
  //   * @return
  //   */
  //    implicit  def man2SuperMan(man: Man):SuperMan = {
  //      new SuperMan(man.name)
  //    }

  def main(args: Array[String]): Unit = {
    val man = new Man("大古")

    //普通人编程superman
    /**
     * 当隐式转换函数不在当前作用域和原类型的伴生对象中时就需要手动导入
     */
    import Utils._
    man.emitLaser()
  }
}
