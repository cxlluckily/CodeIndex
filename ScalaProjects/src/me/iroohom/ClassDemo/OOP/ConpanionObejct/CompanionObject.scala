package me.iroohom.ClassDemo.OOP.ConpanionObejct

/**
 * 伴生类
 *
 * @param name 姓名
 * @param age  年龄
 */
class Person2(val name: String, var age: Int) {
  def buy(): Unit = {
    /**
     * 可以访问伴生对象的私有属性
     */
    println(Person2.money)
  }

}

/**
 * 伴生对象
 */
object Person2 {
  private var money: Double = 89

  /**
   * 在伴生对象Object中使用apply方法，使得在实例化对象时不需要再new了
   *
   * @param name 姓名
   * @param age  年龄
   * @return Person实例
   */
  def apply(name: String, age: Int): Person2 = new Person2(name, age)

}


object CompanionObject {
  def main(args: Array[String]): Unit = {
    val person = new Person2("大锤", 29)
    person.buy()


    val p = Person2("dada",29)
    p.buy()
  }
}
