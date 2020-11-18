package me.iroohom.ClassDemo.Case

/**
 * 编译的时候自动生成伴生对象，实现所有的getter和setter方法 重写equals hashcode和tostring方法
 *
 * @param name 姓名
 * @param age  年龄
 */
case class Person(name: String, var age: Int)


object CaseClassDemo {
  def main(args: Array[String]): Unit = {
    val p = new Person("dada", 12)

    /**
     * 重写toString
     */
    println(p)

    /**
     * 使用apply创建对象
     */
    val person = Person("xixi", 21)
    println(person)

    /**
     * getter方法
     */
    println(person.name)

    /**
     * setter方法
     */
    person.age = 12
    println(person.age)

    /**
     * equals方法
     */
    val p2 = Person("xixi", 12)
    println(p2 == person)


    /**
     * 拷贝一个对象
     */
    val p3 = person.copy(age = 15)
    println(p3)
  }
}
