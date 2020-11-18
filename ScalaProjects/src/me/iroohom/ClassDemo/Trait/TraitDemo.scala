package me.iroohom.ClassDemo.Trait

/**
 * 特质里可以定义抽象方法也可以定义具体方法
 */
trait Logger {
  /**
   * 定义具体字段：变量进行初始化
   */
  val logClazz: String = this.getClass.getSimpleName
  /**
   * 抽象字段
   */
  var logName: String

  /**
   * 抽象方法
   */
  def logging(): Unit

  def printLog(): Unit = {
    println("打印日志")
  }
}

//定义一个类继承特质trait，将日志信息打印控制台
class ConsoleLogger extends Logger() {
  override def logging(): Unit = {
    println("将日志打印控制台")
  }

  override var logName: String = classOf[ConsoleLogger].getSimpleName
}

class FileLogger extends Logger {
  override def logging(): Unit = {
    println("将日志输出到文件")
  }

  override def printLog(): Unit = {
    println("文件log打印")
  }

  override var logName: String = classOf[FileLogger].getSimpleName
}


trait Monitor {
  def printMetrics(): Unit
}

/**
 * 记录日志，监控应用运行状态
 */
class SparkApp extends Logger with Monitor {
  override var logName: String = _

  override def logging(): Unit = ???

  override def printMetrics(): Unit = ???
}


object TraitDemo {

  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger()
    logger.logging()
    println(logger.logName)

    val logger1 = new FileLogger()
    logger1.logging()
    logger1.printLog()
    println(logger1.logName)


  }
}
