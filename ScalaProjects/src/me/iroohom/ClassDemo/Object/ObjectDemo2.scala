package me.iroohom.ClassDemo.Object

import java.text.SimpleDateFormat
import java.util.Date


/**
 * 在scala中object可以作为工具类，其中方法相当于Java中
 * 的静态方法，直接通过对象名.方法名进行方法
 */

object DateUtil {

  //定义日期格式化对象 Java中SimpleDateFormat是线程不安全的 在大数据编程中推荐使用FastDateFormat
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def formatDate(timestamp: Long): String = {
    format.format(new Date(timestamp))
  }
}

object ObjectDemo2 {
  def main(args: Array[String]): Unit = {
    println(DateUtil.formatDate(System.currentTimeMillis()))
  }
}
