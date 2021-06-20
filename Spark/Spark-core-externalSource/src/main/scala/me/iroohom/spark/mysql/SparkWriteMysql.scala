package me.iroohom.spark.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据写入到Mysql
 */
object SparkWriteMysql {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      new SparkContext(sparkConf)
    }

    val inputRDD = sc.textFile("datas\\wordcount.data", minPartitions = 2)
    val resultRDD = inputRDD
      .filter { line =>
        if (line != null && line.trim.length > 0) {
          true
        } else {
          false
        }
      }
      .flatMap {
        line => line.trim.split("\\s+")
      }
      .map {
        word => (word, 1)
      }
      .reduceByKey((temp, item) => temp + item)

    //    resultRDD.foreach(println)
    resultRDD
      .coalesce(1)
      //      .foreachPartition { iter: Iterator[(String, Int)] => saveToMysqlPrimary(iter) }
      .foreachPartition { iter: Iterator[(String, Int)] => saveToMysqlIntermediate(iter) }
    sc.stop()
  }


  /**
   * Spark数据写入MySQL高级写入，批量写入，并且考虑到事务，要么全部插入，要么全部不插入
   *
   * @param datas 需要写入MySQL的数据
   */
  def saveToMysqlAdvanced(datas: Iterator[(String, Int)]) = {

    //注册驱动
    Class.forName("com.mysql.cj.jdbc.Driver")
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      /**
       * 获取连接
       */
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.roohom.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )
      pstmt = conn.prepareStatement("replace INTO db_test.tb_wordcount (word, count) VALUES(?, ?)")

      datas.foreach {
        case (word, count) =>
          pstmt.setString(1, word)
          pstmt.setInt(2, count)
          //添加到批中去
          pstmt.addBatch()
      }

      //用来记录数据库原来事务状态，autoCommit为布尔值，true代表自动提交，false代表手动提交
      val autoCommit: Boolean = conn.getAutoCommit
      //设置数据库的事务状态为手动提交
      conn.setAutoCommit(false)

      /**
       * VITAL:批量插入
       */
      pstmt.executeBatch()

      /**
       * VITAL：手动提交事务，执行批量插入
       */
      conn.commit()

      //恢复数据库原来的事务状态
      conn.setAutoCommit(autoCommit)

    }
    catch {
      case exception: Exception => exception.printStackTrace()
    }
    finally {
      if (pstmt != null) pstmt.close()
      if (conn != null) conn.close()
    }
  }


  /**
   * 数据写入MySQL中级写法 批量插入
   *
   * @param datas 需要写入MySQL的数据
   */
  def saveToMysqlIntermediate(datas: Iterator[(String, Int)]): Any = {

    //加载驱动类
    Class.forName("com.mysql.cj.jdbc.Driver")
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.roohom.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )

      pstmt = conn.prepareStatement("replace INTO db_test.tb_wordcount (word, count) VALUES(?, ?)")

      /**
       * 遍历提交
       */
      datas.foreach {
        case (word, count) =>
          pstmt.setString(1, word)
          pstmt.setInt(2, count)

          pstmt.addBatch()
      }

      pstmt.executeBatch()


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (pstmt != null) pstmt.close()
      if (conn != null) conn.close()
    }

  }


  /**
   * 将数据写入MySQL基本写法 一个一个插入
   *
   * @param datas 需要写入MySQL的数据
   */
  def saveToMysqlPrimary(datas: Iterator[(String, Int)]): Unit = {
    //加载驱动类
    Class.forName("com.mysql.cj.jdbc.Driver")

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      //创建连接
      connection = DriverManager.getConnection(
        "jdbc:mysql://node1.roohom.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )
      //预处理对象
      pstmt = connection.prepareStatement("replace INTO db_test.tb_wordcount (word, count) VALUES(?, ?)")

      datas.foreach {
        case (word, count) =>
          pstmt.setString(1, word)
          pstmt.setInt(2, count)

          pstmt.execute()
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      //关闭连接
      if (pstmt != null) {
        pstmt.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }
}
