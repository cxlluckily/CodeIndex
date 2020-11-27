package me.iroohom.spark.sinks

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mysql.cj.jdbc.Driver
import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * 创建类继承ForeachWriter，将数据写入到MySQL表中，泛型为：Row，针对DataFrame操作，每条数据类型就是Row
 */
class MySQLForeachWriter extends ForeachWriter[Row] {
  var conn: Connection = null
  var pstmt: PreparedStatement = null
  val insertSQL = "REPLACE INTO `tb_word_count` (`id`, `word`, `count`) VALUES (NULL, ?, ?)"

  //设置初始化连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn = DriverManager.getConnection(
      "jdbc:mysql://node1.itcast.cn:3306/db_spark?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "root",
      "123456"
    )

    pstmt = conn.prepareStatement(insertSQL)

    //返回true表示获取连接成功
    true
  }

  //处理
  override def process(value: Row): Unit = {
    pstmt.setString(1, value.getAs[String]("value"))
    pstmt.setLong(2, value.getAs[Long]("count"))
    pstmt.executeUpdate()
  }


  //收尾操作
  override def close(errorOrNull: Throwable): Unit = {
    if (null != pstmt) pstmt.close()
    if (null != conn) conn.close()
  }
}
