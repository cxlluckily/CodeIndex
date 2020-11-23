package me.iroohom.externalSource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.SparkSession

object SparkThriftJDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._

    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")

      conn = DriverManager.getConnection(
        "jdbc:hive2://node1.itcast.cn:10000/db_hive",
        "root",
        "123456"
      )

      /**
       * 查询语句
       */
      val sql =
        """
          |select e.ename, e.sal, d.dname from emp e join dept d on e.deptno = d.deptno
          |""".stripMargin

      pstmt = conn.prepareStatement(sql)

      /**
       * 查询 获取结果
       */
      rs = pstmt.executeQuery()

      while (rs.next()) {
        println(s" ename = ${rs.getString(1)}, sal = ${rs.getDouble(2)}, dname = ${rs.getString(3)}")
      }
    }
    catch {
      case exception: Exception => exception.printStackTrace()
    }
    finally {
      if (rs != null) rs.close()
      if (pstmt != null) pstmt.close()
      if (conn != null) conn.close()
    }


    spark.stop()
  }
}
