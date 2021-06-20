package me.iroohom.spark.report

import java.sql.{Connection, DriverManager, PreparedStatement}

import me.iroohom.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object RegionStateReport {

  /**
   * 结果数据保存到MySQL 功能可以实现，但是不好，重复运行由于主键已经存在而冲突会报异常
   *
   * @param reportDF reportDF
   */
  def saveReportToMySQL(reportDF: DataFrame) = {
    reportDF
      .coalesce(1)
      .write
      //追加写入MySQL，重复运行，主键存在报错
      //      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", ApplicationConfig.MYSQL_JDBC_URL)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable", "roohom_ads_report.region_stat_analysis")
      .save()
  }

  /**
   * 报表数据保存到MySQL 调用foreachPartition
   * 针对每个分区编写一个JDBC insert插入数据代码
   *
   * @param reportDF reportDF
   */
  def saveToMySQL(reportDF: DataFrame): Unit = {
    reportDF.rdd.foreachPartition { iter =>

      //注册驱动
      Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
      var conn: Connection = null
      var pstmt: PreparedStatement = null

      try {
        /**
         * 获取连接
         */
        conn = DriverManager.getConnection(
          ApplicationConfig.MYSQL_JDBC_URL, //
          ApplicationConfig.MYSQL_JDBC_USERNAME, //
          ApplicationConfig.MYSQL_JDBC_PASSWORD
        )

        //执行SQL语句 此处使用了ON DUPLICATE语句，表示仅在重复的主键上进行操作
        pstmt = conn.prepareStatement(
          """
					  |INSERT INTO roohom_ads_report.region_stat_analysis(report_date, province, city, count)
					  |VALUES(?, ?, ?, ?)
					  |ON DUPLICATE KEY UPDATE count=VALUES(count)
					  |""".stripMargin)


        //用来记录数据库原来事务状态，autoCommit为布尔值，true代表自动提交，false代表手动提交
        val autoCommit: Boolean = conn.getAutoCommit
        //设置数据库的事务状态为手动提交
        conn.setAutoCommit(false)

        iter.foreach {
          row =>
            pstmt.setString(1, row.getAs[String]("report_date"))
            pstmt.setString(2, row.getAs[String]("province"))
            pstmt.setString(3, row.getAs[String]("city"))
            pstmt.setLong(4, row.getAs[Long]("count"))
            pstmt.addBatch()
        }

        /**
         * TODO:批量插入
         */
        pstmt.executeBatch()

        /**
         * TODO：手动提交事务，执行批量插入
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

  }

  def doReport(dataFrame: DataFrame) = {
    import dataFrame.sparkSession.implicits._

    //    val reportDF = dataFrame
    //      .groupBy($"province", $"city")
    //      .count()
    //      .withColumn("report_date", date_sub(current_date(), 1).cast(StringType))
    //      .orderBy($"count".desc)
    //    reportDF.printSchema()
    //    reportDF.show(10, truncate = false)

    /**
     * 推荐的DSL分析 使用withColumnRenamed而不是withColumn
     */
    val reportDF = dataFrame
      .groupBy($"date_str", $"province", $"city")
      .count()
      /*使用 <code>withColumnRenamed</code> 将字段名date_str 替换为report_date
      Returns a new Dataset with a column renamed.
        */
      .withColumnRenamed("date_str", "report_date")
      .orderBy($"count".desc)
    reportDF.printSchema()
    reportDF.show(10, truncate = false)

    //    saveReportToMySQL(reportDF)

    //保存至MtSQL
    saveToMySQL(reportDF)
  }
}
