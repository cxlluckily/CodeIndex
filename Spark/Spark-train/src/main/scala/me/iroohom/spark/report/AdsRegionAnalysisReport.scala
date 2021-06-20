package me.iroohom.spark.report

import me.iroohom.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

/**
 * 广告区域统计
 */
object AdsRegionAnalysisReport {
  def doReport(dataFrame: DataFrame) = {
    val resultDF = reportWithDSL(dataFrame)

    saveReportToMySQL(resultDF)
  }


  /**
   * 基于DSL实现报表分析 主要使用WHEN 和 SUM函数
   *
   * @param dataFrame DF
   * @return
   */
  def reportWithDSL(dataFrame: DataFrame): DataFrame = {
    val session = dataFrame.sparkSession
    import session.implicits._

    val reportDF: DataFrame = dataFrame
      // 1. 按照地域信息分组：province和city
      .groupBy($"province", $"city")
      // 2. 对组内数据进行聚合操作
      .agg(
        // 原始请求数量
        sum(
          when(
            $"requestmode" === 1 and $"processnode" >= 1, // 条件表达式，返回boolean
            1 // 条件表达式：true时值
          ).otherwise(0) // 条件表达式： false时值
        ).as("orginal_req_cnt"),
        // 有效请求：requestmode = 1 and processnode >= 2
        sum(
          when(
            $"requestmode".equalTo(1).and($"processnode".geq(2)),
            1
          ).otherwise(0)
        ).as("valid_req_cnt"),
        // 广告请求：requestmode = 1 and processnode = 3
        sum(
          when($"requestmode".equalTo(1)
            .and($"processnode".equalTo(3)), 1
          ).otherwise(0)
        ).as("ad_req_cnt"),
        // 参与竞价数
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("join_rtx_cnt"),
        // 竞价成功数
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("success_rtx_cnt"),
        // 广告主展示数: requestmode = 2 and iseffective = 1
        sum(
          when($"requestmode".equalTo(2)
            .and($"iseffective".equalTo(1)), 1
          ).otherwise(0)
        ).as("ad_show_cnt"),
        // 广告主点击数: requestmode = 3 and iseffective = 1 and adorderid != 0
        sum(
          when($"requestmode".equalTo(3)
            .and($"iseffective".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("ad_click_cnt"),
        // 媒介展示数
        sum(
          when($"requestmode".equalTo(2)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1)), 1
          ).otherwise(0)
        ).as("media_show_cnt"),
        // 媒介点击数
        sum(
          when($"requestmode".equalTo(3)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1)), 1
          ).otherwise(0)
        ).as("media_click_cnt"),
        // DSP 广告消费
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000)), floor($"winprice" / 1000)
          ) otherwise (0)
        ).as("dsp_pay_money"),
        // DSP广告成本
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000)), floor($"adpayment" / 1000)
          ) otherwise (0)
        ).as("dsp_cost_money")
      )
      // 3. 过滤非0数据，计算三率
      .where(
        $"join_rtx_cnt" =!= 0 and $"success_rtx_cnt" =!= 0 and
          $"ad_show_cnt" =!= 0 and $"ad_click_cnt" =!= 0 and
          $"media_show_cnt" =!= 0 and $"media_click_cnt" =!= 0
      )
      // 4. 计算三率
      .withColumn(
        "success_rtx_rate", //
        round($"success_rtx_cnt" / $"join_rtx_cnt".cast(DoubleType), 2) // 保留两位有效数字
      )
      .withColumn(
        "ad_click_rate", //
        round($"ad_click_cnt" / $"ad_show_cnt".cast(DoubleType), 2) // 保留两位有效数字
      )
      .withColumn(
        "media_click_rate", //
        round($"media_click_cnt" / $"media_show_cnt".cast(DoubleType), 2) // 保留两位有效数字
      )
      // 5. 增加报表数据日期：report_date
      .withColumn(
        "report_date", //
        date_sub(current_date(), 1).cast(StringType)
      )
    //reportDF.printSchema()
    //reportDF.show(10, truncate = false)

    // 返回报表的数据
    reportDF
  }


  /**
   * 使用With AS 函数进行报表分析
   */
  def reportWithKpiSQL(dataFrame: DataFrame): DataFrame = {
    val session = dataFrame.sparkSession
    import session.implicits._

    /**
     * 注册临时视图
     */
    dataFrame.createOrReplaceTempView("view_tmp_pmt")

    //SQL语句
    val reportDF: DataFrame = session.sql(
      ReportSQLConstant.reportAdsRegionKpiSQL("view_tmp_pmt")
    )

    reportDF
  }

  /**
   * 使用SQL方式编程：先注册为临时视图，再编写SQL执行
   *
   * @param dataFrame input DF
   * @return
   */
  def reportWithSQL(dataFrame: DataFrame): DataFrame = {
    val session = dataFrame.sparkSession
    import session.implicits._

    //注册临时视图
    dataFrame.createOrReplaceTempView("view_tmp_pmt")
    val reportDF = session.sql(
      ReportSQLConstant.reportAdsRegionSQL("view_tmp_pmt")
    )

    //再次注册临时视图
    reportDF.createOrReplaceTempView("view_tmp_report")
    val df = session.sql(
      ReportSQLConstant.reportAdsRegionRateSQL("view_tmp_report")
    )
    df
  }

  /**
   * 保存到MySQL
   *
   * @param reportDF input DF
   */
  def saveReportToMySQL(reportDF: DataFrame): Unit = {
    reportDF
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", ApplicationConfig.MYSQL_JDBC_URL)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable", "roohom_ads_report.ads_region_analysis")
      .save()
  }
}
