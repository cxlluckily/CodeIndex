package me.iroohom.spark.report

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

/**
 * 广告APP统计 ads_app_analysis 区域统计：省份和城市
 */
object AdsAppAnalysisReport {

  /**
   * 基于DSL进行分析 主要使用WHEN 和 SUM函数
   *
   * @param dataFrame input DF
   */
  def reportWithDSL(dataFrame: DataFrame): DataFrame = {
    val session = dataFrame.sparkSession
    import session.implicits._


    val appReportDF: DataFrame = dataFrame
      //按照APP信息分组appid 和 appname
      .groupBy($"appid", $"appname")
      .agg(
        sum(
          when(
            $"requestmode" === 1 and $"processnode" >= 1,
            1
          ).otherwise(0)
        ).as("original_req_cnt"),
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

    appReportDF
  }

  def doReport(dataFrame: DataFrame) = {
    val resultDF: DataFrame = reportWithDSL(dataFrame)
    resultDF.printSchema()
    resultDF.show(50, truncate = false)
  }


}
