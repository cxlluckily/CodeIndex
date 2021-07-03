package sources;

import org.apache.flink.api.java.DataSet;
import org.apache.hadoop.metrics2.util.SampleQuantiles;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @ClassName: sources.MySQLSource
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/15 21:18
 * @Software: IntelliJ IDEA
 */
public class MySQLSource {

    private static Dataset<Row> mysqlSource(SparkSession sparkSession, String sql, String user, String password) {

        return sparkSession.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
                .option("user", user)
                .option("password", password)
                .option("dbtable", sql)
                .load();
    }

    private static Dataset<Row> mysqlSource(SparkSession sparkSession, String database, String table, Properties properties) {

        return sparkSession.read()
                .jdbc(
                        "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
                        database + "." + table,
                        properties
                );
    }


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[3]")
                .getOrCreate();

        //涉及到多表查询使用如此
        String sql = "(SELECT * FROM roohom.employee) tableA";

        Dataset<Row> source = mysqlSource(spark, sql, "root", "123456");
        source.printSchema();
        source.show();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "123456");
        props.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> rowDF = mysqlSource(spark, "roohom", "vehicle", props);

        rowDF.printSchema();
        rowDF.show();

        rowDF.createOrReplaceTempView("tmp");
//        spark.sql(
//                "WITH tp AS (" +
//                        "SELECT *, rank() OVER(PARTITION BY depid ORDER BY salary DESC) AS rk " +
//                        "FROM tmp" +
//                        ") " +
//                        "SELECT " +
//                        "empid, " +
//                        "ename,  " +
//                        "depid, " +
//                        "salary " +
//                        "FROM tp " +
//                        "WHERE rk=1").show();

        //每辆车在对应的经销商(不同时间可能是对应不同的经销商)时最大的价格(最近的一次时间) 废弃，源表已删除
//        String maxSql =
//                "SELECT vehicle_id, vin, dealer_code, max_salary " +
//                        "FROM (" +
//                        "SELECT  vehicle_id, " +
//                        "        vin, " +
//                        "        dealer_code, " +
//                        "        MAX(price) OVER(PARTITION BY vin,dealer_code) AS max_salary " +
//                        "  FROM  tmp" +
//                        ") AS tep " +
//                        "GROUP BY 1,2,3,4 " +
//                        "ORDER BY 1";

//        String maxPriceSql =
//                "WITH pri AS " +
//                        "  (" +
//                        "   SELECT  vehicle.vin, " +
////                        "           detail.dealer_code AS dealer_code, " +
////                        "           vehicle.vehicle_id AS vehicle_id, " +
////                        "           MAX(detail.price) OVER(PARTITION BY vehicle.vin) AS max_price " +
//                        "           MAX(detail.price) AS max_price " +
//                        "     FROM  roohom.vehicle AS vehicle " +
//                        "LEFT JOIN  roohom.vehicle_detail AS detail " +
//                        "       ON  vehicle.vin=detail.vin " +
//                        " GROUP BY  vehicle.vin " +
//                        "  ) " +
//                        "   SELECT  pri.vin," +
//                        "           ve.vehicle_id," +
//                        "           de.dealer_code," +
//                        "           pri.max_price " +
//                        "     FROM  pri " +
//                        "LEFT JOIN  roohom.vehicle AS ve " +
//                        "       ON  pri.vin=ve.vin " +
//                        "LEFT JOIN  roohom.vehicle_detail AS de " +
//                        "       ON  pri.vin=de.vin ";

        //每辆车在对应的经销商(不同时间可能是对应不同的经销商)时最大的价格(最近的一次时间)
        String maxPriceSql =
                "           SELECT  vehicle_id, vin, dealer_code, max_price " +
                        "     FROM  (" +
                        "            SELECT  vehicle.vehicle_id AS vehicle_id," +
                        "                    vehicle.vin AS vin," +
                        "                    detail.dealer_code AS dealer_code, " +
                        "                    MAX(price) OVER(PARTITION BY vehicle.vin, detail.dealer_code) AS max_price " +
                        "              FROM  roohom.vehicle AS vehicle " +
                        "         LEFT JOIN  roohom.vehicle_detail AS detail " +
                        "                ON  vehicle.vin=detail.vin  " +
                        "           ) AS tmp " +
                        " GROUP BY  1,2,3,4";

        String maxPriceSqlAno =
                "WITH tep AS (" +
                        "     SELECT  vehicle.vehicle_id AS vehicle_id, " +
                        "             vehicle.vin AS vin, " +
                        "             detail.dealer_code AS dealer_code, " +
                        "             detail.price AS price, " +
                        "             ROW_NUMBER() OVER(PARTITION BY vehicle.vin ORDER BY detail.price DESC) AS rk " +
                        "       FROM  roohom.vehicle AS vehicle " +
                        "  LEFT JOIN  roohom.vehicle_detail AS detail " +
                        "         ON  vehicle.vin=detail.vin " +
                        "    ) " +
                        "SELECT vehicle_id, " +
                        "       vin, " +
                        "       dealer_code, " +
                        "       price AS max_price" +
                        "  FROM tep" +
                        " WHERE rk=1 ";

        String sqlHandle =
                "      SELECT  vin,\n" +
                        "      dealer_code,\n" +
                        "      price,\n" +
                        "      mileage,\n" +
                        "      ROW_NUMBER() OVER(PARTITION BY vin ORDER BY price DESC, mileage DESC) AS rk \n" +
                        "FROM  roohom.vehicle_detail";

        Dataset<Row> maxDF = mysqlSource(spark, "( " + sqlHandle + ") AS tp", "root", "123456");
        maxDF.show();

        spark.stop();


    }
}
