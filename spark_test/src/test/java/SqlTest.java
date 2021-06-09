import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * @ClassName: SqlTest
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/24 14:37
 * @Software: IntelliJ IDEA
 */
public class SqlTest {
    @Test
    public void sqlTest() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("localtest")
                .getOrCreate();

        //这里举个栗子
//        Dataset<Row> dtDF = spark.sql("SELECT MAX(dt) AS dt --FROM td表 WHERE dt='2021-05-23' ");
        //这里模拟查出来昨日的数据不为空
        Dataset<Row> dtDF = spark.sql("SELECT * FROM (SELECT '2021-05-23' AS dt) A WHERE dt='2021-05-23'");
        System.out.println(dtDF.count());

        long cnt = dtDF.count();
        //从昨天的td表中按照昨天日期找出来最大的日期, 如果没有最大的日期，也就是说没有计算过或者是第一天计算
        //或者说昨天没有做计算，但前几天做了计算，那么今天在跑数据的时候都得将历史的扫描全表做一次计算
        //这个时候，昨天那个分区里面，应该是没有数据的，那么得到的结果值里面行数就为0才对
        //知道它为0了，那么就可以在这里面运行我们的全表扫描的SQL
        if (cnt == 0) {
            //这里面运行一套计算全表扫描计算的SQL
            spark.sql("SELECT '这是全表扫描计算得到的结果' AS result").show();
        } else if (cnt > 0) {
            //这里面运行一套计算拿昨天的td加上今天的1d的SQL
            spark.sql("SELECT '这是昨天加今天计算得到的结果' AS result").show();
        }


        spark.stop();
    }
}
