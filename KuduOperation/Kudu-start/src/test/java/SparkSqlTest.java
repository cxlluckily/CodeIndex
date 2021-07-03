import org.apache.spark.sql.SparkSession;

public class SparkSqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("sparkSQL").getOrCreate();
        spark.sql("SELECT 1 as test").show();
        spark.stop();
    }
}
