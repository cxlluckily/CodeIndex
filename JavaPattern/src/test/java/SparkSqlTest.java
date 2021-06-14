import org.apache.spark.sql.SparkSession;

public class SparkSqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();

        spark.sql("SELECT 1 AS A").show();

        spark.stop();
    }
}
