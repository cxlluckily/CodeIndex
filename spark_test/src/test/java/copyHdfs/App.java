package copyHdfs;

import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[8]")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("show databases").show();
        spark.stop();

    }
}
