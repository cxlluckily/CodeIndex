package sources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.List;

public class SparkCollectTest {
    @Test
    public void collectTest(){
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.sql("SELECT 'AHA' AS TEXT");
        List<Row> rows = df.takeAsList(1);
        System.out.println(rows.get(0).getString(0));


        List<Row> collect = df.javaRDD().collect();
        String string = collect.get(0).getString(0);
        System.out.println(string);

        spark.stop();
    }
}
