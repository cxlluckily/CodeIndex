import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SparkCollectTest {
    @Test
    public void collectTest(){
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
;
        Dataset<Row> df = spark.sql("SELECT 'AHA' AS TEXT");
        List<Row> rows = df.takeAsList(1);
        System.out.println(rows.get(0).getString(0));

        new StructType(new StructField("TEXT", DataType,true,new Metadata()))
        Row[] take = df.take(1);

        spark.stop();
    }
}
