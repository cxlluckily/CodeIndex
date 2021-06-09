import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @ClassName: ParquetSource
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/16 16:45
 * @Software: IntelliJ IDEA
 */
public class ParquetSource {

    public static Dataset<Row> load(SparkSession sparkSession, String source) {

        return sparkSession.read()
                .format("parquet")
                .load(source);
    }


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkReadParquet")
                .master("local[2]")
                .getOrCreate();


//        Dataset<Row> parquetDF = load(spark, "hdfs://svlhdp012.csvw.com:8020/user/hive/warehouse/test.db/hive_source_test/e54fbbb3f0b463a7-9a4fac3f00000000_1943202371_data.0.parq");
//        parquetDF.printSchema();
//        parquetDF.show();


//        spark.read().parquet("D:\\Hadoop\\Spark\\Basic\\spark_day04_20201122\\05_数据\\resources\\users.parquet").show();


        spark.stop();
    }
}
