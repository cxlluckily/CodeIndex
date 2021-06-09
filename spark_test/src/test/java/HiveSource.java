import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @ClassName: HiveSource
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/16 00:25
 * @Software: IntelliJ IDEA
 */
public class HiveSource {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("SparkHiveSource")
                .enableHiveSupport()
//                .config("hive.metastore.uris", "thrift://svlhdp011.csvw.com:9083")
                .getOrCreate();

//        String basePath = "hdfs://node1:9000/user/hive/warehouse/sparkhive.db/stu/part-00000-a28ffd0b-dad2-47fd-a6c7-8d6c2d12f67a-c000";
//
//        Dataset<String> resultDF = spark.read()
//                //指定读取HDFS的根路径，目的是针对Hive的分区表，如果根路径不同，可是使用read.parquet().union(spark.read.parquet())的方式合并
////                .option("basePath", basePath + "dt=2021-05-04")
//                .textFile(basePath);

        Dataset<Row> resultDF = spark.sql("SHOW DATABASES");

        resultDF.show();

        spark.close();
    }

}
