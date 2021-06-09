import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName: ReadKudu
 * @Author: Roohom
 * @Function: 从一个集群复制一个Kudu表取另一个集群并插入数据(10条)
 * @Date: 2021/4/29 14:02
 * @Software: IntelliJ IDEA
 */
public class ReadKuduDemo {
    public static void main(String[] args) throws IOException {
        Properties properties = Config.getProperties();

        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.ui.enabled", false)
                .appName("SparkReadAndWriteKuduLocal")
                .getOrCreate();

        HashMap<String, String> prodKuduMap = new HashMap<String, String>();
        prodKuduMap.put("kudu.master", ConfigConstant.PROD_KUDU_MASTER);
        prodKuduMap.put("kudu.table", properties.getProperty("prod.kudu.table"));
        Dataset<Row> df = spark
                .read()
                .options(prodKuduMap)
                .format("kudu")
                .load();
        df.createOrReplaceTempView("tab");
        Dataset<Row> destinyDF = spark.sql("SELECT * FROM tab LIMIT 10");

        KuduContext kuduContext = new KuduContext(ConfigConstant.UAT_KUDU_MASTER, spark.sparkContext());


        ArrayList<String> list = new ArrayList<>();
        list.add("id");
        kuduContext.createTable(properties.getProperty("uat.kudu.table"), df.schema(),
                JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq(),
                new CreateTableOptions()
                        .setNumReplicas(3)
                        .addHashPartitions(list, 3)
        );

        HashMap<String, String> uatKuduMap = new HashMap<>();
        uatKuduMap.put("kudu.master", ConfigConstant.UAT_KUDU_MASTER);
        uatKuduMap.put("kudu.table", "spark_tm_product");
        destinyDF.write().mode("append").options(uatKuduMap).format("org.apache.kudu.spark.kudu").save();


        spark.stop();
    }
}
