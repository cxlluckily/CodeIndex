import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

/**
 * @ClassName: KuduScan
 * @Author: Roohom
 * @Function: 读取kudu的表
 * @Date: 2021/1/8 18:11
 * @Software: IntelliJ IDEA
 */
public class KuduScan {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .config("spark.ui.enabled", false)
//                .config("spark.port.maxRetries",100)
                .getOrCreate();
        HashMap<String, String> kuduMap = new HashMap<String, String>();
        //UAT环境
//        kuduMap.put("kudu.master", "10.122.44.118:7051,10.122.44.119:7051,10.122.44.120:7051");
        //生产环境
//        kuduMap.put("kudu.master", "10.122.44.116:7051,10.122.44.117:7051,10.122.44.123:7051");
        //本地环境
        kuduMap.put("kudu.master", "192.168.88.161:7051,192.168.88.161:7051,192.168.88.161:7051");
//        kuduMap.put("kudu.table", "sa_mos-gvs-mysql-vehicle_tm_vehicle_ability");
        kuduMap.put("kudu.table", "cat_apple");
        Dataset<Row> df = spark.read().options(kuduMap).format("kudu").load();
        System.out.println(df.count());
        df.show(100, false);
//        df.coalesce(1).rdd().saveAsTextFile("D:\\VolksWagen\\tt_smart_home_electronfence\\data");
        spark.stop();
    }
}
