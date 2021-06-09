import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @ClassName: KuduSource
 * @Author: Roohom
 * @Function:
 * @Date: 2021/5/15 22:47
 * @Software: IntelliJ IDEA
 */
public class KuduSource {
    public static Dataset<Row> load(SparkSession sparkSession, String kuduMaster, String table) {
        return sparkSession.read()
                .format("kudu")
                .option("kudu.master", kuduMaster)
                .option("kudu.table", table)
                .load();
    }
}
