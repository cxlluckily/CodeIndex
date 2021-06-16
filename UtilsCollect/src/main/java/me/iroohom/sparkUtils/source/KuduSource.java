package me.iroohom.sparkUtils.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * 读取kudu
 */
public class KuduSource {

    /**
     * 读取kudu
     * @param sparkSession sparkSession对象
     * @param properties conf配置
     * @return DataFrame数据集
     */
    public static Dataset<Row> load(SparkSession sparkSession, Properties properties, String table) {
        return sparkSession.read()
                .format("kudu")
                .option("kudu.master", properties.getProperty("kudu.master"))
                .option("kudu.table", table)
                .load();
    }
}
