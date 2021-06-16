package me.iroohom.sparkUtils.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * 读取jdbc
 */
public class JdbcSource {

    /**
     * 读取impala
     *
     * @param sparkSession sparkSession对象
     * @param properties   conf配置
     * @param sql          查询SQL
     * @return DataFrame数据集
     */
    public static Dataset<Row> impala(SparkSession sparkSession, Properties properties, String sql) {
        return sparkSession.read()
                .format("jdbc")
                .option("driver", "com.cloudera.impala.jdbc41.Driver")
                .option("url", properties.getProperty("impala.url"))
                .option("dbtable", "(" + sql + ") as tmp")
                .load();
    }

/*    public static Dataset<Row> withPartImpala(SparkSession sparkSession, Properties properties, String sql,long socketTimeout,long numPartitions,String partitionColumn,long lowerBound,long upperBound) {
        return sparkSession.read()
                .format("jdbc")
                .option("driver", "com.cloudera.impala.jdbc41.Driver")
                .option("socketTimeout",socketTimeout)
                .option("url", properties.getProperty("impala.url"))
                .option("dbtable", "(" + sql + ") as tmp")
                .option("numPartitions",numPartitions)
                .option("partitionColumn",partitionColumn)
                .option("lowerBound",lowerBound)
                .option("upperBound",upperBound)
                .load();
    }*/

}
