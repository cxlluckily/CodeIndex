package me.iroohom.sparkUtils.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 读文本文件
 */
public class FileSource {

    /**
     * 读csv
     */
    public static Dataset<Row> csv(SparkSession sparkSession, String path, Boolean header, String encoding) {
        return sparkSession.read()
                .option("header", header)
                .option("encoding", encoding)
                .csv(path);
    }
}
