package me.iroohom.sparkUtils.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * 写文本文件
 * @author WY
 */
public class FileSink {

    /**
     * 写csv
     */
    public static void csv(Dataset<Row> df, String path, SaveMode saveMode, Boolean header, String encoding) {
        df.write()
                .mode(saveMode)
                .option("header", header)
                .option("encoding", encoding)
                .csv(path);
    }
}
