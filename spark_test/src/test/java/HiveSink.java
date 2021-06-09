
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

/**
 * 写入hive
 * @author WY
 */
public class HiveSink {

    /**
     * 无分区写入
     */
    public static void noPart(SparkSession sparkSession, Dataset<Row> df, String table, String saveMode) {
        df.createOrReplaceTempView("temp_view");
        sparkSession.sql("insert " + saveMode + " table " + table + " " + "select * from temp_view");
    }

    /**
     * 有分区写入
     * @param partInfo 分区信息（字段 或 字段 = 指定值）
     */
    public static void withPart(SparkSession sparkSession, Dataset<?> df, String table, String saveMode, String partInfo) {
        df.createOrReplaceTempView("temp_view");
        sparkSession.sql(SqlUtil.sqlBuilder("insert", saveMode, "table", table, "partition(", partInfo, ")", "select * from temp_view"));
    }

    public static String createTableIfNotExists(Dataset<Row> df, String table) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE if not exists " + table + " (\n");
        for (StructField field : df.schema().fields()) {
            builder.append("`" + field.name() + "` " + field.dataType().typeName() + ",\n");
        }
        builder.deleteCharAt(builder.lastIndexOf(","));
        builder.append(")\nstored as parquet");
        return builder.toString();
    }
}
