package com.csvw.skoda;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Properties;

public class ReadHdfsFileAsJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("ReadTextHdfsFile").getOrCreate();
//        Dataset<Row> df = spark.read().text("hdfs://svldl067.csvw.com/ci_sc/dws_tt_invoice_info/dt=20210623");
        Dataset<Row> df = spark.read().text("/Users/roohom/Code/IDEAJ/CodeIndex/spark_test/data/tt_invoice_info/out/tt_invoice_info.txt");
        ArrayList<StructField> list = new ArrayList<>();
        list.add(DataTypes.createStructField("vin", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("report_date", DataTypes.StringType, true));
//        JavaPairRDD<String, String> rdd = df.toJavaRDD().mapToPair(row ->
//                new Tuple2<String, String>(row.getString(0).split("\t")[0], row.getString(0).split("\t")[1])
//        );
        ////////////////For one hand//////////////////
//        Dataset<Tuple2<String, String>> map = df.map(
//                (MapFunction<Row, Tuple2<String, String>>)
//                        x -> new Tuple2<>(x.getString(0).split("\t")[0], x.getString(0).split("\t")[1])
//                , Encoders.tuple(Encoders.STRING(), Encoders.STRING())
//        );
//        map.foreach(x->{
//            System.out.println(x._1+" : "+x._2);
//        });
        ////////////////For one hand//////////////////

        //注意以下java代码的lambda表达式写法
        Dataset<Invoice> mapDf = df.map(
                (MapFunction<Row, Invoice>)
                        x -> new Invoice(x.getString(0).split("\t")[0], x.getString(0).split("\t")[1])
                , Encoders.bean(Invoice.class)
        );
//
//        StructType schema = DataTypes.createStructType(list);
//        Dataset<Row> df2 = spark.createDataFrame(mapDf.rdd(), Invoice.class).select("vin", "report_date");
//        df2.show();
        mapDf.select("vin", "report_date").show(false);

//        df.createOrReplaceTempView("df");
//        spark.sql("select split(value,'\t')[0] as vin,split(value,'\t')[1] as report_date from df").show();
//        df.show(false);
//        df.write().text("/Users/roohom/Code/IDEAJ/CodeIndex/spark_test/data/tt_invoice_info/out");
        repairProcessor(spark, "", "", "");
    }

    public static void repairProcessor(SparkSession sparkSession, String start, String end, String insertMode) {
        String hdfsPath = "/Users/roohom/Code/IDEAJ/CodeIndex/spark_test/data/tt_repair_yisun_info/out/tt_repair_yisun_info.txt";
        //"2021-06-01" -> "20210601"
//        String dt = start.replaceAll("-", "");
        //拉取数据
        Dataset<Row> repairSource = sparkSession.read().text(hdfsPath);
        Dataset<Repair> repairDF = repairSource.map(
                (MapFunction<Row, Repair>)
                        x -> new Repair(
                                x.getString(0).split("\t")[0],
                                x.getString(0).split("\t")[1],
                                x.getString(0).split("\t")[2],
                                x.getString(0).split("\t")[3],
                                x.getString(0).split("\t")[3]
                        ),
                Encoders.bean(Repair.class)
        );
        Dataset<Row> repairResultDF = repairDF.select("vin", "series_code", "part_name", "report_date", "current_milemetre");
        repairResultDF.show();
    }
}
