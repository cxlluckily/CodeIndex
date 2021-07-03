package com.amazingSaltFish;

import com.amazingsaltfish.spark.LocalSparkSession;
import org.apache.spark.sql.SparkSession;

public class Test {
    public static void main(String[] args) {
        SparkSession spark = LocalSparkSession.getLocalSparkSession();
    }
}
