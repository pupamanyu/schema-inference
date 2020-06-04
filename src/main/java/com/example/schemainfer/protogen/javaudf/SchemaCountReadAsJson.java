package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.utils.Constants;
import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SchemaCountReadAsJson {
    private static String inputFile;
    private static String registeredViewName;

    public SchemaCountReadAsJson() {
    }

    public static void mainn(String[] args) throws IOException {
        SparkConf conf = (new SparkConf()).setAppName("Java UDF Example").setMaster("local[2]").set("spark.driver.host", "localhost");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        readDataFileAsJson(spark);
        spark.udf().register("CTOF", new UDF1<String, String>() {
            public String call(String inStr) {
                return "Helloooo: " + inStr;
            }
        }, DataTypes.StringType);
    }

    private static void readDataFileAsJson(SparkSession spark) {
        Dataset<Row> ds = spark.read().option("mode", "FAILFAST").option("inferSchema", true).json(inputFile);
        ds.createOrReplaceTempView(registeredViewName);
        ds.printSchema();
        ds.show();
        getObjectNodeFromSchemaString(spark, ds);
    }

    private static void getObjectNodeFromSchemaString(SparkSession spark, Dataset<Row> ds) {
        Column schemaCol = ds.col("schema");
       // Dataset<String> dsjson = ds.select(schemaCol).map((row) -> {
       //     return (String)row.getAs("schema"). ;
       // }, Encoders.STRING());
        String dsjson = "{}";
        Dataset<Row> rowDataset = spark.read().option("inferSchema", true).json(dsjson);
        rowDataset.printSchema();
        rowDataset.show();
    }

    static {
        inputFile = Constants.outputFile;
        registeredViewName = "gameevent";
    }
}