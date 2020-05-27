package com.example.schemainfer.protogen.javaudf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class SeqScanAsJson {
    private static String inputFile = "data/json/legs_gameevent.json";
    private static String registeredViewName = "gameevent";

    public static void mainn(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("Java UDF Example").setMaster("local");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String schema = getSchemaFromFile();
        readDataFileAsJson(spark, schema);

        // Register the UDF with our SparkSession
        spark.udf().register("CTOF", new UDF1<String, String>() {
            @Override
            public String call(String inStr) {
                return ("Helloooo: " + inStr);
            }
        }, DataTypes.StringType);

          spark.sql("SELECT CTOF(platformId) AS pschema FROM gameevent").show();
    }

    private static void readDataFileAsJson(SparkSession spark, String schema) {

        StructType ss = (StructType) StructType.fromJson(schema);
        Dataset<Row> ds = spark.read()
                .option("multiline", "true")
                .option("mode", "FAILFAST")
                   .option("inferSchema", true)
                //  .option("badRecordsPath", "~/temp/badRecordsPath")
                //  .option("mode", "DROPMALFORMED")
                .schema(ss)
                .json(inputFile);
        ds.createOrReplaceTempView(registeredViewName);
        ds.printSchema();
        ds.show();
    }

    private static void deriveSchema(Dataset<Row> ds) {
        StructType structType = ds.schema();
        String prettyJsonSchema = ds.schema().prettyJson();
        System.out.println("SchemaString:" + structType.toString());
    }

    private static String getSchemaFromFile() throws IOException {
        URL schemaURL = ClassLoader.getSystemResource("schema.json");
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[1024];
        int bytesRead = inputstream.read(data);
        StringBuffer schemaBuff = new StringBuffer();

        while (bytesRead != -1) {
            String s = new String(data);
            schemaBuff.append(s);
            bytesRead = inputstream.read(data);
        }
        inputstream.close();
        return schemaBuff.toString();
    }


    private static void readFileAsText(SparkSession spark) {
        Dataset<Row> ds = spark.read().text(inputFile);
        ds.createOrReplaceTempView(registeredViewName);
        //  System.out.println("COLUMN value: " + ds.col("value").toString()) ;
        ds.show();
    }
}
