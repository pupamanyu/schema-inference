package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.functions.ProcessStringColumn;
import com.example.schemainfer.protogen.functions.ProcessTextColumn;
import com.example.schemainfer.protogen.utils.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class SeqFilesScan {

    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {

        LOG.info("Starting ScanSeq");

        SparkConf conf = new SparkConf().setAppName("Read Seq UDF")
            //    .setMaster("local[2]").set("spark.driver.host", "localhost") ;
           .set("spark.eventLog.enabled","true")
                .setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        ///readValuesAsString(spark, sc);
        readValuesAsText(spark, sc);

        // Register the UDF with our SparkSession
        spark.udf().register("PSCHEMA", new UDF1<String, String>() {
            @Override
            public String call(String inStr) {
                return ("Helloooo: " + inStr);
            }
        }, DataTypes.StringType);

        // spark.sql("SELECT PSCHEMA(fmapvalue) AS udfvalue FROM gameevent").show();
    }

    private static String getSchemaFromFile() throws IOException {
        URL schemaURL = ClassLoader.getSystemResource("schema.json");
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[2048];
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

    private static void readValuesAsString(SparkSession spark, JavaSparkContext jsc) {
        JavaRDD<String> values = jsc.textFile(Constants.inputFile) ;

        LOG.info("Values: " + values.toDebugString());

        JavaRDD<Protomap> parsedRDD =  transformFValueIntoProromap2S(values) ;
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        LOG.info("Parsed RDD count: " + parsedRDD.count());

    }

    private static void readValuesAsText(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(Constants.inputFile, BytesWritable.class, Text.class);

        LOG.info("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();

        LOG.info("Values: " + values.toDebugString());
        LOG.info("Count of values: " + rdd.values().count());

        JavaRDD<Protomap> parsedRDD = transformFValueIntoProromap3(values);

        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        LOG.info("Parsed RDD count: " + parsedRDD.count());

        // printTop20(parsedRDD);
        // JavaRDD<Protomap> protoRDD = transformFValueIntoProromap(parsedRDD);
        // convertRDDtoDataset(spark, protoRDD);
    }


    private static void convertRDDtoDataset(SparkSession spark, JavaRDD<Protomap> protoRDD) {
        Dataset<Row> dataset = spark.createDataFrame(protoRDD, Protomap.class);
        dataset.createOrReplaceTempView(Constants.registeredViewName);
        dataset.printSchema();
        dataset.show(5);
    }

    private static JavaRDD<Protomap> transformFValueIntoProromap3(JavaRDD<Text> values) {
        return values.flatMap(new ProcessTextColumn() );
    }

    private static JavaRDD<Protomap> transformFValueIntoProromap2S(JavaRDD<String> values) {
        return values.flatMap(new ProcessStringColumn() );
    }

    private static void printTop20F(JavaRDD<String> parsedRDD) {
        List<String> top5List = parsedRDD.take(20);
        top5List.forEach(s -> {
            LOG.info(" FValue = " + s);
        });
    }

    private static void printTop20(JavaRDD<Map<String, String>> parsedRDD) {
        List<Map<String, String>> top5List = parsedRDD.take(20);
        top5List.forEach(s -> {
            LOG.info("-----------------------------------------------------------------------------------------");
            LOG.info("Parsed String MAP: " + s.toString());
            for (Map.Entry<String, String> entry : s.entrySet()) {
                LOG.info("Key = " + entry.getKey() +
                        " Value = " + entry.getValue());
            }
        });
    }
}
