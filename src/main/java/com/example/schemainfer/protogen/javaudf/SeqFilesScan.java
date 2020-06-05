package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.functions.ProcessStringColumn;
import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import com.example.schemainfer.protogen.functions.ProcessTextColumn;
import com.example.schemainfer.protogen.functions.ProcessTextColumn2;
import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
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

    public SeqFilesScan() {
    }

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        LOG.info("Starting ScanSeq");
        SparkConf conf = (new SparkConf()).setAppName("Read Seq UDF").
                set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .setMaster("local[2]").set("spark.driver.host", "localhost") ;
               // .set("spark.eventLog.enabled", "true").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

       // readValuesAsText(spark, sc);
       // readValuesAsString(spark, sc) ;
        readValuesAsStringO(spark, sc) ;

        spark.udf().register("PSCHEMA", new UDF1<String, String>() {
            @Override
            public String call(String inStr) {
                return (inStr);
            }
        }, DataTypes.StringType);

         spark.sql("SELECT PSCHEMA(schema) AS jsonschema FROM gameevent").show();
    }

    private static String getSchemaFromFile() throws IOException {
        URL schemaURL = ClassLoader.getSystemResource("schema.json");
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[2048];
        int bytesRead = inputstream.read(data);

        StringBuffer schemaBuff;
        for(schemaBuff = new StringBuffer(); bytesRead != -1; bytesRead = inputstream.read(data)) {
            String s = new String(data);
            schemaBuff.append(s);
        }

        inputstream.close();
        return schemaBuff.toString();
    }

    private static void readValuesAsString(SparkSession spark, JavaSparkContext jsc) {
        JavaRDD<String> values = jsc.textFile(Constants.inputFile);
        LOG.info("Values: " + values.toDebugString());
        JavaRDD<Protomap> parsedRDD = transformFValueIntoProromap2S(values);
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        LOG.info("Parsed RDD count: " + parsedRDD.count());
    }

    private static void readValuesAsStringO(SparkSession spark, JavaSparkContext jsc) {
        JavaRDD<String> values = jsc.textFile(Constants.inputFile);
        LOG.info("Values: " + values.toDebugString());
        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap2O(values);
        System.out.println("Parsed RDD count: " + parsedRDD.count());
        JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        System.out.println("Distinct RDD count: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        List<SchemaCount> schemaCountList = CommonUtils.printDistinctObjectNodesCount(objectNodeLongMap);
        convertSchemaCountRDDtoDataset(spark, schemaCountList);
    }

    private static void readValuesAsText(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(Constants.inputFile, BytesWritable.class, Text.class);
        LOG.info("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();
        LOG.info("Values: " + values.toDebugString());
        LOG.info("Count of values: " + rdd.values().count());
        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap31(values) ;
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        LOG.info("Parsed RDD count: " + parsedRDD.count());
        JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        LOG.info("Distinct RDD count: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        List<SchemaCount> schemaCountList = CommonUtils.printDistinctObjectNodesCount(objectNodeLongMap);
        convertSchemaCountRDDtoDataset(spark, schemaCountList);
    }

    private static void convertRDDtoDataset(SparkSession spark, JavaRDD<Protomap> protoRDD) {
        Dataset<Row> dataset = spark.createDataFrame(protoRDD, Protomap.class);
        dataset.printSchema();
        dataset.show(5);
    }

    private static void convertSchemaCountRDDtoDataset(SparkSession spark, List<SchemaCount> schemaCountList) {
        Dataset<Row> dataset = spark.createDataFrame(schemaCountList, SchemaCount.class);
        dataset.createOrReplaceTempView(Constants.registeredViewName);
        dataset.printSchema();
        dataset.show(5);
        dataset.write().json(Constants.outputFile);

        getSchemaColumnDataset(dataset) ;
    }

    private static JavaRDD<Protomap> transformFValueIntoProromap3(JavaRDD<Text> values) {
        return values.flatMap(new ProcessTextColumn()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static JavaRDD<ObjectNode> transformFValueIntoProromap31(JavaRDD<Text> values) {
        return values.map(new ProcessTextColumn2()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static JavaRDD<Protomap> transformFValueIntoProromap2S(JavaRDD<String> values) {
        return values.flatMap(new ProcessStringColumn()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static JavaRDD<ObjectNode> transformFValueIntoProromap2O(JavaRDD<String> values) {
        return values.map(new ProcessStringColumnAsObjectNode()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static void getSchemaColumnDataset(Dataset<Row> ds) {
        Column schemaCol = ds.col("schema");
        final Dataset<String> schemadataset = ds.select("schema").as(Encoders.STRING());

        List<String> listOne = schemadataset.collectAsList();

        schemadataset.printSchema();
        schemadataset.show();

        for (String ss : listOne) {
            System.out.println("PP=" + ss);
            parseJson(ss);
        }
    }

    private static void parseJson(String jsonString)  {
        ObjectMapper mapper = new ObjectMapper();
        EventJsonSchema eventJsonSchema = null;
        try {
            eventJsonSchema = mapper.readValue(jsonString, EventJsonSchema.class);
            System.out.println("JsonStructSchema Node: " + eventJsonSchema.getAdditionalProperties().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
