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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
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

/**
 * The Main class.
 *
 * Objective:
 * Given input in a sequential file
 * Derive a superset protobuf schema from nested key value pairs
 *
 * This will be achieved in following steps:
 * Stage 0 - Input Data	8
 * Stage 1- Convert from Sequential format
 * Stage 3 - Column split
 * Stage 4 - Convert key value pairs into Json
 * Stage 5 - Infer data types and generate Json schema
 * Stage 6 - Repeat for all rows/files
 * Stage 7 - Gather distinct schemas and its count
 * Stage 8 - Sort the dataset in descending order of count
 * Stage 9 - Parse json into Java Object
 * Stage 10 - Compare with other json schema objects
 * Stage 11 - Find superset
 * Stage 12 - Persist
 *
 */
public class SeqFilesScan {
    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);

    public SeqFilesScan() {
    }

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        LOG.info("Starting ScanSeq");
        String mode = getRunMode(args);
        SparkConf conf = getSparkConf(mode) ;
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        readAndProcessFiles(mode, sc, spark) ;

        registerSparkUDF(spark);
    }

    /**
     * Determine if it is local run then read local String file
     * Otherwise the job is runnig in cluster then read Sequential format files
     * @param mode
     * @param sc
     * @param spark
     */
    private static void readAndProcessFiles(String mode, JavaSparkContext sc, SparkSession spark) {
        if (mode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
            readValuesAsString(spark, sc) ;
        } else {
            readValuesAsText(spark, sc);
        }
    }

    private static SparkConf getSparkConf(String mode) {
        SparkConf conf = new SparkConf()
                .setAppName("Parse Seq UDF")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ;
        if (mode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
            conf.setMaster("local[2]").set("spark.driver.host", "localhost") ;
        } else {
            conf.setMaster("yarn").set("spark.eventLog.enabled", "true");
        }

        return conf ;
    }

    private static String getRunMode(String[] args) {
        String mode = Constants.RUN_MODE.Cluster.name() ;
        if (args != null && args.length > 0) {
            String argmode = args[0] ;
            if (!argmode.isEmpty()) {
                if (argmode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
                    mode = Constants.RUN_MODE.Local.name() ;
                }
            }
        }
        return mode;
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
        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap2O(values);
        long totalCount = parsedRDD.count();
        System.out.println("Parsed RDD totalCount: " + totalCount);
        JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        System.out.println("Distinct RDD totalCount: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount(objectNodeLongMap, totalCount);
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
        Long totalCount = parsedRDD.count() ;
        LOG.info("Parsed RDD count: " + totalCount);
        JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        LOG.info("Distinct RDD count: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount(objectNodeLongMap, totalCount);
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


    private static void registerSparkUDF(SparkSession spark) {
        spark.udf().register("JSCHEMA", new UDF1<String, String>() {
            @Override
            public String call(String inStr) {
                return (inStr);
            }
        }, DataTypes.StringType);

        spark.sql("SELECT JSCHEMA(schema) AS jsonschema FROM gameevent").show();
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
