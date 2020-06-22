package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import com.example.schemainfer.protogen.functions.ProcessTextColumn2;
import com.example.schemainfer.protogen.json.CompareSchemas;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Main class .
 *
 * Objective:
 * Given input data in a sequence file
 * Derive a superset protobuf schema from nested key value pairs
 *
 * This will be achieved in following steps:
 * Stage 0 - Input Data in Sequence files in GCS
 * Stage 1-  Convert from Sequential format
 * Stage 3 - Column split into key-value pairs
 * Stage 4 - Convert key value pairs into Json
 * Stage 5 - Generate Json schema with json datatypes
 * Stage 6 - Repeat for all rows/files
 * Stage 7 - Gather distinct schemas and its count
 * Stage 8 - Sort the Spark dataset in descending order of count
 * Stage 9 - Parse json from 'schema' column into Java Object
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
        LOG.info("Mode = " + mode) ;
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

    private static void readValuesAsString(SparkSession spark, JavaSparkContext jsc) {
        JavaRDD<String> values = jsc.textFile(Constants.inputFile);
        LOG.info("Values: " + values.toDebugString());
        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap2O(values);
        long totalCount = parsedRDD.count();
        LOG.info("Parsed RDD totalCount: " + totalCount);
        JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        LOG.info("Distinct RDD totalCount: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount(objectNodeLongMap, totalCount);
        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema) ;
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(protoHierarchy) ;
        transformProtobufHierarchy.generate();
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
        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema) ;
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(protoHierarchy) ;
        transformProtobufHierarchy.generate();
    }

    private static void convertRDDtoDataset(SparkSession spark, JavaRDD<Protomap> protoRDD) {
        Dataset<Row> dataset = spark.createDataFrame(protoRDD, Protomap.class);
        dataset.printSchema();
        dataset.show(5);
    }

    private static EventJsonSchema convertSchemaCountRDDtoDataset(SparkSession spark, List<SchemaCount> schemaCountList) {
        Dataset<Row> dataset = spark.createDataFrame(schemaCountList, SchemaCount.class);
        dataset.createOrReplaceTempView(Constants.registeredViewName);
        dataset.printSchema();
       //////// dataset.show(5);
       //////// dataset.write().json(Constants.outputFile);

        List<EventJsonSchema> distinctSchemaList = getSchemaColumnDataset(dataset) ;
        List<EventJsonSchema> mergedSchemaList = new ArrayList<>() ;
        if (distinctSchemaList == null || distinctSchemaList.size()== 0) {
            LOG.error("NO Distinct schemaLIST FOund. Please check.") ;
            return null ;
        }
        EventJsonSchema topSchema = distinctSchemaList.get(0) ;
        EventJsonSchema mergedTopSchema = topSchema ;

        for (int i=1; i< distinctSchemaList.size() ; i++) {
            if (i > 10) {
                break ;
            }
            final EventJsonSchema comparedJsonSchema = CompareSchemas.compareTwoSchemas(mergedTopSchema, distinctSchemaList.get(i));
            if (comparedJsonSchema != null) {
                mergedTopSchema = comparedJsonSchema ;
            }
             LOG.info("Finished comparing schema: " + i) ;
         //   Dataset<Row> mergedSchemaDataset = spark.read().option("inferSchema", true).json(eventJsonSchema.getAdditionalProperties().toString());
         //   Dataset<Row> rowDataset = spark.read().option("inferSchema", true).json(dsjson);
         //   mergedSchemaDataset.write().json(Constants.outputFile2+i);
        }

        return mergedTopSchema ;
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

    private static JavaRDD<ObjectNode> transformFValueIntoProromap31(JavaRDD<Text> values) {
        return values.map(new ProcessTextColumn2()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static JavaRDD<ObjectNode> transformFValueIntoProromap2O(JavaRDD<String> values) {
        return values.map(new ProcessStringColumnAsObjectNode()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static List<EventJsonSchema> getSchemaColumnDataset(Dataset<Row> ds) {
        Column schemaCol = ds.col("schema");
        final Dataset<String> schemadataset = ds.select("schema").as(Encoders.STRING());
        List<EventJsonSchema> distinctSchemaList = new ArrayList<>() ;

        List<String> listOne = schemadataset.collectAsList();

        schemadataset.printSchema();
        schemadataset.show();

        for (String ss : listOne) {
            LOG.info("DISTINCT Schema= " + ss);
            EventJsonSchema eventJsonSchema = parseJson(ss);
            if (eventJsonSchema != null) {
                distinctSchemaList.add(eventJsonSchema);
            }
        }

        return distinctSchemaList ;
    }

    private static EventJsonSchema parseJson(String jsonString)  {
        ObjectMapper mapper = new ObjectMapper();
        EventJsonSchema eventJsonSchema = null;
        try {
            eventJsonSchema = mapper.readValue(jsonString, EventJsonSchema.class);
            LOG.info("JsonStructSchema ParsedNode: " + eventJsonSchema.getAdditionalProperties().toString());
            return eventJsonSchema ;
        } catch (IOException e) {
            LOG.error("Could not parse into EventJsonSchema: " + jsonString + " --> " + e.getMessage()) ;
            e.printStackTrace();
        }
        return null ;
    }
}
