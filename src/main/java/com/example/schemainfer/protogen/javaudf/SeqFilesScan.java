package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.SchemaColumnMap;
import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.functions.MapColumnIntoObjectNode;
import com.example.schemainfer.protogen.functions.MapObjectNodesMapToColumn;
import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import com.example.schemainfer.protogen.functions.ProcessTextColumn2;
import com.example.schemainfer.protogen.json.CompareSchemas;
import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.GCSBlobWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
///import com.google.cloud.storage.Blob;
////import com.google.cloud.storage.BlobId;
////import com.google.cloud.storage.Storage;
////import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
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
       // testStorage() ;

        readAndProcessFiles(mode, sc, spark) ;
        //registerSparkUDF(spark);
    }

    public static void testStorage() throws IOException {
        String newString = "Hello, World!";

        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            byte[] bytes = newString.getBytes(Constants.UTF8_CHARSET);
            Blob blob = storage.get(BlobId.of("schema-inference-out", "protos/gameevent.proto"));
            LOG.info("1) Successful getting Storage ID: " + blob.toString()) ;

            WritableByteChannel channel = blob.writer();
            LOG.info("2) Successful getting WritableByteChannel :" + blob.toString()) ;
            channel.write(ByteBuffer.wrap(newString.getBytes(Constants.UTF8_CHARSET)));
            LOG.info("3) Successful writing TO hannel :" ) ;
            channel.close();
            LOG.info("4) Successful cloding hannel :" ) ;
        } catch (Exception ioc) {
            LOG.error("Got error-1 writing to storage: " + ioc.getMessage()) ;
        }

        GCSBlobWriter testWriter = new GCSBlobWriter("protos/gameevent.proto");

        LOG.info("5) Successful opening GCSBlobWriter : "  + testWriter.toString()) ;
        testWriter.write("Hello testing");
        LOG.info("6) Successful writing GCSBlobWriter : "  + testWriter.toString()) ;
        if (testWriter != null && testWriter.getWriterChannel() != null) {
            testWriter.getWriterChannel().close();
        }
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
      //  LOG.info("Values: " + values.toDebugString());
      //  JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap2O(values);
        JavaRDD<Tuple2<String, ObjectNode>> parseRDD = transformFValueIntoProromap21(values);

        processTransformationsMATCH(spark, parseRDD);
    }

    private static void readValuesAsText(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(Constants.inputFile, BytesWritable.class, Text.class);
        LOG.info("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();
        LOG.info("Values: " + values.toDebugString());
        LOG.info("Count of values: " + rdd.values().count());
        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap31(values) ;
        processTransformations(spark, parsedRDD);
    }

    private static void processMatches(SparkSession spark, JavaRDD<Tuple2<String, ObjectNode>> parsedRDD) {
       // final Dataset<Row> sparkDataFrame = spark.createDataFrame(parsedRDD, Tuple2.class);

        final List<SchemaColumnMap> schemaColumnMaps = parsedRDD.map((v1) -> {
            String colValue = v1._1();
            String schema = v1._2().toString();
            return new SchemaColumnMap(schema, colValue);
        }).collect();

        final Dataset<Row> sparkDataFrame = spark.createDataFrame(schemaColumnMaps, SchemaColumnMap.class);
        System.out.println("Tuple dataset start: " + sparkDataFrame.count()) ;

        Dataset<Row> dataSet1 = sparkDataFrame.groupBy("schema").agg(org.apache.spark.sql.functions.collect_list("colValue")).limit(5).toDF() ;
        dataSet1.show() ;
        System.out.println("Tuple dataset end: " + sparkDataFrame.count()) ;

        if (!Constants.isLocal) {
            dataSet1.write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", Constants.gcsTempLocation)
                    .mode(SaveMode.Overwrite)
                    .save(Constants.BIG_QUERY_DATASET + "." + Constants.BIG_QUERY_SAMPLE_SCHEMA);
        }

    }


    private static void processTransformationsMATCH(SparkSession spark, JavaRDD<Tuple2<String, ObjectNode>> parsedRDD) {
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        Long totalCount = parsedRDD.count() ;
        LOG.info("Parsed RDD count: " + totalCount);

       //// JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.map(t -> t._2()).distinct() ;
        //JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();

        /////// Begin of match objectNode with column
        processMatches(spark, parsedRDD) ;
        ////// End of match objectNode with column

        Map<ObjectNode, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1._2();
        }).countByValue();

        LOG.info("Distinct RDD count: " + objectNodeLongMap.size());
/*        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount2(objectNodeLongMap, totalCount);
        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema) ;
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(spark, protoHierarchy) ;
        transformProtobufHierarchy.generate();*/
    }

    private static void processTransformations2(SparkSession spark, JavaRDD<ObjectNode> parsedRDD) {
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        Long totalCount = parsedRDD.count() ;
        LOG.info("Parsed RDD count: " + totalCount);
        /////JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        /////LOG.info("Distinct RDD count: " + distinctObjectNodeRDD.count());
        Map<String, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.toString();
        }).countByValue();
        LOG.info("Distinct RDD count value: " + objectNodeLongMap.size());
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount(objectNodeLongMap, totalCount);
        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema) ;
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(spark, protoHierarchy) ;
        transformProtobufHierarchy.generate();
    }

    private static void processTransformations(SparkSession spark, JavaRDD<ObjectNode> parsedRDD) {
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        Long totalCount = parsedRDD.count() ;
        LOG.info("Parsed RDD count: " + totalCount);
        /////JavaRDD<ObjectNode> distinctObjectNodeRDD = parsedRDD.distinct();
        /////LOG.info("Distinct RDD count: " + distinctObjectNodeRDD.count());

        Map<ObjectNode, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1;
        }).countByValue();

        LOG.info("Distinct RDD count value " + objectNodeLongMap.size());

       ////// List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount(objectNodeLongMap, totalCount);
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount2(objectNodeLongMap, totalCount);

        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema) ;
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(spark, protoHierarchy) ;
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

        LOG.info("Final Top Schema : " + mergedTopSchema.toString()) ;
        return mergedTopSchema ;
    }


    private static void registerSparkUDF(SparkSession spark) {
        spark.udf().register("JSCHEMA", new UDF1<String, String>() {
            @Override
            public String call(String inStr) {
                return (inStr);
            }
        }, DataTypes.StringType);

       // spark.sql("SELECT JSCHEMA(schema) AS jsonschema FROM gameevent").show();
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

    private static JavaRDD<Tuple2<String, ObjectNode>> transformFValueIntoProromap22(JavaRDD<String> values) {
        final JavaRDD<Tuple2<String, ObjectNode>> tuple2JavaRDD = values.map(new MapColumnIntoObjectNode()).filter((v1) -> {
            return v1 != null;
        });

        return tuple2JavaRDD ;
    }

    private static JavaRDD<Tuple2<String, ObjectNode>> transformFValueIntoProromap21(JavaRDD<String> values) {
        return values.map(new MapColumnIntoObjectNode()).filter((v1) -> {
            return v1 != null;
        });

        //final JavaRDD<ObjectNode> osRDD = tuple2JavaRDD.map(t -> t._2()).filter(s -> s != null) ;
        //return osRDD ;
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
