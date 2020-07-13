package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.SchemaColumnMap;
import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import com.example.schemainfer.protogen.functions.ProcessTextColumn2;
import com.example.schemainfer.protogen.functions.ProcessTextColumn4;
import com.example.schemainfer.protogen.json.CompareSchemas;
import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Main class .
 * <p>
 * Objective:
 * Given input data in a sequence file
 * Derive a superset protobuf schema from nested key value pairs
 * <p>
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
 */
public class SeqFilesScan {
    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);

    @Option(name = "-m", aliases = "--mode", usage = "Mode local/cluster")
    private String runmode;

    @Option(name = "-i", aliases = "--input", usage = "Fully qualified path for input file(s)", required = true)
    private String inputFile;

    @Option(name = "-o", aliases = "--gcsoutputbucketname", usage = "GCS Bucket Name for output file(s)", required = true)
    private String outputBucketName;

    @Option(name = "-t", aliases = "--gcstempbucketname", usage = "GCS Bucket Name for temp file(s)", required = true)
    private String gcsTempBucketName;

    @Option(name = "-tb", aliases = "--bigquerytable", usage = "Output BigQuery table name", required = true)
    private String bqouttablename;

    @Option(name = "-ds", aliases = "--dataset", usage = "BigQuery destination dataset name", required = true)
    private String bqdatasetname;

    @Option(name = "-s", aliases = "--writesample", usage = "Skip writing matching sample data to BQ", required = false)
    private String skipSampleDataMatch;

    @Option(name = "-n", aliases = "--numerOfTopSchemasToMerge", usage = "Number of top schemas to be consider for merge", required = false)
    private String numberOfTopSchemasToMerge;

    @Option(name = "-pa", aliases = "--numberOfPartitions", usage = "Number of Partitions for repartitioning", required = false)
    private String numberOfPartitions ;

    public SeqFilesScan() {
    }

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        LOG.info("Starting ScanSeq");
        SeqFilesScan seqFilesScan = new SeqFilesScan();
        seqFilesScan.doSparkMain(args);
        LOG.info("Finished ScanSeq");
    }

    private void doSparkMain(String[] args) {
        parseArgs(args);
        SparkConf conf = getSparkConf(this.runmode);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        readAndProcessFiles(this.runmode, sc, spark);
        registerSparkUDF(spark);
    }

    private void parseArgs(final String[] arguments) {
        final CmdLineParser parser = new CmdLineParser(this);
        if (arguments.length < 1) {
            parser.printUsage(System.out);
            System.exit(-1);
        }
        try {
            parser.parseArgument(arguments);
        } catch (CmdLineException clEx) {
            throw new IllegalArgumentException("ERROR: Unable to parse command-line options: " + clEx);
        }

        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance();
        boolean skipSampleDatawrite = isSkipSampleDatawrite();
        int numberOfSchemasToCOnsider = getNumberOfTopSchemasToMerge() ;
        int numOfPartitions = getNumberOfPartitions() ;
        schemaInferConfig.build(this.runmode, this.inputFile, this.outputBucketName, this.gcsTempBucketName, this.bqouttablename, skipSampleDatawrite, numberOfSchemasToCOnsider, this.bqdatasetname, numOfPartitions);
    }

    private boolean isSkipSampleDatawrite() {
        boolean skipSampleDatawrite = true ;
        if (this.skipSampleDataMatch == null || this.skipSampleDataMatch.isEmpty()) {
            skipSampleDatawrite = false ;
        } else {
            skipSampleDatawrite = Boolean.parseBoolean(skipSampleDataMatch) ;
        }
        return skipSampleDatawrite;
    }

    private Integer getNumberOfTopSchemasToMerge() {
        int numberOfTopSchemas  ;
        if (this.numberOfTopSchemasToMerge == null || this.numberOfTopSchemasToMerge.isEmpty()) {
            numberOfTopSchemas = Constants.defaultNumberOfTopSchemas ;
        } else {
            try {
                numberOfTopSchemas = Integer.valueOf(this.numberOfTopSchemasToMerge);
            } catch (NumberFormatException nbe) {
                LOG.warn("Arguments of 'number of schemas to be considered for merge' is not a valid integer. Default to application default: " + Constants.defaultNumberOfTopSchemas);
                numberOfTopSchemas = Constants.defaultNumberOfTopSchemas ;
            }
        }
        return numberOfTopSchemas;
    }

    private Integer getNumberOfPartitions() {
        int numberOfPartitions  ;
        if (this.numberOfPartitions == null || this.numberOfPartitions.isEmpty()) {
            numberOfPartitions = 0 ;
        } else {
            try {
                numberOfPartitions = Integer.valueOf(this.numberOfPartitions);
            } catch (NumberFormatException nbe) {
                LOG.warn("Arguments of 'number of schemas to be considered for merge' is not a valid integer. Default to application default: " + Constants.defaultNumberOfTopSchemas);
                numberOfPartitions = 0 ;
            }
        }
        return numberOfPartitions;
    }

    /**
     * Determine if it is local run then read local String file
     * Otherwise the job is runnig in cluster then read Sequential format files
     *
     * @param mode
     * @param sc
     * @param spark
     */
    private void readAndProcessFiles(String mode, JavaSparkContext sc, SparkSession spark) {
        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance();
        if (mode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
            readValuesAsString(spark, sc);
        } else {
            if (schemaInferConfig.isSkipWriteSampleDataWIthSchema()) {
                readValuesAsText2(spark, sc);
            } else {
                readValuesAsTextWithMatchSampleData(spark, sc);
            }
        }
    }

    private static SparkConf getSparkConf(String mode) {
        LOG.info("Mode = " + mode);
        SparkConf conf = new SparkConf()
                .setAppName("Parse Seq UDF")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        if (mode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
            conf.setMaster("local[2]").set("spark.driver.host", "localhost");
        } else {
            conf.setMaster("yarn").set("spark.eventLog.enabled", "true");
        }
        return conf;
    }

    private static void readValuesAsString(SparkSession spark, JavaSparkContext jsc) {
        JavaRDD<String> values = jsc.textFile(SchemaInferConfig.getInstance().getInputFile());
        final JavaRDD<ObjectNode> objectNodeJavaRDD = transformFValueIntoProromap2O(values);
        processTransformations(spark, objectNodeJavaRDD);
    }

    private static void readValuesAsText2(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(SchemaInferConfig.getInstance().getInputFile(), BytesWritable.class, Text.class);
        LOG.info("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();
        LOG.info("Values: " + values.toDebugString());
       // LOG.info("Count of values: " + rdd.values().count());
        LOG.info("Count of #Partitions: " + rdd.getNumPartitions()) ;
        int numOfPartitions = SchemaInferConfig.getInstance().getNumberOfPartitions() ;
        JavaPairRDD<BytesWritable, Text> pairedRDD ;
        if (numOfPartitions > 0) {
            pairedRDD = rdd.repartition(numOfPartitions);
        } else {
            pairedRDD = rdd ;
        }
        LOG.info("Count of #Partitions AFTER: " + pairedRDD.getNumPartitions()) ;

        JavaRDD<ObjectNode> parsedRDD = transformFValueIntoProromap31(pairedRDD.values());
       //// LOG.info("Finished Transforming data to ObjectNode: " + parsedRDD.count());
        processTransformations(spark, parsedRDD);
    }

    private static void readValuesAsTextWithMatchSampleData(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(SchemaInferConfig.getInstance().getInputFile(), BytesWritable.class, Text.class);
        LOG.info("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();
        LOG.info("Values: " + values.toDebugString());
        LOG.info("Count of values: " + rdd.values().count());
        final JavaRDD<SchemaColumnMap> tuple2JavaRDD = transformFValueIntoProromap32(values);
        processTransformationsMatch2(spark, jsc, tuple2JavaRDD);
    }

    private static void processSchemaColMapDF(Dataset<Row> sparkDataFrame) {
        sparkDataFrame.printSchema();
        Dataset<Row> dataSet1 = sparkDataFrame.groupBy("schema").agg(functions.first("colvalue")).toDF("schema", "colvalue");
        dataSet1.printSchema();
        LOG.info("GOT top schema data: " + dataSet1.count());
        dataSet1.show();
        SchemaInferConfig inferConfig = SchemaInferConfig.getInstance() ;
        String datasetname = inferConfig.getBqdatasetName() ;

        if (!inferConfig.isLocal()) {
            dataSet1.write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(datasetname + "." + Constants.BIG_QUERY_SAMPLE_SCHEMA);
        }
    }

    private static void processTransformationsMatch2(SparkSession spark, JavaSparkContext jsc, JavaRDD<SchemaColumnMap> parsedRDD) {
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        Long totalCount = parsedRDD.count();
        LOG.info("Parsed RDD count: " + totalCount);

        LOG.info("Starting processTransformationsMatch2 with sample data") ;
        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("schema", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("colvalue", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = parsedRDD.map((SchemaColumnMap scp) -> {
            return RowFactory.create(scp.getSchema().toString(), scp.getColvalue());
        });

        // Apply the schema to the RDD.
        Dataset<Row> rowDataset = spark.createDataFrame(rowRDD, schema);
        LOG.info("Parsed SchemaColumnMap count: " + rowDataset.count());
        rowDataset.show();

        processSchemaColMapDF(rowDataset);

        ////////postProcessAfterDistinctForSchemaMap(spark, parsedRDD, totalCount);
    }

    private static void processTransformations(SparkSession spark, JavaRDD<ObjectNode> parsedRDD) {
        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        Long totalCount = parsedRDD.count();
        LOG.info("Parsed RDD count: " + totalCount);

        LOG.info("Starting processTransformations without writing sample data") ;
       // Map<ObjectNode, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
        //    return v1;
       // }).countByValue();

        Map<ObjectNode, Integer> objectNodeLongMap = countDistinctObjectNodes(parsedRDD) ;

        findDistinctAndPersist(spark, totalCount, objectNodeLongMap);
    }

    public static Map<ObjectNode, Integer> countDistinctObjectNodes(JavaRDD<ObjectNode> data) {

        SchemaInferConfig inferConfig = SchemaInferConfig.getInstance() ;
        int numPartitions = inferConfig.getNumberOfPartitions() ;
        JavaPairRDD<ObjectNode, Integer> counts ;
        JavaPairRDD<ObjectNode, Integer> ones = data
                .mapToPair(s -> new Tuple2<>(s, 1));

        if (numPartitions > 0) {
           counts = ones.reduceByKey((i1, i2) -> {
                return  (i1 + i2) ;
            }, numPartitions);
        } else {
            counts = ones.reduceByKey((i1, i2) -> {
                return  (i1 + i2) ;
            });
        }

        return counts.collectAsMap();
    }

    private static void findDistinctAndPersist(SparkSession spark, Long totalCount, Map<ObjectNode, Integer> objectNodeLongMap) {
        LOG.info("Distinct RDD count value " + objectNodeLongMap.size());
        List<SchemaCount> schemaCountList = CommonUtils.calcDistinctObjectNodesCount2(objectNodeLongMap, totalCount);
        EventJsonSchema mergedTopSchema = convertSchemaCountRDDtoDataset(spark, schemaCountList);
        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema);
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(spark, protoHierarchy);
        transformProtobufHierarchy.generate();
    }

    /*
    private static void postProcessAfterDistinctForSchemaMap(SparkSession spark, JavaRDD<SchemaColumnMap> parsedRDD, Long totalCount) {
        Map<ObjectNode, Long> objectNodeLongMap = parsedRDD.map((v1) -> {
            return v1.getSchema();
        }).countByValue();
        findDistinctAndPersist(spark, totalCount, objectNodeLongMap);
    } */

    private static EventJsonSchema convertSchemaCountRDDtoDataset(SparkSession spark, List<SchemaCount> schemaCountList) {
        Dataset<Row> dataset = spark.createDataFrame(schemaCountList, SchemaCount.class);
        dataset.createOrReplaceTempView(Constants.registeredViewName);
        dataset.printSchema();
      /////  dataset.show(5);
      /////  dataset.write().json(Constants.outputFileJson);

        return getEventJsonSchemaFromJson(dataset);
    }

    public static EventJsonSchema getEventJsonSchemaFromJson(Dataset<Row> dataset) {
        List<EventJsonSchema> distinctSchemaList = getSchemaColumnDataset(dataset);
        List<EventJsonSchema> mergedSchemaList = new ArrayList<>();
        if (distinctSchemaList == null || distinctSchemaList.size() == 0) {
            LOG.error("NO Distinct schemaLIST FOund. Please check.");
            return null;
        }
        EventJsonSchema topSchema = distinctSchemaList.get(0);
        EventJsonSchema mergedTopSchema = topSchema;
        int numberOfSchemasToConsiderForMerge = SchemaInferConfig.getInstance().getNumberOfTopSchemasToMerge() ;

        for (int i = 1; i < distinctSchemaList.size(); i++) {
            if (i > numberOfSchemasToConsiderForMerge) {
                break;
            }
            final EventJsonSchema comparedJsonSchema = CompareSchemas.compareTwoSchemas(mergedTopSchema, distinctSchemaList.get(i));
            if (comparedJsonSchema != null) {
                mergedTopSchema = comparedJsonSchema;
            }
            LOG.info("Finished comparing schema: " + i);
            //   Dataset<Row> mergedSchemaDataset = spark.read().option("inferSchema", true).json(eventJsonSchema.getAdditionalProperties().toString());
            //   Dataset<Row> rowDataset = spark.read().option("inferSchema", true).json(dsjson);
            //   mergedSchemaDataset.write().json(Constants.outputFile2+i);
        }

        LOG.info("Final Top Schema : " + mergedTopSchema.toString());
        return mergedTopSchema;
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

    private static JavaRDD<SchemaColumnMap> transformFValueIntoProromap32(JavaRDD<Text> values) {
        return values.map(new ProcessTextColumn4()).filter((v1) -> {
            return v1 != null;
        });
    }

    private static List<EventJsonSchema> getSchemaColumnDataset(Dataset<Row> ds) {
        Column schemaCol = ds.col("schema");
        final Dataset<String> schemadataset = ds.select("schema").as(Encoders.STRING());
        List<EventJsonSchema> distinctSchemaList = new ArrayList<>();

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
        return distinctSchemaList;
    }

    private static EventJsonSchema parseJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        EventJsonSchema eventJsonSchema = null;
        try {
            eventJsonSchema = mapper.readValue(jsonString, EventJsonSchema.class);
            LOG.info("JsonStructSchema ParsedNode: " + eventJsonSchema.getAdditionalProperties().toString());
            return eventJsonSchema;
        } catch (IOException e) {
            LOG.error("Could not parse into EventJsonSchema: " + jsonString + " --> " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
}
