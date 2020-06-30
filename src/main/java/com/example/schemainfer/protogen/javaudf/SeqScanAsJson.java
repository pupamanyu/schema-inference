package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class SeqScanAsJson {
 //   private static String inputFile = "data/distinct/part-00003-55119ac7-2bd7-4974-88b5-3d4d50b2d636-c000.json";
  private static String inputFile = "data/distinct/";
    private static String registeredViewName = "gameevent";
    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);
    private String runmode;


    public static void mainn(String[] args) throws IOException {

        SeqScanAsJson seqFilesScan = new SeqScanAsJson();
        seqFilesScan.doSparkMain();
        LOG.info("Finished ScanSeq");
    }

    private  void doSparkMain() {
        SparkConf conf = getSparkConf("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance() ;
        schemaInferConfig.build(Constants.RUN_MODE.Local.name(), null, null, null, null, true, 20,  null);

        final Dataset<Row> rowJavaRDD = readDataFileAsJson(spark);
        convertRDDIntoString(rowJavaRDD, spark) ;
    }

    private  void convertRDDIntoString( Dataset<Row> rowJavaRDD, SparkSession spark) {
        final EventJsonSchema mergedTopSchema = SeqFilesScan.getEventJsonSchemaFromJson(rowJavaRDD);

        GenerateProtobufHierarchy newSchemaGen = new GenerateProtobufHierarchy(mergedTopSchema);
        Map<String, Map<String, String>> protoHierarchy = newSchemaGen.generate();
        TransformProtobufHierarchy transformProtobufHierarchy = new TransformProtobufHierarchy(spark, protoHierarchy);
        transformProtobufHierarchy.generate();
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

    private Dataset<Row> readDataFileAsJson(SparkSession spark) {

     //   StructType ss = (StructType) StructType.fromJson(schema);
        Dataset<Row> ds = spark.read()
                   .option("inferSchema", true)
                .json(inputFile);
        ds.printSchema();
        ds.show();
        System.out.println("Json data count: " + ds.count()) ;
       // final Dataset<String> schemadataset = ds.select("schema").as(Encoders.STRING());
        return ds.select("schema");
    }




}
