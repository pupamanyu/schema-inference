package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.ConvertUtils;
import org.apache.hadoop.io.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class SeqFilesScan {

    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {

        LOG.info("Starting ScanSeq");

        SparkConf conf = new SparkConf().setAppName("Read Seq UDF")
                .setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        readValues(spark, sc);

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

    private static void readValues(SparkSession spark, JavaSparkContext jsc) {
        JavaPairRDD<BytesWritable, Text> rdd = jsc.sequenceFile(Constants.inputFile, BytesWritable.class, Text.class);

        System.out.println("RDD out: " + rdd.toString());
        JavaRDD<Text> values = rdd.values();

        LOG.info("Values: " + values.toDebugString());
        LOG.info("Count of values: " + rdd.values().count());

        JavaRDD<Map<String, String>> parsedRDD = transformTextRDDIntoMap(values);

        LOG.info("Parsed RDD: " + parsedRDD.toDebugString());
        LOG.info("Parsed RDD count: " + parsedRDD.count());

        printTop20(parsedRDD);

        JavaRDD<Protomap> protoRDD = transformFValueIntoProromap(parsedRDD);

        convertRDDtoDataset(spark, protoRDD);
    }

    private static JavaRDD<String> deriveFValue(JavaRDD<Text> values) {
        return values.map(new Function<Text, String>() {
            @Override
            public String call(Text text) throws Exception {
                String s = ConvertUtils.bytesToString(text.getBytes(), 0, text.getLength());
                String[] ss = s.split(Constants.SEQUENCE_FIELD_DELIMITER, -1);
                String fvalue = ss[2] ;

                return fvalue;
            }
        });
    }

    private static JavaRDD<Map<String, String>> transformTextRDDIntoMap(JavaRDD<Text> values) {
        return values.map(new Function<Text, Map<String, String>>() {
                @Override
                public Map<String, String> call(Text text) throws Exception {
                    String s = ConvertUtils.bytesToString(text.getBytes(), 0, text.getLength());
                    String[] ss = s.split(Constants.SEQUENCE_FIELD_DELIMITER, -1);
                    String fvalue = ss[2] ;
                    Map<String, String> keyMap = splitFColumnIntoMap(fvalue) ;

                    return keyMap;
                }
            });
    }

    private static void convertRDDtoDataset(SparkSession spark, JavaRDD<Protomap> protoRDD) {
        Dataset<Row> dataset = spark.createDataFrame(protoRDD, Protomap.class) ;
        dataset.createOrReplaceTempView(Constants.registeredViewName);
        dataset.printSchema();
        dataset.show(5);
    }

    private static void genProtomapSchema() {
        StructType structType = new StructType();
        structType = structType.add("key", DataTypes.StringType, false);
        structType = structType.add("type", DataTypes.StringType, false);
    }

    private static JavaRDD<Protomap> transformFValueIntoProromap(JavaRDD<Map<String, String>> parsedRDD) {
        return parsedRDD.flatMap(new FlatMapFunction<Map<String, String>, Protomap>() {
                @Override
                public Iterator<Protomap> call(Map<String, String> seqf3map) throws Exception {
                    Set<String> keyset = seqf3map.keySet() ;
                    List<Protomap> protomapList = new ArrayList<>();
                    List<Row> rows = new ArrayList<Row>();
                    for (String key : keyset) {
                        Protomap protomap = new Protomap() ;
                        protomap.setKey(key);
                        protomap.setType("string");
                        Row row = RowFactory.create(protomap) ;
                        protomapList.add(protomap) ;
                        rows.add(row) ;
                    }
                    return  protomapList.iterator();
                }
            });
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
            LOG.info("-----------------------------------------------------------------------------------------") ;
            LOG.info("Parsed String MAP: " + s.toString());
            for (Map.Entry<String,String> entry : s.entrySet()) {
                LOG.info("Key = " + entry.getKey() +
                        " Value = " + entry.getValue());
            }
        });
    }

    /**
     * Convert the column name "f" into a Map
     * Row has 3 columns. "f" column is #3
     * @param fvalue
     * @return
     */
    private static Map<String, String> splitFColumnIntoMap(String  fvalue) {
        LOG.info("fValue: " + fvalue) ;
        String[] keyvalues = fvalue.split(Constants.SEQUENCE_MAP_DELIM, -1) ;
        Map<String, String> derivedMap = new HashMap<String, String>() ;
        for (int i=0 ; i < keyvalues.length ; i++) {
            String s = keyvalues[i] ;
            String[] keyvalue = s.split(Constants.SEQUENCE_MAP_EQUAL, 2) ;
            String key = keyvalue[0] ;
            String value = keyvalue[1] ;
            derivedMap.put(key, value) ;
        }
        return derivedMap ;
    }

    private static String splitFColumnIntoJson(String  fvalue) {
        LOG.info("fValue: " + fvalue) ;
        String[] keyvalues = fvalue.split(Constants.SEQUENCE_MAP_DELIM, -1) ;
        StringBuilder sb = new StringBuilder("{") ;
        for (int i=0 ; i < keyvalues.length ; i++) {

            String s = keyvalues[i] ;
            String[] keyvalue = s.split(Constants.SEQUENCE_MAP_EQUAL, 2) ;
            String key = keyvalue[0] ;
            String value = keyvalue[1] ;
            sb.append("\"") ;
            sb.append(key) ;
            sb.append("\"") ;
            sb.append(":") ;
            sb.append("\"") ;
            sb.append(value) ;
            sb.append("\"") ;
        }
        sb.append("},") ;
        return sb.toString() ;
    }

}
