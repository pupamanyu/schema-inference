package com.example.schemainfer.protogen.javaudf;

import java.io.IOException;
import java.util.*;

import com.example.schemainfer.protogen.json.CompareSchemas;
import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SchemaCompareAsDS {
    //  private static String inputFile2 = "/Users/rajnish.malik/temp/rt/e.out/part-00001-9fa6df99-660f-46ca-9c4e-25b719dcb855-c000.json";
    private static final Logger LOG = LoggerFactory.getLogger(SeqFilesScan.class);

    private static String inputFile2 = "/Users/rajnish.malik/temp/rt/e.out";

    public SchemaCompareAsDS() {
    }

    public static void mainn(String[] args) throws IOException {
        SparkConf conf = (new SparkConf()).setAppName("Java UDF Example").setMaster("local[2]").set("spark.driver.host", "localhost");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        readAndCompareDataFileAsJson(spark, inputFile2);
        //   compareSchemaColumns(ds1.schema(), ds2.schema());
    }

    private static void readAndCompareDataFileAsJson(SparkSession spark, String inFile1) {
        Dataset<Row> ds = spark.read().option("mode", "FAILFAST").option("inferSchema", true).json(inFile1);
        System.out.println("Input DATASET count: " + ds.count());
        Tuple2<List<EventJsonSchema>, List<JSONObject>> jsonEventsTuple = getSchemaColumnDataset(spark, ds);
        List<EventJsonSchema> eventsList = jsonEventsTuple._1() ;
        List<JSONObject> jsonObjList = jsonEventsTuple._2() ;
        System.out.println("JSON Events count: " + eventsList.size());
        CompareSchemas.compareTwoSchemas(eventsList.get(0), eventsList.get(2)) ;
    }

    private static Tuple2<List<EventJsonSchema>, List<JSONObject>> getSchemaColumnDataset(SparkSession spark, Dataset<Row> ds) {
        List<EventJsonSchema> eventList = new ArrayList<EventJsonSchema>();
        List<JSONObject> jsonObjList = new ArrayList<>( );
        final Dataset<String> schemadataset = ds.select("schema").as(Encoders.STRING());

        List<String> listOne = schemadataset.collectAsList();

        schemadataset.printSchema();
        schemadataset.show();

        for (String ss : listOne) {
            System.out.println("Input DS= " + ss);
            EventJsonSchema eventJsonSchema = parseJson(ss);
            eventList.add(eventJsonSchema);

            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse(ss);
                jsonObjList.add(json) ;
                System.out.println("JSONObject = " + json) ;
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

        return new Tuple2<>(eventList, jsonObjList);
    }

    private static EventJsonSchema parseJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        EventJsonSchema eventJsonSchema = null;
        try {
            eventJsonSchema = mapper.readValue(jsonString, EventJsonSchema.class);
           // JsonUtils.printJsonProperties(eventJsonSchema);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("JsonStructSchema Node: " + eventJsonSchema.toString());
        return eventJsonSchema;
    }

    private static void compareSchemaColumns(Dataset<Row> ds) {
        StructField[] fields = ds.schema().fields();
        StructField[] var2 = fields;
        int var3 = fields.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            StructField field = var2[var4];
            System.out.println("field name: " + field.name() + " type: " + field.dataType());
        }
    }

    private static void compareSchemaColumns(StructType schema1, StructType schema2) {
        Set<StructField> set1 = new HashSet(Arrays.asList(schema1.fields()));
        Set<StructField> set2 = new HashSet(Arrays.asList(schema2.fields()));
        boolean result = set1.equals(set2);
        System.out.println("Are two schemas equal: " + result);
        System.out.println("set1 size: " + set1.size());
        System.out.println("set2 size: " + set1.size());
    }

    private static boolean schemasHaveTheSameColumnNames(List<String> firstSchema, List<String> secondSchema) {
        if (firstSchema.size() != secondSchema.size()) {
            return false;
        } else {
            Iterator var2 = secondSchema.iterator();

            String column;
            do {
                if (!var2.hasNext()) {
                    return true;
                }
                column = (String) var2.next();
            } while (firstSchema.contains(column));

            return false;
        }
    }
}
