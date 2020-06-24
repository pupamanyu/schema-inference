package com.example.schemainfer.protogen.utils;

import java.nio.charset.Charset;

public class Constants {

    public static String EVENT_TYPE = "gameevent" ;
    // Path to input SEQ files. Hard coded for now...

    public static String inputFile = "gs://schema-inference-sample-data/internal__legs_gameevents/dt=2020-05-15/h=06/batchid=190936cc-84d9-45f9-af54-81de9f460ee2/000000_0";
   //public static String inputFile = "/Users/rajnish.malik/temp/rt/h1.txt";

   public static String outputFile = "gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out3";
   // public static String outputFile = "/Users/rajnish.malik/temp/rt/h1.out";


    public static String sparkProtoOut = "gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out4";
   // public static String sparkProtoOut = "/Users/rajnish.malik/temp/rt/out4";


    public static String localProtoFileLocation = "/Users/rajnish.malik/temp/rt/protout/";
    public static String gcsProtoLocation = "gs://schema-inference-out/protos/";

    public static String gcsBucketName = "schema-inference-out";

    // View name used in SQL
    public static String registeredViewName = EVENT_TYPE;

    // Field delimeters used in Seq file format
    public static String SEQUENCE_FIELD_DELIMITER = "\01"; // ^A => "|"
    public static String SEQUENCE_MAP_EQUAL = "\02"; // ^B => "="
    public static String SEQUENCE_MAP_DELIM = "\03"; // ^C => ","

    public static final String UTF8_ENCODING = "UTF-8";

    // When we encode strings, we always specify UTF8 encoding
    public static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
    public static enum RUN_MODE {
        Local,
        Cluster
    }

    public static enum DATATYPES {
        Integer,
        Long,
        Float,
        Double,
        Boolean,
        String,
        Null
    }

    // Protobuf Configuration hardcoded for now

    public static enum ENTITY_NAMES {
        Killer,
        Victim
    }

    public static final String GAME_ROOT = "lol" ;
    public static final String GAME_ENTITIES = GAME_ROOT + "/entities/" ;

    public static final String protoLine1 = "syntax proto3" ;
    public static final String protoPackage = GAME_ROOT ;
    public static final String NESTED_PROTO = "SUB-PROTO" ;
    public static final String NESTED_ARRAY_PROTO = "SUB-ARRAY-PROTO" ;


}
