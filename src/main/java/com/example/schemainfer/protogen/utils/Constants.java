package com.example.schemainfer.protogen.utils;

import java.nio.charset.Charset;

public class Constants {

    public static String EVENT_TYPE = "gameevent" ;
    public static String METADATA_TYPE = "metadata" ;
    public static String ADDITIONAL_PROPERTIES_TYPE = "additionalproperties/" ;

    public static boolean isLocal = false ;

    public static String localProtoFileLocation = "/Users/rajnish.malik/temp/rt/protout2/";
    public static String outputFileJson = "gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out3";

    public static final Integer defaultNumberOfTopSchemas = 10 ;

    public static String BIG_QUERY_SAMPLE_SCHEMA = "sample_schema3" ;

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
    public static final String PROTO_FOLDER_NAME = "protos3/" ;
    public static final String GAME_ENTITIES = GAME_ROOT + "/entities/" ;

    public static final String protoLine1 = "syntax proto3" ;
    public static final String protoJavaPackageName = "com.example.schemainfer.lol.proto" ;
    public static final String protoPackage = GAME_ROOT ;
    public static final String NESTED_PROTO = "SUB-PROTO" ;
    public static final String NESTED_ARRAY_PROTO = "SUB-ARRAY-PROTO" ;

}
