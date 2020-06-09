package com.example.schemainfer.protogen.utils;

import java.nio.charset.Charset;

public class Constants {
    // Path to input SEQ files. Hard coded for now...
    public static String inputFile = "gs://schema-inference-sample-data/internal__legs_gameevents/dt=2020-05-15/h=06/batchid=190936cc-84d9-45f9-af54-81de9f460ee2/000000_0";
   // public static String inputFile = "/Users/rajnish.malik/temp/rt/e.txt";

    // Path to output files. Hard coded for now...
    public static String outputFile = "gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out3";
    public static String outputFile2 = "gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out3";
  //  public static String outputFile = "/Users/rajnish.malik/temp/rt/f.out";
  //  public static String outputFile2 = "/Users/rajnish.malik/temp/rt/f2.out";

    // View name used in SQL
    public static String registeredViewName = "gameevent";

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
}
