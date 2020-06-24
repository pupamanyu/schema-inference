package com.example.schemainfer.protogen.functions;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProcessProtoColumn implements Function<String, String>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessProtoColumn.class);

    private String colName ;
    private String datatype ;

    public ProcessProtoColumn(String colName, String datatype) {
        this.colName = colName;
        this.datatype = datatype;
    }

    public String call(String s) throws Exception {
        return processRow(s);
    }

    public static String processRow(String s) {
       return s ;
    }

}