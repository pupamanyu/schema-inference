package com.example.schemainfer.protogen.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class FindKeyFunction implements Function<Row, String>, Serializable {

    @Override
    public String call(Row row) throws Exception {
        String filename = row.getAs("file_name");
        Integer lineNumber = row.getAs("line_number");
        String ll = String.valueOf(lineNumber) ;
        return filename + ll;
    }
}
