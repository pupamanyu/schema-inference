package com.example.schemainfer.protogen.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConcatLineFunction implements Function2<Row, Row, Row>, Serializable {

    @Override
    public Row call(Row c1, Row c2) throws Exception {
        StringBuffer buff = new StringBuffer();

        Integer line_number1 = null ;
        Integer line_number2 = null ;

        Object lineN1 = (Object) c1.<Integer>getAs("line_number");
        Object lineN2 = (Object) c2.<Integer>getAs("line_number");

        if (lineN1 != null) {
            line_number1 = (Integer) lineN1 ;
        }

        if (lineN2 != null) {
            line_number2 = (Integer) lineN2 ;
        }

        String file_name = c1.<String>getAs("file_name");
        String job_id = c1.<String>getAs("job_id");

        String line1 = c1.<String>getAs("line");
        String line2 = c2.<String>getAs("line");
        Object newLineNumber = 0  ;

        if ( line_number1 == null || line_number2 == null || line_number1 < line_number2) {
            buff.append(line1).append("\n").append(line2);
        } else {
            buff.append(line2).append("\n").append(line1);
        }
        Row rr = RowFactory.create(file_name, buff.toString(), job_id);
        StructType schema = getStruct() ;
        Object[] valuesarray = new String[4] ;
        valuesarray[0] = file_name ;
        try {
            valuesarray[1] = (Object) newLineNumber;
        } catch (ArrayStoreException ee) {
            valuesarray[1] = null ;
        }
        valuesarray[2] = buff.toString() ;
        valuesarray[3] = job_id ;
        Row newRow = new GenericRowWithSchema(valuesarray, schema) ;
        return newRow ;
    }

    private StructType getStruct() {
        List<StructField> fields = new ArrayList<>(4);
        fields.add(DataTypes.createStructField("file_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_number", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("job_id", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        return schema ;
    }
}
