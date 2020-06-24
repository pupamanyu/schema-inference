package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple5;

import java.util.*;

public class TransformProtoIntoSparkDataset {
    private Map<String, List<Tuple5>> outSparkDatasetMap;
    private StructType protoStructType;
    private SparkSession spark;

    public TransformProtoIntoSparkDataset(SparkSession spark, Map<String, List<Tuple5>> outSparkDatasetMap) {
        this.protoStructType = createStruct();
        this.spark = spark;
        this.outSparkDatasetMap = outSparkDatasetMap;
        writeSpark();
    }

    private StructType createStruct() {
        List<org.apache.spark.sql.types.StructField> listOfStructField = new ArrayList<StructField>();
        listOfStructField.add(DataTypes.createStructField("col0", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("col1", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("col2", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("col3", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("col4", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("col5", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(listOfStructField);
        // Dataset<Row> data=spark.createDataFrame(list,structType);
        // data.show();
        return structType;
    }

    public void writeSpark() {

        List<ProtoLine> protoListList = new ArrayList<>();

        outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).forEach(e -> {
            String fileName = e.getKey();
            List<Tuple5> tupleList = e.getValue();
            tupleList.stream().forEach(t -> {
                ProtoLine protoLine = new ProtoLine(t._1(), t._2(), t._3(), t._4(), t._5());
                protoListList.add(protoLine);
            });
        });

        Dataset<Row> sparoRows = spark.createDataFrame(protoListList, ProtoLine.class);
        sparoRows.write().json(Constants.sparkProtoOut);
    }
}
