package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.utils.Constants;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple7;

import java.util.*;
import java.util.stream.Collectors;

public class TransformProtoIntoSparkDataset {

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtoIntoSparkDataset.class);

    private Map<String, List<ProtoLine>> outSparkDatasetMap;
    private Map<String, List<String>> outProtoTextMap;
    private StructType protoStructType;
    private SparkSession spark;

    public TransformProtoIntoSparkDataset(SparkSession spark, Map<String, List<ProtoLine>> outSparkDatasetMap, Map<String, List<String>> outProtoTextMap) {
        this.protoStructType = createStruct();
        this.spark = spark;
        this.outSparkDatasetMap = outSparkDatasetMap;
        this.outProtoTextMap = outProtoTextMap ;
        writeSpark();
    }

    private StructType createStruct() {
        List<org.apache.spark.sql.types.StructField> listOfStructField = new ArrayList<StructField>();
        listOfStructField.add(DataTypes.createStructField("col0", DataTypes.StringType, true));
//        listOfStructField.add(DataTypes.createStructField("col1", DataTypes.StringType, true));
//        listOfStructField.add(DataTypes.createStructField("col2", DataTypes.StringType, true));
//        listOfStructField.add(DataTypes.createStructField("col3", DataTypes.StringType, true));
//        listOfStructField.add(DataTypes.createStructField("col4", DataTypes.StringType, true));
//        listOfStructField.add(DataTypes.createStructField("col5", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(listOfStructField);
        // Dataset<Row> data=spark.createDataFrame(list,structType);
        // data.show();
        return structType;
    }

    public void writeSpark() {

        List<ProtoLine> protoLineList = new ArrayList<>();
        List<String> protoTextList = new ArrayList<>();
        List<Row> rowList = new ArrayList<Row>();
        String logName = this.spark.logName() ;
        LOG.info("Spark lognmE: " + logName) ;
        final String applicationId = spark.sparkContext().applicationId();

        final List<List<ProtoLine>> listList = outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).filter(p -> {
            return p.getValue() != null;
        }).map(o -> {
            return o.getValue();
        }).collect(Collectors.toList());


  /*              .forEach(e -> {
            String fileName = e.getKey();
          //  String relativeFileName = determineRelativeFileName(fileName) ;
            List<ProtoLine> tupleList = e.getValue();
            tupleList.stream().filter((v1) -> {
                return v1 != null;
            }).collect(Collectors.toList()) ;*/
               //     .forEach(t -> {
               //         ProtoLine protoLine = new ProtoLine(applicationId, relativeFileName, null, t._1(), t._2(), t._3(), t._4(), t._5());
               //         protoLineList.add(protoLine);
               //     });
       // });

/*        outProtoTextMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).forEach(e -> {
            String fileName = e.getKey();
            List<String> tupleList = e.getValue();
            tupleList.stream().filter((v1) -> {
                return v1 != null;
            })
                    .forEach(t -> {
                        StringBuffer buff = new StringBuffer() ;
                        if (t != null) {
                            buff.append(t) ;
                        }
                        String ss = buff.toString() ;
                        protoTextList.add(ss);
                        System.out.println("Last Spark Text col: " + ss ) ;
                        rowList.add(RowFactory.create(ss)) ;
                    });
        });*/

        listList.stream().forEach(p -> {
            protoLineList.addAll(p) ;
        });

        LOG.info("Totdal ProtoLines : " + protoLineList.size()) ;
        Dataset<Row> bigqueryRows = spark.createDataFrame(protoLineList, ProtoLine.class);


        LOG.info("Totdal BigQuery Rows: " + bigqueryRows.count()) ;
       // sparoRows.write().text(Constants.sparkProtoOut);
       // final StructType structType = createStruct();
       // Dataset<Row> sparoRows = spark.createDataFrame(rowList, structType);
       //  sparoRows.write().text(Constants.sparkProtoOut);

        if (!Constants.isLocal) {
            bigqueryRows.write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", Constants.gcsTempLocation)
                    .mode(SaveMode.Overwrite)
                    .save(Constants.BIG_QUERY_DATASET + "." + Constants.BIG_QUERY_TABLE);
        }

    }

    private String determineRelativeFileName(String longProtoName) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
            fileName = shortProtoName + ".proto";
        } else {
            fileName = Constants.GAME_ENTITIES + shortProtoName + ".proto";
        }
        return fileName;
    }

    private String getShortProtoname(String longProtoName) {
        String shortName = longProtoName.replaceFirst("additionalproperties/", "");
        if (shortName.isEmpty()) {
            shortName = Constants.EVENT_TYPE;
        }
        if (shortName.endsWith("/")) {
            int i = shortName.indexOf("/");
            String ss = shortName.substring(0, i);
            return ss;
        }
        return shortName;
    }
}
