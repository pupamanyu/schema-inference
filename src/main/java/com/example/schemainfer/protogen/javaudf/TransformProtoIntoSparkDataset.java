package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransformProtoIntoSparkDataset {

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtoIntoSparkDataset.class);

    private Map<String, List<ProtoLine>> outSparkDatasetMap;
    private Map<String, List<String>> outProtoTextMap;
    private SparkSession spark;

    public TransformProtoIntoSparkDataset(SparkSession spark, Map<String, List<ProtoLine>> outSparkDatasetMap, Map<String, List<String>> outProtoTextMap) {
        this.spark = spark;
        this.outSparkDatasetMap = outSparkDatasetMap;
        this.outProtoTextMap = outProtoTextMap ;
      //  writeSpark();
    }

    public void writeSpark() {

        List<ProtoLine> protoLineList = new ArrayList<>();
        List<String> protoTextList = new ArrayList<>();
        List<Row> rowList = new ArrayList<Row>();
        String logName = this.spark.logName() ;
        final String applicationId = spark.sparkContext().applicationId();

        final List<List<ProtoLine>> listList = outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).filter(p -> {
            return p.getValue() != null;
        }).map(o -> {
            return o.getValue();
        }).collect(Collectors.toList());

        listList.stream().forEach(p -> {
            protoLineList.addAll(p) ;
        });

        LOG.info("Total ProtoLines : " + protoLineList.size()) ;
        Dataset<Row> bigqueryRows = spark.createDataFrame(protoLineList, ProtoLine.class);

        LOG.info("Total BigQuery Rows: " + bigqueryRows.count()) ;

        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance() ;
        String outbqtable = schemaInferConfig.getOutputBQtableName() ;
        String outbqdataset = schemaInferConfig.getBqdatasetName() ;

        if (!Constants.isLocal) {
            bigqueryRows.write()
                    .format("bigquery")
                 //   .option("temporaryGcsBucket", Constants.gcsTempLocation)
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }
    }
}
