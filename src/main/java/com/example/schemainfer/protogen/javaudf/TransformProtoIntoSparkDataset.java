package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
        String outputBucketName = SchemaInferConfig.getInstance().getOutputBucketName() ;
        String path = "protodf" ;

        final List<List<ProtoLine>> listList = outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).filter(p -> {
            return p.getValue() != null;
        }).map(o -> {
            String protoName = o.getKey() ;
            String shortProtoName = TransformProtobufHierarchy.determineRelativeFileName(protoName);
            String gspath = "gs://" + outputBucketName + "/" + path + "/" + shortProtoName   ;
            Dataset<Row> sparkrows = spark.createDataFrame(o.getValue(), ProtoLine.class);
            Dataset<Row> sortedDS = sparkrows.repartition(1).sort("line_number");
            sortedDS.show();
            sortedDS.select("concat_columns")
                    // .option("delimiter", "\t")
                    .write()
                    .option("delimiter", "\n")
                    .mode(SaveMode.Overwrite).csv(gspath);

            return o.getValue();
        }).collect(Collectors.toList());

        listList.stream().forEach(p -> {
            protoLineList.addAll(p) ;
        });

        LOG.info("Total ProtoLines : " + protoLineList.size()) ;
        Dataset<Row> bigqueryRows = spark.createDataFrame(protoLineList, ProtoLine.class);

        LOG.info("Total BigQuery Rows: " + bigqueryRows.count()) ;
        bigqueryRows.show();
        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance() ;
        String outbqtable = schemaInferConfig.getOutputBQtableName() ;
        String outbqdataset = schemaInferConfig.getBqdatasetName() ;

        if (!Constants.isLocal) {
            bigqueryRows.select(functions.col("concat_columns").as("line"), functions.col("file_name"), functions.col("line_number"), functions.col("job_id"))
                    .write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }
    }

    private void collapseProtoLinesByFile(Dataset<Row> bigqueryRows) {
        Dataset<String> cclines = bigqueryRows.map((MapFunction<Row, String>) row -> row.<String>getAs("concat_columns"), Encoders.STRING());

        Dataset<String> newYears = cclines.flatMap((FlatMapFunction<String, String>) year -> {
            return Arrays.asList(year).iterator();
        }, Encoders.STRING()) ;
    }
}
