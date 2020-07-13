package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.functions.ConcatLineFunction;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class TransformProtoIntoSparkDataset {

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtoIntoSparkDataset.class);

    private Map<String, List<ProtoLine>> outSparkDatasetMap;
    private Map<String, List<String>> outProtoTextMap;
    private SparkSession spark;


    public TransformProtoIntoSparkDataset(SparkSession spark, Map<String, List<ProtoLine>> outSparkDatasetMap, Map<String, List<String>> outProtoTextMap) {
        this.spark = spark;
        this.outSparkDatasetMap = outSparkDatasetMap;
        this.outProtoTextMap = outProtoTextMap;
    }

    public void writeSpark() {

        List<ProtoLine> protoLineList = new ArrayList<>();
        List<String> protoTextList = new ArrayList<>();
        List<Row> rowList = new ArrayList<Row>();
        String logName = this.spark.logName();
        final String applicationId = spark.sparkContext().applicationId();
        String outputBucketName = SchemaInferConfig.getInstance().getOutputBucketName();
       // String path = "protodf";

        final List<List<ProtoLine>> listList = outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).filter(p -> {
            return p.getValue() != null;
        }).map(o -> {
            String protoName = o.getKey();
            String shortProtoName = TransformProtobufHierarchy.determineRelativeFileName(protoName);
         //   String gspath = "gs://" + outputBucketName + "/" + path + "/" + shortProtoName;
            String gspath = outputBucketName + "/" + shortProtoName;
            Dataset<Row> sparkrows = spark.createDataFrame(o.getValue(), ProtoLine.class);
            Dataset<Row> sortedDataset = sparkrows.sort("line_number");
            sortedDataset.select("concat_columns")
                    .repartition(1)
                    .sort("line_number")
                    .repartition(1)
                    .write()
                    .option("delimiter", "\n")
                   .mode(SaveMode.Overwrite).csv(gspath);

          //  rowDataset.show();
            return o.getValue();
        }).collect(Collectors.toList());

        listList.stream().forEach(p -> {
            protoLineList.addAll(p);
        });

        LOG.info("Total ProtoLines : " + protoLineList.size());
        Dataset<Row> bigqueryRows = spark.createDataFrame(protoLineList, ProtoLine.class);

        LOG.info("Total BigQuery Rows: " + bigqueryRows.count());

        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance();
        String outbqtable = schemaInferConfig.getOutputBQtableName();
        String outbqdataset = schemaInferConfig.getBqdatasetName();
        final Dataset<Row> dataset = bigqueryRows.select(functions.col("concat_columns").as("line"), functions.col("file_name"), functions.col("line_number"), functions.col("job_id"))
                .sort("file_name", "line_number");

        if (!schemaInferConfig.isLocal()) {
            dataset
                    .write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }

        //collapseProtoLinesByFile2(dataset) ;
        final List<ProtoLine> protoList = concatLinesByFile(dataset);
        persistBQ(protoList) ;
    }

    private List<ProtoLine> concatLinesByFile(Dataset<Row> inRows) {

            Dataset<Row> bigqueryRows = inRows.sort("file_name", "line_number");
            final JavaRDD<Row> rowJavaRDD = bigqueryRows.toJavaRDD().repartition(1);
            CommonUtils.printRows(rowJavaRDD) ;

            JavaPairRDD<String, Row> keyvaluepair =
                    rowJavaRDD.flatMapToPair(
                            line -> {
                                String cc = line.<String>getAs("line");
                                String filename = line.<String>getAs("file_name");
                                Integer line_number = line.<Integer>getAs("line_number");
                                System.out.println("LIINNEEE:" + line_number) ;
                                return Arrays.asList(new Tuple2<>(filename, line)).iterator();
                            });

        final Tuple2<String, Row> first = keyvaluepair.first() ;

        final JavaPairRDD<String, Row> collapsedRows = keyvaluepair.reduceByKey(new ConcatLineFunction());

        final Map<String, Row> stringRowMap = collapsedRows.collectAsMap();
            String applicationId = spark.sparkContext().applicationId();
            List<ProtoLine> bqProtoList = new ArrayList<>();

            for (Map.Entry<String, Row> e : stringRowMap.entrySet()) {
                String filename = e.getKey();
                Row row = e.getValue();
                String concatline = row.<String>getAs("line").toString();
                ProtoLine bqproto = new ProtoLine(applicationId, filename, concatline);
                bqProtoList.add(bqproto);
            }
            return bqProtoList ;
    }

    private void persistBQ(List<ProtoLine> bqProtoList) {
        Dataset<Row> sparkrows = spark.createDataFrame(bqProtoList, ProtoLine.class);
        final Dataset<Row> rowDataset = sparkrows.select(functions.col("concat_columns").as("line"), functions.col("file_name"), functions.col("job_id"));
        final List<Row> rowList = rowDataset.collectAsList();
        CommonUtils.printTop20F(rowList) ;

        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance();
        String outbqdataset = schemaInferConfig.getBqdatasetName();
        String outbqtable = "protocollapsed" ;
        //rowDataset.show();

        if (!schemaInferConfig.isLocal()) {
            rowDataset.select(functions.col("line"), functions.col("file_name"), functions.col("job_id"))
                    .write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }
    }

    class LineComparator2 implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public int compare(String v1, String v2) {
            return  v2.compareTo(v1);
        }
    }

     class LineComparator implements Comparator<Tuple2<String, String>>, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public int compare(Tuple2<String, String> v1, Tuple2<String, String> v2) {
            return  v2._1().compareTo(v1._1());
        }
    }
}
