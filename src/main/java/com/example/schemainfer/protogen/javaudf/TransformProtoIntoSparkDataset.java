package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.functions.FindKeyFunction;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.SchemaInferConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
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
        String path = "protodf";

        final List<List<ProtoLine>> listList = outSparkDatasetMap.entrySet().stream().filter((v1) -> {
            return v1 != null;
        }).filter(p -> {
            return p.getValue() != null;
        }).map(o -> {
            String protoName = o.getKey();
            String shortProtoName = TransformProtobufHierarchy.determineRelativeFileName(protoName);
            String gspath = "gs://" + outputBucketName + "/" + path + "/" + shortProtoName;
            Dataset<Row> sparkrows = spark.createDataFrame(o.getValue(), ProtoLine.class);
            Dataset<Row> sortedDataset = sparkrows.sort("line_number");
            sortedDataset.select("concat_columns")
                    .repartition(1)
                    .sort("line_number")
                    .write()
                    .option("delimiter", "\n")
                   .mode(SaveMode.Overwrite).text(gspath);

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

        if (!schemaInferConfig.isLocal()) {
            bigqueryRows.select(functions.col("concat_columns").as("line"), functions.col("file_name"), functions.col("line_number"), functions.col("job_id"))
                    .write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }

        collapseProtoLinesByFile(bigqueryRows) ;
    }

    private void collapseProtoLinesByFile(Dataset<Row> inRows) {

        Dataset<Row> bigqueryRows = inRows.repartition(1).sort("file_name", "line_number");
        final JavaRDD<Row> rowJavaRDD = bigqueryRows.toJavaRDD();
        CommonUtils.printRows(rowJavaRDD) ;

        JavaPairRDD<String, String> keyvaluepair =
                rowJavaRDD.flatMapToPair(
                        line -> {
                            String cc = line.<String>getAs("concat_columns");
                            String filename = line.<String>getAs("file_name");
                            return Arrays.asList(new Tuple2<>(filename, cc)).iterator();
                        });

        CommonUtils.printPairRows(keyvaluepair, "keyValuePair") ;

        JavaPairRDD<String, String> collapsedRows = keyvaluepair.reduceByKey((c1, c2) -> {
            StringBuffer buff = new StringBuffer();
            buff.append(c1).append("\n").append(c2);

            return buff.toString();
        });

        CommonUtils.printPairRows(collapsedRows, "collapsed") ;

       // LOG.info("Total Collapse BigQuery Rows count: " + collapsedRows.count());
       // LOG.info("Total Collapse BigQuery Rows: " + collapsedRows.toString());
        final Map<String, String> stringMap = collapsedRows.collectAsMap();
        LOG.info("LastMap: " + stringMap.toString());
        String applicationId = spark.sparkContext().applicationId();
        List<ProtoLine> bqProtoList = new ArrayList<>();

        for (Map.Entry<String, String> e : stringMap.entrySet()) {
            String filename = e.getKey();
            String coll = e.getValue();
            ProtoLine bqproto = new ProtoLine(applicationId, filename, coll);
            bqProtoList.add(bqproto);
        }

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

    private void sortRDD(JavaRDD<Row> rowJavaRDD) {
        /*        final JavaRDD<Row> rowJavaRDD1 = rowJavaRDD.sortBy(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row value) throws Exception {
                String filename = value.getAs("file_name");
                String lineNumber = value.getAs("line_number");
                return filename + lineNumber;
            }
        }, true, 1);*/


/*        JavaRDD<Row> rowJavaRDD1 = rowJavaRDD.keyBy(new Function<Row, String> () {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                String filename = row.getAs("file_name");
                String lineNumber = row.getAs("line_number");
                return filename + lineNumber;
            }
        }).sortByKey(new LineComparator2()).values();*/

        final JavaRDD<Row> rowJavaRDD1 = rowJavaRDD.keyBy(new FindKeyFunction()).sortByKey(new LineComparator2(), true, 1).values();
        // Dataset<Row> sparkrowsss = spark.createDataFrame(rowJavaRDD1, ProtoLine.class);
        // sparkrowsss.show();
        // final List<Row> rowList = rowJavaRDD.collect();

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
