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
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.convert.Decorators;
import scala.collection.mutable.WrappedArray;

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
                .sort("line_number");

        if (!schemaInferConfig.isLocal()) {
            dataset
                    .write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", SchemaInferConfig.getInstance().getGcsTempBucketName())
                    .mode(SaveMode.Overwrite)
                    .save(outbqdataset + "." + outbqtable);
        }

        collapseProtoLinesByFile2(dataset) ;
    }

    private void collapseProtoLinesByFile2(Dataset<Row> inRows) {
        List<Row> rowList = inRows.groupBy("file_name").agg(functions.collect_list("line").as("linelist")).repartition(1).collectAsList();
        String jobid = spark.sparkContext().applicationId();
        List<ProtoLine> protoList = new ArrayList<ProtoLine>() ;

        final Seq<Row> rowSeq = JavaConverters.asScalaBuffer(rowList).toSeq();
        final Decorators.AsJava<List<Row>> listAsJava = JavaConverters.seqAsJavaListConverter(rowSeq) ;
        final List<Row> rowList1 = listAsJava.asJava() ;

        rowList1.forEach(i -> {
            String filename = i.<String>getAs("file_name").toString();
            //final Object lineArray = i.getAs("line") ;
            final Seq<Object> iSeq = i.getSeq(1) ;

            WrappedArray wrappedArray = (WrappedArray) iSeq ;
            StringBuilder buff = new StringBuilder();
            scala.collection.Iterator<String> iterator = wrappedArray.iterator();
            while (iterator.hasNext()) {
                String line1 = iterator.next() ;
                buff.append(line1).append("\n") ;
            }
            String line = buff.toString() ;
            LOG.info("Line after append: " + line) ;
            ProtoLine pl = new ProtoLine(jobid, filename, line) ;
            protoList.add(pl) ;
        });

        persistBQ(protoList) ;
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
