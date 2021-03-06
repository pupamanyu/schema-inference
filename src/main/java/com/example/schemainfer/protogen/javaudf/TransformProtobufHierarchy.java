package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.domain.ProtoLine;
import com.example.schemainfer.protogen.rules.InferProtoDatatype;
import com.example.schemainfer.protogen.utils.*;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.validation.Schema;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformProtobufHierarchy {
    private Map<String, Map<String, String>> inProtoMap;
    private Set<String> arrayProtoList = new HashSet<>();
    private Map<String, Map<String, String>> outProtoMap = new HashMap<>();
    private Map<String, List<ProtoLine>> outSparkDatasetMap = new HashMap<>();
    private Map<String, List<String>> outProtoTextMap = new HashMap<>();

    private boolean isLocal = CommonUtils.isLocal();
    String applicationId;
    SparkSession spark;
    SchemaInferConfig config ;

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtobufHierarchy.class);

    public TransformProtobufHierarchy(SparkSession spark, Map<String, Map<String, String>> inProtoMap) {
        this.inProtoMap = inProtoMap;
        this.spark = spark;
        applicationId = spark.sparkContext().applicationId();
        this.config = SchemaInferConfig.getInstance() ;
    }

    public void generate() {
        this.isLocal = this.config.isLocal() ;
        LOG.info("** Input keyHierarchy: " + inProtoMap.toString());
        createMetadataFileWriter();
        checkShortProtoMap();
        checkShortProtoMap(); // delibratly called twice to take care of elements could be in different order
        printShortProtoMap();
        checkAndStoreColumnsInOutProto();
        printShortProtoMap();
        openFileWriters(isLocal);
        writeToProtoFile(isLocal);

        TransformProtoIntoSparkDataset tt = new TransformProtoIntoSparkDataset(this.spark, this.outSparkDatasetMap, this.outProtoTextMap);
        tt.writeSpark();
    }

    private void printShortProtoMap() {
        AtomicInteger protoctr = new AtomicInteger();
        outProtoMap.entrySet().stream()
                .forEach(e -> {
                    String longProtoName = e.getKey();
                    Map<String, String> colDatatypeMap = e.getValue();
                    AtomicInteger colCtr = new AtomicInteger();
                    protoctr.getAndIncrement();
                    LOG.info(protoctr + ") -------- OUT Proto ----------> : " + longProtoName + " columuns: " + colDatatypeMap.toString());
                });

        LOG.info("----- Array Protoes: " + arrayProtoList.toString());
    }

    private void writeToProtoFile(boolean isLocal) {
        AtomicInteger protoctr = new AtomicInteger();

        outProtoMap.entrySet().stream().forEach(proto -> {
            String longProtoName = proto.getKey();
            List<ProtoLine> sparkProtoLinesList = null;
            List<String> outProtoTextLinesList = null;
            AtomicInteger lineNumber = new AtomicInteger();
            lineNumber.getAndIncrement() ;
            try {
                Map<String, String> colDatatypeMap = proto.getValue();

                String shortProtoName = getShortProtoname(longProtoName);
                String relativeFileName = determineRelativeFileName(longProtoName);
                protoctr.getAndIncrement();

                final List<?> importProtoList = colDatatypeMap.entrySet().stream()
                        .flatMap(coltypemap -> {
                            final List<String> allImportProtos = findAllImportProtos(coltypemap);
                            final Stream<?> stream = allImportProtos.stream();
                            return stream;
                        }).collect(Collectors.toList());

                sparkProtoLinesList = outSparkDatasetMap.get(longProtoName);
                outProtoTextLinesList = outProtoTextMap.get(longProtoName);


                String syntaxLine = "syntax \"proto3\"\n";
                addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "P", "syntax", "proto3", null, null, null);


                String packaLine = "package " + Constants.GAME_ROOT + "\n";
                addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "O", "option", "package", Constants.GAME_ROOT, null, null);

                for (Object imp : importProtoList) {
                    String importedLine = "import " + imp + "\n";
                    addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "I", "import", imp.toString(), null, null, null);

                }

                String javaPackage = "option java_package = \"" + Constants.protoJavaPackageName + "\";\n\n";
                addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "O", "option", "java_package =", Constants.protoJavaPackageName, null, null);


                String multipleFilesLine = "option java_multiple_files = true;\n\n";
                addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "O", "option", "java_multiple_files =", "true", null, null);

                String capitalizedName = StringUtils.capitalize(shortProtoName);
                String messageLine = "message " + capitalizedName ; //+ "\t{\n";
                addSparkLine(lineNumber.getAndIncrement(), sparkProtoLinesList, outProtoTextLinesList, relativeFileName, "M", "message", capitalizedName, "\t{", null, null);

                AtomicInteger colCtr = new AtomicInteger();

                List<ProtoLine> finalSparkProtoLinesList = sparkProtoLinesList;
                List<String> finalOutProtoTextLinesList = outProtoTextLinesList;
                colDatatypeMap.entrySet()
                        .forEach(coltype -> {
                            writeColumnsToProtoFile(lineNumber.getAndIncrement(), colCtr, coltype, relativeFileName, protoctr, finalSparkProtoLinesList, finalOutProtoTextLinesList);
                        });
            } finally {

            }
        });
    }

    public static String determineRelativeFileName(String longProtoName) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
            fileName = shortProtoName + ".proto";
        } else {
            fileName = Constants.GAME_ENTITIES + shortProtoName + ".proto";
        }
        return fileName;
    }

    private void addSparkLine(Integer lineNumber, List<ProtoLine> sparkProtoLinesList, List<String> outProtoTextLinesList, String fileName, String lineType,
                              String col0, String col1, String col2, String col3, String col4) {
        ProtoLine newTuple = new ProtoLine(lineNumber, this.applicationId, fileName, lineType, col0, col1, col2, col3, col4);

        sparkProtoLinesList.add(newTuple);
        StringBuffer buff = new StringBuffer();
        if (col0 != null) {
            buff.append(col0).append("\t");
        }
        if (col1 != null) {
            buff.append(col1).append("\t");
        }
        if (col2 != null) {
            buff.append(col2).append("\t");
        }
        if (col3 != null) {
            buff.append(col3).append("\t");
        }
        if (col4 != null) {
            buff.append(col4).append("\t");
        }

        outProtoTextLinesList.add(buff.toString());
    }

    private void checkShortProtoMap() {
        AtomicInteger protoctr = new AtomicInteger();
        inProtoMap.entrySet().stream()
                .forEach(e -> {
                    String longProtoName = e.getKey();
                    Map<String, String> colDatatypeMap = e.getValue();
                    AtomicInteger colCtr = new AtomicInteger();
                    protoctr.getAndIncrement();
                    LOG.info("-------- Proto IN ----------> : " + longProtoName);

                    if (!isArrayDetailProto(longProtoName)) {
                        outProtoMap.put(longProtoName, new HashMap<>());
                    } else {
                        final Map<String, String> remove = outProtoMap.remove(longProtoName);
                        LOG.info("Found items. Removed: " + remove);
                    }
                    colDatatypeMap.entrySet().stream()
                            .forEach(coltype -> {
                                determineIfArrayDatatypeInColumn(colCtr.getAndIncrement(), coltype, longProtoName, protoctr);
                            });
                });
    }

    private void checkAndStoreColumnsInOutProto() {
        AtomicInteger protoctr = new AtomicInteger();
        inProtoMap.entrySet().stream()
                .forEach(e -> {
                    String longProtoName = e.getKey();
                    Map<String, String> colDatatypeMap = e.getValue();
                    AtomicInteger colCtr = new AtomicInteger();
                    protoctr.getAndIncrement();
                    LOG.info("-------- Proto ----------> : " + longProtoName);

                    if (isArrayDetailProto(longProtoName)) {
                        longProtoName = longProtoName.replace("items/", "").trim();
                    }
                    String finalLongProtoName = longProtoName;
                    colDatatypeMap.entrySet().stream()
                            .forEach(coltype -> {
                                storeColumnInOutProto(colCtr.getAndIncrement(), coltype, finalLongProtoName, protoctr);
                            });
                });
    }

    private boolean isArrayDetailProto(String longProtoName) {
        return StringUtils.checkIfProtoExist(longProtoName, arrayProtoList, StringUtils.CheckIfStringEqualsFn);
    }

    public static String getShortProtoname(String longProtoName) {
        String shortName = longProtoName.replaceFirst(Constants.ADDITIONAL_PROPERTIES_TYPE, "");
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

    private void openFileWriters(boolean isLocal) {
        AtomicInteger protoctr = new AtomicInteger();
        for (Map.Entry<String, Map<String, String>> e : outProtoMap.entrySet()) {
            String longProtoName = e.getKey();

            protoctr.getAndIncrement();
            String fileName = determineProtoFileName(longProtoName, isLocal);
            String gcsfileName = determineGCSProtoFileName(longProtoName);

            outSparkDatasetMap.put(longProtoName, new ArrayList<>());
            outProtoTextMap.put(longProtoName, new ArrayList<>());
        }
    }

    private void createMetadataFileWriter() {
        String protoKey = Constants.ADDITIONAL_PROPERTIES_TYPE + Constants.METADATA_TYPE + "/";
        Map<String, String> metadataColMap = new HashMap<>();
        metadataColMap.put("ts", "integer");
        metadataColMap.put("hostname", "string");
        metadataColMap.put("dt", "string");
        metadataColMap.put("h", "string");
        this.inProtoMap.put(protoKey, metadataColMap);
    }

    private String determineProtoFileName(String longProtoName, boolean isLocal) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (isLocal) {
            if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE) || shortProtoName.equalsIgnoreCase(Constants.METADATA_TYPE)) {
                fileName = Constants.localProtoFileLocation + shortProtoName + ".proto";
            } else {
                fileName = Constants.localProtoFileLocation + Constants.GAME_ENTITIES + shortProtoName + ".proto";
            }
        } else {
            if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE) || shortProtoName.equalsIgnoreCase(Constants.METADATA_TYPE)) {
                //  fileName = Constants.gcsProtoLocation + shortProtoName + ".proto";
                fileName = SchemaInferConfig.getInstance().getOutputBucketName() + shortProtoName + ".proto";
            } else {
                //  fileName = Constants.gcsProtoLocation + Constants.GAME_ENTITIES + shortProtoName + ".proto";
                fileName = SchemaInferConfig.getInstance().getOutputBucketName() + Constants.GAME_ENTITIES + shortProtoName + ".proto";
            }
        }
        return fileName;
    }

    private String determineGCSProtoFileName(String longProtoName) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
            fileName = Constants.PROTO_FOLDER_NAME + shortProtoName + ".proto";
        } else {
            fileName = Constants.PROTO_FOLDER_NAME + Constants.GAME_ENTITIES + shortProtoName + ".proto";
        }
        return fileName;
    }

    private void printProtos(String[] protoArray, int andIncrement) {
        for (int i = 0; i < protoArray.length; i++) {
            String sf = String.format("%d)   %s", andIncrement, protoArray[i]);
            LOG.info(sf);
        }
    }

    private void storeColumnInOutProto(int i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();

        final Map<String, String> stringStringMap = outProtoMap.get(longProtoName);
        if (stringStringMap == null) {
            LOG.info("Incorrect protoMapName: " + longProtoName);
        }

        if (arrayProtoList.contains(longProtoName) && colName.equals("items")) {
            return;
        }

        stringStringMap.put(colName, datatype);
    }

    private String determineIfArrayDatatypeInColumn(int i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr) {
        String datatype = m.getValue();
        String colName = m.getKey();
        if (datatype.equalsIgnoreCase(Constants.NESTED_ARRAY_PROTO)) {
            String sf = String.format("FOUND ARRAY --> proto: %s datatype: %s colname %s ", longProtoName, datatype, colName);
            LOG.info(sf);
            LOG.info("FOUND ARRAY: \t" + datatype + "\t" + colName + "\t = " + i);
            String ss = longProtoName + colName + "/";
            arrayProtoList.add(ss);
            LOG.info(String.format("Added to ArrayList --> %s ", ss));
        }
        return colName;
    }

    private List<String> findAllImportProtos(Map.Entry<String, String> m) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();
        List<String> importProtosList = new ArrayList<>();
        String importProto = null;

        if (datatype.equals(Constants.NESTED_PROTO) || datatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            String importedProtoName = Constants.ADDITIONAL_PROPERTIES_TYPE + colName + "/";
            if (!inProtoMap.containsKey(importedProtoName)) {
                LOG.warn("EMPTY IMPORT: " + importedProtoName);
            } else {
                importProto = Constants.GAME_ENTITIES + colName + ".proto";
                importProtosList.add(importProto);
            }
        }
        return importProtosList;
    }

    private String writeColumnsToProtoFile(Integer lineNumber, AtomicInteger i, Map.Entry<String, String> m, String relativeFileName, AtomicInteger protoctr,
                                           List<ProtoLine> sparkList,
                                           List<String> textLinesList) {
        String jsondatatype = m.getValue();
        String colName = m.getKey();
        String transformedDatatype = jsondatatype;
        if (jsondatatype.equals(Constants.NESTED_PROTO) || jsondatatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            String importedProtoName = Constants.ADDITIONAL_PROPERTIES_TYPE + colName + "/";
            if (!inProtoMap.containsKey(importedProtoName)) {
                LOG.warn("EMPTY Column: " + importedProtoName);
                return colName;
            }

            transformedDatatype = Constants.GAME_ENTITIES + StringUtils.capitalize(colName);
            transformedDatatype = StringUtils.replaceStringInString(transformedDatatype, "/", ".");
        }

        String col0 = null;
        if (jsondatatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            col0 = "\trepeated ";

        }

        int colNum = i.incrementAndGet();
        String protoDataype = InferProtoDatatype.matchProtoDatatype(transformedDatatype);
        String columnLine = "\t" + protoDataype + "\t" + colName + "\t = " + colNum + " \n";

        addSparkLine(lineNumber++, sparkList, textLinesList, relativeFileName, "C", col0, protoDataype, colName, String.valueOf(colNum), null);

        return colName;
    }

}
