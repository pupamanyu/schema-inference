package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.GCSBlobWriter;
import com.example.schemainfer.protogen.utils.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple5;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformProtobufHierarchy {
    private Map<String, Map<String, String>> inProtoMap;
    private Set<String> arrayProtoList = new HashSet<>();
    private Map<String, Map<String, String>> outProtoMap = new HashMap<>();
    private Map<String, PrintWriter> outProtoFileMap = new HashMap<>();
    private Map<String, GCSBlobWriter> outGCSProtoFileMap = new HashMap<>();
    private Map<String, List<Tuple5>> outSparkDatasetMap = new HashMap<>();
    private boolean isLocal = false ;
    SparkSession spark ;

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtobufHierarchy.class);

    public TransformProtobufHierarchy(SparkSession spark, Map<String, Map<String, String>> inProtoMap) {
        this.inProtoMap = inProtoMap;
        this.spark = spark ;
    }

    public void generate() {
        this.isLocal = false ;
        LOG.info("** Input keyHierarchy: " + inProtoMap.toString());
        checkShortProtoMap();
        checkShortProtoMap(); // delibratly called twice to take care of elements could be in different order
        printShortProtoMap();
        checkAndStoreColumnsInOutProto();
        printShortProtoMap();
        openFileWriters(isLocal);
        writeToProtoFile(isLocal);

        TransformProtoIntoSparkDataset tt = new TransformProtoIntoSparkDataset(this.spark, this.outSparkDatasetMap) ;
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
            PrintWriter printWriter = null;
            GCSBlobWriter gcsBlobWriter = null ;
            List<Tuple5> sparkProtoLinesList = null ;
            try {
                Map<String, String> colDatatypeMap = proto.getValue();

                String shortProtoName = getShortProtoname(longProtoName);
                protoctr.getAndIncrement();

                final List<?> importProtoList = colDatatypeMap.entrySet().stream()
                        .flatMap(coltypemap -> {
                            final List<String> allImportProtos = findAllImportProtos(coltypemap);
                            final Stream<?> stream = allImportProtos.stream();
                            return stream;
                        }).collect(Collectors.toList());

                if (this.isLocal) {
                    printWriter = outProtoFileMap.get(longProtoName);
                } else {
                    gcsBlobWriter = outGCSProtoFileMap.get(longProtoName);
                    sparkProtoLinesList = outSparkDatasetMap.get(longProtoName);
                }

                String syntaxLine = "syntax \"proto3\"\n" ;
                if (this.isLocal) {
                    printWriter.write(syntaxLine);
                } else {
                    gcsBlobWriter.write(syntaxLine);
                    addSparkLine(sparkProtoLinesList, null, "syntax", "proto3", null, null) ;
                }

                String packaLine = "package " + Constants.GAME_ROOT + "\n" ;
                if (this.isLocal) {
                    printWriter.write(packaLine);
                } else {
                    gcsBlobWriter.write(packaLine);
                    addSparkLine(sparkProtoLinesList, null, "package", Constants.GAME_ROOT, null, null) ;
                }

                PrintWriter finalPrintWriter = printWriter;

                for (Object imp : importProtoList) {
                    String importedLine = "import " + imp + "\n" ;
                    if (this.isLocal) {
                        finalPrintWriter.write(importedLine);
                    } else {
                        gcsBlobWriter.writeToGCS(importedLine);
                        addSparkLine(sparkProtoLinesList, null, "import", imp.toString(), null, null) ;
                    }
                }

                String javaPackage = "option java_package = \"com.example.schemainfer.lol.proto\";\n\n" ;
                if (this.isLocal) {
                    printWriter.write(javaPackage);
                } else {
                    gcsBlobWriter.writeToGCS(javaPackage);
                    addSparkLine(sparkProtoLinesList, null, "option", "java_package =", "com.example.schemainfer.lol.proto", null) ;
                }

                String multipleFilesLine = "option java_multiple_files = true;\n\n" ;
                if (this.isLocal) {
                    printWriter.write(multipleFilesLine);
                } else {
                    gcsBlobWriter.writeToGCS("option java_multiple_files = true;\n\n");
                    addSparkLine(sparkProtoLinesList, null, "option", "java_multiple_files =", "true", null) ;
                }

                String capitalizedName = StringUtils.capitalize(shortProtoName) ;
                String messageLine = "message " + capitalizedName + "\t{\n" ;
                if (this.isLocal) {
                    printWriter.write(messageLine);
                } else {
                    gcsBlobWriter.writeToGCS(messageLine);
                    addSparkLine(sparkProtoLinesList, null, "message", capitalizedName, "\t{", null) ;
                }

                AtomicInteger colCtr = new AtomicInteger();
                PrintWriter finalPrintWriter1 = printWriter;
                GCSBlobWriter finalGCSBlobWriter = gcsBlobWriter;
                List<Tuple5> finalSparkProtoLinesList = sparkProtoLinesList;
                colDatatypeMap.entrySet()
                        .forEach(coltype -> {
                            writeColumnsToProtoFile(colCtr, coltype, longProtoName, protoctr, finalPrintWriter1, finalGCSBlobWriter, finalSparkProtoLinesList);
                        });
            } finally {
                if (this.isLocal) {
                    printWriter.write("}\n");
                    printWriter.flush();
                    printWriter.close();
                } else {
                    gcsBlobWriter.writeToGCS("}\n");
                    try {
                        if (gcsBlobWriter != null && gcsBlobWriter.getWriterChannel() != null) {
                            gcsBlobWriter.getWriterChannel().close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void addSparkLine(List<Tuple5> sparkProtoLinesList, String col1, String col2, String col3, String col4, String col5) {
        Tuple5 newTuple = new Tuple5<>(null, col1, col2, col3, col4) ;
        sparkProtoLinesList.add(newTuple) ;
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
                        LOG.info("Founddddd items. Removed: " + remove);
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

    private void openFileWriters(boolean isLocal) {
        AtomicInteger protoctr = new AtomicInteger();
        for (Map.Entry<String, Map<String, String>> e : outProtoMap.entrySet()) {
            String longProtoName = e.getKey();

            protoctr.getAndIncrement();
            String fileName = determineProtoFileName(longProtoName, isLocal);
            String gcsfileName = determineGCSProtoFileName(longProtoName);

            FileWriter myWriter = null;
            PrintWriter printwiter = null;
            GCSBlobWriter gcsBlobWriter = null ;
            try {
                if (isLocal) {
                    myWriter = new FileWriter(fileName);
                    printwiter = new PrintWriter(myWriter);
                    outProtoFileMap.put(longProtoName, printwiter);
                } else {
                    LOG.info("GCSFileName : " + gcsfileName);
                    gcsBlobWriter = new GCSBlobWriter(gcsfileName);
                    outGCSProtoFileMap.put(longProtoName, gcsBlobWriter);
                    outSparkDatasetMap.put(longProtoName, new ArrayList<>()) ;
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private String determineProtoFileName(String longProtoName, boolean isLocal) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (isLocal) {
            if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
                fileName = Constants.localProtoFileLocation + shortProtoName + ".proto";
            } else {
                fileName = Constants.localProtoFileLocation + Constants.GAME_ENTITIES + shortProtoName + ".proto";
            }
        } else {
            if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
                fileName = Constants.gcsProtoLocation + shortProtoName + ".proto";
            } else {
                fileName = Constants.gcsProtoLocation + Constants.GAME_ENTITIES + shortProtoName + ".proto";
            }
        }
        return fileName;
    }

    private String determineGCSProtoFileName(String longProtoName) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
            fileName = "protos/" + shortProtoName + ".proto";
        } else {
            fileName = "protos/" + Constants.GAME_ENTITIES + shortProtoName + ".proto";
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

        // if (datatype.equals(Constants.NESTED_PROTO) || datatype.equals(Constants.NESTED_ARRAY_PROTO) ) {
        //     datatype = Constants.GAME_ENTITIES + StringUtils.capitalize(colName) ;
        //     datatype = StringUtils.replaceStringInString(datatype, "/", ".") ;
        // }

        stringStringMap.put(colName, datatype);
    }

    private String determineIfArrayDatatypeInColumn(int i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();
        //LOG.info("\t" + datatype + "\t" + colName + "\t = " + i);
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
            importProto = Constants.GAME_ENTITIES + colName + ".proto";
            importProtosList.add(importProto);
        }
        return importProtosList;
    }

    private String writeColumnsToProtoFile(AtomicInteger i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr,
                                           PrintWriter printWriter, GCSBlobWriter finalGCSBlobWriter, List<Tuple5> sparkList) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();
        String transformedDatatype = datatype;
        //  LOG.info("\t" + datatype + "\t" + colName + "\t = " + i);
        if (datatype.equals(Constants.NESTED_PROTO) || datatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            transformedDatatype = Constants.GAME_ENTITIES + StringUtils.capitalize(colName);
            transformedDatatype = StringUtils.replaceStringInString(transformedDatatype, "/", ".");
        }

        String col0 = null ;
        if (datatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            col0 = "\trepeated " ;
            if (this.isLocal) {
                printWriter.write(col0);
            } else {
                finalGCSBlobWriter.writeToGCS(col0);
            }
        }

        int colNum = i.incrementAndGet() ;
        String columnLine = "\t" + transformedDatatype + "\t" + colName + "\t = " + colNum + " \n" ;
        if (this.isLocal) {
            printWriter.write(columnLine);
        } else {
            finalGCSBlobWriter.writeToGCS(columnLine);
            addSparkLine(sparkList, col0, transformedDatatype, colName, String.valueOf(colNum), null);
        }
        return colName;
    }

}
