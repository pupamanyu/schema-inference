package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(TransformProtobufHierarchy.class);

    public TransformProtobufHierarchy(Map<String, Map<String, String>> inProtoMap) {
        this.inProtoMap = inProtoMap;
    }

    public void generate() {
        System.out.println("** Input keyHierarchy: " + inProtoMap.toString());
        checkShortProtoMap();
        checkShortProtoMap(); // delibratly called twice to take care of elements could be in different order
        // writeFileWriters();
        printShortProtoMap();
        checkAndStoreColumnsInOutProto();
        printShortProtoMap();
        openFileWriters();
        writeToProtoFile();
    }

    private void printShortProtoMap() {
        AtomicInteger protoctr = new AtomicInteger();
        outProtoMap.entrySet().stream()
                .forEach(e -> {
                    String longProtoName = e.getKey();
                    Map<String, String> colDatatypeMap = e.getValue();
                    AtomicInteger colCtr = new AtomicInteger();
                    protoctr.getAndIncrement();
                    System.out.println(protoctr + ") -------- OUT Proto ----------> : " + longProtoName + " columuns: " + colDatatypeMap.toString());
                });

        System.out.println("----- Array Protoes: " + arrayProtoList.toString());
    }

    private void writeToProtoFile() {
        AtomicInteger protoctr = new AtomicInteger();


        outProtoMap.entrySet().stream().forEach(proto -> {
            String longProtoName = proto.getKey();
            PrintWriter printWriter = null;
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

                printWriter = outProtoFileMap.get(longProtoName);
                printWriter.write("syntax \"proto3\"\n");
                printWriter.write("package lol\n");
                PrintWriter finalPrintWriter = printWriter;
                importProtoList.stream().forEach(imp -> {
                    finalPrintWriter.write("import " + imp + "\n");
                });
                printWriter.write("option java_package = \"com.example.schemainfer.lol.proto\";\n\n");
                printWriter.write("option java_multiple_files = true;\n\n");
                printWriter.write("message " + StringUtils.capitalize(shortProtoName) + "\t{\n");

                AtomicInteger colCtr = new AtomicInteger();
                PrintWriter finalPrintWriter1 = printWriter;
                colDatatypeMap.entrySet().stream()
                        .forEach(coltype -> {
                            writeColumnsToProtoFile(colCtr, coltype, longProtoName, protoctr, finalPrintWriter1);
                        });
            } finally {
                printWriter.write("}\n");
                printWriter.flush();
                printWriter.close();
            }

        });


    }

    private void checkShortProtoMap() {
        AtomicInteger protoctr = new AtomicInteger();
        inProtoMap.entrySet().stream()
                .forEach(e -> {
                    String longProtoName = e.getKey();
                    Map<String, String> colDatatypeMap = e.getValue();
                    AtomicInteger colCtr = new AtomicInteger();
                    protoctr.getAndIncrement();
                    System.out.println("-------- Proto IN ----------> : " + longProtoName);

                    if (!isArrayDetailProto(longProtoName)) {
                        outProtoMap.put(longProtoName, new HashMap<>());
                    } else {
                        final Map<String, String> remove = outProtoMap.remove(longProtoName);
                        System.out.println("Founddddd items. Removed: " + remove);
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
                    System.out.println("-------- Proto ----------> : " + longProtoName);

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

    private void openFileWriters() {
        AtomicInteger protoctr = new AtomicInteger();
        for (Map.Entry<String, Map<String, String>> e : outProtoMap.entrySet()) {
            String longProtoName = e.getKey();

            protoctr.getAndIncrement();
            String fileName = determineProtoFileName(longProtoName);

            System.out.println("FileName : " + fileName);
            FileWriter myWriter = null;
            PrintWriter printwiter = null;
            try {
                myWriter = new FileWriter(fileName);
                printwiter = new PrintWriter(myWriter);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            outProtoFileMap.put(longProtoName, printwiter);
        }
    }

    private String determineProtoFileName(String longProtoName) {
        String fileName;
        String shortProtoName = getShortProtoname(longProtoName);
        if (shortProtoName.equalsIgnoreCase(Constants.EVENT_TYPE)) {
            fileName = Constants.protoFileLocation + shortProtoName + ".proto";
        } else {
            fileName = Constants.protoFileLocation + Constants.GAME_ENTITIES + shortProtoName + ".proto";
        }
        return fileName;
    }

    private void printProtos(String[] protoArray, int andIncrement) {
        for (int i = 0; i < protoArray.length; i++) {
            String sf = String.format("%d)   %s", andIncrement, protoArray[i]);
            System.out.println(sf);
        }
    }

    private void storeColumnInOutProto(int i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();

        final Map<String, String> stringStringMap = outProtoMap.get(longProtoName);
        if (stringStringMap == null) {
            System.out.println("Incorrect protoMapName: " + longProtoName);
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
        //System.out.println("\t" + datatype + "\t" + colName + "\t = " + i);
        if (datatype.equalsIgnoreCase(Constants.NESTED_ARRAY_PROTO)) {
            String sf = String.format("FOUND ARRAY --> proto: %s datatype: %s colname %s ", longProtoName, datatype, colName);
            System.out.println(sf);
            System.out.println("FOUND ARRAY: \t" + datatype + "\t" + colName + "\t = " + i);
            String ss = longProtoName + colName + "/";
            arrayProtoList.add(ss);
            System.out.println(String.format("Added to ArrayList --> %s ", ss));
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

    private String writeColumnsToProtoFile(AtomicInteger i, Map.Entry<String, String> m, String longProtoName, AtomicInteger protoctr, PrintWriter printWriter) {
        // printProtos(protoArray, protoctr.get());
        String datatype = m.getValue();
        String colName = m.getKey();
        String transformedDatatype = datatype;
        //  System.out.println("\t" + datatype + "\t" + colName + "\t = " + i);
        if (datatype.equals(Constants.NESTED_PROTO) || datatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            transformedDatatype = Constants.GAME_ENTITIES + StringUtils.capitalize(colName);
            transformedDatatype = StringUtils.replaceStringInString(transformedDatatype, "/", ".");
        }

        if (datatype.equals(Constants.NESTED_ARRAY_PROTO)) {
            printWriter.write("\trepeated ");
        }
        printWriter.write("\t" + transformedDatatype + "\t" + colName + "\t = " + i.incrementAndGet() + " \n");
        return colName;
    }

}
