package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.UniqueQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GenerateProtobufHierarchy {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateProtobufHierarchy.class);

    EventJsonSchema schemaToConvert;
    UniqueQueue<String> keyHierarchy = new UniqueQueue<>();

    StringBuilder sbuff = new StringBuilder();
    Map<String, Map<String, String>> keyMap = new HashMap<>();

    public GenerateProtobufHierarchy(EventJsonSchema schemaToConvert) {
        this.schemaToConvert = schemaToConvert;
    }

    public Map<String, Map<String, String>> generate() {
        final Map<String, Object> firstAdditionalPropertiesMap = schemaToConvert.getAdditionalProperties();
        Map<Integer, String> iterKeyMap = new HashMap<>();
        iterKeyMap.put(0, "additionalproperties");
        keyHierarchy.add("additionalproperties");
        checkProtosRecursevely(firstAdditionalPropertiesMap, 0, iterKeyMap);
        LOG.info("** Final keyProto: {}", keyMap.toString());

        checkRecursively(firstAdditionalPropertiesMap, 0, iterKeyMap);
        LOG.info("** Final keyHierarchy: {}", keyMap.toString());
        return this.keyMap ;
    }

    private void checkProtosRecursevely(Map<String, Object> firstAdditionalPropertiesMap, int i, Map<Integer, String> iterKeyMap) {
        if (firstAdditionalPropertiesMap != null && firstAdditionalPropertiesMap.size() > 0) {
            firstAdditionalPropertiesMap.entrySet().stream()
                    .forEach(e -> {
                        checkTheProto(iterKeyMap, e.getKey(), e.getValue(), i);
                    });
        }
    }

    private void checkTheProto(Map<Integer, String> iterKeyMap, String key, Object obj, Integer i) {
        Integer j = i + 1;
        Map<Integer, String> iterKeyMapNew = new HashMap<>(iterKeyMap);
        if (obj instanceof Map) {
            if (!key.equalsIgnoreCase("properties")) {
                iterKeyMapNew.put(i, key);
                final String protoName = determineProtoName(iterKeyMapNew, obj);
                storeProto(protoName);
            }
            checkProtosRecursevely((Map<String, Object>) obj, j, iterKeyMapNew);
        } else {
            iterKeyMapNew.remove(i - 1);
            iterKeyMapNew.remove(i);
        }
    }

    private Map<String, String> storeProto(String protoName) {
        if (keyMap.containsKey(protoName)) {
            return keyMap.get(protoName);
        }

        Map<String, String> stringStringMultimap = keyMap.get(protoName);
        if (stringStringMultimap == null) {
            stringStringMultimap = new HashMap<>();
        }

        keyMap.put(protoName, stringStringMultimap);
        return stringStringMultimap;
    }

    private void storeColumnInProto(String protoName, String colnameToAdd, String datatype) {
        if (colnameToAdd == null || datatype == null || protoName == null) {
            LOG.error("Column || datatype || protoName is null. {}, {}, {}", protoName, colnameToAdd, datatype);
            return;
        }
        Map<String, String> stringStringMultimap = keyMap.get(protoName);
        if (stringStringMultimap == null) {
            LOG.info("Got NON protoname: " + protoName + " for column: " + colnameToAdd);
        }

        LOG.info("PROTO: " + protoName + " \tCOLUMN: " + colnameToAdd + "\tDATATYPE:" + datatype);
        stringStringMultimap.put(colnameToAdd, datatype);
        keyMap.put(protoName, stringStringMultimap);
    }

    private void checkRecursively(Map<String, Object> firstAdditionalPropertiesMap, int i, Map<Integer, String> iterKeyMap) {
        if (firstAdditionalPropertiesMap != null && firstAdditionalPropertiesMap.size() > 0) {
            firstAdditionalPropertiesMap.entrySet().stream()
                    .forEach(e -> {
                        checkIt(iterKeyMap, e.getKey(), e.getValue(), i);
                    });
        }
    }

    private void checkIt(Map<Integer, String> iterKeyMap, String key, Object obj, Integer i) {
        Integer j = i + 1;
        LOG.info(CommonUtils.printTabs(i) + "\tkey:" + key + "\t valueclass:" + obj.getClass() + "\tvalue --> " + obj.toString());
        Map<Integer, String> iterKeyMapNew = new HashMap<>(iterKeyMap);
        if (obj instanceof Map) {
            if (!key.equalsIgnoreCase("properties")) {
                iterKeyMapNew.put(i, key);
                determineDatatypeForNonColumn(iterKeyMapNew, obj);
                String protoName = determineProtoName(iterKeyMapNew, obj);
                LOG.info(CommonUtils.printTabs(i) + " Got protoName: {} \t for attribute: {} \t iterKeyMapNew --> {}", protoName, key, iterKeyMapNew.toString());
            }
            checkRecursively((Map<String, Object>) obj, j, iterKeyMapNew);
        } else {
            convertIt(key, obj, i, iterKeyMapNew);
            iterKeyMapNew.remove(i - 1);
            iterKeyMapNew.remove(i);
        }
    }

    private void convertIt(String key, Object obj, int i, Map<Integer, String> iterKeyMap) {
        String popped = null;
        String value = (String) obj;

        if (value.equalsIgnoreCase("object") || value.equalsIgnoreCase("array")) {
            if (value.equalsIgnoreCase("array")) {
                String colName = CommonUtils.findMaxValue(iterKeyMap);
                String protoName = determineProtoNameForColumn(iterKeyMap);
                storeColumnInProto(protoName, colName, Constants.NESTED_ARRAY_PROTO);
            }
        } else {
            determineDatatypeForColumn(iterKeyMap, obj);
        }
    }

    private void determineDatatypeForColumn(Map<Integer, String> iterKeyMap, Object obj) {
        LOG.info("trying to determine datatype for {}", iterKeyMap.toString());
        String colName = CommonUtils.findMaxValue(iterKeyMap);
        String datatype = null;
        String protoName = determineProtoNameForColumn(iterKeyMap);

        String ss = colName + "/";
        if (ss.equalsIgnoreCase(protoName)) {
            return;
        }
        if (obj instanceof Map) {
            //  datatype = Constants.NESTED_PROTO ;
        } else {
            datatype = obj.toString();
            storeColumnInProto(protoName, colName, datatype);
        }
    }

    private void determineDatatypeForNonColumn(Map<Integer, String> iterKeyMap, Object obj) {
        String colName = CommonUtils.findMaxValue(iterKeyMap);
        String protoName = determineProtoName(iterKeyMap, obj);
        ;
        String datatype = null;
        String ss = colName + "/";
        if (ss.equalsIgnoreCase(protoName)) {
            return;
        }
        if (obj instanceof Map) {
            datatype = Constants.NESTED_PROTO;
            storeColumnInProto(protoName, colName, datatype);
        } else {
            //  datatype = obj.toString();
        }
    }

    private String determineProtoNameForColumn(Map<Integer, String> iterKeyMap) {
        Map<Integer, String> iterKeyMapNew = new HashMap<>(iterKeyMap);
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < iterKeyMap.size() - 1; i++) {
            String s = (String) iterKeyMap.get(i);
            if (s != null) {
                sb.append(s).append("/");
            }
        }
        String mm = sb.toString();
        if (mm == null || mm.isEmpty()) {
            LOG.warn("Could not determine protoname for : " + iterKeyMap.toString());
        }
        return mm;
    }

    private String determineProtoName(Map<Integer, String> iterKeyMap, Object obj) {
        String maxKey = null;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < iterKeyMap.size() - 1; i++) {
            String s = (String) iterKeyMap.get(i);
            if (s != null) {
                sb.append(s).append("/");
            }
        }
        maxKey = sb.toString();
        return maxKey;
    }

}
