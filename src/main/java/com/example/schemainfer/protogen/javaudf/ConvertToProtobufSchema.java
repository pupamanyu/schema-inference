package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.json.CompareMaps;
import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.UniqueQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

import java.util.*;

public class ConvertToProtobufSchema {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertToProtobufSchema.class);

    EventJsonSchema schemaToConvert;
    UniqueQueue<String> keyHierarchy = new UniqueQueue<>() ;

    StringBuilder sbuff = new StringBuilder() ;
    Map<String, Multimap<String, String>> keyMap = new HashMap<>() ;

    public ConvertToProtobufSchema(EventJsonSchema schemaToConvert) {
        this.schemaToConvert = schemaToConvert;
    }

    public void generate() {
        final Map<String, Object> firstAdditionalPropertiesMap = schemaToConvert.getAdditionalProperties();
        Map<Integer, String> iterKeyMap = new HashMap<>() ;
        iterKeyMap.put(0, "additionalproperties") ;
        keyHierarchy.add("additionalproperties") ;
        checkRecursively(firstAdditionalPropertiesMap, 0, iterKeyMap);
    }

    private void storeProto(String protoName) {

        Multimap<String, String> stringStringMultimap = keyMap.get(protoName);
        if (stringStringMultimap == null) {
            stringStringMultimap = HashMultimap.create();
        }

        keyMap.put(protoName, stringStringMultimap) ;
    }

    private void storeColumn(String protoName, String colnameToAdd, String datatype) {

        Multimap<String, String> stringStringMultimap = keyMap.get(protoName);
        if (stringStringMultimap == null) {
            stringStringMultimap = HashMultimap.create();
        }
        stringStringMultimap.put(colnameToAdd, datatype) ;

        keyMap.put(protoName, stringStringMultimap) ;
    }

    private void checkRecursively(Map<String, Object> firstAdditionalPropertiesMap, int i, Map<Integer, String> iterKeyMap) {
        if (firstAdditionalPropertiesMap != null && firstAdditionalPropertiesMap.size() > 0) {
            firstAdditionalPropertiesMap.entrySet().stream()
                    .forEach(e -> {
                        checkIt(iterKeyMap, e.getKey(), e.getValue(), i) ;
                    });
        }
    }

    private void checkIt(Map<Integer, String> iterKeyMap, String key, Object obj, Integer i) {
        Integer j = i + 1 ;
        System.out.println(CommonUtils.printTabs(i) + "\tkey:" + key + "\t valueclass:" + obj.getClass() + "\tvalue --> " + obj.toString()) ;
        Map<Integer, String> iterKeyMapNew = new HashMap<>(iterKeyMap) ;
        if (obj instanceof Map) {
            if (!key.equalsIgnoreCase("properties")) {
                iterKeyMapNew.put(i, key) ;
            }
            checkRecursively((Map<String, Object>) obj, j, iterKeyMapNew) ;
        } else {
            convertIt(key, obj, i , iterKeyMapNew) ;

            iterKeyMapNew.remove(i-1) ;
            iterKeyMapNew.remove(i) ;

        }
    }

    private void convertIt(String key, Object obj, int i, Map<Integer, String> iterKeyMap) {
        String popped = null ;
        String value = (String) obj ;
        if (value.equalsIgnoreCase("object") || value.equalsIgnoreCase("array") ) {
            // Create a new Proto file here
            final String maxKey = CommonUtils.findMaxValue(iterKeyMap);
           // keyHierarchy.add(maxKey) ;
            storeProto(maxKey);
          //  LOG.info(CommonUtils.printTabs(i) + " Bottom key POP : (" + key + ") : value: (" + obj + ") Class: " + obj.getClass() +  "\tPrev Key: " + prevKey + " \tpopped: " + popped + " Hierarchy:" + keyHierarchy.toString()) ;
        } else {
          //  LOG.info(CommonUtils.printTabs(i) + " Bottom key PUSH: (" + key + ") : value: (" + obj + ") Class: " + obj.getClass() +  "\tPrev Key: " + prevKey + " \tpopped: " + popped + " Hierarchy:" + keyHierarchy.toString()) ;

        }

        System.out.println(CommonUtils.printTabs(i) + "** Bottom key (" + key + ") : value: (" + obj + ") Class: " + obj.getClass() + " keyMap: " + iterKeyMap.toString() + " \t keyProto: " + keyMap.toString() ) ;
    }

    private void determineProto(String key, Object obj, int i, Map<Integer, String> iterKeyMap) {
        if (iterKeyMap.size() <= 1) {
            return  ;
        }
        int size = iterKeyMap.size() ;

    }

}
