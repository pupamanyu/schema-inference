package com.example.schemainfer.protogen.utils;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferDatatype;
import com.github.openjson.JSONArray;
import com.github.openjson.JSONException;
import com.github.openjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

import java.util.*;

public class JsonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    public static boolean isJSONValid(String test) {
        try {
            new JSONObject(test);
        } catch (JSONException ex) {
            try {
                new JSONArray(test);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }

    public static Object getJsonObject(String test) {
        try {
            return new JSONObject(test);
        } catch (JSONException ex) {
            try {
                return new JSONArray(test);
            } catch (JSONException ex1) {
                return null;
            }
        }
    }

    private static Multimap<String, String> extractValuesFromJson(String value, List<Protomap> protomapList) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        Object jSonObject = JsonUtils.getJsonObject(value);
        if (jSonObject == null) {
            LOG.error("No json could be derived from value: " + value);
        }
        // System.out.println("jSonObj from Value: " + jSonObject) ;
        Multimap<String, String> derivedMap = null;
        if (jSonObject instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) jSonObject;
            derivedMap = processJson(jsonObj, protomapList);
        } else if (jSonObject instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) jSonObject;
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                Object oo = iterator.next();
                if (oo instanceof JSONObject) {
                    JSONObject jsonObj = (JSONObject) oo;
                    derivedMap = processJson(jsonObj, protomapList);
                } else {
                    System.out.println("oo not instance of jjSonObj: " + oo.toString());
                    System.out.println("jjSonObj from Value: " + ((JSONArray) jSonObject).toString());
                    LOG.error("oo not instance of jjSonObj: {}", oo.toString());
                    LOG.error("jjSonObj from Value: {}", jSonObject.toString());
                    break;
                }
            }
        } else {
            System.out.println("Not sure ehat jjSonObj type: " + jSonObject.toString());
            LOG.error("Not sure what jjSonObj type: {}", jSonObject.toString());
        }
        return derivedMap;
    }

    private static Multimap<String, String> processJson(JSONObject jsonObj, List<Protomap> protomapList) {
        Set<String> keyset = jsonObj.keySet();
        Multimap<String, String> entityMap = HashMultimap.create();
        for (String k : keyset) {
            // String datatype = InferDatatype.determineInferDatatype(jsonObj.getString(k)) ;
            entityMap.put(k, jsonObj.getString(k));

            // Uncomment if there are more than 2 levels deep of map found
            // checkAndProcessIfJson(k, jsonObj.getString(k), entityMap, protomapList) ;
        }
        return entityMap;
    }

    public static void checkAndProcessIfJson(String key, String value, Multimap<String, String> allMap, List<Protomap> protomapList) {
        if (value == null || value.isEmpty()) {
            allMainKeyValue(key, value, allMap, protomapList);
            return;
        }
        boolean isValidJson = JsonUtils.isJSONValid(value);
        if (isValidJson) {
            //String pr = "key: " + key + "\t\t\t--> " + value + "\t\t " + value;
            //LOG.info(pr) ;
            Multimap<String, String> entityMap = extractValuesFromJson(value, protomapList);
            if (entityMap != null) {
                addEntitiesToProtomap(key, protomapList, entityMap);
                allMap.putAll(entityMap);
            } else {
                LOG.info("jjSonObj Could not get row details for {}", value);
                System.out.println("jjSonObj Could not get row details for : " + value);
            }
        } else {
            allMainKeyValue(key, value, allMap, protomapList);
        }
    }

    public static void allMainKeyValue(String key, String value, Multimap<String, String> allMap, List<Protomap> protomapList) {
        String datatype = InferDatatype.determineInferDatatype(value);
        allMap.put(key, value);
        Protomap protomap = new Protomap();
        protomap.setKey(key);
        protomap.setEntity(null);
        protomap.setType(datatype);
        protomapList.add(protomap);
    }

    public static void addEntitiesToProtomap(String key, List<Protomap> protomapList, Multimap<String, String> entityMap) {
        Set<String> keyset = entityMap.keySet();
        for (String entitykey : keyset) {
            Protomap protomap = new Protomap();
            String valueOfentityKey = null;
            String datatype = null;
            Collection<String> entityValueSet = entityMap.get(entitykey);
            if (entityValueSet != null && entityValueSet.size() > 0) {
                Optional<String> ov = entityValueSet.stream().findFirst();
                if (ov.isPresent() && ov.get() != null) {
                    valueOfentityKey = ov.get();
                    datatype = InferDatatype.determineInferDatatype(valueOfentityKey);
                }
            }

            protomap.setKey(entitykey);
            protomap.setEntity(key);
            protomap.setType(valueOfentityKey);
            protomapList.add(protomap);
        }
    }
}
