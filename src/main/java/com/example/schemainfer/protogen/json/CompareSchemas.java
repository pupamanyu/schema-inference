package com.example.schemainfer.protogen.json;

import com.example.schemainfer.protogen.utils.CommonUtils;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CompareSchemas {

    public static void compareTwoSchemas(EventJsonSchema firstSchema, EventJsonSchema secondSchema) {
        final Map<String, Object> firstAdditionalProperties = firstSchema.getAdditionalProperties();
        final Map<String, Object> secondAdditionalProperties = secondSchema.getAdditionalProperties();

        compareTopJsonValues(firstAdditionalProperties, secondAdditionalProperties);
        Map<String, Object> resMap = areEqualKeyValues(firstAdditionalProperties, secondAdditionalProperties);
        CommonUtils.printMap(resMap, "EqualKeyValuesTest: ");
        areEqualKeySets(firstAdditionalProperties, secondAdditionalProperties) ;

        CompareMaps comapreMaps = new CompareMaps(firstAdditionalProperties, secondAdditionalProperties, 0) ;
        List<String> keyHierarchy = new ArrayList<>() ;
        Map<String, Object> mergedMap = comapreMaps.compareUsingGauva("additionalproperties", keyHierarchy) ;
        CommonUtils.printMap(mergedMap, "Gauava AFTER-Merge: ");

       // System.out.println("KeyHierarchy: " + comapreMaps.getKeyHierarchy()) ;
       // compareUsingGauva();
    }

    public static Map<String, Object> areEqualKeyValues(Map<String, Object> first, Map<String, Object> second) {
        return first.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(),
                        e -> e.getValue().equals(second.get(e.getKey()))));
    }

    public static boolean areEqualKeySets(Map<String, Object> first, Map<String, Object> second) {
        boolean isMatch = first.keySet().equals(second.keySet());
        if (!isMatch) {
            System.out.println("NOt a match in EqualKeySets") ;
        } else {
            System.out.println("ALL match in EqualKeySets") ;
        }
        return isMatch ;
    }

    public static boolean compareTopJsonValues(Map<String, Object> first, Map<String, Object> second) {
        boolean isMatch =
         first.entrySet().stream()
                .allMatch(e -> {
                    return e.getValue().equals(second.get(e.getKey())) ;
                });

        if (!isMatch) {
            System.out.println("NOt a match in Top JSON Values") ;
        } else {
            System.out.println("ALL match in Top JSON Values") ;
        }

        return isMatch ;
    }

    public static boolean compareProperties(LinkedHashMap<String, Object> first, LinkedHashMap<String, Object> second) {
        return first.entrySet().stream()
                .allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }


}
