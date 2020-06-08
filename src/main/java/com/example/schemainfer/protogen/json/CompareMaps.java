package com.example.schemainfer.protogen.json;

import com.example.schemainfer.protogen.functions.MergeBiFunction;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompareMaps {
    List<Map<String, Object>> missingObjectsList = new ArrayList<>();

    List<String> keyHierarchy = new ArrayList<>() ;
    Map<String, Object> first ;
    Map<String, Object> second ;
    int iterationNum ;
    Map<String, Object> mergedFinalMap;

    public CompareMaps(Map<String, Object> leftObject, Map<String, Object> rightObject, int iterationNum) {
        this.first = leftObject;
        this.second = rightObject;
        this.iterationNum = iterationNum ;
        mergedFinalMap = new HashMap<>(first) ;
        System.out.println(CommonUtils.printTabs(this.iterationNum) + " ENTRY: entries on Right :" + rightObject.size());
        System.out.println(CommonUtils.printTabs(this.iterationNum) + " ENTRY: entries on Left :" + leftObject.size());
    }

    public Map<String, Object> compareUsingGauva(String mainKeyName, List<String> keyHierarchy) {
        boolean issame = true ;
        this.keyHierarchy = keyHierarchy ;
        System.out.println(CommonUtils.printTabs(this.iterationNum) + " Starting compareUsingGauva :" + mainKeyName) ;
        MapDifference<String, Object> diff = Maps.difference(first, second);
        keyHierarchy.add(mainKeyName) ;

        if (diff.entriesOnlyOnRight().size() != diff.entriesOnlyOnLeft().size()) {
            issame = false ;
            System.out.println(CommonUtils.printTabs(this.iterationNum) + " ----------------------------------------------------------");
            System.out.println(CommonUtils.printTabs(this.iterationNum) + " Different # of entries: " + diff.entriesDiffering().size());
            System.out.println(CommonUtils.printTabs(this.iterationNum) + " Missing entries on Right :" + diff.entriesOnlyOnRight().size());
            System.out.println(CommonUtils.printTabs(this.iterationNum) + " Missing entries on Left  :" + diff.entriesOnlyOnLeft().size());
            Map<String, Object> entriesOnlyOnRight = diff.entriesOnlyOnRight();
            Map<String, Object> entriesOnlyOnLeft = diff.entriesOnlyOnLeft();
            Map<String, Object> entriesInCommon = diff.entriesInCommon();
            System.out.println(CommonUtils.printTabs(this.iterationNum) + " Mismatch in number of entries: " + entriesOnlyOnRight.size() + " :: " + entriesOnlyOnLeft.size());
            addMissingEntry(entriesOnlyOnLeft, this.iterationNum, mergedFinalMap);
            addMissingEntry(entriesOnlyOnRight, this.iterationNum, mergedFinalMap);
        }

        if (diff.entriesDiffering() != null && diff.entriesDiffering().size() > 0) {
            issame = false ;
            mergedFinalMap = compareMapsDifferences(first, second, this.iterationNum + 1, mainKeyName) ;
        }

        System.out.println(CommonUtils.printTabs(this.iterationNum) + " EXIT: Merged Total Entries ON EXIT:" + mergedFinalMap.size());

        return mergedFinalMap;
    }

    private Map<String, Object> compareMapsDifferences(Object left, Object right, int i, String mainKey) {

        Map<String, Object> mergedDiffMap = (Map) left ;

        if (left instanceof Map && right instanceof Map) {

            final MapDifference difference = Maps.difference((Map) left, (Map) right);
            if (difference != null && difference.entriesDiffering() != null) {
                Map<String, MapDifference.ValueDifference<Object>> entriesDiffering = difference.entriesDiffering();

                entriesDiffering.entrySet().stream()
                        .forEach(entry -> {
                            System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " Name : " + (String) entry.getKey());
                            String key = entry.getKey();
                            final MapDifference.ValueDifference<Object> valueDiff = entry.getValue();
                            System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " DIFF-LEFT  = " + valueDiff.leftValue());
                            System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " DIFF-RIGHT = " + valueDiff.rightValue());
                            if (valueDiff.leftValue() instanceof Map && valueDiff.rightValue() instanceof Map) {
                                final MapDifference diff2 = Maps.difference((Map) valueDiff.leftValue(), (Map) valueDiff.rightValue());
                                if (diff2.entriesDiffering() != null && diff2.entriesDiffering().size() > 0) {
                                    keyHierarchy.add(key);
                                    compareMapsDifferences(valueDiff.leftValue(), valueDiff.rightValue(), i + 1, key);
                                } else {
                                    CompareMaps compareMaps = new CompareMaps(diff2.entriesOnlyOnLeft(), diff2.entriesOnlyOnRight(), this.iterationNum + 1);
                                    compareMaps.compareUsingGauva(key, keyHierarchy);
                                }
                            } else {
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " KEY: " + mainKey + " \t--> " + key + "\tDIFF-LEFT  = " + valueDiff.leftValue());
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " KEY: " + mainKey + " \t--> " + key + "\tDIFF-RIGHT = " + valueDiff.rightValue());
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " Key Hierarchy of this instance: " + keyHierarchy);
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " ENTRYYY : " + entry.getClass() + " " + entry.toString());

                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " mergedDiffMap BEFORE : " +  mergedDiffMap.toString());
                                final Object mergedfinal = mergedDiffMap.merge(key, ((Map) right).get(key), new MergeBiFunction() ) ;
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " mergedfinal  : " +  mergedfinal.toString());
                                System.out.println(CommonUtils.printSubtabs(this.iterationNum, i) + " mergedDiffMap AFTER : " +  mergedDiffMap.toString());
                            }
                        });
            }
        }
        return mergedDiffMap;
    }


    public void addMissingEntry(final Map<String, Object> missingEntriesMap, int i, Map<String, Object> entriesToAddIn) {
        if (missingEntriesMap != null && missingEntriesMap.size() > 0) {
            missingEntriesMap.entrySet().stream()
                    .forEach(e -> {
                        System.out.println(CommonUtils.printTabs(this.iterationNum) + " Missing: " + e.getKey() + "  :\t" + e.getValue() + "  :\t" + e.getValue().getClass());
                        System.out.println(CommonUtils.printTabs(this.iterationNum) + " Key Hierarchy of this Missing instance: " + keyHierarchy) ;
                    });

            entriesToAddIn.putAll(missingEntriesMap);
        }

    }

}
