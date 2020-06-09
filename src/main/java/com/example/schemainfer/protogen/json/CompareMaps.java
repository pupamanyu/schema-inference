package com.example.schemainfer.protogen.json;

import com.example.schemainfer.protogen.functions.MergeBiFunction;
import com.example.schemainfer.protogen.javaudf.SeqFilesScan;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompareMaps {
    private static final Logger LOG = LoggerFactory.getLogger(CompareMaps.class);

    List<Map<String, Object>> missingObjectsList = new ArrayList<>();
    List<String> keyHierarchy = new ArrayList<>() ;
    Map<String, Object> first ;
    Map<String, Object> second ;
    int depthLevel;
    Map<String, Object> mergedFinalMap;

    public CompareMaps(Map<String, Object> leftObject, Map<String, Object> rightObject, int depthLevel) {
        this.first = leftObject;
        this.second = rightObject;
        this.depthLevel = depthLevel;
        mergedFinalMap = new HashMap<>(first) ;
        LOG.info(CommonUtils.printTabs(this.depthLevel) + " ENTRY: entries on Right :" + rightObject.size());
        LOG.info(CommonUtils.printTabs(this.depthLevel) + " ENTRY: entries on Left :" + leftObject.size());
    }

    public Map<String, Object> compareUsingGauva(String mainKeyName, List<String> keyHierarchy) {
        boolean issame = true ;
        this.keyHierarchy = keyHierarchy ;
        LOG.info(CommonUtils.printTabs(this.depthLevel) + " Starting compareUsingGauva :" + mainKeyName) ;
        MapDifference<String, Object> diff = Maps.difference(first, second);
        keyHierarchy.add(mainKeyName) ;

        if (diff.entriesOnlyOnRight().size() != diff.entriesOnlyOnLeft().size()) {
            issame = false ;
            LOG.info(CommonUtils.printTabs(this.depthLevel) + " ----------------------------------------------------------");
            LOG.info(CommonUtils.printTabs(this.depthLevel) + " Different # of entries: " + diff.entriesDiffering().size());
            LOG.info(CommonUtils.printTabs(this.depthLevel) + " Missing entries on Right :" + diff.entriesOnlyOnRight().size());
            LOG.info(CommonUtils.printTabs(this.depthLevel) + " Missing entries on Left  :" + diff.entriesOnlyOnLeft().size());
            Map<String, Object> entriesOnlyOnRight = diff.entriesOnlyOnRight();
            Map<String, Object> entriesOnlyOnLeft = diff.entriesOnlyOnLeft();
            Map<String, Object> entriesInCommon = diff.entriesInCommon();
            LOG.info(CommonUtils.printTabs(this.depthLevel) + " Mismatch in number of entries: " + entriesOnlyOnRight.size() + " :: " + entriesOnlyOnLeft.size());
            addMissingEntry(entriesOnlyOnLeft, this.depthLevel, mergedFinalMap);
            addMissingEntry(entriesOnlyOnRight, this.depthLevel, mergedFinalMap);
        }

        if (diff.entriesDiffering() != null && diff.entriesDiffering().size() > 0) {
            issame = false ;
            mergedFinalMap = compareMapsDifferences(first, second, this.depthLevel + 1, mainKeyName) ;
        }

        LOG.info(CommonUtils.printTabs(this.depthLevel) + " EXIT: Merged Total Entries ON EXIT:" + mergedFinalMap.size());

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
                            LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " Name : " + (String) entry.getKey());
                            String key = entry.getKey();
                            final MapDifference.ValueDifference<Object> valueDiff = entry.getValue();
                            LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " DIFF-LEFT  = " + valueDiff.leftValue());
                            LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " DIFF-RIGHT = " + valueDiff.rightValue());
                            if (valueDiff.leftValue() instanceof Map && valueDiff.rightValue() instanceof Map) {
                                final MapDifference diff2 = Maps.difference((Map) valueDiff.leftValue(), (Map) valueDiff.rightValue());
                                if (diff2.entriesDiffering() != null && diff2.entriesDiffering().size() > 0) {
                                    keyHierarchy.add(key);
                                    compareMapsDifferences(valueDiff.leftValue(), valueDiff.rightValue(), i + 1, key);
                                } else {
                                    // Recursion
                                    CompareMaps compareMaps = new CompareMaps(diff2.entriesOnlyOnLeft(), diff2.entriesOnlyOnRight(), this.depthLevel + 1);
                                    compareMaps.compareUsingGauva(key, keyHierarchy);
                                }
                            } else {
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " KEY: " + mainKey + " \t--> " + key + "\tDIFF-LEFT  = " + valueDiff.leftValue());
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " KEY: " + mainKey + " \t--> " + key + "\tDIFF-RIGHT = " + valueDiff.rightValue());
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " Key Hierarchy of this instance: " + keyHierarchy);
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " ENTRYYY : " + entry.getClass() + " " + entry.toString());

                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " mergedDiffMap BEFORE : " +  mergedDiffMap.toString());
                                final Object mergedfinal = mergedDiffMap.merge(key, ((Map) right).get(key), new MergeBiFunction() ) ;
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " mergedfinal  : " +  mergedfinal.toString());
                                LOG.info(CommonUtils.printSubtabs(this.depthLevel, i) + " mergedDiffMap AFTER : " +  mergedDiffMap.toString());
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
                        LOG.info(CommonUtils.printTabs(this.depthLevel) + " Missing: " + e.getKey() + "  :\t" + e.getValue() + "  :\t" + e.getValue().getClass());
                        LOG.info(CommonUtils.printTabs(this.depthLevel) + " Key Hierarchy of this Missing instance: " + keyHierarchy) ;
                    });

            entriesToAddIn.putAll(missingEntriesMap);
        }

    }

}
