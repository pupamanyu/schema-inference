package com.example.schemainfer.protogen.utils;

import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferJsonDatatype;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Multimap;

public class CommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);
    private static Pattern p = Pattern.compile("^[a-zA-Z]*$");
    private static String ss = ".*[a-zA-Z]+.*";
    private static Pattern p2;

    public static void printMap(Map<String, Object> someMap, String referenceText) {
        StringBuilder sb = new StringBuilder();
        someMap.entrySet().stream()
                .forEach(e -> {
                    LOG.info(referenceText + "\t\t Keyy: " + e.getKey() + " having Value: " + e.getValue() + "\n");
                });
    }

    public static void printMultiMap(Multimap<String, String> seqf3map) {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> iterator = seqf3map.entries().iterator();
        //if (iterator.hasNext()) {
        //    Map.Entry<String, String> entry = iterator.next();
        //}
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            int keyValuesSize = seqf3map.get(entry.getKey()).size();
            if (entry.getValue() != null) {
                LOG.info("----\t MKey: " + entry.getKey() + " \t size: " + keyValuesSize + "\t" + "MValue: " + entry.getValue());
            }
        }
    }

    public static void printProtomapList(List<Protomap> protomapList) {
        for (Protomap protomap : protomapList) {
            LOG.info("Protoo: key:\t" + protomap.getKey() + " Entity: \t" + protomap.getEntity() + "\t Type: " + protomap.getType());
        }
    }

    public static boolean isAlpha2(String s) {
        return p.matcher(s).find();
    }

    public static boolean isAlpha3(String s) {
        return s.matches(ss);
    }

    public static boolean isAlpha(String s) {
        return StringUtils.isAlpha(s);
    }

    public static boolean isFloat(String input1) {
        try {
            float f = Float.parseFloat(input1);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isDouble2(String input1) {
        try {
            Double.parseDouble(input1);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isDouble(String input1) {
        // TODO: May be find a better way to determine if this should be a double
        return InferJsonDatatype.precisionGreatherThan3(input1);
    }

    public static Boolean isBoolean(String instr) {
        if (instr == null || instr.isEmpty()) {
            return null;
        }
        try {
            String val = instr.toLowerCase();
            return (val.equals("true") || val.equals("false"));
        } catch (Exception e) {
            return false;
        }
    }

    private static void printTop20F(JavaRDD<String> parsedRDD) {
        List<String> top5List = parsedRDD.take(20);
        top5List.forEach((s) -> {
            LOG.info(" FValue = " + s);
        });
    }

    public static boolean isPureAscii(String v) {
        byte bytearray[] = v.getBytes();
        CharsetDecoder d = Charset.forName("US-ASCII").newDecoder();
        try {
            CharBuffer r = d.decode(ByteBuffer.wrap(bytearray));
            r.toString();
        } catch (CharacterCodingException e) {
            return false;
        }
        return true;
    }

    private static Map<String, Integer> sortMap2(Map<ObjectNode, Integer> unSortedMap) {
        LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>();

        //Use Comparator.reverseOrder() for reverse ordering
        unSortedMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey().toString(), x.getValue()));

        return reverseSortedMap;
    }

    private static Map<String, Long> sortMap(Map<String, Long> unSortedMap) {
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();

        //Use Comparator.reverseOrder() for reverse ordering
        unSortedMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        return reverseSortedMap;
    }

    public static String printTabs(int numtabs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numtabs; i++) {
            sb.append("\t");
        }
        sb.append(numtabs);
        sb.append(") ");
        return sb.toString();
    }

    public static String printSubtabs(int numtabs, int subtabs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numtabs; i++) {
            sb.append("\t");
        }
        sb.append(numtabs);
        sb.append(") ");
        for (int i = 0; i < subtabs; i++) {
            sb.append("\t");
        }
        sb.append(subtabs);
        sb.append(" -> ");
        return sb.toString();
    }

    public static List<SchemaCount> calcDistinctObjectNodesCount2(Map<ObjectNode, Integer> objectNodeLongMap, long totalCount) {
        Map<String, Integer> sorttedMap = sortMap2(objectNodeLongMap);
        List<SchemaCount> schemaCountList = new ArrayList();
        final List<SchemaCount> schemaCounts = sorttedMap.entrySet().stream().limit(20).map(entry -> {
            SchemaCount schemaCount = new SchemaCount();
            schemaCount.setSchema((String) entry.getKey().toString());
           // long thisCount = (Long) entry.getValue();
            final Integer thisCount = entry.getValue();
            schemaCount.setCount(thisCount);
            schemaCount.setPercent(calculatePercentage(thisCount, totalCount));
            schemaCountList.add(schemaCount);
            return schemaCount;
        }).collect(Collectors.toList());

        return schemaCounts ;
    }

/*
    public static List<SchemaCount> calcDistinctObjectNodesCount(Map<String, Long> objectNodeLongMap, long totalCount) {
        Map<String, Long> sorttedMap = sortMap(objectNodeLongMap);

        List<SchemaCount> schemaCountList = new ArrayList();
        Iterator var2 = sorttedMap.entrySet().iterator();
        int i = 0;

        while (var2.hasNext()) {
            if (i > 20) {
                break;
            }
            Entry<String, Long> entry = (Entry) var2.next();
            LOG.info("Distinct ObjectNode: " + (String) entry.getKey());
            LOG.info("Distinct count: " + entry.getValue());
            SchemaCount schemaCount = new SchemaCount();
            schemaCount.setSchema((String) entry.getKey());
            long thisCount = (Long) entry.getValue();
            entry.getValue()
            schemaCount.setCount(thisCount);
            schemaCount.setPercent(calculatePercentage(thisCount, totalCount));
            schemaCountList.add(schemaCount);
            i++;
        }

        return schemaCountList;
    }
*/

    public static Float calculatePercentage(long current, long total) {
        if (total == 0) {
            return null;
        }
        final float l = current * 100 / total;
        return l;
    }

    public static <K, V extends Comparable<V>> K findMinKey(Map<K, V> map) {
        Optional<Entry<K, V>> maxEntry = map.entrySet()
                .stream()
                .min(Comparator.comparing(Map.Entry::getValue));
        return maxEntry.get()
                .getKey() ;
    }
    public static boolean isLocal() {
        String mode = SchemaInferConfig.getInstance().getRunMode() ;
        if (mode == null || mode.isEmpty()) {
            return false ;
        }
        if (mode.equalsIgnoreCase(Constants.RUN_MODE.Local.name())) {
            return true ;
        }
        return false ;
    }

    public static Integer findMaxKey(Map<Integer, String> mapGroup) {
        if(mapGroup.isEmpty()) {
            return null;
        }
        int max = mapGroup.keySet().stream().max(Comparator.naturalOrder()).get();
        Optional<Integer> first = mapGroup.entrySet().stream()
                .filter(e -> e.getKey() == max)
                .map(Entry::getKey).findFirst();

        if (first.isPresent()) {
            return first.get() ;
        }
        return null ;
    }

    public static String findMaxValue(Map<Integer, String> mapGroup) {
        final Integer maxKey = findMaxKey(mapGroup);
        return mapGroup.get(maxKey) ;
    }

    public static <K, V extends Comparable<V>> V findMinValue(Map<K, V> map) {
        K minKey = findMinKey(map);
        return map.get(minKey) ;
    }

}
