package com.example.schemainfer.protogen.utils;

import com.example.schemainfer.protogen.domain.SchemaCount;
import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferDatatype;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Multimap;

public class CommonUtils {


    private static Pattern p = Pattern.compile("^[a-zA-Z]*$");
    private static String ss = ".*[a-zA-Z]+.*";
    private static Pattern p2;


    public static void printMultiMap(Multimap<String, String> seqf3map) {
        StringBuilder sb = new StringBuilder() ;
        Iterator<Map.Entry<String, String>> iterator = seqf3map.entries().iterator();
        //if (iterator.hasNext()) {
        //    Map.Entry<String, String> entry = iterator.next();
        //}
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            int keyValuesSize = seqf3map.get(entry.getKey()).size() ;
            if (entry.getValue() != null) {
                System.out.println("----\t MKey: " + entry.getKey() + " \t size: " + keyValuesSize + "\t" + "MValue: " + entry.getValue()) ;
            }
        }
    }

    public static void printProtomapList(List<Protomap> protomapList) {
        for (Protomap protomap : protomapList) {
            System.out.println("Protoo: key:\t" + protomap.getKey() + " Entity: \t" + protomap.getEntity() + "\t Type: " + protomap.getType()) ;
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
            return true ;
        } catch (NumberFormatException e) {
            return false ;
        }
    }

    public static boolean isDouble2(String input1) {
        try {
            Double.parseDouble(input1) ;
            return true ;
        } catch (NumberFormatException e) {
            return false ;
        }
    }

    public static boolean isDouble(String input1) {
        // TODO: May be find a better way to determine if this should be a double
        return InferDatatype.precisionGreatherThan3(input1) ;
    }

    public static Boolean isBoolean(String instr) {
        if (instr == null || instr.isEmpty()) {
            return null ;
        }
        try {
            String val = instr.toLowerCase();
            return (val.equals("true") || val.equals("false"));
        } catch (Exception e) {
            return false ;
        }
    }

    private static void printTop20F(JavaRDD<String> parsedRDD) {
        List<String> top5List = parsedRDD.take(20);
        top5List.forEach((s) -> {
            System.out.println(" FValue = " + s);
        });
    }

    public static boolean isPureAscii(String v) {
        byte bytearray []  = v.getBytes();
        CharsetDecoder d = Charset.forName("US-ASCII").newDecoder();
        try {
            CharBuffer r = d.decode(ByteBuffer.wrap(bytearray));
            r.toString();
        }
        catch(CharacterCodingException e) {
            return false;
        }
        return true;
    }

    private static Map<String, Long> sortMap(Map<String, Long> unSortedMap) {
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();

        //Use Comparator.reverseOrder() for reverse ordering
        unSortedMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        return reverseSortedMap ;
    }

    public static String printTabs(int numtabs) {
        StringBuilder sb = new StringBuilder() ;
        for (int i=0 ; i<numtabs ; i++) {
            sb.append("\t") ;
        }
        return sb.toString() ;
    }

    public static List<SchemaCount> printDistinctObjectNodesCount(Map<String, Long> objectNodeLongMap) {
        Map<String, Long> sorttedMap = sortMap(objectNodeLongMap) ;

        List<SchemaCount> schemaCountList = new ArrayList();
        Iterator var2 = sorttedMap.entrySet().iterator();
        int i=0 ;

        while(var2.hasNext()) {
            if (i > 20) {
                break ;
            }
            Entry<String, Long> entry = (Entry)var2.next();
            System.out.println("Distinct ObjectNode: " + (String)entry.getKey());
            System.out.println("Distinct count: " + entry.getValue());
            SchemaCount schemaCount = new SchemaCount();
            schemaCount.setSchema((String)entry.getKey());
            schemaCount.setCount((Long)entry.getValue());
            schemaCountList.add(schemaCount);
            i++ ;
        }

        return schemaCountList;
    }

}
