package com.example.schemainfer.protogen.utils;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferDatatype;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.spark_project.guava.collect.Multimap;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CommonUtils {

    private static Pattern p = Pattern.compile("^[a-zA-Z]*$") ;

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
        return p.matcher(s).find() ;
    }

    public static boolean isAlpha(String s) {
        return StringUtils.isAlpha(s) ;
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

}
