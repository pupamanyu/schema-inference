package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.functions.MergeBiFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MapMergeTest {
    @org.junit.jupiter.api.Test
    void processFValue() throws IOException {
        Map<String, Object> m1 = new HashMap<>() ;
        Map<String, String> lh1 = new LinkedHashMap<String, String>() ;
        lh1.put("type", "integer") ;
        m1.put("x1Pos", lh1) ;

        Map<String, Object> m2 = new HashMap<>() ;
        Map<String, String> lh2 = new LinkedHashMap<String, String>() ;
        lh2.put("type", "string") ;
        m2.put("x1Pos", lh2) ;

        Map<String, Object> merged = new HashMap<String, Object>(m1) ;
        m2.forEach(
                (key, value) -> {
                    System.out.println("KEY = " + key) ;
                    System.out.println("VALUE = " + value + " \t class:" + value.getClass()) ;
                  //  final Object mergedfinal = merged.merge(key, value, (v1, v2) -> new MergeBiFunction(v1, v2));
                 //   final Object mergedfinal = merged.merge(key, value, (v1, v2) -> checkIfStringEndsWithFn);
                    final Object mergedfinal = merged.merge(key, value, new MergeBiFunction() ) ;
                    System.out.println("Merged : " + merged.toString());
                    System.out.println("Merged Final: " + mergedfinal.toString());
                })  ;

    }

    public static BiFunction<Object, Object, String> checkIfStringEndsWithFn = (s1, s2) -> {
        System.out.println("S1 = " + s1) ;
        System.out.println("S2 = " + s2) ;
        return "Hello" ;
    };

}
