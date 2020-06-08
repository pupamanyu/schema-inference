package com.example.schemainfer.protogen.functions;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeBiFunction implements BiFunction<Object, Object, String> {
    @Override
    public String apply(Object s1, Object s2) {
        if (s1.toString().equalsIgnoreCase("string") ||  s2.toString().equalsIgnoreCase("string")) {
            return "string" ;
        }
        return s2.toString();
    }


}
