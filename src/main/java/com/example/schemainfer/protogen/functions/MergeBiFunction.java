package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.json.CompareMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeBiFunction implements BiFunction<Object, Object, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MergeBiFunction.class);

    @Override
    public String apply(Object s1, Object s2) {

        LOG.info("Inside MergeBiFunction with: " + s1.toString() + " AND " + s2.toString()) ;
        if (s1.toString().equalsIgnoreCase("string") ||  s2.toString().equalsIgnoreCase("string")) {
            return "string" ;
        }
        return s2.toString();
    }

}
