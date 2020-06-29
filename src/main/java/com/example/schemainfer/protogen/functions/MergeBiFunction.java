package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.json.CompareMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Infer Datatype rules implementation
 * Gather the richer datatype in case of any conflicts
 */
public class MergeBiFunction implements BiFunction<Object, Object, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MergeBiFunction.class);

    @Override
    public String apply(Object s1, Object s2) {

        // TODO: Finish implementation to infer datatype rules
        LOG.info("Inside MergeBiFunction with: " + s1.toString() + " AND " + s2.toString()) ;
        if (s1 == null && s2 == null) {
            return null ;
        }

        if (s1 == null || s1.toString().equalsIgnoreCase("null")) {
            return s2.toString() ;
        }

        if (s2 == null || s2.toString().equalsIgnoreCase("null")) {
            return s1.toString() ;
        }

        if (s1.toString().equalsIgnoreCase("string") ||  s2.toString().equalsIgnoreCase("string")) {
            return "string" ;
        }

        if (s1.toString().equalsIgnoreCase("float") &&  s2.toString().equalsIgnoreCase("double")) {
            return "double" ;
        }

        if (s2.toString().equalsIgnoreCase("double") &&  s1.toString().equalsIgnoreCase("float")) {
            return "double" ;
        }

        return s2.toString();
    }

}
