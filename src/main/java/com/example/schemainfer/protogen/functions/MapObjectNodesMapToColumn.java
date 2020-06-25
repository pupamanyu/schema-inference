package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.utils.Constants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MapObjectNodesMapToColumn implements Function<ObjectNode, String>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MapObjectNodesMapToColumn.class);

    public MapObjectNodesMapToColumn() {
    }

    public String call(ObjectNode s) throws Exception {
        return processRow(s.toString());
    }

    public static String processRow(String s) {
        return s ;
    }

}