package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.utils.Constants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MapColumnIntoObjectNode implements Function<String, Tuple2<String, ObjectNode>>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MapColumnIntoObjectNode.class);

    public MapColumnIntoObjectNode() {
    }

    public  ObjectNode processRow(String s) {
        String[] ss = s.split(Constants.SEQUENCE_FIELD_DELIMITER, -1);
        List<Protomap> protomapList = new ArrayList();
        if (ss.length != 3) {
            LOG.error("Number of columns != 3. Skipping row");
            return null;
        } else {
            String fvalue = ss[2];
            if (fvalue != null && !fvalue.isEmpty()) {
                ProcessColumn pc = new ProcessColumn(s, protomapList) ;
                ObjectNode schema = pc.splitFColumnIntoMap() ;
                LOG.info("-----------------------------------------------------------");
                return schema;
            } else {
                LOG.error("Col #3 is empty. Skipping");
                return null;
            }
        }
    }


    @Override
    public Tuple2<String, ObjectNode> call(String v1) throws Exception {
        ObjectNode onode = processRow(v1) ;
        Tuple2 tuple = new Tuple2<>(v1, onode) ;
        return tuple ;
    }
}