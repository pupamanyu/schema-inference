package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.utils.Constants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessStringColumnAsObjectNode implements Function<String, ObjectNode>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessStringColumnAsObjectNode.class);

    public ProcessStringColumnAsObjectNode() {
    }

    public ObjectNode call(String s) throws Exception {
        return processRow(s);
    }

    public static ObjectNode processRow(String s) {
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

}