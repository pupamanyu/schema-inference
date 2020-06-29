package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.domain.SchemaColumnMap;
import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.ConvertUtils;
import com.example.schemainfer.protogen.utils.StringUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProcessTextColumn4 implements Function<Text, SchemaColumnMap>, Serializable {
    static final Logger LOG = LoggerFactory.getLogger(ProcessTextColumn4.class);

    public ProcessTextColumn4() {
    }

    public SchemaColumnMap call(Text text) throws Exception {
        String s = ConvertUtils.bytesToString(text.getBytes(), 0, text.getLength());
        ObjectNode onode = this.processRow(s);
        String ss = replaceBinaryCharactersForVisibility(s) ;
        return new SchemaColumnMap(onode, ss) ;
    }

    public ObjectNode processRow(String s) {

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
                return schema;
            } else {
                LOG.error("Col #3 is empty. Skipping");
                return null;
            }
        }
    }

    private String replaceBinaryCharactersForVisibility(String s) {
        if (s == null) return "" ;

        return s.replaceAll(Constants.SEQUENCE_FIELD_DELIMITER, "|")
                .replaceAll(Constants.SEQUENCE_MAP_EQUAL, "=")
                .replaceAll(Constants.SEQUENCE_MAP_DELIM, ",") ;
    }

}