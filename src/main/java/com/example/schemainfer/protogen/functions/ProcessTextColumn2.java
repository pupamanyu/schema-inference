package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferDatatype;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.ConvertUtils;
import com.example.schemainfer.protogen.utils.JsonGenUtils;
import com.example.schemainfer.protogen.utils.JsonUtils;
import com.example.schemainfer.protogen.utils.Constants.DATATYPES;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

public class ProcessTextColumn2 implements Function<Text, ObjectNode>, Serializable {
    static final Logger LOG = LoggerFactory.getLogger(ProcessTextColumn2.class);

    public ProcessTextColumn2() {
    }

    public ObjectNode call(Text text) throws Exception {
        String s = ConvertUtils.bytesToString(text.getBytes(), 0, text.getLength());
        return this.processRow(s);
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
                LOG.info("-----------------------------------------------------------");
                return schema;
            } else {
                LOG.error("Col #3 is empty. Skipping");
                return null;
            }
        }
    }

}