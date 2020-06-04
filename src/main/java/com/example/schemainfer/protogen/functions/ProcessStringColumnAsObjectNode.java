package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferDatatype;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.JsonGenUtils;
import com.example.schemainfer.protogen.utils.JsonUtils;
import com.example.schemainfer.protogen.utils.Constants.DATATYPES;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

public class ProcessStringColumnAsObjectNode implements Function<String, ObjectNode>, Serializable {
    static final Logger LOG = LoggerFactory.getLogger(ProcessStringColumnAsObjectNode.class);

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
                ObjectNode schema = splitFColumnIntoMap(fvalue, protomapList);
                LOG.info("-----------------------------------------------------------");
                return schema;
            } else {
                LOG.error("Col #3 is empty. Skipping");
                return null;
            }
        }
    }

    public static ObjectNode splitFColumnIntoMap(String fvalue, List<Protomap> allProtomapList) {
        LOG.info("fValue: " + fvalue);
        String[] keyvalues = fvalue.split(Constants.SEQUENCE_MAP_DELIM, -1);
        Multimap<String, String> allMap = HashMultimap.create();
        StringBuilder jsonBuffer = new StringBuilder("{");

        for (int i = 0; i < keyvalues.length; ++i) {
            if (i > 0) {
                jsonBuffer.append(",");
            }

            String s = keyvalues[i];
            String[] keyvalue = s.split(Constants.SEQUENCE_MAP_EQUAL, 2);
            if (keyvalue.length == 2) {
                String key = keyvalue[0];
                String value = keyvalue[1];
                JsonUtils.checkAndProcessIfJson(key, value, allMap, allProtomapList);
                formJsonFromRow(jsonBuffer, key, value);
            }
        }

        jsonBuffer.append("}");
        String finalJsonRow = jsonBuffer.toString();
        LOG.info("Final JSON String: " + finalJsonRow);
        boolean isValidJson = JsonUtils.isJSONValid(finalJsonRow);
        LOG.info("isValidJson: " + isValidJson);
        return JsonGenUtils.generateSchemaFromJsonString(finalJsonRow);
    }

    public static void formJsonFromRow(StringBuilder sb, String key, String value) {
        if (key != null && !key.isEmpty()) {
            sb.append("\"");
            sb.append(key);
            sb.append("\"");
            sb.append(":");
            if (value != null && !value.isEmpty()) {
                if (CommonUtils.isPureAscii(value)) {
                    String v = value.replace("\u0000", "").replace("\n", "").replace("\r", "");
                    if (v.isEmpty()) {
                        sb.append("null");
                    } else if (JsonUtils.isJSONValid(v)) {
                        sb.append(v);
                    } else if (InferDatatype.determineInferDatatype(v) == DATATYPES.String.name()) {
                        sb.append("\"");
                        sb.append(v);
                        sb.append("\"");
                    } else {
                        sb.append(v);
                    }
                }

            } else {
                sb.append("null");
            }
        }
    }
}