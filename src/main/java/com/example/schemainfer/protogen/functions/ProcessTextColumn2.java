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
            System.out.println("Number of columns != 3. Skipping row: " + s);
            return null;
        } else {
            String fvalue = ss[2];
            if (fvalue != null && !fvalue.isEmpty()) {
                ObjectNode schema = this.splitFColumnIntoMap(fvalue, protomapList);
                LOG.info("-----------------------------------------------------------");
                return schema;
            } else {
                LOG.error("Col #3 is empty. Skipping");
                System.out.println("Col #3 is empty. Skipping: " + s);
                return null;
            }
        }
    }

    private ObjectNode splitFColumnIntoMap(String fvalue, List<Protomap> allProtomapList) {
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
                this.formJsonFromRow(jsonBuffer, key, value);
            }
        }

        jsonBuffer.append("}");
        String finalJsonRow = jsonBuffer.toString();
        LOG.info("Final JSON String: " + finalJsonRow);
        boolean isValidJson = JsonUtils.isJSONValid(finalJsonRow);
        LOG.info("isValidJson: " + isValidJson);
        return JsonGenUtils.generateSchemaFromJsonString(finalJsonRow);
    }

    public void formJsonFromRow(StringBuilder sb, String key, String value) {
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