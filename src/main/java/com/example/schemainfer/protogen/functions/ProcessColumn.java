package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.rules.InferJsonDatatype;
import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import com.example.schemainfer.protogen.utils.JsonGenUtils;
import com.example.schemainfer.protogen.utils.JsonUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

import java.util.List;

public class ProcessColumn {
    static final Logger LOG = LoggerFactory.getLogger(ProcessColumn.class);

    private String colValue;
    private List<Protomap> allProtomapList;

    public ProcessColumn(String colValue, List<Protomap> allProtomapList) {
        this.colValue = colValue;
        this.allProtomapList = allProtomapList;
    }

    public ObjectNode splitFColumnIntoMap() {
        LOG.info("fValue: " + this.colValue);
        String[] keyvalues = this.colValue.split(Constants.SEQUENCE_MAP_DELIM, -1);
        Multimap<String, String> allMap = HashMultimap.create();
        StringBuilder jsonBuffer = new StringBuilder("{");
        int j = 0;

        for (int i = 0; i < keyvalues.length; ++i) {
            String s = keyvalues[i];
            String[] keyvalue = s.split(Constants.SEQUENCE_MAP_EQUAL, 2);
            if (keyvalue.length == 2) {
                String key = keyvalue[0];
                String value = keyvalue[1];

                boolean isadded = JsonUtils.checkAndProcessIfJson(key, value, allMap, allProtomapList);
                if (isadded) {
                    if (j > 0) {
                        jsonBuffer.append(",");
                    }
                    formJsonFromRow(jsonBuffer, key, value);
                    j++;
                }
            }
        }

        jsonBuffer.append("}");
        String finalJsonRow = jsonBuffer.toString();
        LOG.info("Final JSON String: " + finalJsonRow);
        boolean isValidJson = JsonUtils.isJSONValid(finalJsonRow);
        LOG.info("isValidJson: " + isValidJson);
        return JsonGenUtils.generateSchemaFromJsonString(finalJsonRow);
    }

    private void formJsonFromRow(StringBuilder sb, String key, String value) {
        if (value == null || value.isEmpty()) {
            return;
        }

        if (key == null || key.isEmpty()) {
            return;
        }

        String v = value.trim().replace("\u0000", "").replace("\n", "").replace("\r", "");
        if (v == null || v.isEmpty() || v.equalsIgnoreCase("null")) {
            return;
        }

        if (CommonUtils.isPureAscii(v)) {
            sb.append("\"");
            sb.append(key);
            sb.append("\"");
            sb.append(":");
            if (JsonUtils.isJSONValid(v)) {
                sb.append(v);
            } else if (InferJsonDatatype.determineInferDatatype(v) == Constants.DATATYPES.String.name()) {
                sb.append("\"");
                sb.append(v);
                sb.append("\"");
            } else {
                sb.append(v);
            }
        }

    }
}
