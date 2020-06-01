package com.example.schemainfer.protogen.functions;

import com.example.schemainfer.protogen.javaudf.Protomap;
import com.example.schemainfer.protogen.utils.*;
import org.apache.hadoop.io.Text;


import org.apache.spark.api.java.function.FlatMapFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.HashMultimap;
import org.spark_project.guava.collect.Multimap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ProcessTextColumn implements FlatMapFunction<Text, Protomap>, Serializable {

   static final Logger LOG = LoggerFactory.getLogger(ProcessTextColumn.class) ;

    @Override
    public Iterator<Protomap> call(Text text) throws Exception {
        String s = ConvertUtils.bytesToString(text.getBytes(), 0, text.getLength());
        return processRow(s);
    }

    public Iterator<Protomap> processRow(String s) {
        String[] ss = s.split(Constants.SEQUENCE_FIELD_DELIMITER, -1);
        List<Protomap> protomapList = new ArrayList<>();
        if (ss.length != 3) {
            LOG.error("Number of columns != 3. Skipping row") ;
            System.out.println("Number of columns != 3. Skipping row: " + s) ;
            return protomapList.iterator();
        }
        String fvalue = ss[2];
        if (fvalue == null || fvalue.isEmpty()) {
            LOG.error("Col #3 is empty. Skipping") ;
            System.out.println("Col #3 is empty. Skipping: " + s) ;
            return protomapList.iterator();
        }
        Multimap<String, String> allMap = splitFColumnIntoMap(fvalue, protomapList);
        LOG.info("-----------------------------------------------------------");
        // CommonUtils.printMultiMap(allMap);
        // CommonUtils.printProtomapList(protomapList);
        return protomapList.iterator();
    }

    private Multimap<String, String> splitFColumnIntoMap(String fvalue, List<Protomap> allProtomapList) {
        LOG.info("fValue: " + fvalue);
        String[] keyvalues = fvalue.split(Constants.SEQUENCE_MAP_DELIM, -1);
        Multimap<String, String> allMap = HashMultimap.create();
        StringBuilder jsonBuffer = new StringBuilder("{");
        for (int i = 0; i < keyvalues.length; i++) {
            if (i > 0) {
                jsonBuffer.append(",");
            }
            String s = keyvalues[i];
            String[] keyvalue = s.split(Constants.SEQUENCE_MAP_EQUAL, 2);
            if (keyvalue.length != 2) {
                continue ;
            }
            String key = keyvalue[0];
            String value = keyvalue[1];
            //LOG.info(i + ") Processing key: " + key + " Value: " + value) ;
            JsonUtils.checkAndProcessIfJson(key, value, allMap, allProtomapList);
            formJsonFromRow(jsonBuffer, key, value);
        }
        jsonBuffer.append("}");
        final String finalJsonRow = jsonBuffer.toString() ;
        LOG.info("Final JSON String: " + finalJsonRow) ;
        boolean isValidJson =  JsonUtils.isJSONValid(finalJsonRow);
        LOG.info("isValidJson: " + isValidJson) ;
        JsonGenUtils.generateSchemaFromJsonString(finalJsonRow) ;

        return allMap;
    }

    public void formJsonFromRow(StringBuilder sb, String key, String value) {
        if (key == null || key.isEmpty()) {
            return;
        }

        sb.append("\"");
        sb.append(key);
        sb.append("\"");
        sb.append(":");

        if (value == null || value.isEmpty()) {
            sb.append("null");
            return;
        } else {
            if (CommonUtils.isPureAscii(value)) {
                String v = value.replace("\u0000", "").replace("\n", "").replace("\r", "");
                if (v.isEmpty()) {
                    sb.append("null");
                } else {
                    if (JsonUtils.isJSONValid(v)) {
                        sb.append(v);
                    } else {
                        sb.append("\"");
                        sb.append(v);
                        sb.append("\"");
                    }
                }
            }
        }
    }
}
