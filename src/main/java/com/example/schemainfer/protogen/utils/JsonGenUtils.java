package com.example.schemainfer.protogen.utils;


import com.example.schemainfer.protogen.jsongen.SchemaGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonGenUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JsonGenUtils.class);

    public static ObjectNode generateSchemaFromJsonString(String json) {
        JsonNode jsonNode = createJsonNodeFromJsonString(json);
        return generateSchemaFromJsonNode(jsonNode);
    }

    public static JsonNode createJsonNodeFromJsonString(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(json);
            return jsonNode;
        } catch (IOException var3) {
            var3.printStackTrace();
            return null;
        }
    }

    public static ObjectNode generateSchemaFromJsonNode(JsonNode jsonNode) {
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        ObjectNode obj = schemaGenerator.schemaFromJsonNode(jsonNode);
        LOG.info("schema generated: " + obj.toString());
        return obj;
    }
}
