package com.example.schemainfer.protogen.jsongen;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonschema.SchemaAware;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.std.NullSerializer;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

public class SchemaGenerator {

    private final ObjectMapper objectMapper;

    public SchemaGenerator() {
        this(null);
    }

    public SchemaGenerator(JsonFactory jsonFactory) {
        this.objectMapper = new ObjectMapper(jsonFactory)
                .enable(JsonParser.Feature.ALLOW_COMMENTS)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }

    public ObjectNode schemaFromJsonUrl(URL example) {
        try {
            JsonNode content = this.objectMapper.readTree(example);
            return schemaFromJsonNode(content);
        } catch (IOException e) {
            throw new GenerationException("Could not process JSON in source file", e);
        }
    }

    public ObjectNode schemaFromJsonNode(JsonNode example) {
        if (example.isObject()) {
            return objectSchema(example);
        } else if (example.isArray()) {
            return arraySchema(example);
        } else {
            return simpleTypeSchema(example);
        }
    }

    private ObjectNode objectSchema(JsonNode exampleObject) {
        ObjectNode schema = this.objectMapper.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = this.objectMapper.createObjectNode();
        for (Iterator<String> iter = exampleObject.fieldNames(); iter.hasNext(); ) {
            String property = iter.next();
            properties.set(property, schemaFromJsonNode(exampleObject.get(property)));
        }
        schema.set("properties", properties);
        return schema;
    }

    private ObjectNode arraySchema(JsonNode exampleArray) {
        ObjectNode schema = this.objectMapper.createObjectNode();
        schema.put("type", "array");

        if (exampleArray.size() > 0) {
            JsonNode exampleItem = exampleArray.get(0).isObject() ? mergeArrayItems(exampleArray) : exampleArray.get(0);
            schema.set("items", schemaFromJsonNode(exampleItem));
        }

        return schema;
    }

    private JsonNode mergeArrayItems(JsonNode exampleArray) {
        ObjectNode mergedItems = this.objectMapper.createObjectNode();

        for (JsonNode item : exampleArray) {
            if (item.isObject()) {
                mergeObjectNodes(mergedItems, (ObjectNode) item);
            }
        }
        return mergedItems;
    }

    private ObjectNode mergeObjectNodes(ObjectNode targetNode, ObjectNode updateNode) {
        Iterator<String> fieldNames = updateNode.fieldNames();
        while (fieldNames.hasNext()) {

            String fieldName = fieldNames.next();
            JsonNode targetValue = targetNode.get(fieldName);
            JsonNode updateValue = updateNode.get(fieldName);

            if (targetValue == null) {
                // Target node doesn't have this field from update node: just add it
                targetNode.set(fieldName, updateValue);

            } else {
                // Both nodes have the same field: merge the values
                if (targetValue.isObject() && updateValue.isObject()) {
                    // Both values are objects: recurse
                    targetNode.set(fieldName, mergeObjectNodes((ObjectNode) targetValue, (ObjectNode) updateValue));
                } else if (targetValue.isArray() && updateValue.isArray()) {
                    // Both values are arrays: concatenate them to be merged later
                    ((ArrayNode) targetValue).addAll((ArrayNode) updateValue);
                } else {
                    // Values have different types: use the one from the update node
                    targetNode.set(fieldName, updateValue);
                }
            }
        }

        return targetNode;
    }

    private ObjectNode simpleTypeSchema(JsonNode exampleValue) {

        try {
            Object valueAsJavaType = this.objectMapper.treeToValue(exampleValue, Object.class);
            SchemaAware valueSerializer = getValueSerializer(valueAsJavaType);
            return (ObjectNode) valueSerializer.getSchema(this.objectMapper.getSerializerProvider(), null);
        } catch (JsonProcessingException e) {
            throw new GenerationException("Unable to generate a schema for this json example: " + exampleValue, e);
        }

    }

    private SchemaAware getValueSerializer(Object valueAsJavaType) throws JsonMappingException {
        SerializerProvider serializerProvider = new DefaultSerializerProvider.Impl().createInstance(this.objectMapper.getSerializationConfig(), BeanSerializerFactory.instance);

        if (valueAsJavaType == null) {
            return NullSerializer.instance;
        } else if (valueAsJavaType instanceof Long) {
            // longs are 'integers' in schema terms
            JsonSerializer<Object> valueSerializer = serializerProvider.findValueSerializer(Integer.class, null);
            return (SchemaAware) valueSerializer;
        } else {
            Class<? extends Object> javaTypeForValue = valueAsJavaType.getClass();
            JsonSerializer<Object> valueSerializer = serializerProvider.findValueSerializer(javaTypeForValue, null);
            return (SchemaAware) valueSerializer;
        }
    }
}
