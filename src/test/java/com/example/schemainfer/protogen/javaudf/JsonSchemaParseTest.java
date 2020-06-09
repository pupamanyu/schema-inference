package com.example.schemainfer.protogen.javaudf;


import com.example.schemainfer.protogen.json.EventJsonSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static org.junit.Assert.assertEquals;

class JsonSchemaParseTest {

    @org.junit.jupiter.api.Test
    void processFValue() throws IOException {
        String jsonString = getSchemaForFile("json/12.json");
        parseJson(jsonString);
    //    processJson(jsonString);
    }

/*    private void parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonStructSchema jsonSchema = mapper.readValue(jsonString, JsonStructSchema.class);
        System.out.println("JsonStructSchema Node: " + jsonSchema.toString());
    }*/

    private void parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        EventJsonSchema eventJsonSchema = mapper.readValue(jsonString, EventJsonSchema.class);
        System.out.println("JsonStructSchema Node: " + eventJsonSchema.toString());
    }

    void processJson(String jsonstring) throws IOException {
        ObjectMapper MAPPER = new ObjectMapper();
        final ObjectSchema schema;
        try {
            schema = MAPPER.readerFor(ObjectSchema.class).readValue(jsonstring);
            System.out.println("schema out: " + schema.toString());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private String getSchemaForFile(String fileOne) throws IOException {
        URL schemaURL = ClassLoader.getSystemResource(fileOne);
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[12288];
        int bytesRead = inputstream.read(data);
        ObjectNode schema = null;
        StringBuffer sb = new StringBuffer();

        while (bytesRead != -1) {
            String s = new String(data);
            System.out.println("input row = " + s);
            sb.append(s);
            bytesRead = inputstream.read(data);
        }
        inputstream.close();
        return sb.toString();
    }


}