package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.functions.ProcessStringColumn;
import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import com.example.schemainfer.protogen.utils.Constants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

class SchemaCompare2Test {

    @org.junit.jupiter.api.Test
    void processFValue() throws IOException {
        ObjectNode schema1 = getSchemaForFile("fvalues5.txt");
        ObjectNode schema2 = getSchemaForFile("fvalues7.txt");

        assertEquals(schema1, schema2) ;
        return;
    }

    private ObjectNode getSchemaForFile(String fileOne) throws IOException {
        URL schemaURL = ClassLoader.getSystemResource(fileOne);
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[2048];
        int bytesRead = inputstream.read(data);
        ObjectNode schema = null;

        while (bytesRead != -1) {
            String s = new String(data);
            System.out.println("input row = " + s) ;
            schema = parseRow(s) ;

            bytesRead = inputstream.read(data);
        }
        inputstream.close();
        return schema ;
    }

    private static ObjectNode parseRow(String s) {
        List<Protomap> protomapList = new ArrayList<>();
        String[] ss = s.split(Constants.SEQUENCE_FIELD_DELIMITER, -1);

        if (ss.length != 3) {
            System.err.println("Number of columns != 3. Skipping row") ;
            return null ;
        }
        String fvalue = ss[2];
        if (fvalue == null || fvalue.isEmpty()) {
            System.err.println("Col #3 is empty. Skipping") ;
            return null;
        }
        return ProcessStringColumnAsObjectNode.splitFColumnIntoMap(fvalue, protomapList);
    }

}