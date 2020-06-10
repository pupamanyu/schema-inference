package com.example.schemainfer.protogen.javaudf;

import com.example.schemainfer.protogen.functions.ProcessStringColumnAsObjectNode;
import scala.xml.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

class SeqFilesScanTest {

    @org.junit.jupiter.api.Test
    void processFValue() throws IOException {
        URL schemaURL = ClassLoader.getSystemResource("fvalues.txt");
        InputStream inputstream = Source.fromFile(schemaURL.getFile()).getByteStream();
        byte[] data = new byte[2048];
        int bytesRead = inputstream.read(data);

        while (bytesRead != -1) {
            String s = new String(data);
            System.out.println("row = " + s) ;
          //  ProcessStringColumn.processRow(s) ;
            ProcessStringColumnAsObjectNode node = new ProcessStringColumnAsObjectNode() ;
            node.processRow(s) ;
            bytesRead = inputstream.read(data);
        }
        inputstream.close();
    }

}