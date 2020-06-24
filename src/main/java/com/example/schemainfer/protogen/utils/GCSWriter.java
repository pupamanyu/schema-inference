package com.example.schemainfer.protogen.utils;

//import com.google.appengine.tools.cloudstorage.*;
////import com.google.cloud.storage.Blob;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;

public class GCSWriter {

/*    GcsService gcsService = null ;
    GcsFilename file = null ;
    GcsFileOptions.Builder builder = null ;
    GcsFileOptions options = null ;
    GcsOutputChannel channel = null;*/
    PrintWriter writer = null;


    public GCSWriter(String relativePathtOGcsObject) {
        System.out.println("Inside GCSWriter") ;
      //////  gcsService = GcsServiceFactory.createGcsService();
        System.out.println("Created gcsService") ;

       // builder = new GcsFileOptions.Builder();
        //options = builder.mimeType("hello").build();
       // options = GcsFileOptions.getDefaultInstance();
       /// try {
           //// file = new GcsFilename("schema-inference-outttt", "asdas");
           //// System.out.println("GCS Bucket: " + file.toString()) ;
           //// System.out.println("GCS Bucket: " + file.getObjectName() + " Object: " + file.toString()) ;
           //// ListResult protos = gcsService.list("protos", ListOptions.DEFAULT);
           //// System.out.println("GCS FileList: " + protos.toString()) ;
            System.out.println("GCS relative path: " + relativePathtOGcsObject) ;

           // channel = gcsService.createOrReplace(file, options);
           // writer = new PrintWriter(Channels.newWriter(channel, "UTF8"));
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
    }

    public void writeToGCS(String fileContent)  {
        writer.print(fileContent);
    }

    public void close() {
/*        try {
            this.writer.close();
         //   this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
