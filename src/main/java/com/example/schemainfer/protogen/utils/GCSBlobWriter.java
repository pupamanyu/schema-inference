package com.example.schemainfer.protogen.utils;


//import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;

public class GCSBlobWriter {

    private static final Logger LOG = LoggerFactory.getLogger(GCSBlobWriter.class);

    WritableByteChannel writerChannel = null;

    //Blob blob = null;
   // Storage storage = null;

    public GCSBlobWriter(String relativePathtOGcsObject) {
/*        SchemaInferConfig schemaInferConfig = SchemaInferConfig.getInstance() ;
        String gcsBucketName = schemaInferConfig.getOutputBucketName() ;
        try {
           this.storage = StorageOptions.getDefaultInstance().getService();
            if (this.storage == null) {
                LOG.error("Could not get storage in GCSBlobWriter");
                return;
            }
            this.blob = this.storage.get(BlobId.of(gcsBucketName, relativePathtOGcsObject));
            if (blob == null || !blob.exists()) {
                blob = storage.create(
                                BlobInfo.newBuilder(gcsBucketName, relativePathtOGcsObject)
                                        .setContentType("application/text")
                                        .setAcl(new ArrayList<>(Arrays.asList(Acl.of(Acl.User.ofAllAuthenticatedUsers(), Acl.Role.READER))))
                                        .build());
            }

            this.writerChannel = blob.writer();
        } catch (Throwable e) {
            LOG.error("Error opening channel to GCS in GCSBlobWriter: " + e.getMessage());
            e.printStackTrace();
        }*/
    }


    public void writeToGCS(String newLine) {
        write(newLine);
    }

    public void write(String relativePathtOGcsObject) {
/*        if (writerChannel == null) {
            LOG.error("writerChannel is null in GCSBlobWriter");
            return;
        }
        try {
            final byte[] ssBytes = relativePathtOGcsObject.getBytes(Constants.UTF8_ENCODING);
            writerChannel.write(ByteBuffer.wrap(ssBytes));
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    public WritableByteChannel getWriterChannel() {
        return writerChannel;
    }

}
