package com.example.schemainfer.protogen.utils;

public class SchemaInferConfig {
    private static SchemaInferConfig configInstance = null;
    private String runMode ;
    private String inputFile ;
    private String outputBucketName;
    private String gcsTempBucketName ;
    private boolean writeSampleDataWIthSchema ;
    private  String outputBQtableName ;

    public static SchemaInferConfig getConfigInstance() {
        return configInstance;
    }

    public String getRunMode() {
        return runMode;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getOutputBucketName() {
        return outputBucketName;
    }

    public String getGcsTempBucketName() {
        return gcsTempBucketName;
    }

    public boolean isWriteSampleDataWIthSchema() {
        return writeSampleDataWIthSchema;
    }

    public String getOutputBQtableName() {
        return outputBQtableName;
    }

    private SchemaInferConfig() {
    }

    public static SchemaInferConfig getInstance()
    {
        if (configInstance == null) {
            configInstance = new SchemaInferConfig();
        }

        return configInstance;
    }

    public void build(String runMode, String inputFile, String outputFile, String gcsTempBucketName, String outputBQtableName, boolean writeSampleDataWIthSchema) {
        this.runMode = runMode ;
        this.inputFile = inputFile ;
        this.outputBucketName = outputFile ;
        this.gcsTempBucketName = gcsTempBucketName ;
        this.outputBQtableName = outputBQtableName ;
        this.writeSampleDataWIthSchema = writeSampleDataWIthSchema ;
    }

}

