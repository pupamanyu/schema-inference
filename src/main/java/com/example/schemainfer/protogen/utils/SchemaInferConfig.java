package com.example.schemainfer.protogen.utils;

public class SchemaInferConfig {
    private static SchemaInferConfig configInstance = null;
    private String runMode ;
    private String inputFile ;
    private String outputBucketName;
    private String gcsTempBucketName ;
    private boolean skipWriteSampleDataWIthSchema;
    private  String outputBQtableName ;
    private Integer numberOfTopSchemasToMerge ;
    private String bqdatasetName ;
    public Integer numberOfPartitions ;

    public String getBqdatasetName() {
        return bqdatasetName;
    }
    public static SchemaInferConfig getConfigInstance() {
        return configInstance;
    }

    public Integer getNumberOfPartitions() {
        return numberOfPartitions;
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

    public boolean isSkipWriteSampleDataWIthSchema() {
        return skipWriteSampleDataWIthSchema;
    }

    public String getOutputBQtableName() {
        return outputBQtableName;
    }

    public Integer getNumberOfTopSchemasToMerge() {
        return numberOfTopSchemasToMerge;
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

    public void build(String runMode, String inputFile, String outputFile, String gcsTempBucketName, String outputBQtableName,
                      boolean writeSampleDataWIthSchema, Integer numberOfTopSchemasToMerge, String bsDatasetName, Integer numOfPartitions) {
        this.runMode = runMode ;
        this.inputFile = inputFile ;
        this.outputBucketName = outputFile ;
        this.gcsTempBucketName = gcsTempBucketName ;
        this.outputBQtableName = outputBQtableName ;
        this.skipWriteSampleDataWIthSchema = writeSampleDataWIthSchema ;
        this.numberOfTopSchemasToMerge = numberOfTopSchemasToMerge ;
        this.bqdatasetName = bsDatasetName ;
        this.numberOfPartitions = numOfPartitions ;
    }

}

