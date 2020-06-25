package com.example.schemainfer.protogen.domain;

public class SchemaColumnMap {
    private String schema ;
    private String colvalue ;

    public SchemaColumnMap(String schema, String colvalue) {
        this.schema = schema;
        this.colvalue = colvalue;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getColvalue() {
        return colvalue;
    }

    public void setColvalue(String colvalue) {
        this.colvalue = colvalue;
    }
}
