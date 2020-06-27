package com.example.schemainfer.protogen.domain;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaColumnMap {
    private ObjectNode schema ;
    private String colvalue ;

    public SchemaColumnMap(ObjectNode schema, String colvalue) {
        this.schema = schema;
        this.colvalue = colvalue;
    }

    public ObjectNode getSchema() {
        return schema;
    }

    public void setSchema(ObjectNode schema) {
        this.schema = schema;
    }

    public String getColvalue() {
        return colvalue;
    }

    public void setColvalue(String colvalue) {
        this.colvalue = colvalue;
    }
}
