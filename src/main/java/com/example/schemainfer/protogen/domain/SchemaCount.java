package com.example.schemainfer.protogen.domain;

import java.io.Serializable;

public class SchemaCount implements Serializable {
    String schema;
    Long count;

    public SchemaCount() {
    }

    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Long getCount() {
        return this.count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
