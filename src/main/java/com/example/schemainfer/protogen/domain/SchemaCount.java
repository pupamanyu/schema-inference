package com.example.schemainfer.protogen.domain;

import java.io.Serializable;

public class SchemaCount implements Serializable {
    String schema;
    Long count;
    Float percent;

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

    public Float getPercent() {
        return percent;
    }

    public void setPercent(Float percent) {
        this.percent = percent;
    }
}
