package com.example.schemainfer.protogen.domain;

import java.io.Serializable;

public class SchemaCount implements Serializable {
    String schema;
    Integer count;
    Float percent;

    public SchemaCount() {
    }

    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Integer getCount() {
        return this.count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Float getPercent() {
        return percent;
    }

    public void setPercent(Float percent) {
        this.percent = percent;
    }
}
