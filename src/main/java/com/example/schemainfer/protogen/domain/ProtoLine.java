package com.example.schemainfer.protogen.domain;

public class ProtoLine {
    private String col0 ;
    private String col1 ;
    private String col2 ;
    private String col3 ;
    private String col4 ;

    public ProtoLine(Object col0, Object col1, Object col2, Object col3, Object col4) {
        this.col0 = col0.toString();
        this.col1 = col1.toString();
        this.col2 = col2.toString();
        this.col3 = col3.toString();
        this.col4 = col4.toString();
    }

    public String getCol0() {
        return col0;
    }

    public void setCol0(String col0) {
        this.col0 = col0;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public String getCol2() {
        return col2;
    }

    public void setCol2(String col2) {
        this.col2 = col2;
    }

    public String getCol3() {
        return col3;
    }

    public void setCol3(String col3) {
        this.col3 = col3;
    }

    public String getCol4() {
        return col4;
    }

    public void setCol4(String col4) {
        this.col4 = col4;
    }
}
