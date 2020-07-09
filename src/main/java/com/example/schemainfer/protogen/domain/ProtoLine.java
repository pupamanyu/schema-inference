package com.example.schemainfer.protogen.domain;

public class ProtoLine {
    Integer line_number ;
    private String job_id ;
    private String file_name ;
    private String line_type ;
    private String col0 ;
    private String col1 ;
    private String col2 ;
    private String col3 ;
    private String col4 ;
    private String concat_columns ;

    public ProtoLine(Integer lineNumber, String jobId, String fileName, String lineType, Object col0, Object col1, Object col2, Object col3, Object col4) {
        this.line_number = lineNumber ;
        this.job_id = jobId ;
        this.file_name = fileName ;
        this.line_type = lineType ;
        this.col0 = (String) col0 ;
        this.col1 = (String) col1 ;
        this.col2 = (String) col2 ;
        this.col3 = (String) col3 ;
        this.col4 = (String) col4 ;
        this.concatenateColmns();
    }

    private void concatenateColmns() {
        StringBuffer sb = new StringBuffer() ;
        if (col0 != null) {
            sb.append(col0).append("\t") ;
        }
        if (col1 != null) {
            sb.append(col1).append("\t")  ;
        }
        if (col2 != null) {
            sb.append(col2).append("\t")  ;
        }
        if (col3 != null) {
            if (line_type.equalsIgnoreCase("C")) {
                sb.append("\t=\t") ;
            }
            sb.append(col3).append("\t")  ;
        }
        if (col4 != null) {
            sb.append(col4).append("\t") ;
        }
        this.concat_columns = sb.toString() ;
    }

    public Integer getLine_number() {
        return line_number;
    }

    public void setLine_number(Integer line_number) {
        this.line_number = line_number;
    }

    public String getJob_id() {
        return job_id;
    }

    public void setJob_id(String job_id) {
        this.job_id = job_id;
    }

    public String getLine_type() {
        return line_type;
    }

    public void setLine_type(String line_type) {
        this.line_type = line_type;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
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

    public String getConcat_columns() {
        return concat_columns;
    }

    public void setConcat_columns(String concat_columns) {
        this.concat_columns = concat_columns;
    }
}
