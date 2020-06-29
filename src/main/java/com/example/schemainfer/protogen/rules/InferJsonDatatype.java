package com.example.schemainfer.protogen.rules;

import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants ;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;

public class InferJsonDatatype {

    public static String determineInferDatatype(String instr) {
        if (instr != null && !instr.isEmpty()) {
            if (CommonUtils.isBoolean(instr)) {
                return Constants.DATATYPES.Boolean.name();
            } else if (CommonUtils.isAlpha3(instr)) {
                return Constants.DATATYPES.String.name();
            } else if (StringUtils.isNumeric(instr)) {
                return Constants.DATATYPES.Integer.name();
            } else if (CommonUtils.isDouble(instr)) {
                return Constants.DATATYPES.Double.name();
            } else {
                return CommonUtils.isFloat(instr) ? Constants.DATATYPES.Float.name() : Constants.DATATYPES.Null.name();
            }
        } else {
            return null;
        }
    }

    public static String determineProtoDatatype(String datatype1, String datatype2) {
        if (datatype1 != null && !datatype1.isEmpty()) {
            if (CommonUtils.isBoolean(datatype1)) {
                return Constants.DATATYPES.Boolean.name();
            } else if (CommonUtils.isAlpha3(datatype1)) {
                return Constants.DATATYPES.String.name();
            } else if (StringUtils.isNumeric(datatype1)) {
                return Constants.DATATYPES.Integer.name();
            } else if (CommonUtils.isDouble(datatype1)) {
                return Constants.DATATYPES.Double.name();
            } else {
                return CommonUtils.isFloat(datatype1) ? Constants.DATATYPES.Float.name() : Constants.DATATYPES.Null.name();
            }
        } else {
            return null;
        }
    }

    public static boolean precisionGreatherThan3(String s) {
        return s.replaceAll(".*\\.", "").length() > 3;
    }

}
