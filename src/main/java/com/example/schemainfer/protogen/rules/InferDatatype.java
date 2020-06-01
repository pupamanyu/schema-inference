package com.example.schemainfer.protogen.rules;

import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants ;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;

public class InferDatatype {

    public static String determineInferDatatype(String instr) {
        if (instr == null || instr.isEmpty()) {
            return null ;
        }
        if (CommonUtils.isBoolean(instr)) {
            return Constants.DATATYPES.Boolean.name() ;
        } else if (CommonUtils.isAlpha(instr)) {
            return Constants.DATATYPES.String.name() ;
        } else if (StringUtils.isNumeric(instr)) {
            return Constants.DATATYPES.Integer.name() ;
        } else if (CommonUtils.isDouble(instr)) {
            return Constants.DATATYPES.Double.name() ;
        }  else if (CommonUtils.isFloat(instr)) {
            return Constants.DATATYPES.Float.name() ;
        } else {
            return Constants.DATATYPES.Boolean.name() ;
        }
    }

    public static boolean precisionGreatherThan3(String s) {
        return s.replaceAll(".*\\.", "").length() > 3;
    }

}
