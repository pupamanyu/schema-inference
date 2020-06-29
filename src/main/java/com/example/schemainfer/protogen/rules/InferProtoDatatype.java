package com.example.schemainfer.protogen.rules;

import com.example.schemainfer.protogen.utils.CommonUtils;
import com.example.schemainfer.protogen.utils.Constants;
import org.apache.commons.lang3.StringUtils;

/**
 * Infer Proto datatypes form json datatypes
 * Match the datatypes here
 *
 */
public class InferProtoDatatype {

    public static String matchProtoDatatype(String datatype) {
        // TODO: FInish this implementation of mapping Json schema datatpes to Proto schema datatypes
        String returnProtoType = datatype ;

        switch(datatype)
        {
            case "integer":
                returnProtoType = "int32" ;
                break;
            case "number":
                returnProtoType = "float" ;
                break;
            default:

        }

        return returnProtoType ;
    }

}
