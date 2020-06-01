package com.example.schemainfer.protogen.javaudf;


import com.example.schemainfer.protogen.rules.InferDatatype;
import static org.junit.jupiter.api.Assertions.*;

import com.example.schemainfer.protogen.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.jupiter.api.Test ;

class DataTypesTest {

    @Test
    void testNumericValue() throws IOException {
        assertFalse(StringUtils.isNumeric("13.64")) ;
        assertTrue(StringUtils.isNumeric("13")) ;
    }

    @Test
    void testIfFloatValue() throws IOException {
        String input1 = "sss121";
        try {
            // checking valid float using parseInt() method
            Float.parseFloat(input1);
            System.out.println(input1 + " is a valid float number");
        } catch (NumberFormatException e) {
            System.out.println(input1 + " is not a valid float number");
        }
    }

    @Test
    void testPrecision() throws IOException {
        assertTrue(InferDatatype.precisionGreatherThan3("20.44567567"));
        assertTrue(InferDatatype.precisionGreatherThan3("1923232.4456"));
        assertFalse(InferDatatype.precisionGreatherThan3("202497397395454.444"));
        assertFalse(InferDatatype.precisionGreatherThan3("20.1"));
        assertFalse(InferDatatype.precisionGreatherThan3("20"));

        assertTrue(CommonUtils.isDouble("20.44567567"));
        assertTrue(CommonUtils.isDouble("1923232.4456"));
        assertTrue(CommonUtils.isFloat("2029754.444"));
        assertTrue(CommonUtils.isFloat("20.1"));
        assertTrue(CommonUtils.isFloat("20"));

      //  assertFalse(CommonUtils.isFloat("204343434344934839.44567567343434343"));
       // assertFalse(CommonUtils.isFloat("1923232.4456"));
    }

    @Test
    void testDatatype() throws IOException {
        String s = "9283493.23353453" ;
        String ss = InferDatatype.determineInferDatatype(s);
        System.out.println("Got datatype for: " + s + " datatype: " + ss) ;
    }

    @Test
    void testBoolean() throws IOException {
        String s = "false" ;
         Boolean b = CommonUtils.isBoolean(s) ;
        System.out.println("isBoolean: " + b) ;
    }

}