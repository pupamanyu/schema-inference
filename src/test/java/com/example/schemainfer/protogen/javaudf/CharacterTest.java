package com.example.schemainfer.protogen.javaudf;


import com.example.schemainfer.protogen.rules.InferDatatype;
import com.example.schemainfer.protogen.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CharacterTest {

    @Test
    void testCharacter() throws IOException {
        assertFalse(CommonUtils.isPureAscii("                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     ")) ;
    }


}