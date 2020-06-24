package com.example.schemainfer.protogen.utils;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Utility for strings.
 */

public class StringUtils {
    /**
     * Returns an arraylist of strings.
     *
     * @param str the comma seperated string values
     * @return the arraylist of the comma seperated string values
     */
    public static String[] getStrings(String str, String separator) {
        Collection<String> values = getStringCollection(str, separator);
        if (values.isEmpty()) {
            return null;
        }
        return values.toArray(new String[values.size()]);
    }

    /**
     * Returns a collection of strings.
     *
     * @param str comma seperated string values
     * @return an <code>ArrayList</code> of string values
     */
    public static Collection<String> getStringCollection(String str, String separator) {
        List<String> values = new ArrayList<>();
        if (str == null) {
            return values;
        }
        StringTokenizer tokenizer = new StringTokenizer(str, separator);
        values = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            values.add(tokenizer.nextToken());
        }
        return values;
    }

    /**
     * Splits a comma separated value <code>String</code>, trimming leading and
     * trailing whitespace on each value.
     *
     * @param str a comma separated <String> with values
     * @return a <code>Collection</code> of <code>String</code> values
     */
    public static Collection<String> getTrimmedStringCollection(String str) {
        return new ArrayList<>(Arrays.asList(getTrimmedStrings(str)));
    }

    /**
     * Splits a comma separated value <code>String</code>, trimming leading and
     * trailing whitespace on each value.
     *
     * @param str a comma separated <String> with values
     * @return an array of <code>String</code> values
     */
    public static String[] getTrimmedStrings(String str) {
        if (null == str || "".equals(str.trim())) {
            return EMPTY_STRING_ARRAY;
        }

        return str.trim().split("\\s*,\\s*");
    }

    public static final String[] EMPTY_STRING_ARRAY = {};

    /**
     * Given an array of strings, return a comma-separated list of its elements.
     *
     * @param strs Array of strings
     * @return Empty string if strs.length is 0, comma separated list of strings
     *         otherwise
     */

    public static String arrayToString(String[] strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append(",");
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }

    public static BiFunction<String, String, Boolean> CheckIfStringEqualsFn = (s1, s2) -> {
        if (s1.equalsIgnoreCase(s2)) {
            return true ;
        }
        return false;
    };

    public static BiFunction<String, String, Boolean> CheckIfStringExistFn = (s1, s2) -> {
        if (s1.equalsIgnoreCase(s2)) {
            return true ;
        }
        String s2Upper = s2.toUpperCase();
        String s1Upper = s1.toUpperCase();
        if (s2Upper.indexOf(s1Upper) >= 0) {
            return true;
        }

        if (s1Upper.indexOf(s2Upper) >= 0) {
            return true;
        }
        return false;
    };

    public static BiFunction<String, String, Boolean> CheckIfStringEndsWithFn = (s1, s2) -> {
        String s2Upper = s2.toUpperCase();
        String s1Upper = s1.toUpperCase();
        if (s2Upper.endsWith(s1Upper)) {
            return true;
        }

        return false;
    };

    public static boolean checkIfStringEndsWith(String s1, String[] stringArray) {
        List<String> listString = Arrays.asList(stringArray);

        return checkIfStringExist(s1, listString, CheckIfStringEndsWithFn);
    }

    public static boolean checkIfStringExist(String s1, String[] stringArray) {
        List<String> listString = Arrays.asList(stringArray);

        return checkIfStringExist(s1, listString, CheckIfStringExistFn);
    }

    private static boolean checkIfStringExist(String s1, List<String> listString, BiFunction<String, String, Boolean> checkIfStringExistFn) {
        Optional<Boolean> anyTrue = listString.parallelStream().map(s -> {
            return checkIfStringExistFn.apply(s1, s);
        }).filter(aBoolean -> aBoolean == true).findAny();

        if (anyTrue.isPresent()) {
            return anyTrue.get();
        } else {
            return false;
        }
    }

    public static boolean checkIfProtoExist(String s1, Set<String> listString, BiFunction<String, String, Boolean> checkIfStringIsContainedFn) {
        Optional<Boolean> anyTrue = listString.parallelStream().map(s -> {
            return checkIfStringIsContainedFn.apply(s1, s+"items/");
        }).filter(aBoolean -> aBoolean == true).findAny();

        if (anyTrue.isPresent()) {
            return anyTrue.get();
        } else {
            return false;
        }
    }

    public static String[] getTrimmedStringsBySemiColon(String str) {
        if (null == str || "".equals(str.trim())) {
            return EMPTY_STRING_ARRAY;
        }

        return str.trim().split("\\s*;\\s*");
    }

    public static String replaceCharInString(String str, char charToReplce, char charToReplaceBy) {
        if (null == str || "".equals(str.trim())) {
            return str;
        }

        return str.replace(charToReplce, charToReplaceBy);
    }

    public static String replaceStringInString(String str, String strStringToRepalce, String stringToReplaceBy) {
        if (null == str || "".equals(str.trim())) {
            return str;
        }

        CharSequence csreplace = strStringToRepalce;
        CharSequence csreplaceby = stringToReplaceBy;
        return str.replace(csreplace, csreplaceby);
    }

    public static boolean checkIfAllNumeric(String str) {
        return org.apache.commons.lang3.StringUtils.isNumericSpace(str) ;
    }

    public static String capitalize(String str) {
        if(str == null || str.isEmpty()) {
            return str;
        }

        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

}