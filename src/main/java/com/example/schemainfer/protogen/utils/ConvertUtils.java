package com.example.schemainfer.protogen.utils;

public class ConvertUtils {

    /**
     * Convert Bytes into String using UTF8 characterset
     * @param b
     * @param off
     * @param len
     * @return
     */
    public static String bytesToString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, Constants.UTF8_CHARSET);
    }
}
