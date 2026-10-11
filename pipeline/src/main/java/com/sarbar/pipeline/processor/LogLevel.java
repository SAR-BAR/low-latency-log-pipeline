package com.sarbar.pipeline.processor;

/** Level codes, identical to the ClickHouse {@code level} Enum8 values. */
public final class LogLevel {

    public static final int TRACE = 0;
    public static final int DEBUG = 1;
    public static final int INFO = 2;
    public static final int WARN = 3;
    public static final int ERROR = 4;
    public static final int FATAL = 5;

    private LogLevel() {
    }

    /**
     * Level code of the ASCII word in {@code b[from, to)}, or -1 if it is not a known level.
     * Compares bytes directly, so no String is created.
     */
    public static int parse(byte[] b, int from, int to) {
        int length = to - from;
        if (length == 4) {
            if (matches(b, from, "INFO")) return INFO;
            if (matches(b, from, "WARN")) return WARN;
        } else if (length == 5) {
            if (matches(b, from, "ERROR")) return ERROR;
            if (matches(b, from, "DEBUG")) return DEBUG;
            if (matches(b, from, "TRACE")) return TRACE;
            if (matches(b, from, "FATAL")) return FATAL;
        }
        return -1;
    }

    private static boolean matches(byte[] b, int from, String word) {
        for (int i = 0; i < word.length(); i++) {
            if (b[from + i] != word.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
