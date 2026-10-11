package com.sarbar.pipeline.processor;

/**
 * Reads timestamp, level and service from a raw log line, straight from bytes.
 *
 * <p>Expected format (as produced by the simulator):
 * <pre>
 * 2026-10-11 08:41:12.556 [worker-2] ERROR payment-service - Timeout connecting to 10.0.3.17 after 2500ms
 * |---- UTC timestamp ---| |thread-| |lvl-| |-- service ---|   |------------- message --------------|
 * </pre>
 *
 * <p>No objects are created: digits are read at fixed positions, the date becomes epoch millis
 * with integer arithmetic, and the service is looked up by its bytes. Not thread-safe: one
 * instance per processor thread. After a successful {@link #parse}, the getters hold its results.
 */
public final class LogParser {

    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final int TIMESTAMP_LENGTH = 23; // "yyyy-MM-dd HH:mm:ss.SSS"

    private final ServiceRegistry services;

    private long timestampMillis;
    private int level;
    private int serviceId;
    private int messageOffset;

    public LogParser(ServiceRegistry services) {
        this.services = services;
    }

    /**
     * Parses {@code b[0, length)}.
     *
     * @return false if the line does not match the format; the getters then keep their old values
     */
    public boolean parse(byte[] b, int length) {
        // 1. Timestamp, at fixed positions.
        if (length < TIMESTAMP_LENGTH + 2) return false;
        if (b[4] != '-' || b[7] != '-' || b[10] != ' ' || b[13] != ':' || b[16] != ':' || b[19] != '.') return false;
        int year = digits(b, 0, 4);
        int month = digits(b, 5, 2);
        int day = digits(b, 8, 2);
        int hour = digits(b, 11, 2);
        int minute = digits(b, 14, 2);
        int second = digits(b, 17, 2);
        int millis = digits(b, 20, 3);
        if (year < 0 || month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) return false;
        if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 || millis < 0) return false;

        // 2. " [thread] ": skipped, only checked for shape.
        if (b[23] != ' ' || b[24] != '[') return false;
        int closeBracket = indexOf(b, 25, length, (byte) ']');
        if (closeBracket < 0 || closeBracket + 1 >= length || b[closeBracket + 1] != ' ') return false;

        // 3. "LEVEL "
        int levelStart = closeBracket + 2;
        int levelEnd = indexOf(b, levelStart, length, (byte) ' ');
        if (levelEnd < 0) return false;
        int parsedLevel = LogLevel.parse(b, levelStart, levelEnd);
        if (parsedLevel < 0) return false;

        // 4. "service - "
        int serviceStart = levelEnd + 1;
        int serviceEnd = indexOf(b, serviceStart, length, (byte) ' ');
        if (serviceEnd <= serviceStart || serviceEnd + 2 >= length || b[serviceEnd + 1] != '-' || b[serviceEnd + 2] != ' ') return false;

        // All checks passed: only now store the results, so a bad line never leaves half-updated fields.
        timestampMillis = daysFromCivil(year, month, day) * MILLIS_PER_DAY + hour * 3_600_000L + minute * 60_000L + second * 1_000L + millis;
        level = parsedLevel;
        serviceId = services.idOf(b, serviceStart, serviceEnd);
        messageOffset = serviceEnd + 3;
        return true;
    }

    /** Milliseconds since 1970-01-01T00:00:00Z. */
    public long timestampMillis() {
        return timestampMillis;
    }

    /** One of the {@link LogLevel} codes. */
    public int level() {
        return level;
    }

    /** Id from the {@link ServiceRegistry}, or {@link ServiceRegistry#TOO_MANY}. */
    public int serviceId() {
        return serviceId;
    }

    /** Index of the first byte of the message text. */
    public int messageOffset() {
        return messageOffset;
    }

    /** Reads {@code count} ASCII digits as a number, or -1 if any byte is not a digit. */
    private static int digits(byte[] b, int from, int count) {
        int value = 0;
        for (int i = from; i < from + count; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) return -1;
            value = value * 10 + d;
        }
        return value;
    }

    private static int indexOf(byte[] b, int from, int to, byte target) {
        for (int i = from; i < to; i++) {
            if (b[i] == target) return i;
        }
        return -1;
    }

    private static int daysInMonth(int year, int month) {
        return switch (month) {
            case 2 -> isLeapYear(year) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            default -> 31;
        };
    }

    private static boolean isLeapYear(int year) {
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    }

    /**
     * Days since 1970-01-01 for a calendar date (Howard Hinnant's days_from_civil).
     * Counts years from March, so the leap day is the last day of the year and month lengths
     * follow a simple formula.
     */
    static long daysFromCivil(int year, int month, int day) {
        int y = month <= 2 ? year - 1 : year;
        int era = (y >= 0 ? y : y - 399) / 400;
        int yearOfEra = y - era * 400;
        int dayOfYear = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
        int dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;
        return era * 146_097L + dayOfEra - 719_468;
    }
}
