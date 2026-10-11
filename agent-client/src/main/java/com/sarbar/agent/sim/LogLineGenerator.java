package com.sarbar.agent.sim;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.SplittableRandom;

/**
 * Builds realistic log lines straight into a caller's byte array.
 *
 * <p>Line format (ASCII, timestamps in UTC):
 * <pre>
 * 2026-10-11 14:03:27.481 [worker-2] ERROR payment-service - Timeout connecting to 10.0.3.17 after 2500ms
 * </pre>
 * Fields: timestamp, thread, level, service, " - ", message. Messages come from a fixed set of
 * templates with changing values (ids, IPs, durations), which is what the pattern finder learns.
 *
 * <p>Nothing is allocated per line except the date text, which is rebuilt once per second.
 * Not thread-safe: use one instance per thread.
 */
public final class LogLineGenerator {

    /** Upper bound for one generated line; the longest template stays well below it. */
    public static final int MAX_LINE_BYTES = 512;

    private static final DateTimeFormatter SECOND_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private static final String[] SQL_ERRORS = {"Connection refused", "Timeout expired", "Invalid username or password"};

    private final String[] services;
    private final SplittableRandom random;

    private long cachedSecond = Long.MIN_VALUE;
    private String cachedSecondText;

    private byte[] buf;
    private int pos;

    /**
     * @param services service names to choose from
     * @param seed     same seed, same sequence of lines (useful for repeatable checks)
     */
    public LogLineGenerator(String[] services, long seed) {
        this.services = services.clone();
        this.random = new SplittableRandom(seed);
    }

    public int serviceCount() {
        return services.length;
    }

    /** Picks a random service index. */
    public int randomService() {
        return random.nextInt(services.length);
    }

    /** Picks a level: {@code errorPercent}% ERROR, 15% WARN, 10% DEBUG, the rest INFO. */
    public Level randomLevel(int errorPercent) {
        int r = random.nextInt(100);
        if (r < errorPercent) return Level.ERROR;
        if (r < errorPercent + 15) return Level.WARN;
        if (r < errorPercent + 25) return Level.DEBUG;
        return Level.INFO;
    }

    /**
     * Writes one line into {@code out} starting at index 0.
     *
     * @return the number of bytes written
     */
    public int generate(byte[] out, long epochMillis, int serviceIndex, Level level) {
        buf = out;
        pos = 0;

        putTimestamp(epochMillis);
        put(" [worker-");
        putLong(random.nextInt(8));
        put("] ");
        put(level.name());
        put(" ");
        put(services[serviceIndex]);
        put(" - ");
        putMessage(level);

        buf = null;
        return pos;
    }

    private void putMessage(Level level) {
        switch (level) {
            case DEBUG -> {
                put("Fetched ");
                putLong(random.nextInt(500));
                put(" rows from table orders in ");
                putLong(random.nextInt(1, 40));
                put("ms");
            }
            case INFO -> {
                switch (random.nextInt(3)) {
                    case 0 -> {
                        put("User logged in successfully [userId=");
                        putLong(random.nextInt(100, 1000));
                        put(", ipAddress=192.168.");
                        putLong(random.nextInt(1, 256));
                        put(".");
                        putLong(random.nextInt(1, 256));
                        put("]");
                    }
                    case 1 -> {
                        put("Order ");
                        putLong(random.nextInt(100_000, 1_000_000));
                        put(" created for customer ");
                        putLong(random.nextInt(1_000, 10_000));
                        put(" in ");
                        putLong(random.nextInt(2, 200));
                        put("ms");
                    }
                    default -> {
                        put("Cache refreshed for region eu-");
                        putLong(random.nextInt(1, 4));
                        put(", ");
                        putLong(random.nextInt(1_000, 50_000));
                        put(" entries");
                    }
                }
            }
            case WARN -> {
                switch (random.nextInt(3)) {
                    case 0 -> {
                        put("Request timeout for API call [apiEndpoint=/api/data/");
                        putLong(random.nextInt(10));
                        put(", requestId=");
                        putLong(random.nextInt(100, 1000));
                        put("]");
                    }
                    case 1 -> {
                        put("Slow query took ");
                        putLong(random.nextInt(500, 5_000));
                        put("ms on shard ");
                        putLong(random.nextInt(16));
                    }
                    default -> {
                        put("Retrying payment ");
                        putLong(random.nextInt(100_000, 1_000_000));
                        put(", attempt ");
                        putLong(random.nextInt(1, 4));
                        put(" of 3");
                    }
                }
            }
            case ERROR -> {
                switch (random.nextInt(3)) {
                    case 0 -> {
                        put("Database connection failed [exception=java.sql.SQLException: ");
                        put(SQL_ERRORS[random.nextInt(SQL_ERRORS.length)]);
                        put("]");
                    }
                    case 1 -> {
                        put("Payment ");
                        putLong(random.nextInt(100_000, 1_000_000));
                        put(" declined by gateway: insufficient funds");
                    }
                    default -> {
                        put("Timeout connecting to 10.0.");
                        putLong(random.nextInt(256));
                        put(".");
                        putLong(random.nextInt(256));
                        put(" after ");
                        putLong(random.nextInt(1_000, 5_000));
                        put("ms");
                    }
                }
            }
        }
    }

    /** "yyyy-MM-dd HH:mm:ss.SSS"; the part up to seconds is formatted once per second and reused. */
    private void putTimestamp(long epochMillis) {
        long second = Math.floorDiv(epochMillis, 1000);
        if (second != cachedSecond) {
            cachedSecond = second;
            cachedSecondText = SECOND_FORMAT.format(Instant.ofEpochSecond(second));
        }
        put(cachedSecondText);
        put(".");
        int millis = (int) Math.floorMod(epochMillis, 1000);
        buf[pos++] = (byte) ('0' + millis / 100);
        buf[pos++] = (byte) ('0' + millis / 10 % 10);
        buf[pos++] = (byte) ('0' + millis % 10);
    }

    /** Copies an ASCII string. */
    private void put(String s) {
        for (int i = 0; i < s.length(); i++) {
            buf[pos++] = (byte) s.charAt(i);
        }
    }

    /** Writes a non-negative number in decimal without creating a String. */
    private void putLong(long value) {
        if (value == 0) {
            buf[pos++] = '0';
            return;
        }
        int start = pos;
        while (value > 0) {
            buf[pos++] = (byte) ('0' + value % 10);
            value /= 10;
        }
        // Digits were written lowest first; reverse them in place.
        for (int i = start, j = pos - 1; i < j; i++, j--) {
            byte t = buf[i];
            buf[i] = buf[j];
            buf[j] = t;
        }
    }
}
