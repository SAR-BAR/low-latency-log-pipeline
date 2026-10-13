package com.sarbar.pipeline.processor;

import com.sarbar.agent.sim.Level;
import com.sarbar.agent.sim.LogLineGenerator;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.SplittableRandom;

/**
 * Check for {@link LogParser}:
 * <ol>
 *   <li>Correctness: lines from the simulator's generator, with random timestamps from 1970 to 2099,
 *       parse back to exactly the timestamp, level, service and message start that were used.</li>
 *   <li>Rejection: malformed lines return false.</li>
 *   <li>Allocation: after warm-up, parsing allocates less than 1 byte per line.</li>
 * </ol>
 * Exit code 0 = PASS, 1 = FAIL.
 */
public final class LogParserCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final int LINES = 10_000;
    private static final long MAX_MILLIS = Instant.parse("2100-01-01T00:00:00Z").toEpochMilli();

    public static void main(String[] args) {
        boolean correct = checkCorrectness();
        boolean rejects = checkRejects();
        boolean allocation = checkAllocation();
        boolean pass = correct && rejects && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    private static boolean checkCorrectness() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        NameRegistry registry = new NameRegistry(16);
        LogParser parser = new LogParser(registry);
        SplittableRandom random = new SplittableRandom(7);
        byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];
        int failures = 0;

        for (int i = 0; i < LINES; i++) {
            long millis = random.nextLong(MAX_MILLIS);
            int service = generator.randomService();
            Level level = generator.randomLevel(25);
            int length = generator.generate(line, millis, service, level);
            String text = new String(line, 0, length, StandardCharsets.US_ASCII);

            if (!parser.parse(line, length)) {
                failures += fail("rejected a valid line: " + text);
                continue;
            }
            if (parser.timestampMillis() != millis) failures += fail("timestamp " + parser.timestampMillis() + " != " + millis + ": " + text);
            if (parser.level() != expectedLevel(level)) failures += fail("level " + parser.level() + " != " + level + ": " + text);
            if (!SERVICES[service].equals(registry.name(parser.serviceId()))) failures += fail("service " + registry.name(parser.serviceId()) + " != " + SERVICES[service] + ": " + text);
            int expectedOffset = text.indexOf(" - ") + 3;
            if (parser.messageOffset() != expectedOffset) failures += fail("message offset " + parser.messageOffset() + " != " + expectedOffset + ": " + text);
        }
        System.out.printf("correctness: %,d lines, %,d failures, %d services registered%n", LINES, failures, registry.size());
        return failures == 0 && registry.size() == SERVICES.length;
    }

    private static boolean checkRejects() {
        String[] bad = {
                "",
                "2026-10-11 08:41:12.556",
                "2026-13-11 08:41:12.556 [w] INFO svc - month 13",
                "2026-02-29 08:41:12.556 [w] INFO svc - not a leap year",
                "2026-10-11 24:00:00.000 [w] INFO svc - hour 24",
                "2026-10-11 08:41:12.5x6 [w] INFO svc - bad millis",
                "2026-10-11 08:41:12.556 [w INFO svc - no closing bracket",
                "2026-10-11 08:41:12.556 [w] NOTICE svc - unknown level",
                "2026-10-11 08:41:12.556 [w] INFO svc missing dash",
                "2026-10-11 08:41:12.556 [w] INFO svc -",
        };
        LogParser parser = new LogParser(new NameRegistry(16));
        int failures = 0;
        for (String s : bad) {
            byte[] b = s.getBytes(StandardCharsets.US_ASCII);
            if (parser.parse(b, b.length)) {
                failures += fail("accepted a bad line: \"" + s + "\"");
            }
        }
        byte[] leapDay = "2024-02-29 23:59:59.999 [w] FATAL svc - leap day".getBytes(StandardCharsets.US_ASCII);
        if (!parser.parse(leapDay, leapDay.length) || parser.timestampMillis() != Instant.parse("2024-02-29T23:59:59.999Z").toEpochMilli()) {
            failures += fail("leap day not parsed correctly");
        }
        System.out.printf("rejects: %d bad lines + 1 leap day, %d failures%n", bad.length, failures);
        return failures == 0;
    }

    private static boolean checkAllocation() {
        // Generate lines up front so the measured loop does nothing but parse.
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 1);
        byte[][] lines = new byte[LINES][LogLineGenerator.MAX_LINE_BYTES];
        int[] lengths = new int[LINES];
        long now = System.currentTimeMillis();
        for (int i = 0; i < LINES; i++) {
            lengths[i] = generator.generate(lines[i], now + i, generator.randomService(), generator.randomLevel(5));
        }
        LogParser parser = new LogParser(new NameRegistry(16));

        long checksum = 0;
        for (int round = 0; round < 200; round++) { // warm-up: give the JIT time to compile parse()
            checksum += parseAll(parser, lines, lengths);
        }

        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        int rounds = 100;
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int round = 0; round < rounds; round++) {
            checksum += parseAll(parser, lines, lengths);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        long parsed = (long) rounds * LINES;
        double perLine = (double) allocated / parsed;
        System.out.printf("allocation: %,d bytes over %,d parses = %.4f bytes/line (checksum %d)%n", allocated, parsed, perLine, checksum);
        return perLine < 1.0;
    }

    /** Parses every line once; the returned sum stops the JIT from removing the work as unused. */
    private static long parseAll(LogParser parser, byte[][] lines, int[] lengths) {
        long sum = 0;
        for (int i = 0; i < lines.length; i++) {
            if (parser.parse(lines[i], lengths[i])) {
                sum += parser.timestampMillis() + parser.level() + parser.serviceId() + parser.messageOffset();
            }
        }
        return sum;
    }

    private static int expectedLevel(Level level) {
        return switch (level) {
            case DEBUG -> LogLevel.DEBUG;
            case INFO -> LogLevel.INFO;
            case WARN -> LogLevel.WARN;
            case ERROR -> LogLevel.ERROR;
        };
    }

    private static int fail(String message) {
        System.out.println("  FAIL " + message);
        return 1;
    }
}
