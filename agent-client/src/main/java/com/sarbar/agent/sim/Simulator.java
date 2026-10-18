package com.sarbar.agent.sim;

import com.sarbar.agent.shm.Mailbox;
import com.sarbar.agent.shm.ShmLogWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public final class Simulator {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};

    /** Below this much remaining wait, spin instead of sleeping (sleep wake-up is too imprecise). */
    private static final long SPIN_THRESHOLD_NANOS = 100_000;

    private static final int MAILBOX_BOXES = 4096;
    private static final int MAILBOX_BOX_SIZE = 1024;

    public static void main(String[] args) throws IOException {
        Map<String, String> opts = parse(args);
        int source = Integer.parseInt(opts.getOrDefault("source", "1"));
        double rate = Double.parseDouble(opts.getOrDefault("rate", "10000"));
        long durationSeconds = Long.parseLong(opts.getOrDefault("duration", "60"));
        Scenario scenario = Scenario.valueOf(opts.getOrDefault("scenario", "steady").toUpperCase());
        String out = opts.getOrDefault("out", "-");
        long seed = Long.parseLong(opts.getOrDefault("seed", Long.toString(source)));

        LogSink sink = sink(out, source);
        System.err.printf("simulator source=%d rate=%.0f/s duration=%ds scenario=%s out=%s%n", source, rate, durationSeconds, scenario, out);

        long sent = run(new LogLineGenerator(SERVICES, seed), sink, scenario, rate, durationSeconds * 1_000_000_000L);
        sink.close();
        System.err.printf("simulator source=%d sent=%,d%n", source, sent);
    }

    private static LogSink sink(String out, int source) throws IOException {
        if (out.equals("-")) return new StreamLogSink(System.out);
        if (out.startsWith("shm:")) return new ShmLogWriter(Mailbox.file(Path.of(out.substring(4)), source), MAILBOX_BOXES, MAILBOX_BOX_SIZE);
        return new StreamLogSink(new FileOutputStream(out, true));
    }

    /** @return how many lines were sent */
    static long run(LogLineGenerator generator, LogSink sink, Scenario scenario, double rate, long durationNanos) throws IOException {
        byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];
        long start = System.nanoTime();
        long scheduled = start;
        long sequence = 0;

        while (scheduled - start < durationNanos) {
            waitUntil(scheduled);

            long elapsedMillis = (scheduled - start) / 1_000_000;
            int service = generator.randomService();
            Level level = generator.randomLevel(scenario.errorPercent(elapsedMillis, service));
            int length = generator.generate(line, System.currentTimeMillis(), service, level);
            sink.write(line, length, sequence);
            sequence++;

            double effectiveRate = rate * scenario.rateMultiplier(elapsedMillis);
            scheduled += Math.max(1L, (long) (1_000_000_000.0 / effectiveRate));
        }
        return sequence;
    }

    private static void waitUntil(long deadlineNanos) {
        long remaining;
        while ((remaining = deadlineNanos - System.nanoTime()) > 0) {
            if (remaining > SPIN_THRESHOLD_NANOS) {
                LockSupport.parkNanos(remaining - SPIN_THRESHOLD_NANOS);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("expected key=value, got: " + arg);
            }
            opts.put(arg.substring(0, eq), arg.substring(eq + 1));
        }
        return opts;
    }
}
