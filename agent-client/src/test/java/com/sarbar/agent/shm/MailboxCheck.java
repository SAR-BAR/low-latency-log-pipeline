package com.sarbar.agent.shm;

import com.sarbar.agent.sim.LogLineGenerator;

import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Check for the mailbox: same JVM, two separate JVMs, corrupt boxes, and allocation. Exit code 0 = PASS. */
public final class MailboxCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final long START = 1_790_000_000_000L;
    private static final int BOX_SIZE = 1024;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("write")) {
            writeAll(Path.of(args[1]), Long.parseLong(args[2]), Integer.parseInt(args[3]));
            return;
        }
        Path dir = Files.createTempDirectory("mailbox-check");
        boolean sameJvm = checkSameJvm(dir.resolve("same.shm"));
        boolean twoJvms = checkTwoJvms(dir.resolve("two.shm"));
        boolean corrupt = checkCorruptBox(dir.resolve("corrupt.shm"));
        boolean allocation = checkAllocation(dir.resolve("alloc.shm"));
        try (var files = Files.list(dir)) {
            for (Path p : files.toList()) Files.delete(p);
        }
        Files.delete(dir);
        boolean pass = sameJvm && twoJvms && corrupt && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    /** Writer side, also run as the child process in the two-JVM check. */
    static void writeAll(Path file, long n, int boxCount) throws Exception {
        try (ShmLogWriter writer = new ShmLogWriter(file, boxCount, BOX_SIZE)) {
            LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
            byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];
            for (long seq = 0; seq < n; seq++) {
                int length = generator.generate(line, START + seq, generator.randomService(), generator.randomLevel(5));
                writer.write(line, length, seq);
            }
        }
    }

    /** Regenerates the writer's lines (same seed, same calls) and compares every log. */
    static final class Verifier implements MailboxReader.Handler {
        private final LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        private final byte[] expected = new byte[LogLineGenerator.MAX_LINE_BYTES];
        long next;
        long failures;
        String firstFailure;

        @Override
        public void onLog(byte[] bytes, int length, long sequence) {
            int expectedLength = generator.generate(expected, START + next, generator.randomService(), generator.randomLevel(5));
            if (sequence != next) fail("sequence " + sequence + ", expected " + next);
            else if (!Arrays.equals(bytes, 0, length, expected, 0, expectedLength)) fail("bytes differ at sequence " + sequence);
            next = sequence + 1;
        }

        private void fail(String m) {
            if (failures++ == 0) firstFailure = m;
        }
    }

    private static boolean checkSameJvm(Path file) throws Exception {
        long n = 2_000_000;
        Thread writer = Thread.ofPlatform().start(() -> {
            try {
                writeAll(file, n, 8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Verifier verifier = readAll(file, n, writer::isAlive);
        writer.join();
        return report("same JVM, 8 boxes", verifier, n);
    }

    private static boolean checkTwoJvms(Path file) throws Exception {
        long n = 2_000_000;
        String java = ProcessHandle.current().info().command().orElse("java");
        Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), MailboxCheck.class.getName(), "write", file.toString(), Long.toString(n), "64").inheritIO().start();
        Verifier verifier = readAll(file, n, child::isAlive);
        boolean exited = child.waitFor(30, TimeUnit.SECONDS);
        boolean ok = report("two JVMs, 64 boxes", verifier, n);
        if (!exited || child.exitValue() != 0) {
            System.out.println("  FAIL writer process did not exit cleanly");
            return false;
        }
        return ok;
    }

    /** Opens the mailbox once the writer has created it, then reads until {@code n} logs arrived or the writer died. */
    private static Verifier readAll(Path file, long n, java.util.function.BooleanSupplier writerAlive) throws Exception {
        Verifier verifier = new Verifier();
        try (MailboxReader reader = openWhenReady(file)) {
            while (verifier.next < n) {
                if (reader.poll(verifier, 256) == 0) {
                    if (!writerAlive.getAsBoolean() && reader.poll(verifier, 256) == 0) break;
                    Thread.onSpinWait();
                }
            }
        }
        return verifier;
    }

    private static MailboxReader openWhenReady(Path file) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            try {
                return new MailboxReader(file);
            } catch (Exception notReadyYet) {
                if (System.nanoTime() > deadline) throw notReadyYet;
                Thread.sleep(5);
            }
        }
    }

    private static boolean checkCorruptBox(Path file) throws Exception {
        byte[] line = "2026-10-18 09:00:00.000 [w] INFO svc - hello".getBytes();
        try (ShmLogWriter writer = new ShmLogWriter(file, 8, BOX_SIZE); Mailbox raw = Mailbox.open(file); MailboxReader reader = new MailboxReader(file)) {
            for (int seq = 0; seq < 3; seq++) writer.write(line, line.length, seq);
            raw.segment().set(ValueLayout.JAVA_INT, raw.boxOffset(1), 999_999);
            long[] seen = new long[3];
            int[] count = {0};
            int consumed = reader.poll((b, len, seq) -> seen[count[0]++] = seq, 10);
            boolean ok = consumed == 3 && count[0] == 2 && seen[0] == 0 && seen[1] == 2 && reader.corruptBoxes() == 1;
            System.out.printf("corrupt box: consumed=%d delivered=%d corrupt=%d -> %s%n", consumed, count[0], reader.corruptBoxes(), ok ? "ok" : "FAIL");
            return ok;
        }
    }

    private static boolean checkAllocation(Path file) throws Exception {
        byte[] line = "2026-10-18 09:00:00.000 [worker-1] ERROR payment-service - Timeout connecting to 10.0.3.17 after 2500ms".getBytes();
        long[] sum = {0};
        MailboxReader.Handler handler = (b, len, seq) -> sum[0] += len;
        try (ShmLogWriter writer = new ShmLogWriter(file, 1024, BOX_SIZE); MailboxReader reader = new MailboxReader(file)) {
            for (int i = 0; i < 2_000_000; i++) { // warm-up
                writer.write(line, line.length, i);
                reader.poll(handler, 1);
            }
            com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            int n = 1_000_000;
            long before = threads.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < n; i++) {
                writer.write(line, line.length, i);
                reader.poll(handler, 1);
            }
            long allocated = threads.getCurrentThreadAllocatedBytes() - before;
            double perLog = (double) allocated / n;
            System.out.printf("allocation: %,d bytes over %,d write+read = %.4f bytes/log (sum %d)%n", allocated, n, perLog, sum[0]);
            return perLog < 1.0;
        }
    }

    private static boolean report(String name, Verifier v, long n) {
        boolean ok = v.failures == 0 && v.next == n;
        System.out.printf("%s: received %,d of %,d, failures %,d%s -> %s%n", name, v.next, n, v.failures, v.firstFailure == null ? "" : " (first: " + v.firstFailure + ")", ok ? "ok" : "FAIL");
        return ok;
    }
}
