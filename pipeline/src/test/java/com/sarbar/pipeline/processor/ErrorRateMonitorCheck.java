package com.sarbar.pipeline.processor;

import com.sarbar.agent.sim.Level;
import com.sarbar.agent.sim.LogLineGenerator;
import com.sarbar.agent.sim.Scenario;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.TreeSet;

/** Check for {@link ErrorRateMonitor}: ERROR_SPIKE run in simulated time, 60 s counts, and allocation. */
public final class ErrorRateMonitorCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final int RATE = 2_000;
    private static final int SECONDS = 90;
    private static final double SPIKE_FACTOR = 3.0;
    private static final int MIN_ERRORS = 10;

    public static void main(String[] args) {
        boolean spikes = checkErrorSpikeScenario();
        boolean allocation = checkAllocation();
        boolean pass = spikes && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    private static boolean checkErrorSpikeScenario() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        NameRegistry services = new NameRegistry(16);
        LogParser parser = new LogParser(services);
        ErrorRateMonitor monitor = new ErrorRateMonitor(16, SPIKE_FACTOR, MIN_ERRORS);
        byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];

        long start = 1_790_000_000_000L; // a whole second, so simulated second n starts at elapsed n * 1000 ms
        long[] lastSecond = new long[16];
        int[][] errorsPerSecond = new int[16][SECONDS];
        @SuppressWarnings("unchecked") Set<Long>[] spikeSeconds = new Set[16];
        for (int i = 0; i < 16; i++) spikeSeconds[i] = new TreeSet<>();

        long total = (long) RATE * SECONDS;
        for (long i = 0; i < total; i++) {
            long elapsed = i * 1000 / RATE;
            int service = generator.randomService();
            Level level = generator.randomLevel(Scenario.ERROR_SPIKE.errorPercent(elapsed, service));
            int length = generator.generate(line, start + elapsed, service, level);
            parser.parse(line, length);

            int id = parser.serviceId();
            long second = (parser.timestampMillis() - start) / 1000;
            boolean error = parser.level() >= LogLevel.ERROR;
            if (error) errorsPerSecond[id][(int) second]++;
            if (monitor.onLog(id, parser.timestampMillis(), error)) spikeSeconds[id].add(lastSecond[id]);
            lastSecond[id] = second;
        }

        int failures = 0;
        Set<Long> expected = new TreeSet<>();
        for (long s = 30; s < 50; s++) expected.add(s);
        for (int id = 0; id < services.size(); id++) {
            String name = services.name(id);
            Set<Long> want = name.equals("payment-service") ? expected : Set.of();
            if (!spikeSeconds[id].equals(want)) failures += fail(name + " spike seconds " + spikeSeconds[id] + ", expected " + want);

            int lastMinute = 0;
            for (int s = SECONDS - ErrorRateMonitor.WINDOW_SECONDS; s < SECONDS; s++) lastMinute += errorsPerSecond[id][s];
            if (monitor.errorsLastMinute(id) != lastMinute) failures += fail(name + " errorsLastMinute " + monitor.errorsLastMinute(id) + ", expected " + lastMinute);

            System.out.printf("  %-17s spikes at %s, errors last 60 s = %,d, normal = %.1f/s%n", name, spikeSeconds[id], monitor.errorsLastMinute(id), monitor.normalRate(id));
        }
        System.out.printf("error spike scenario: %,d logs over %d simulated seconds, %d failures%n", total, SECONDS, failures);
        return failures == 0;
    }

    private static boolean checkAllocation() {
        ErrorRateMonitor monitor = new ErrorRateMonitor(16, SPIKE_FACTOR, MIN_ERRORS);
        long millis = 1_790_000_000_000L;
        long sink = 0;
        for (int i = 0; i < 2_000_000; i++) { // warm-up
            sink += monitor.onLog(i & 3, millis + i / 4, (i & 15) == 0) ? 1 : 0;
        }

        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        int calls = 1_000_000;
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < calls; i++) {
            sink += monitor.onLog(i & 3, millis + 500_000 + i / 4, (i & 15) == 0) ? 1 : 0;
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        double perCall = (double) allocated / calls;
        System.out.printf("allocation: %,d bytes over %,d logs = %.4f bytes/log (sink %d)%n", allocated, calls, perCall, sink);
        return perCall < 1.0;
    }

    private static int fail(String message) {
        System.out.println("  FAIL " + message);
        return 1;
    }
}
