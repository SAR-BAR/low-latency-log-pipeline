package com.sarbar.pipeline.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.sarbar.agent.sim.Level;
import com.sarbar.agent.sim.LogLineGenerator;
import com.sarbar.agent.sim.Scenario;
import com.sarbar.pipeline.ring.LogEvent;
import com.sarbar.pipeline.ring.LogRing;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** End-to-end check: generated lines (some broken) go through LogRing into Processor; every slot, spike and template is verified. */
public final class ProcessorCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final int RATE = 2_000;
    private static final int SECONDS = 90;
    private static final int MALFORMED_EVERY = 997;
    private static final long START = 1_790_000_000_000L;
    private static final int RING_SIZE = 64;

    private static final int TOTAL = RATE * SECONDS;
    private static final byte[][] lines = new byte[TOTAL][];
    private static final long[] millis = new long[TOTAL];
    private static final int[] service = new int[TOTAL];
    private static final int[] level = new int[TOTAL];
    private static final int[] messageOffset = new int[TOTAL];
    private static final String[] message = new String[TOTAL];
    private static final boolean[] malformed = new boolean[TOTAL];

    public static void main(String[] args) throws Exception {
        generate();
        boolean pipeline = checkPipeline();
        boolean allocation = checkAllocation();
        boolean pass = pipeline && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    private static void generate() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        byte[] buf = new byte[LogLineGenerator.MAX_LINE_BYTES];
        for (int i = 0; i < TOTAL; i++) {
            if (i % MALFORMED_EVERY == MALFORMED_EVERY - 1) {
                lines[i] = ("garbage line " + i + " without the expected format").getBytes(StandardCharsets.US_ASCII);
                malformed[i] = true;
                level[i] = LogEvent.UNPARSED;
                continue;
            }
            long elapsed = (long) i * 1000 / RATE;
            int s = generator.randomService();
            Level l = generator.randomLevel(Scenario.ERROR_SPIKE.errorPercent(elapsed, s));
            int length = generator.generate(buf, START + elapsed, s, l);
            lines[i] = Arrays.copyOf(buf, length);
            String text = new String(lines[i], StandardCharsets.US_ASCII);
            millis[i] = START + elapsed;
            service[i] = s;
            level[i] = switch (l) {
                case DEBUG -> LogLevel.DEBUG;
                case INFO -> LogLevel.INFO;
                case WARN -> LogLevel.WARN;
                case ERROR -> LogLevel.ERROR;
            };
            messageOffset[i] = text.indexOf(" - ") + 3;
            message[i] = text.substring(messageOffset[i]);
        }
    }

    /** Runs after the processor; fields are plain because main reads them only after the ring has shut down. */
    static final class SlotVerifier implements EventHandler<LogEvent> {
        private final Processor processor;
        final int[] templateIds = new int[TOTAL];
        long next;
        long failures;
        String firstFailure;

        SlotVerifier(Processor processor) {
            this.processor = processor;
        }

        @Override
        public void onEvent(LogEvent e, long sequence, boolean endOfBatch) {
            int i = (int) e.sourceSequence();
            if (i != next) fail("source sequence " + i + ", expected " + next);
            next = i + 1;
            if (malformed[i]) {
                if (e.level() != LogEvent.UNPARSED || e.serviceId() != LogEvent.UNPARSED || e.templateId() != LogEvent.UNPARSED) fail("malformed line " + i + " has parsed fields");
                return;
            }
            if (e.timestampMillis() != millis[i]) fail("line " + i + " timestamp " + e.timestampMillis() + " != " + millis[i]);
            if (e.level() != level[i]) fail("line " + i + " level " + e.level() + " != " + level[i]);
            if (e.messageOffset() != messageOffset[i]) fail("line " + i + " message offset " + e.messageOffset() + " != " + messageOffset[i]);
            if (e.serviceId() < 0 || !SERVICES[service[i]].equals(processor.services().name(e.serviceId()))) fail("line " + i + " wrong service id " + e.serviceId());
            if (e.templateId() < 0) fail("line " + i + " has no template");
            templateIds[i] = e.templateId();
        }

        private void fail(String m) {
            if (failures++ == 0) firstFailure = m;
        }
    }

    /** Runs after the processor; records which second closed as a spike, per service. */
    static final class SpikeRecorder implements EventHandler<LogEvent> {
        private final Processor processor;
        final Map<String, Set<Long>> spikeSeconds = new HashMap<>();

        SpikeRecorder(Processor processor) {
            this.processor = processor;
        }

        @Override
        public void onEvent(LogEvent e, long sequence, boolean endOfBatch) {
            if (!e.errorSpike()) return;
            long closedSecond = (e.timestampMillis() - START) / 1000 - 1;
            spikeSeconds.computeIfAbsent(processor.services().name(e.serviceId()), k -> new TreeSet<>()).add(closedSecond);
        }
    }

    private static boolean checkPipeline() {
        Processor processor = new Processor();
        SlotVerifier verifier = new SlotVerifier(processor);
        SpikeRecorder spikes = new SpikeRecorder(processor);
        LogRing ring = new LogRing(RING_SIZE, new YieldingWaitStrategy(), (e, s, b) -> { }, processor, verifier, spikes);
        ring.start();
        for (int i = 0; i < TOTAL; i++) {
            ring.publish(lines[i], 0, lines[i].length, 1, i);
        }
        ring.close();

        int failures = 0;
        if (verifier.failures > 0) failures += fail("slots: " + verifier.failures + " failures, first: " + verifier.firstFailure);
        if (verifier.next != TOTAL) failures += fail("verifier saw " + verifier.next + " of " + TOTAL + " logs");

        long expectedMalformed = 0;
        for (boolean m : malformed) if (m) expectedMalformed++;
        if (processor.malformedCount() != expectedMalformed) failures += fail("malformed " + processor.malformedCount() + ", expected " + expectedMalformed);

        Set<Long> window = new TreeSet<>();
        for (long s = 30; s < 50; s++) window.add(s);
        Map<String, Set<Long>> want = Map.of("payment-service", window);
        if (!spikes.spikeSeconds.equals(want)) failures += fail("spikes " + spikes.spikeSeconds + ", expected " + want);

        // Templates: the same messages in the same order through the String reference must give the same final templates.
        TemplateTreeCheck.ReferenceDrain reference = new TemplateTreeCheck.ReferenceDrain();
        Set<String> texts = new HashSet<>();
        int templateMismatches = 0;
        int[] refIds = new int[TOTAL];
        for (int i = 0; i < TOTAL; i++) {
            if (!malformed[i]) refIds[i] = reference.match(MessageTokenizerCheck.reference(message[i]));
        }
        for (int i = 0; i < TOTAL; i++) {
            if (malformed[i]) continue;
            String actual = processor.templates().text(verifier.templateIds[i], processor.words());
            texts.add(actual);
            if (!actual.equals(reference.text(refIds[i])) && templateMismatches++ == 0) fail("line " + i + " template \"" + actual + "\" != \"" + reference.text(refIds[i]) + "\"");
        }
        if (templateMismatches > 0) failures += fail(templateMismatches + " template mismatches");
        if (texts.size() != 11) failures += fail(texts.size() + " templates, expected 11");

        System.out.printf("pipeline: %,d logs (%,d malformed) through a %d-slot ring, %d templates, spikes %s, %d failures%n", TOTAL, processor.malformedCount(), RING_SIZE, texts.size(), spikes.spikeSeconds, failures);
        return failures == 0;
    }

    private static boolean checkAllocation() {
        Processor processor = new Processor();
        int n = 10_000;
        LogEvent[] events = new LogEvent[n];
        for (int i = 0; i < n; i++) {
            events[i] = new LogEvent();
            events[i].setRaw(lines[i], 0, lines[i].length, 1, i, 0);
        }
        for (int round = 0; round < 200; round++) { // warm-up
            for (int i = 0; i < n; i++) processor.onEvent(events[i], i, true);
        }

        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        int rounds = 100;
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < n; i++) processor.onEvent(events[i], i, true);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        long calls = (long) rounds * n;
        double perLog = (double) allocated / calls;
        System.out.printf("allocation: %,d bytes over %,d logs = %.4f bytes/log%n", allocated, calls, perLog);
        return perLog < 1.0;
    }

    private static int fail(String m) {
        System.out.println("  FAIL " + m);
        return 1;
    }
}
