package com.sarbar.pipeline.ingest;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.sarbar.pipeline.processor.Processor;
import com.sarbar.pipeline.ring.LogEvent;
import com.sarbar.pipeline.ring.LogRing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Three simulator processes -> mailboxes -> bridge -> belt -> processor; simulator 3 is killed mid-run. Exit code 0 = PASS. */
public final class BridgeCheck {

    private static final int SIMULATORS = 3;
    private static final int KILLED = 3;
    private static final int RATE = 20_000;
    private static final int DURATION_SECONDS = 3;
    private static final Pattern SENT = Pattern.compile("sent=([\\d,]+)");

    /** Runs after the processor: per source, every sequence must arrive exactly once and in order. */
    static final class SourceVerifier implements EventHandler<LogEvent> {
        final long[] next = new long[SIMULATORS + 1];
        volatile long total;
        long failures;
        String firstFailure;

        @Override
        public void onEvent(LogEvent e, long sequence, boolean endOfBatch) {
            int source = e.sourceId();
            if (source < 1 || source > SIMULATORS) {
                fail("unknown source " + source);
            } else {
                if (e.sourceSequence() != next[source]) fail("source " + source + " sequence " + e.sourceSequence() + ", expected " + next[source]);
                next[source] = e.sourceSequence() + 1;
            }
            if (e.level() == LogEvent.UNPARSED) fail("source " + source + " sequence " + e.sourceSequence() + " was not parsed");
            total++;
        }

        private void fail(String m) {
            if (failures++ == 0) firstFailure = m;
        }
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("bridge-check");
        Processor processor = new Processor();
        SourceVerifier verifier = new SourceVerifier();
        LogRing ring = new LogRing(1024, new YieldingWaitStrategy(), (e, s, b) -> { }, processor, verifier, (e, s, b) -> { });
        ring.start();
        ShmIngestBridge bridge = new ShmIngestBridge(dir, ring);
        Thread bridgeThread = Thread.ofPlatform().name("shm-bridge").start(bridge);

        Process[] simulators = new Process[SIMULATORS + 1];
        for (int s = 1; s <= SIMULATORS; s++) {
            simulators[s] = startSimulator(s, dir);
        }

        Thread.sleep(1_000);
        simulators[KILLED].destroyForcibly().waitFor();

        long[] sent = new long[SIMULATORS + 1];
        for (int s = 1; s <= SIMULATORS; s++) {
            if (s == KILLED) continue;
            if (!simulators[s].waitFor(30, TimeUnit.SECONDS)) throw new IllegalStateException("simulator " + s + " did not finish");
            sent[s] = sentCount(simulators[s]);
        }

        long seen;
        do { // wait until the bridge has nothing left to move
            seen = verifier.total;
            Thread.sleep(300);
        } while (verifier.total != seen);
        bridge.stop();
        bridgeThread.join();
        ring.close();

        int failures = 0;
        if (verifier.failures > 0) failures += fail(verifier.failures + " failures, first: " + verifier.firstFailure);
        for (int s = 1; s <= SIMULATORS; s++) {
            if (s == KILLED) {
                if (verifier.next[s] == 0) failures += fail("killed simulator " + s + " delivered nothing");
                System.out.printf("  simulator %d: killed after 1 s, delivered %,d logs, all in order%n", s, verifier.next[s]);
            } else {
                if (verifier.next[s] != sent[s]) failures += fail("simulator " + s + " sent " + sent[s] + ", received " + verifier.next[s]);
                System.out.printf("  simulator %d: sent %,d, received %,d%n", s, sent[s], verifier.next[s]);
            }
        }
        if (processor.malformedCount() != 0) failures += fail(processor.malformedCount() + " malformed logs");

        try (var files = Files.list(dir)) {
            for (Path p : files.toList()) Files.delete(p);
        }
        Files.delete(dir);

        System.out.printf("bridge: %d simulator processes, %,d logs through mailboxes -> belt -> processor, %d failures%n", SIMULATORS, verifier.total, failures);
        System.out.println(failures == 0 ? "PASS" : "FAIL");
        System.exit(failures == 0 ? 0 : 1);
    }

    private static Process startSimulator(int source, Path dir) throws IOException {
        String java = ProcessHandle.current().info().command().orElse("java");
        return new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "com.sarbar.agent.sim.Simulator", "source=" + source, "rate=" + RATE, "duration=" + DURATION_SECONDS, "out=shm:" + dir).redirectErrorStream(true).start();
    }

    private static long sentCount(Process simulator) throws IOException {
        String output = new String(simulator.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Matcher m = SENT.matcher(output);
        if (!m.find()) throw new IllegalStateException("no sent= in simulator output: " + output);
        return Long.parseLong(m.group(1).replace(",", ""));
    }

    private static int fail(String m) {
        System.out.println("  FAIL " + m);
        return 1;
    }
}
