package com.sarbar.pipeline.ring;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.CountDownLatch;

/**
 * Stress check for the conveyor belt: many producers push numbered events through a tiny ring,
 * and every consumer verifies that nothing was lost, repeated or reordered.
 *
 * <p>Consumer graph (same shape as the real pipeline):
 * <pre>
 *              +--> first A --+
 * producers ---+              +--> second (runs only after A and B finished the slot)
 *              +--> first B --+
 * </pre>
 *
 * <p>Usage: {@code RingStressCheck [producers] [eventsPerProducer] [ringSize] [busyspin|yielding|sleeping|blocking]}
 * <br>Defaults: 4 producers, 5,000,000 events each, 8 slots, yielding.
 * Exit code 0 = PASS, 1 = FAIL.
 */
public final class RingStressCheck {

    /** One reusable slot. Created once per ring position at startup and overwritten on every lap. */
    static final class Slot {
        int producerId;
        long value;
        // Written by the two first-stage consumers; read by the second stage to prove ordering.
        long seenByA = -1;
        long seenByB = -1;
    }

    /**
     * Checks one consumer's view of the stream. Fields are plain (single thread writes them);
     * the main thread reads them only after {@link Disruptor#shutdown()} has waited for this
     * consumer's progress counter to reach the end, which makes the writes visible.
     */
    static final class Verifier implements EventHandler<Slot> {
        private final String name;
        private final int stage; // 0 = first A, 1 = first B, 2 = second
        private final long[] lastValuePerProducer;

        long expectedSequence = 0;
        long count = 0;
        long checksum = 0;
        long failures = 0;
        String firstFailure;

        Verifier(String name, int stage, int producers) {
            this.name = name;
            this.stage = stage;
            this.lastValuePerProducer = new long[producers];
            java.util.Arrays.fill(lastValuePerProducer, -1);
        }

        @Override
        public void onEvent(Slot slot, long sequence, boolean endOfBatch) {
            // 1. Sequences arrive gap-free and in order.
            if (sequence != expectedSequence) {
                fail("sequence " + sequence + ", expected " + expectedSequence);
            }
            expectedSequence = sequence + 1;

            // 2. Each producer's values arrive exactly once and in order: 0, 1, 2, ...
            int p = slot.producerId;
            long expectedValue = lastValuePerProducer[p] + 1;
            if (slot.value != expectedValue) {
                fail("producer " + p + " value " + slot.value + ", expected " + expectedValue + " (seq " + sequence + ")");
            }
            lastValuePerProducer[p] = slot.value;

            // 3. The second stage must only see slots both first-stage consumers have finished.
            switch (stage) {
                case 0 -> slot.seenByA = sequence;
                case 1 -> slot.seenByB = sequence;
                default -> {
                    if (slot.seenByA != sequence || slot.seenByB != sequence) {
                        fail("seq " + sequence + " reached second stage before first stage finished" + " (A=" + slot.seenByA + ", B=" + slot.seenByB + ")");
                    }
                }
            }

            // 4. Order-sensitive fingerprint; every consumer must end with the same value.
            checksum = 31 * checksum + (sequence ^ ((long) p << 48) ^ slot.value);
            count++;
        }

        private void fail(String message) {
            if (failures++ == 0) {
                firstFailure = message;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int producers = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        long perProducer = args.length > 1 ? Long.parseLong(args[1]) : 5_000_000L;
        int ringSize = args.length > 2 ? Integer.parseInt(args[2]) : 8;
        String waitName = args.length > 3 ? args[3] : "yielding";

        Disruptor<Slot> disruptor = new Disruptor<>(
                Slot::new,
                ringSize,
                Thread.ofPlatform().name("ring-consumer-", 0).factory(),
                ProducerType.MULTI,
                waitStrategy(waitName));

        Verifier a = new Verifier("first-A", 0, producers);
        Verifier b = new Verifier("first-B", 1, producers);
        Verifier second = new Verifier("second", 2, producers);
        disruptor.handleEventsWith(a, b).then(second);

        RingBuffer<Slot> ring = disruptor.start();

        CountDownLatch startGun = new CountDownLatch(1);
        Thread[] producerThreads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            int producerId = p;
            producerThreads[p] = Thread.ofPlatform().name("ring-producer-" + p).start(() -> {
                awaitQuietly(startGun);
                for (long v = 0; v < perProducer; v++) {
                    // Claim, fill, publish. Publish sits in finally: a claimed slot that is never
                    // published would stall every consumer forever.
                    long seq = ring.next();
                    try {
                        Slot slot = ring.get(seq);
                        slot.producerId = producerId;
                        slot.value = v;
                    } finally {
                        ring.publish(seq);
                    }
                }
            });
        }

        System.out.printf("producers=%d eventsPerProducer=%,d ringSize=%d wait=%s%n", producers, perProducer, ringSize, waitName);
        long start = System.nanoTime();
        startGun.countDown();
        for (Thread t : producerThreads) {
            t.join();
        }
        disruptor.shutdown(); // waits until every consumer has processed everything published
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        long expectedTotal = producers * perProducer;
        boolean pass = true;
        for (Verifier v : new Verifier[] {a, b, second}) {
            pass &= report(v, expectedTotal, perProducer);
        }
        if (a.checksum != b.checksum || a.checksum != second.checksum) {
            System.out.printf("  checksums differ: A=%d B=%d second=%d%n", a.checksum, b.checksum, second.checksum);
            pass = false;
        }

        System.out.printf("%s in %,d ms%n", pass ? "PASS" : "FAIL", elapsedMs);
        System.exit(pass ? 0 : 1);
    }

    private static boolean report(Verifier v, long expectedTotal, long perProducer) {
        boolean ok = v.failures == 0 && v.count == expectedTotal;
        for (long last : v.lastValuePerProducer) {
            ok &= last == perProducer - 1;
        }
        System.out.printf("  %-8s count=%,d failures=%,d%s%n", v.name, v.count, v.failures,
                v.firstFailure == null ? "" : " first: " + v.firstFailure);
        return ok;
    }

    private static WaitStrategy waitStrategy(String name) {
        return switch (name) {
            case "busyspin" -> new BusySpinWaitStrategy();
            case "yielding" -> new YieldingWaitStrategy();
            case "sleeping" -> new SleepingWaitStrategy();
            case "blocking" -> new BlockingWaitStrategy();
            default -> throw new IllegalArgumentException("unknown wait strategy: " + name);
        };
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
