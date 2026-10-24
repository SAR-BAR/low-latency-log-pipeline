package com.sarbar.pipeline.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;


public final class EngineCheck {

    public static void main(String[] args) throws Exception {
        fastJob();
        deadlineExceeded();
        laneFull();
        deadlineScopedValue();
        System.out.println("\nAll engine checks passed.");
    }

    /** A job that returns immediately should complete the future with its result. */
    private static void fastJob() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(5);
        CompletableFuture<String> f = engine.submit(Lane.STORAGE, deadline, () -> "hello");
        String result = f.join();
        check(result.equals("hello"), "fast job: expected 'hello', got '" + result + "'");
        check(engine.inFlight(Lane.STORAGE) == 0, "fast job: inFlight should be 0 after completion");
        engine.shutdown();
        System.out.println("[PASS] fast job");
    }

    /** A job that takes too long should be cancelled when the deadline expires. */
    private static void deadlineExceeded() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusMillis(100);
        CompletableFuture<String> f = engine.submit(Lane.STORAGE, deadline, () -> {
            LockSupport.parkNanos(5_000_000_000L); // 5 seconds — way past the 100ms deadline
            return "should not reach here";
        });
        try {
            f.join();
            check(false, "deadline: should have thrown");
        } catch (Exception e) {
            check(true, "deadline: job cancelled as expected");
        }
        Thread.sleep(50); // let the virtual thread fully exit
        check(engine.inFlight(Lane.STORAGE) == 0, "deadline: inFlight should be 0 after cancellation");
        engine.shutdown();
        System.out.println("[PASS] deadline exceeded");
    }

    /** Submitting beyond the lane limit should throw LaneFullException. */
    private static void laneFull() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(10);
        int max = Lane.ALERT.maxConcurrent(); // 4
        List<CompletableFuture<Void>> held = new ArrayList<>();

        for (int i = 0; i < max; i++) {
            held.add(engine.submit(Lane.ALERT, deadline, () -> {
                LockSupport.parkNanos(2_000_000_000L); // hold the slot for 2 seconds
                return null;
            }));
        }
        check(engine.inFlight(Lane.ALERT) == max, "lane full: expected " + max + " in flight");

        boolean rejected = false;
        try {
            engine.submit(Lane.ALERT, deadline, () -> null);
        } catch (LaneFullException e) {
            rejected = true;
            check(e.lane() == Lane.ALERT, "lane full: exception should name the ALERT lane");
        }
        check(rejected, "lane full: 5th submit should have been rejected");

        // STORAGE lane should still be open
        CompletableFuture<String> ok = engine.submit(Lane.STORAGE, deadline, () -> "still works");
        check(ok.join().equals("still works"), "lane full: STORAGE lane unaffected");

        engine.shutdown();
        for (CompletableFuture<Void> f : held) {
            try { f.join(); } catch (Exception ignored) {}
        }
        System.out.println("[PASS] lane full");
    }

    /** The deadline ScopedValue should be readable inside the job. */
    private static void deadlineScopedValue() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(5);
        CompletableFuture<Boolean> f = engine.submit(Lane.STORAGE, deadline, () -> {
            Deadline.check(); // should not throw — 5 seconds left
            long nanos = Deadline.remainingNanos();
            return Deadline.isSet() && nanos > 0 && nanos < 5_000_000_000L;
        });
        check(f.join(), "scoped value: deadline should be readable inside the job");
        engine.shutdown();
        System.out.println("[PASS] deadline scoped value");
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
