package com.sarbar.pipeline.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


public final class EngineCheck {

    public static void main(String[] args) throws Exception {
        fastJob();
        deadlineExceeded();
        laneFull();
        deadlineScopedValue();
        bulkheadLimits();
        retrySucceedsOnSecondAttempt();
        retryGivesUpOnNonRetryable();
        retryRespectsDeadline();
        circuitBreakerTrips();
        circuitBreakerRecovers();
        circuitBreakerRejectsWhileOpen();
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

    /** Bulkhead with 2 permits: two jobs hold permits, third waits until one finishes. */
    private static void bulkheadLimits() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(5);
        Bulkhead bulkhead = new Bulkhead(2);
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger running = new AtomicInteger();

        EngineJob<Void> holdPermit = () -> bulkhead.call(() -> {
            int r = running.incrementAndGet();
            peak.accumulateAndGet(r, Math::max);
            LockSupport.parkNanos(300_000_000L);
            running.decrementAndGet();
            return null;
        });

        CompletableFuture<Void> f1 = engine.submit(Lane.STORAGE, deadline, holdPermit);
        CompletableFuture<Void> f2 = engine.submit(Lane.STORAGE, deadline, holdPermit);
        CompletableFuture<Void> f3 = engine.submit(Lane.STORAGE, deadline, holdPermit);

        f1.join(); f2.join(); f3.join();
        check(peak.get() == 2, "bulkhead: peak concurrency should be 2, got " + peak.get());
        check(bulkhead.available() == 2, "bulkhead: all permits returned");
        engine.shutdown();
        System.out.println("[PASS] bulkhead limits");
    }

    /** Retry succeeds on the second attempt after a retryable failure. */
    private static void retrySucceedsOnSecondAttempt() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(5);
        Retry retry = new Retry(3, 10, e -> e instanceof java.io.IOException);

        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<String> f = engine.submit(Lane.STORAGE, deadline, () -> {
            return retry.call(() -> {
                if (attempts.incrementAndGet() == 1) throw new java.io.IOException("transient");
                return "recovered";
            });
        });

        check(f.join().equals("recovered"), "retry: should recover on second attempt");
        check(attempts.get() == 2, "retry: expected 2 attempts, got " + attempts.get());
        engine.shutdown();
        System.out.println("[PASS] retry succeeds on second attempt");
    }

    /** Non-retryable exceptions are thrown immediately without further attempts. */
    private static void retryGivesUpOnNonRetryable() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusSeconds(5);
        Retry retry = new Retry(3, 10, e -> e instanceof java.io.IOException);

        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> f = engine.submit(Lane.STORAGE, deadline, () -> {
            return retry.call(() -> {
                attempts.incrementAndGet();
                throw new IllegalArgumentException("bad SQL");
            });
        });

        try {
            f.join();
            check(false, "retry non-retryable: should have thrown");
        } catch (Exception e) {
            check(attempts.get() == 1, "retry non-retryable: should stop after 1 attempt, got " + attempts.get());
        }
        engine.shutdown();
        System.out.println("[PASS] retry gives up on non-retryable");
    }

    /** Retry stops when the deadline expires, even if attempts remain. */
    private static void retryRespectsDeadline() throws Exception {
        Engine engine = new Engine();
        Instant deadline = Instant.now().plusMillis(200);
        Retry retry = new Retry(10, 100, e -> e instanceof java.io.IOException);

        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<Void> f = engine.submit(Lane.STORAGE, deadline, () -> {
            return retry.call(() -> {
                attempts.incrementAndGet();
                throw new java.io.IOException("keeps failing");
            });
        });

        try {
            f.join();
            check(false, "retry deadline: should have thrown");
        } catch (Exception ignored) {}
        check(attempts.get() < 10, "retry deadline: should stop before 10 attempts (got " + attempts.get() + ")");
        engine.shutdown();
        System.out.println("[PASS] retry respects deadline");
    }

    /** Enough failures trip the breaker to OPEN. */
    private static void circuitBreakerTrips() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(10, 0.5, 4, 5_000);
        check(cb.state() == CircuitBreaker.State.CLOSED, "cb trips: should start CLOSED");

        for (int i = 0; i < 4; i++) {
            try {
                cb.call(() -> { throw new java.io.IOException("fail"); });
            } catch (java.io.IOException ignored) {}
        }
        check(cb.state() == CircuitBreaker.State.OPEN, "cb trips: should be OPEN after 4/4 failures");
        System.out.println("[PASS] circuit breaker trips");
    }

    /** After the open duration, a successful probe closes the breaker. */
    private static void circuitBreakerRecovers() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(10, 0.5, 4, 50);

        for (int i = 0; i < 4; i++) {
            try { cb.call(() -> { throw new java.io.IOException("fail"); }); } catch (Exception ignored) {}
        }
        check(cb.state() == CircuitBreaker.State.OPEN, "cb recovers: should be OPEN");

        Thread.sleep(80); // wait past the 50ms open duration
        check(cb.state() == CircuitBreaker.State.HALF_OPEN, "cb recovers: should be HALF_OPEN after wait");

        String result = cb.call(() -> "ok");
        check(result.equals("ok"), "cb recovers: probe should succeed");
        check(cb.state() == CircuitBreaker.State.CLOSED, "cb recovers: should be CLOSED after successful probe");
        System.out.println("[PASS] circuit breaker recovers");
    }

    /** While OPEN, calls are rejected immediately without running the job. */
    private static void circuitBreakerRejectsWhileOpen() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(10, 0.5, 4, 5_000);

        for (int i = 0; i < 4; i++) {
            try { cb.call(() -> { throw new java.io.IOException("fail"); }); } catch (Exception ignored) {}
        }

        AtomicInteger called = new AtomicInteger();
        try {
            cb.call(() -> { called.incrementAndGet(); return "should not run"; });
            check(false, "cb rejects: should have thrown CircuitOpenException");
        } catch (CircuitOpenException e) {
            check(called.get() == 0, "cb rejects: job should not have been called");
        }
        System.out.println("[PASS] circuit breaker rejects while open");
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
