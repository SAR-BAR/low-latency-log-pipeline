package com.sarbar.pipeline.engine;

import java.util.concurrent.atomic.AtomicInteger;

public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int windowSeconds;
    private final double failureThreshold;
    private final int minimumCalls;
    private final long openDurationNanos;

    private final AtomicInteger[] successes;
    private final AtomicInteger[] failures;
    private volatile long currentSecond;

    private volatile State state = State.CLOSED;
    private volatile long openedAtNanos;

    public CircuitBreaker(int windowSeconds, double failureThreshold, int minimumCalls, long openDurationMs) {
        this.windowSeconds = windowSeconds;
        this.failureThreshold = failureThreshold;
        this.minimumCalls = minimumCalls;
        this.openDurationNanos = openDurationMs * 1_000_000L;
        this.successes = new AtomicInteger[windowSeconds];
        this.failures = new AtomicInteger[windowSeconds];
        for (int i = 0; i < windowSeconds; i++) {
            successes[i] = new AtomicInteger();
            failures[i] = new AtomicInteger();
        }
        this.currentSecond = System.nanoTime() / 1_000_000_000L;
    }

    /** Convenience: 50% failure over 10s (min 20 calls), 5s open. */
    public CircuitBreaker() {
        this(10, 0.5, 20, 5_000);
    }

    public <T> T call(EngineJob<T> job) throws Exception {
        if (!tryAcquire()) throw new CircuitOpenException(this);
        try {
            T result = job.call();
            recordSuccess();
            return result;
        } catch (Exception e) {
            recordFailure();
            throw e;
        }
    }

    public State state() {
        if (state == State.OPEN && System.nanoTime() - openedAtNanos >= openDurationNanos) {
            state = State.HALF_OPEN;
        }
        return state;
    }

    boolean tryAcquire() {
        return switch (state()) {
            case CLOSED -> true;
            case HALF_OPEN -> {
                state = State.OPEN;
                openedAtNanos = System.nanoTime();
                state = State.HALF_OPEN;
                yield true;
            }
            case OPEN -> false;
        };
    }

    void recordSuccess() {
        advance();
        successes[slot()].incrementAndGet();
        if (state == State.HALF_OPEN) {
            resetBuckets();
            state = State.CLOSED;
        }
    }

    void recordFailure() {
        advance();
        failures[slot()].incrementAndGet();
        if (state == State.HALF_OPEN) {
            trip();
            return;
        }
        if (state == State.CLOSED && shouldTrip()) trip();
    }

    private boolean shouldTrip() {
        int totalSuccess = 0, totalFailure = 0;
        for (int i = 0; i < windowSeconds; i++) {
            totalSuccess += successes[i].get();
            totalFailure += failures[i].get();
        }
        int total = totalSuccess + totalFailure;
        if (total < minimumCalls) return false;
        return (double) totalFailure / total >= failureThreshold;
    }

    private void trip() {
        state = State.OPEN;
        openedAtNanos = System.nanoTime();
    }

    private void advance() {
        long now = System.nanoTime() / 1_000_000_000L;
        long prev = currentSecond;
        if (now <= prev) return;
        currentSecond = now;
        long stale = Math.min(now - prev, windowSeconds);
        for (long t = prev + 1; t <= prev + stale; t++) {
            int idx = (int) Math.floorMod(t, windowSeconds);
            successes[idx].set(0);
            failures[idx].set(0);
        }
    }

    private int slot() {
        return (int) Math.floorMod(currentSecond, windowSeconds);
    }

    private void resetBuckets() {
        for (int i = 0; i < windowSeconds; i++) {
            successes[i].set(0);
            failures[i].set(0);
        }
    }
}
