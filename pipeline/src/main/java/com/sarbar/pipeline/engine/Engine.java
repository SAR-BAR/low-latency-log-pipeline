package com.sarbar.pipeline.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;

public final class Engine {

    private final AtomicInteger[] inFlight;
    private volatile boolean running = true;

    public Engine() {
        Lane[] lanes = Lane.values();
        inFlight = new AtomicInteger[lanes.length];
        for (int i = 0; i < lanes.length; i++) {
            inFlight[i] = new AtomicInteger();
        }
    }

    /**
     * Submits a job to run on a virtual thread in the given lane.
     */
    public <T> CompletableFuture<T> submit(Lane lane, Instant deadline, EngineJob<T> job) throws LaneFullException {
        if (!running) throw new RejectedExecutionException("engine is shut down");

        AtomicInteger counter = inFlight[lane.ordinal()];
        int current = counter.incrementAndGet();
        if (current > lane.maxConcurrent()) {
            counter.decrementAndGet();
            throw new LaneFullException(lane);
        }

        CompletableFuture<T> future = new CompletableFuture<>();
        Thread.ofVirtual()
                .name("engine-" + lane.name().toLowerCase() + "-", current - 1)
                .start(() -> {
                    try {
                        T result = runWithDeadline(deadline, job);
                        future.complete(result);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    } finally {
                        counter.decrementAndGet();
                    }
                });
        return future;
    }

    public void shutdown() {
        running = false;
    }

    public int inFlight(Lane lane) {
        return inFlight[lane.ordinal()].get();
    }

    private <T> T runWithDeadline(Instant deadline, EngineJob<T> job) throws Exception {
        return ScopedValue.where(Deadline.CURRENT, deadline).call(() -> {
            Duration timeout = Duration.between(Instant.now(), deadline);
            if (timeout.isNegative() || timeout.isZero()) throw new DeadlineExceededException();

            try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.<T>allSuccessfulOrThrow(), cf -> cf.withTimeout(timeout))) {
                scope.fork(job::call);
                var results = scope.join();
                return results.getFirst();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DeadlineExceededException();
            }
        });
    }
}
