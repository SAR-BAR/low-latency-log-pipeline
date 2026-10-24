package com.sarbar.pipeline.engine;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

/** Retries a job with exponential backoff + jitter, bounded by the deadline. */
public final class Retry {

    private final int maxAttempts;
    private final long baseDelayNanos;
    private final Predicate<Throwable> retryable;
    
    public Retry(int maxAttempts, long baseDelayMs, Predicate<Throwable> retryable) {
        this.maxAttempts = maxAttempts;
        this.baseDelayNanos = baseDelayMs * 1_000_000L;
        this.retryable = retryable;
    }

    public <T> T call(EngineJob<T> job) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Deadline.check();
            try {
                return job.call();
            } catch (Exception e) {
                last = e;
                if (attempt == maxAttempts || !retryable.test(e)) throw e;
                backoff(attempt);
            }
        }
        throw last;
    }

    private void backoff(int attempt) throws DeadlineExceededException {
        long delay = baseDelayNanos << (attempt - 1); 
        long jitter = ThreadLocalRandom.current().nextLong(delay); 
        long sleepNanos = Math.min(jitter, Deadline.remainingNanos());
        if (sleepNanos <= 0) throw new DeadlineExceededException();
        LockSupport.parkNanos(sleepNanos);
    }
}
