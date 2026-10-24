package com.sarbar.pipeline.engine;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Limits how many calls to a resource can be in flight at once. */
public final class Bulkhead {

    private final Semaphore semaphore;

    public Bulkhead(int maxConcurrent) {
        this.semaphore = new Semaphore(maxConcurrent);
    }

    /** Runs the job while holding a permit. Waits up to the deadline for a permit. */
    public <T> T call(EngineJob<T> job) throws Exception {
        Deadline.check();
        long waitNanos = Deadline.remainingNanos();
        if (!semaphore.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
            throw new DeadlineExceededException();
        }
        try {
            return job.call();
        } finally {
            semaphore.release();
        }
    }

    public int available() {
        return semaphore.availablePermits();
    }
}
