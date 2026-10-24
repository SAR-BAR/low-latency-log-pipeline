package com.sarbar.pipeline.engine;

import java.time.Duration;
import java.time.Instant;

/**
 * Carries the job's deadline
 */
public final class Deadline {
    static final ScopedValue<Instant> CURRENT = ScopedValue.newInstance();

    private Deadline() {}
    
    public static Duration remaining() {
        Duration d = Duration.between(Instant.now(), CURRENT.get());
        return d.isNegative() ? Duration.ZERO : d;
    }

    public static long remainingNanos() {
        return remaining().toNanos();
    }

    /** Throws if the deadline has passed.  */
    public static void check() throws DeadlineExceededException {
        if (Instant.now().isAfter(CURRENT.get())) {
            throw new DeadlineExceededException();
        }
    }

    /** True when called inside an engine job (a ScopedValue binding exists). */
    public static boolean isSet() {
        return CURRENT.isBound();
    }
}
