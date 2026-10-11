package com.sarbar.agent.sim;

/**
 * Load shapes for a simulator run.
 *
 * <ul>
 *   <li>{@code STEADY}: constant rate, 5% errors in every service.</li>
 *   <li>{@code BURST}: the first second of every 10 seconds runs at 10x the rate.</li>
 *   <li>{@code ERROR_SPIKE}: steady rate, but from 30 s to 50 s the first service logs 50% errors.</li>
 * </ul>
 */
public enum Scenario {
    STEADY, BURST, ERROR_SPIKE;

    static final int NORMAL_ERROR_PERCENT = 5;
    static final int SPIKE_ERROR_PERCENT = 50;

    double rateMultiplier(long elapsedMillis) {
        return this == BURST && elapsedMillis % 10_000 < 1_000 ? 10.0 : 1.0;
    }

    int errorPercent(long elapsedMillis, int serviceIndex) {
        boolean spiking = this == ERROR_SPIKE && serviceIndex == 0 && elapsedMillis >= 30_000 && elapsedMillis < 50_000;
        return spiking ? SPIKE_ERROR_PERCENT : NORMAL_ERROR_PERCENT;
    }
}
