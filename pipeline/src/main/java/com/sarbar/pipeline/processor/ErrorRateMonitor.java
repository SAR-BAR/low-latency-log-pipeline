package com.sarbar.pipeline.processor;

/** Per-service error counts for the last 60 seconds, plus spike detection against a moving average. */
public final class ErrorRateMonitor {

    static final int WINDOW_SECONDS = 60;
    static final int WARMUP_SECONDS = 10;
    static final double ALPHA = 0.1;

    private static final long NO_SECOND = Long.MIN_VALUE;

    private final double spikeFactor;
    private final int minErrors;
    private final int[][] buckets;
    private final long[] currentSecond;
    private final double[] normalRate;
    private final int[] secondsSeen;

    public ErrorRateMonitor(int maxServices, double spikeFactor, int minErrors) {
        this.spikeFactor = spikeFactor;
        this.minErrors = minErrors;
        this.buckets = new int[maxServices][WINDOW_SECONDS];
        this.currentSecond = new long[maxServices];
        java.util.Arrays.fill(currentSecond, NO_SECOND);
        this.normalRate = new double[maxServices];
        this.secondsSeen = new int[maxServices];
    }

    /** Records one log; returns true if the service's previous second just closed as a spike. */
    public boolean onLog(int serviceId, long epochMillis, boolean error) {
        if (serviceId < 0) return false;
        long second = Math.floorDiv(epochMillis, 1000);
        boolean spike = advance(serviceId, second);
        long age = currentSecond[serviceId] - second;
        if (error && age >= 0 && age < WINDOW_SECONDS) {
            buckets[serviceId][slot(second)]++;
        }
        return spike;
    }

    public int errorsLastMinute(int serviceId) {
        int sum = 0;
        for (int count : buckets[serviceId]) sum += count;
        return sum;
    }

    public double normalRate(int serviceId) {
        return normalRate[serviceId];
    }

    private boolean advance(int s, long second) {
        long current = currentSecond[s];
        if (current == NO_SECOND) {
            currentSecond[s] = second;
            return false;
        }
        if (second <= current) return false;

        boolean spike = close(s, buckets[s][slot(current)]);
        // Seconds skipped without any log for this service count as zero errors.
        long emptySeconds = Math.min(second - current - 1, WINDOW_SECONDS);
        for (long k = 0; k < emptySeconds; k++) close(s, 0);
        // Boxes being entered still hold counts from a minute ago.
        for (long t = current + 1; t <= second && t <= current + WINDOW_SECONDS; t++) buckets[s][slot(t)] = 0;
        currentSecond[s] = second;
        return spike;
    }

    private boolean close(int s, int count) {
        boolean spike = secondsSeen[s] >= WARMUP_SECONDS && count >= minErrors && count > spikeFactor * normalRate[s];
        // Spike seconds are kept out of the average, so a spike doesn't become the new normal.
        if (!spike) normalRate[s] = secondsSeen[s] == 0 ? count : (1 - ALPHA) * normalRate[s] + ALPHA * count;
        secondsSeen[s]++;
        return spike;
    }

    private static int slot(long second) {
        return (int) Math.floorMod(second, WINDOW_SECONDS);
    }
}
