package com.sarbar.pipeline.storage;

import java.util.concurrent.atomic.AtomicLong;

public final class CheckpointTracker {

    private final AtomicLong checkpoint = new AtomicLong(-1);
    private long[] pending;
    private int pendingCount;

    public CheckpointTracker(int initialCapacity) {
        this.pending = new long[initialCapacity * 2];
    }

    public synchronized void complete(long minSeq, long maxSeq) {
        addRange(minSeq, maxSeq);
        advance();
    }

    public long checkpoint() {
        return checkpoint.get();
    }

    private void addRange(long min, long max) {
        if (pendingCount * 2 >= pending.length) {
            advance();
        }
        if (pendingCount * 2 >= pending.length) {
            grow();
        }
        int i = pendingCount * 2;
        pending[i] = min;
        pending[i + 1] = max;
        pendingCount++;
    }

    private void grow() {
        long[] bigger = new long[pending.length * 2];
        System.arraycopy(pending, 0, bigger, 0, pendingCount * 2);
        pending = bigger;
    }

    private void advance() {
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            long cp = checkpoint.get();
            for (int i = 0; i < pendingCount; i++) {
                long min = pending[i * 2];
                long max = pending[i * 2 + 1];
                if (min <= cp + 1) {
                    if (max > cp) {
                        checkpoint.set(max);
                        progressed = true;
                    }
                    removePending(i);
                    i--;
                }
            }
        }
    }

    private void removePending(int index) {
        int last = pendingCount - 1;
        if (index != last) {
            pending[index * 2] = pending[last * 2];
            pending[index * 2 + 1] = pending[last * 2 + 1];
        }
        pendingCount--;
    }

    public synchronized int pendingRanges() {
        return pendingCount;
    }
}
