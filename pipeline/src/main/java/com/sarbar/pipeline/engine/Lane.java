package com.sarbar.pipeline.engine;

/** A category of work the engine runs, each with its own concurrency limit. */
public enum Lane {

    STORAGE(16),
    ALERT(4);

    private final int maxConcurrent;

    Lane(int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    public int maxConcurrent() {
        return maxConcurrent;
    }
}
