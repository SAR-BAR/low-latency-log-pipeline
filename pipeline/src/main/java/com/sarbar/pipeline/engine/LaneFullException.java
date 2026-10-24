package com.sarbar.pipeline.engine;

public final class LaneFullException extends Exception {

    private final Lane lane;

    public LaneFullException(Lane lane) {
        super(lane.name() + " lane full (" + lane.maxConcurrent() + " concurrent)");
        this.lane = lane;
    }

    public Lane lane() {
        return lane;
    }
}
