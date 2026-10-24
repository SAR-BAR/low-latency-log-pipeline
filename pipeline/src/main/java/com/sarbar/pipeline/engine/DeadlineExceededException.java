package com.sarbar.pipeline.engine;

public final class DeadlineExceededException extends Exception {

    public DeadlineExceededException() {
        super("deadline exceeded");
    }
}
