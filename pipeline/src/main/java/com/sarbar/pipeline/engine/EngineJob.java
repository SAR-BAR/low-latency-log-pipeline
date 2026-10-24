package com.sarbar.pipeline.engine;

@FunctionalInterface
public interface EngineJob<T> {
    T call() throws Exception;
}
