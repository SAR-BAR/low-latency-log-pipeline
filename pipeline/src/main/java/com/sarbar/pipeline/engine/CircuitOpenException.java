package com.sarbar.pipeline.engine;

public final class CircuitOpenException extends Exception {

    private final CircuitBreaker breaker;

    public CircuitOpenException(CircuitBreaker breaker) {
        super("circuit breaker is open");
        this.breaker = breaker;
    }

    public CircuitBreaker breaker() {
        return breaker;
    }
}
