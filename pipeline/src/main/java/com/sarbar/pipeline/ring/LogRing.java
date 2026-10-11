package com.sarbar.pipeline.ring;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * The conveyor belt and the workers attached to it.
 *
 * <pre>
 *              +--> journaler ------------------------------+
 * producers ---+                                            +--> slot can be reused
 *              +--> processor --+--> storage batcher -------+
 *                               +--> alert evaluator -------+
 * </pre>
 *
 * The journaler and the processor read each slot in parallel. The storage batcher and the alert
 * evaluator start on a slot only after the processor has written the parsed fields into it.
 * Producers wait when the belt is full, so a slow worker slows the producers down instead of
 * logs being dropped.
 *
 * <p>Each worker runs on its own platform thread. If a worker throws, the Disruptor's default
 * handler stops that worker; the belt then fills and producers stop. That is deliberate: skipping
 * the failed log instead would lose it.
 */
public final class LogRing implements AutoCloseable {

    private final Disruptor<LogEvent> disruptor;
    private RingBuffer<LogEvent> ring;

    /**
     * @param size must be a power of 2
     */
    public LogRing(int size, WaitStrategy waitStrategy, EventHandler<LogEvent> journaler, EventHandler<LogEvent> processor, EventHandler<LogEvent> storageBatcher, EventHandler<LogEvent> alertEvaluator) {
        this.disruptor = new Disruptor<>(
                LogEvent::new,
                size,
                Thread.ofPlatform().name("belt-worker-", 0).factory(),
                ProducerType.MULTI,
                waitStrategy);
        disruptor.handleEventsWith(journaler, processor);
        disruptor.after(processor).handleEventsWith(storageBatcher, alertEvaluator);
    }

    /** Starts the worker threads. Call once, before the first {@link #publish}. */
    public void start() {
        ring = disruptor.start();
    }

    /**
     * Copies one log onto the belt. Waits if the belt is full. Safe to call from several threads.
     *
     * @throws IllegalArgumentException if the log is longer than {@link LogEvent#MAX_LOG_BYTES};
     *                                  checked before claiming a slot, because a claimed slot must
     *                                  always be published or every worker would stall
     */
    public void publish(byte[] src, int offset, int length, int sourceId, long sourceSequence) {
        if (length < 0 || length > LogEvent.MAX_LOG_BYTES) {
            throw new IllegalArgumentException("log length " + length + " outside 0.." + LogEvent.MAX_LOG_BYTES);
        }
        long receivedNanos = System.nanoTime();
        long seq = ring.next();
        try {
            ring.get(seq).setRaw(src, offset, length, sourceId, sourceSequence, receivedNanos);
        } finally {
            ring.publish(seq);
        }
    }

    /** Waits until every published log has been handled by every worker, then stops the workers. */
    @Override
    public void close() {
        disruptor.shutdown();
    }
}
