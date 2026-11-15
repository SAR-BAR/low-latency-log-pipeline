package com.sarbar.pipeline.storage;

import com.lmax.disruptor.EventHandler;
import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.engine.Lane;
import com.sarbar.pipeline.engine.LaneFullException;
import com.sarbar.pipeline.processor.LogLevel;
import com.sarbar.pipeline.ring.LogEvent;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class StorageBatcher implements EventHandler<LogEvent> {

    static final int BATCH_SIZE = 5_000;
    static final long FLUSH_INTERVAL_NANOS = 100_000_000L;
    static final long DEADLINE_SECONDS = 5;
    static final int POOL_SIZE = 3;
    static final long RETRY_DELAY_MS = 1_000;

    private final Engine engine;
    private final BatchPool pool;
    private final Consumer<ColumnarBatch> insertJob;
    private final int unknownServiceId;
    private final AtomicInteger retryCount = new AtomicInteger();
    private final Object lock = new Object();
    private volatile boolean running = true;
    private ColumnarBatch batch;

    public StorageBatcher(Engine engine, Consumer<ColumnarBatch> insertJob, int unknownServiceId) {
        this.engine = engine;
        this.pool = new BatchPool(POOL_SIZE, BATCH_SIZE);
        this.insertJob = insertJob;
        this.unknownServiceId = unknownServiceId;
        try {
            this.batch = pool.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        Thread.ofVirtual().name("batch-flush-timer").start(this::flushTimer);
    }

    @Override
    public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
        long ts = event.timestampMillis();
        int serviceId = event.serviceId();
        int level = event.level();
        int templateId = event.templateId();
        int msgOffset = event.messageOffset();

        if (level == LogEvent.UNPARSED) {
            ts = System.currentTimeMillis();
            serviceId = unknownServiceId;
            level = LogLevel.INFO;
            templateId = 0;
            msgOffset = 0;
        }

        synchronized (lock) {
            batch.addRow(ts, serviceId, level, templateId, sequence, event.bytes(), msgOffset, event.length());

            if (batch.isFull()) {
                flush();
                return;
            }
            if (endOfBatch && !batch.isEmpty() && System.nanoTime() - batch.firstAddedNanos() >= FLUSH_INTERVAL_NANOS) {
                flush();
            }
        }
    }

    private void flushTimer() {
        while (running) {
            try {
                Thread.sleep(FLUSH_INTERVAL_NANOS / 1_000_000);
            } catch (InterruptedException e) {
                return;
            }
            synchronized (lock) {
                if (!batch.isEmpty() && System.nanoTime() - batch.firstAddedNanos() >= FLUSH_INTERVAL_NANOS) {
                    try {
                        flush();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

    private void flush() throws InterruptedException {
        ColumnarBatch toSend = batch;
        submitBatch(toSend);
        batch = pool.acquire();
    }

    private void submitBatch(ColumnarBatch toSend) {
        try {
            engine.submit(Lane.STORAGE, Instant.now().plusSeconds(DEADLINE_SECONDS), () -> {
                insertJob.accept(toSend);
                return null;
            }).whenComplete((result, error) -> {
                if (error == null) {
                    retryCount.decrementAndGet();
                    pool.release(toSend);
                } else {
                    scheduleRetry(toSend);
                }
            });
            retryCount.incrementAndGet();
        } catch (LaneFullException e) {
            scheduleRetry(toSend);
        }
    }

    private void scheduleRetry(ColumnarBatch batch) {
        retryCount.incrementAndGet();
        Thread.ofVirtual().name("batch-retry").start(() -> {
            try {
                Thread.sleep(RETRY_DELAY_MS);
                engine.submit(Lane.STORAGE, Instant.now().plusSeconds(DEADLINE_SECONDS), () -> {
                    insertJob.accept(batch);
                    return null;
                }).whenComplete((result, error) -> {
                    retryCount.decrementAndGet();
                    if (error == null) {
                        pool.release(batch);
                    } else {
                        scheduleRetry(batch);
                    }
                });
            } catch (LaneFullException e) {
                retryCount.decrementAndGet();
                scheduleRetry(batch);
            } catch (InterruptedException ignored) {
                retryCount.decrementAndGet();
            }
        });
    }

    public void close() {
        running = false;
        synchronized (lock) {
            if (batch != null && !batch.isEmpty()) {
                submitBatch(batch);
                batch = null;
            }
        }
    }

    public BatchPool pool() { return pool; }

    public int retryCount() { return retryCount.get(); }
}
