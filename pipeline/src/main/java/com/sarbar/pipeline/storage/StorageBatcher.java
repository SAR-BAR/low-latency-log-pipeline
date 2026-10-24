package com.sarbar.pipeline.storage;

import com.lmax.disruptor.EventHandler;
import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.engine.Lane;
import com.sarbar.pipeline.engine.LaneFullException;
import com.sarbar.pipeline.ring.LogEvent;

import java.time.Instant;
import java.util.function.Consumer;

public final class StorageBatcher implements EventHandler<LogEvent> {

    static final int BATCH_SIZE = 5_000;
    static final long FLUSH_INTERVAL_NANOS = 100_000_000L;
    static final long DEADLINE_SECONDS = 5;
    static final int POOL_SIZE = 3;

    private final Engine engine;
    private final BatchPool pool;
    private final Consumer<ColumnarBatch> insertJob;
    private ColumnarBatch batch;

    public StorageBatcher(Engine engine, Consumer<ColumnarBatch> insertJob) {
        this.engine = engine;
        this.pool = new BatchPool(POOL_SIZE, BATCH_SIZE);
        this.insertJob = insertJob;
        try {
            this.batch = pool.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.level() == LogEvent.UNPARSED) return;

        batch.addRow(event.timestampMillis(), event.serviceId(), event.level(), event.templateId(), sequence, event.bytes(), event.messageOffset(), event.length());

        if (batch.isFull()) {
            flush();
            return;
        }
        if (endOfBatch && !batch.isEmpty() && System.nanoTime() - batch.firstAddedNanos() >= FLUSH_INTERVAL_NANOS) {
            flush();
        }
    }

    private void flush() throws InterruptedException {
        ColumnarBatch toSend = batch;
        try {
            engine.submit(Lane.STORAGE, Instant.now().plusSeconds(DEADLINE_SECONDS), () -> {
                insertJob.accept(toSend);
                return null;
            }).whenComplete((result, error) -> pool.release(toSend));
        } catch (LaneFullException e) {
            pool.release(toSend);
        }
        batch = pool.acquire();
    }

    public BatchPool pool() { return pool; }
}
