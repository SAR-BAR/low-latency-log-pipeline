package com.sarbar.pipeline.storage;

import java.util.concurrent.ArrayBlockingQueue;

public final class BatchPool {

    private final ArrayBlockingQueue<ColumnarBatch> pool;

    public BatchPool(int poolSize, int batchCapacity) {
        this.pool = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.add(new ColumnarBatch(batchCapacity));
        }
    }

    /** Blocks if all batches are in flight. That's the backpressure path. */
    public ColumnarBatch acquire() throws InterruptedException {
        return pool.take();
    }

    public void release(ColumnarBatch batch) {
        batch.reset();
        pool.add(batch);
    }

    public int available() {
        return pool.size();
    }
}
