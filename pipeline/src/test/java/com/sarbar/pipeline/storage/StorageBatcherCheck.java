package com.sarbar.pipeline.storage;

import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.ring.LogEvent;
import com.sarbar.pipeline.ring.LogRing;

import com.lmax.disruptor.BusySpinWaitStrategy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class StorageBatcherCheck {

    public static void main(String[] args) throws Exception {
        flushOnBatchSize();
        flushOnTime();
        batchContentsCorrect();
        dedupToken();
        System.out.println("\nAll storage batcher checks passed.");
    }

    /** 5,000 events should trigger exactly one flush. */
    private static void flushOnBatchSize() throws Exception {
        Engine engine = new Engine();
        CountDownLatch flushed = new CountDownLatch(1);
        CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

        StorageBatcher batcher = new StorageBatcher(engine, batch -> {
            batchSizes.add(batch.count());
            flushed.countDown();
        });

        LogRing ring = noOpRing(batcher);
        ring.start();

        byte[] line = makeLine("2026-10-23 08:00:00.000 [w-1] INFO payment-service - hello world");
        for (int i = 0; i < StorageBatcher.BATCH_SIZE; i++) {
            ring.publish(line, 0, line.length, 1, i);
        }

        check(flushed.await(5, TimeUnit.SECONDS), "flush on size: batch should have flushed within 5s");
        check(batchSizes.getFirst() == StorageBatcher.BATCH_SIZE, "flush on size: expected " + StorageBatcher.BATCH_SIZE + " rows, got " + batchSizes.getFirst());
        ring.close();
        engine.shutdown();
        System.out.println("[PASS] flush on batch size");
    }

    /** A partial batch should flush after 100ms. */
    private static void flushOnTime() throws Exception {
        Engine engine = new Engine();
        CountDownLatch flushed = new CountDownLatch(1);
        CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

        StorageBatcher batcher = new StorageBatcher(engine, batch -> {
            batchSizes.add(batch.count());
            flushed.countDown();
        });

        LogRing ring = noOpRing(batcher);
        ring.start();

        byte[] line = makeLine("2026-10-23 08:00:00.000 [w-1] ERROR payment-service - timeout");
        for (int i = 0; i < 10; i++) {
            ring.publish(line, 0, line.length, 1, i);
        }

        // The 100ms timer won't fire until a new event arrives after the interval.
        // Send one more event after sleeping to trigger the endOfBatch check.
        Thread.sleep(150);
        ring.publish(line, 0, line.length, 1, 10);

        check(flushed.await(5, TimeUnit.SECONDS), "flush on time: batch should have flushed within 5s");
        check(batchSizes.getFirst() == 11, "flush on time: expected 11 rows, got " + batchSizes.getFirst());
        ring.close();
        engine.shutdown();
        System.out.println("[PASS] flush on time");
    }

    /** Verify the batch contains the correct parsed fields. */
    private static void batchContentsCorrect() throws Exception {
        Engine engine = new Engine();
        CountDownLatch flushed = new CountDownLatch(1);
        ColumnarBatch[] captured = new ColumnarBatch[1];

        StorageBatcher batcher = new StorageBatcher(engine, batch -> {
            captured[0] = batch;
            flushed.countDown();
            // hold the latch so the batch isn't released before we inspect it
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        });

        LogRing ring = noOpRing(batcher);
        ring.start();

        byte[] line = makeLine("2026-10-23 08:00:00.000 [w-1] ERROR payment-service - db connection lost");
        for (int i = 0; i < StorageBatcher.BATCH_SIZE; i++) {
            ring.publish(line, 0, line.length, 1, i);
        }

        check(flushed.await(5, TimeUnit.SECONDS), "contents: batch should have flushed");
        ColumnarBatch b = captured[0];
        check(b.level(0) == 4, "contents: level should be ERROR (4), got " + b.level(0));
        String msg = new String(b.message(0), StandardCharsets.UTF_8);
        check(msg.equals("db connection lost"), "contents: message should be 'db connection lost', got '" + msg + "'");
        ring.close();
        engine.shutdown();
        System.out.println("[PASS] batch contents correct");
    }

    /** Dedup token should contain the sequence range. */
    private static void dedupToken() {
        ColumnarBatch batch = new ColumnarBatch(10);
        byte[] line = makeLine("2026-10-23 08:00:00.000 [w-1] INFO payment-service - test");
        batch.addRow(0, 0, 2, 0, 100, line, 51, line.length);
        batch.addRow(0, 0, 2, 0, 101, line, 51, line.length);
        batch.addRow(0, 0, 2, 0, 102, line, 51, line.length);
        String token = batch.dedupToken();
        check(token.equals("batch-100-102"), "dedup: expected 'batch-100-102', got '" + token + "'");
        System.out.println("[PASS] dedup token");
    }

    private static LogRing noOpRing(StorageBatcher batcher) {
        return new LogRing(1024, new BusySpinWaitStrategy(), (e, s, b) -> {}, new com.sarbar.pipeline.processor.Processor(), batcher, (e, s, b) -> {});
    }

    private static byte[] makeLine(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
