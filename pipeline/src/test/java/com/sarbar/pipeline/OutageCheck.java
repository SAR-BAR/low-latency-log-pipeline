package com.sarbar.pipeline;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.sarbar.pipeline.alerts.AlertEvaluator;
import com.sarbar.pipeline.alerts.WebhookSender;
import com.sarbar.pipeline.engine.Bulkhead;
import com.sarbar.pipeline.engine.CircuitBreaker;
import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.engine.Retry;
import com.sarbar.pipeline.processor.Processor;
import com.sarbar.pipeline.ring.LogRing;
import com.sarbar.pipeline.storage.CheckpointTracker;
import com.sarbar.pipeline.storage.ClickHouseWriter;
import com.sarbar.pipeline.storage.StorageBatcher;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class OutageCheck {

    private static final String CLICKHOUSE_URL = "http://localhost:8123";
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {
        System.out.println("=== ClickHouse Outage Check ===\n");

        System.out.println("[setup] Verifying ClickHouse is reachable...");
        truncateEvents();
        System.out.println("[setup] ClickHouse is up. Events table truncated.\n");

        Engine engine = new Engine();
        Processor processor = new Processor();
        CheckpointTracker checkpoint = new CheckpointTracker(64);
        Bulkhead bulkhead = new Bulkhead(8);
        Retry storageRetry = new Retry(3, 200, e -> e instanceof IOException);
        CircuitBreaker storageBreaker = new CircuitBreaker();
        ClickHouseWriter writer = new ClickHouseWriter(CLICKHOUSE_URL, bulkhead, storageRetry, storageBreaker, processor.services(), processor.templates(), processor.words(), checkpoint, 10_000);
        StorageBatcher batcher = new StorageBatcher(engine, batch -> {
            try {
                writer.insertBatch(batch);
            } catch (Exception e) {
                throw e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
        });

        Retry alertRetry = new Retry(2, 500, e -> e instanceof IOException);
        CircuitBreaker alertBreaker = new CircuitBreaker();
        WebhookSender webhook = new WebhookSender("http://localhost:19999/noop", alertRetry, alertBreaker);
        AlertEvaluator alertEvaluator = new AlertEvaluator(engine, webhook, processor.services(), 256);

        LogRing ring = new LogRing(1024, new YieldingWaitStrategy(), (event, sequence, endOfBatch) -> {}, processor, batcher, alertEvaluator);
        ring.start();
        System.out.println("[setup] Pipeline started: ring=1024\n");

        AtomicLong sent = new AtomicLong();
        AtomicBoolean producerRunning = new AtomicBoolean(true);

        // A steady producer: ~500 logs/sec (2ms between logs)
        Thread producerThread = Thread.ofPlatform().name("steady-producer").start(() -> {
            while (producerRunning.get()) {
                long seq = sent.incrementAndGet();
                String line = TS.format(Instant.now()) + " [outage-check] INFO test-service - Log message number " + seq + " for outage check";
                byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                ring.publish(bytes, 0, bytes.length, 0, seq);
                try { Thread.sleep(2); } catch (InterruptedException e) { break; }
            }
        });

        // --- Phase 1: steady state (5 seconds) ---
        System.out.println("[phase 1] Running with ClickHouse healthy for 5 seconds...");
        for (int i = 1; i <= 5; i++) {
            Thread.sleep(1_000);
            long count = queryCountSafe();
            System.out.println("[phase 1]   t+" + i + "s — sent=" + sent.get() + ", clickhouse.count=" + count + ", breaker=" + storageBreaker.state() + ", checkpoint=" + checkpoint.checkpoint());
        }
        long countAfterPhase1 = queryCount();
        check(countAfterPhase1 > 0, "phase 1: expected some rows in ClickHouse, got 0");
        System.out.println("[phase 1] PASS — logs flowing to ClickHouse.\n");

        // --- Phase 2: kill ClickHouse, keep sending ---
        System.out.println("[phase 2] Stopping ClickHouse...");
        docker("stop", "clickhouse");
        System.out.println("[phase 2] ClickHouse stopped. Producer keeps sending...");

        for (int i = 1; i <= 15; i++) {
            Thread.sleep(1_000);
            System.out.println("[phase 2]   t+" + i + "s — sent=" + sent.get() + ", breaker=" + storageBreaker.state() + ", checkpoint=" + checkpoint.checkpoint() + ", batchPool.available=" + batcher.pool().available() + ", retrying=" + batcher.retryCount());
        }
        System.out.println("[phase 2] Storage breaker: " + storageBreaker.state() + "\n");

        // --- Phase 3: restart ClickHouse ---
        System.out.println("[phase 3] Starting ClickHouse...");
        docker("start", "clickhouse");
        System.out.println("[phase 3] ClickHouse started. Waiting for recovery...");

        for (int i = 1; i <= 15; i++) {
            Thread.sleep(1_000);
            long count = queryCountSafe();
            System.out.println("[phase 3]   t+" + i + "s — sent=" + sent.get() + ", breaker=" + storageBreaker.state() + ", checkpoint=" + checkpoint.checkpoint() + ", clickhouse.count=" + count + ", retrying=" + batcher.retryCount());
        }
        System.out.println("[phase 3] Storage breaker: " + storageBreaker.state() + "\n");

        // --- Phase 4: stop producer, drain, verify ---
        System.out.println("[phase 4] Stopping producer...");
        producerRunning.set(false);
        producerThread.join(5_000);
        long totalSent = sent.get();
        System.out.println("[phase 4] Total logs sent: " + totalSent);

        System.out.println("[phase 4] Draining pipeline...");
        ring.close();
        batcher.close();
        Thread.sleep(5_000);

        long finalCount = queryCount();
        System.out.println("[phase 4] ClickHouse final count: " + finalCount);
        System.out.println("[phase 4] Final checkpoint: " + checkpoint.checkpoint());
        System.out.println("[phase 4] Pending ranges: " + checkpoint.pendingRanges());

        engine.shutdown();

        // Verdict
        System.out.println("\n=== Results ===");
        System.out.println("Sent:     " + totalSent);
        System.out.println("Stored:   " + finalCount);
        long lost = totalSent - finalCount;
        System.out.println("Lost:     " + lost);

        if (finalCount >= totalSent) {
            System.out.println("\n[PASS] Zero data loss. Every log made it to ClickHouse.");
        } else {
            System.out.println("\n[FAIL] " + lost + " logs lost!");
            System.exit(1);
        }
    }

    private static long queryCount() throws Exception {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(CLICKHOUSE_URL + "/?query=" + encode("SELECT count() FROM logs.events"))).GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new IOException("ClickHouse query failed: " + resp.body());
        return Long.parseLong(resp.body().trim());
    }

    private static long queryCountSafe() {
        try { return queryCount(); } catch (Exception e) { return -1; }
    }

    private static void truncateEvents() throws Exception {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(CLICKHOUSE_URL + "/?query=" + encode("TRUNCATE TABLE logs.events"))).POST(HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new IOException("Truncate failed: " + resp.body());
    }

    private static void docker(String action, String service) throws Exception {
        int exit = new ProcessBuilder("docker", "compose", action, service).inheritIO().start().waitFor();
        if (exit != 0) throw new RuntimeException("docker compose " + action + " " + service + " failed with exit code " + exit);
    }

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
