package com.sarbar.pipeline;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.sarbar.pipeline.alerts.AlertEvaluator;
import com.sarbar.pipeline.alerts.WebhookSender;
import com.sarbar.pipeline.engine.Bulkhead;
import com.sarbar.pipeline.engine.CircuitBreaker;
import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.engine.Retry;
import com.sarbar.pipeline.ingest.ShmIngestBridge;
import com.sarbar.pipeline.processor.Processor;
import com.sarbar.pipeline.ring.LogRing;
import com.sarbar.pipeline.storage.CheckpointTracker;
import com.sarbar.pipeline.storage.ClickHouseWriter;
import com.sarbar.pipeline.storage.StorageBatcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class PipelineMain {

    public static void main(String[] args) throws Exception {
        PipelineConfig config = args.length > 0
                ? PipelineConfig.load(Path.of(args[0]))
                : PipelineConfig.defaults();

        Engine engine = new Engine();
        Processor processor = new Processor();

        CheckpointTracker checkpoint = new CheckpointTracker(64);

        Bulkhead bulkhead = new Bulkhead(config.bulkheadPermits());
        Retry storageRetry = new Retry(3, 200, e -> e instanceof IOException);
        CircuitBreaker storageBreaker = new CircuitBreaker();

        ClickHouseWriter writer = new ClickHouseWriter(
                config.clickhouseUrl(), bulkhead, storageRetry, storageBreaker,
                processor.services(), processor.templates(), processor.words(),
                checkpoint, config.maxTemplates());

        StorageBatcher storageBatcher = new StorageBatcher(engine, batch -> {
            try {
                writer.insertBatch(batch);
            } catch (Exception e) {
                throw e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
        });

        Retry alertRetry = new Retry(2, 500, e -> e instanceof IOException);
        CircuitBreaker alertBreaker = new CircuitBreaker();
        WebhookSender webhook = new WebhookSender(config.webhookUrl(), alertRetry, alertBreaker);
        AlertEvaluator alertEvaluator = new AlertEvaluator(engine, webhook, processor.services(), config.maxServices());

        LogRing ring = new LogRing(
                config.ringSize(), new YieldingWaitStrategy(),
                (event, sequence, endOfBatch) -> {},
                processor, storageBatcher, alertEvaluator);
        ring.start();

        Path shmDir = Path.of(config.shmDir());
        Files.createDirectories(shmDir);
        ShmIngestBridge bridge = new ShmIngestBridge(shmDir, ring);
        Thread bridgeThread = Thread.ofPlatform().name("ingest-bridge").start(bridge);

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            System.out.println("Shutting down...");
            bridge.stop();
            try { bridgeThread.join(5000); } catch (InterruptedException ignored) {}
            ring.close();
            engine.shutdown();
            System.out.println("Shutdown complete.");
        }));

        System.out.println("Pipeline started: ring=" + config.ringSize() + ", shm=" + config.shmDir() + ", clickhouse=" + config.clickhouseUrl());
        bridgeThread.join();
    }
}
