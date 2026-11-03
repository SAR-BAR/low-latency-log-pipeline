package com.sarbar.pipeline.alerts;

import com.lmax.disruptor.EventHandler;
import com.sarbar.pipeline.engine.Engine;
import com.sarbar.pipeline.engine.Lane;
import com.sarbar.pipeline.engine.LaneFullException;
import com.sarbar.pipeline.processor.NameRegistry;
import com.sarbar.pipeline.ring.LogEvent;

import java.time.Instant;

public final class AlertEvaluator implements EventHandler<LogEvent> {

    static final long COOLDOWN_NANOS = 5L * 60 * 1_000_000_000L;
    static final long DEADLINE_SECONDS = 3;

    private final Engine engine;
    private final WebhookSender webhook;
    private final NameRegistry serviceNames;
    private final long[] lastAlertNanos;

    public AlertEvaluator(Engine engine, WebhookSender webhook, NameRegistry serviceNames, int maxServices) {
        this.engine = engine;
        this.webhook = webhook;
        this.serviceNames = serviceNames;
        this.lastAlertNanos = new long[maxServices];
    }

    @Override
    public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.errorSpike()) return;

        int serviceId = event.serviceId();
        if (serviceId < 0) return;

        long now = System.nanoTime();
        if (lastAlertNanos[serviceId] != 0 && now - lastAlertNanos[serviceId] < COOLDOWN_NANOS) return;
        lastAlertNanos[serviceId] = now;

        String service = serviceNames.name(serviceId);
        long timestampMillis = event.timestampMillis();

        try {
            engine.submit(Lane.ALERT, Instant.now().plusSeconds(DEADLINE_SECONDS), () -> {
                webhook.send(service, timestampMillis);
                return null;
            });
        } catch (LaneFullException ignored) {
        }
    }
}
