package com.sarbar.pipeline.alerts;

import com.sarbar.pipeline.engine.CircuitBreaker;
import com.sarbar.pipeline.engine.Retry;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class WebhookSender {

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final HttpClient http;
    private final URI webhookUri;
    private final Retry retry;
    private final CircuitBreaker breaker;

    public WebhookSender(String webhookUrl, Retry retry, CircuitBreaker breaker) {
        this.http = HttpClient.newBuilder().build();
        this.webhookUri = URI.create(webhookUrl);
        this.retry = retry;
        this.breaker = breaker;
    }

    public void send(String service, long eventTimestampMillis) throws Exception {
        String body = "{\"service\":\"" + escapeJson(service)
                + "\",\"alert\":\"error_spike\""
                + ",\"event_ts\":\"" + TS_FORMAT.format(Instant.ofEpochMilli(eventTimestampMillis))
                + "\",\"fired_at\":\"" + TS_FORMAT.format(Instant.now())
                + "\"}";

        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        retry.call(() -> breaker.call(() -> {
            post(bytes);
            return null;
        }));
    }

    private void post(byte[] body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(webhookUri)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/json")
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Webhook returned " + response.statusCode() + ": " + response.body());
        }
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') sb.append("\\\"");
            else if (c == '\\') sb.append("\\\\");
            else sb.append(c);
        }
        return sb.toString();
    }
}
