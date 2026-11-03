package com.sarbar.pipeline.storage;

import com.sarbar.pipeline.engine.Bulkhead;
import com.sarbar.pipeline.engine.CircuitBreaker;
import com.sarbar.pipeline.engine.Retry;
import com.sarbar.pipeline.processor.NameRegistry;
import com.sarbar.pipeline.processor.TemplateTree;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class ClickHouseWriter {

    private static final String[] LEVEL_NAMES = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final HttpClient http;
    private final URI insertEventsUri;
    private final URI insertTemplatesUri;
    private final Bulkhead bulkhead;
    private final Retry retry;
    private final CircuitBreaker breaker;
    private final NameRegistry serviceNames;
    private final TemplateTree templateTree;
    private final NameRegistry words;
    private final CheckpointTracker checkpoint;

    private final boolean[] templatesSent;

    public ClickHouseWriter(String baseUrl, Bulkhead bulkhead, Retry retry, CircuitBreaker breaker, NameRegistry serviceNames, TemplateTree templateTree, NameRegistry words, CheckpointTracker checkpoint, int maxTemplates) {
        this.http = HttpClient.newBuilder().build();
        this.insertEventsUri = URI.create(baseUrl + "/?query=" + encode("INSERT INTO logs.events FORMAT TabSeparated"));
        this.insertTemplatesUri = URI.create(baseUrl + "/?query=" + encode("INSERT INTO logs.templates FORMAT TabSeparated"));
        this.bulkhead = bulkhead;
        this.retry = retry;
        this.breaker = breaker;
        this.serviceNames = serviceNames;
        this.templateTree = templateTree;
        this.words = words;
        this.checkpoint = checkpoint;
        this.templatesSent = new boolean[maxTemplates];
    }

    public void insertBatch(ColumnarBatch batch) throws Exception {
        String dedupToken = batch.dedupToken();
        byte[] body = formatEvents(batch);
        long minSeq = batch.sequence(0);
        long maxSeq = batch.sequence(batch.count() - 1);

        bulkhead.call(() -> retry.call(() -> breaker.call(() -> {
            post(insertEventsUri, body, dedupToken);
            return null;
        })));

        sendNewTemplates(batch);
        checkpoint.complete(minSeq, maxSeq);
    }

    private byte[] formatEvents(ColumnarBatch batch) {
        StringBuilder sb = new StringBuilder(batch.count() * 200);
        for (int i = 0; i < batch.count(); i++) {
            sb.append(TS_FORMAT.format(Instant.ofEpochMilli(batch.timestamp(i))));
            sb.append('\t');
            sb.append(serviceNames.name(batch.serviceId(i)));
            sb.append('\t');
            sb.append(levelName(batch.level(i)));
            sb.append('\t');
            sb.append(batch.templateId(i));
            sb.append('\t');
            sb.append(batch.sequence(i));
            sb.append('\t');
            appendEscaped(sb, batch.message(i));
            sb.append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void sendNewTemplates(ColumnarBatch batch) {
        StringBuilder sb = null;
        String now = TS_FORMAT.format(Instant.now());
        for (int i = 0; i < batch.count(); i++) {
            int tid = batch.templateId(i);
            if (tid < 0 || tid >= templatesSent.length || templatesSent[tid]) continue;
            templatesSent[tid] = true;
            if (sb == null) sb = new StringBuilder();
            sb.append(tid);
            sb.append('\t');
            sb.append(serviceNames.name(batch.serviceId(i)));
            sb.append('\t');
            appendEscaped(sb, templateTree.text(tid, words).getBytes(StandardCharsets.UTF_8));
            sb.append('\t');
            sb.append(now);
            sb.append('\n');
        }
        if (sb == null) return;
        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        try {
            post(insertTemplatesUri, body, null);
        } catch (Exception e) {
        }
    }

    private void post(URI uri, byte[] body, String dedupToken) throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "text/plain");
        if (dedupToken != null) {
            builder.header("X-ClickHouse-Deduplication-Id", dedupToken);
        }
        HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("ClickHouse returned " + response.statusCode() + ": " + response.body());
        }
    }

    private static String levelName(int level) {
        if (level >= 0 && level < LEVEL_NAMES.length) return LEVEL_NAMES[level];
        return "INFO";
    }

    private static void appendEscaped(StringBuilder sb, byte[] bytes) {
        for (byte b : bytes) {
            char c = (char) (b & 0xFF);
            if (c == '\t') { sb.append("\\t"); }
            else if (c == '\n') { sb.append("\\n"); }
            else if (c == '\\') { sb.append("\\\\"); }
            else { sb.append(c); }
        }
    }

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
