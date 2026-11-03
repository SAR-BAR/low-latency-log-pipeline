package com.sarbar.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class PipelineConfig {

    private final Properties props;

    private PipelineConfig(Properties props) {
        this.props = props;
    }

    public static PipelineConfig load(Path file) throws IOException {
        Properties props = new Properties();
        try (var in = Files.newInputStream(file)) {
            props.load(in);
        }
        return new PipelineConfig(props);
    }

    public static PipelineConfig defaults() {
        return new PipelineConfig(new Properties());
    }

    public String clickhouseUrl() { return get("clickhouse.url", "http://localhost:8123"); }
    public String webhookUrl() { return get("webhook.url", "http://localhost:8080/alert"); }
    public String shmDir() { return get("shm.dir", "data/shm"); }

    public int ringSize() { return getInt("ring.size", 65536); }
    public int batchSize() { return getInt("batch.size", 5000); }
    public int batchFlushMs() { return getInt("batch.flush.ms", 100); }
    public int batchPoolSize() { return getInt("batch.pool.size", 3); }

    public int bulkheadPermits() { return getInt("bulkhead.permits", 8); }
    public int storageDeadlineSeconds() { return getInt("storage.deadline.seconds", 5); }
    public int alertDeadlineSeconds() { return getInt("alert.deadline.seconds", 3); }
    public int alertCooldownSeconds() { return getInt("alert.cooldown.seconds", 300); }

    public int maxServices() { return getInt("max.services", 256); }
    public int maxTemplates() { return getInt("max.templates", 10000); }

    private String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    private int getInt(String key, int defaultValue) {
        String value = props.getProperty(key);
        return value != null ? Integer.parseInt(value.trim()) : defaultValue;
    }
}
