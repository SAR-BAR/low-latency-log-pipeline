package com.sarbar.pipeline.processor;

import com.lmax.disruptor.EventHandler;
import com.sarbar.pipeline.ring.LogEvent;

/** Belt worker: parses each log, finds its template and tracks error spikes, writing the results into the slot. */
public final class Processor implements EventHandler<LogEvent> {

    static final int MAX_SERVICES = 1024;
    static final int MAX_WORDS = 4096;
    static final int MAX_TEMPLATES = 4096;
    static final double SIMILARITY_THRESHOLD = 0.5;
    static final int MAX_GROUPS_PER_WORD_COUNT = 100;
    static final double SPIKE_FACTOR = 3.0;
    static final int MIN_SPIKE_ERRORS = 10;

    private final NameRegistry services = new NameRegistry(MAX_SERVICES);
    private final NameRegistry words = new NameRegistry(MAX_WORDS);
    private final LogParser parser = new LogParser(services);
    private final MessageTokenizer tokenizer = new MessageTokenizer(words);
    private final TemplateTree templates = new TemplateTree(MAX_TEMPLATES, SIMILARITY_THRESHOLD, MAX_GROUPS_PER_WORD_COUNT);
    private final ErrorRateMonitor errors = new ErrorRateMonitor(MAX_SERVICES, SPIKE_FACTOR, MIN_SPIKE_ERRORS);
    private long malformed;

    @Override
    public void onEvent(LogEvent event, long sequence, boolean endOfBatch) {
        byte[] b = event.bytes();
        int length = event.length();
        if (!parser.parse(b, length)) {
            malformed++; // parsed fields stay UNPARSED; the raw log still goes to the journal and storage
            return;
        }
        int wordCount = tokenizer.tokenize(b, parser.messageOffset(), length);
        int templateId = templates.match(tokenizer.ids(), wordCount);
        boolean spike = errors.onLog(parser.serviceId(), parser.timestampMillis(), parser.level() >= LogLevel.ERROR);
        event.setParsed(parser.timestampMillis(), parser.level(), parser.serviceId(), parser.messageOffset(), templateId, spike);
    }

    public NameRegistry services() {
        return services;
    }

    public NameRegistry words() {
        return words;
    }

    public TemplateTree templates() {
        return templates;
    }

    public ErrorRateMonitor errors() {
        return errors;
    }

    public long malformedCount() {
        return malformed;
    }
}
