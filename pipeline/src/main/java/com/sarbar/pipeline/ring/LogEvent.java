package com.sarbar.pipeline.ring;

/**
 * One slot on the conveyor belt. The ring creates one instance per position at startup and
 * reuses it on every lap, so nothing is allocated per log.
 *
 * <p>Who writes what:
 * <ul>
 *   <li>Raw fields (bytes, source, receive time): the producer, before publishing.</li>
 *   <li>Parsed fields (timestamp, level, service, template): the processor. Consumers that run
 *       after the processor may read them; consumers running alongside it must not.</li>
 * </ul>
 */
public final class LogEvent {

    /** Largest log line accepted, in bytes. Longer lines are rejected before they reach the belt. */
    public static final int MAX_LOG_BYTES = 4096;

    /** Value of a parsed field that the processor has not filled in (yet). */
    public static final int UNPARSED = -1;

    // Raw fields, written by the producer.
    private final byte[] bytes = new byte[MAX_LOG_BYTES];
    private int length;
    private int sourceId;
    private long sourceSequence;
    private long receivedNanos;

    // Parsed fields, written by the processor.
    private long timestampMillis;
    private int level;
    private int serviceId;
    private int messageOffset;
    private int templateId;

    /**
     * Copies a log line into this slot and clears the parsed fields left over from the previous lap.
     *
     * @param sourceId       which simulator the log came from
     * @param sourceSequence the simulator's own number for this log (used to detect resends)
     * @param receivedNanos  {@link System#nanoTime()} when the log entered the analyzer
     */
    public void setRaw(byte[] src, int offset, int length, int sourceId, long sourceSequence, long receivedNanos) {
        if (length < 0 || length > MAX_LOG_BYTES) {
            throw new IllegalArgumentException("log length " + length + " outside 0.." + MAX_LOG_BYTES);
        }
        System.arraycopy(src, offset, bytes, 0, length);
        this.length = length;
        this.sourceId = sourceId;
        this.sourceSequence = sourceSequence;
        this.receivedNanos = receivedNanos;

        this.timestampMillis = UNPARSED;
        this.level = UNPARSED;
        this.serviceId = UNPARSED;
        this.messageOffset = UNPARSED;
        this.templateId = UNPARSED;
    }

    /** Called by the processor once it has parsed this log. */
    public void setParsed(long timestampMillis, int level, int serviceId, int messageOffset, int templateId) {
        this.timestampMillis = timestampMillis;
        this.level = level;
        this.serviceId = serviceId;
        this.messageOffset = messageOffset;
        this.templateId = templateId;
    }

    /** The backing array; only the first {@link #length()} bytes belong to this log. Do not keep a reference. */
    public byte[] bytes() {
        return bytes;
    }

    public int length() {
        return length;
    }

    public int sourceId() {
        return sourceId;
    }

    public long sourceSequence() {
        return sourceSequence;
    }

    public long receivedNanos() {
        return receivedNanos;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public int level() {
        return level;
    }

    public int serviceId() {
        return serviceId;
    }

    /** Index in {@link #bytes()} where the message text starts. */
    public int messageOffset() {
        return messageOffset;
    }

    public int templateId() {
        return templateId;
    }
}
