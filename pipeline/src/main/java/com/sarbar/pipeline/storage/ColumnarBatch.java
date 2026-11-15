package com.sarbar.pipeline.storage;

import com.sarbar.pipeline.ring.LogEvent;

public final class ColumnarBatch {

    private final int capacity;
    private final long[] timestamps;
    private final int[] serviceIds;
    private final int[] levels;
    private final int[] templateIds;
    private final long[] sequences;
    private final byte[] messageBuffer;
    private final int[] msgOffsets;
    private final int[] msgLengths;
    private int count;
    private int msgCursor;
    private long firstAddedNanos;

    public ColumnarBatch(int capacity) {
        this.capacity = capacity;
        this.timestamps = new long[capacity];
        this.serviceIds = new int[capacity];
        this.levels = new int[capacity];
        this.templateIds = new int[capacity];
        this.sequences = new long[capacity];
        this.messageBuffer = new byte[capacity * LogEvent.MAX_LOG_BYTES];
        this.msgOffsets = new int[capacity];
        this.msgLengths = new int[capacity];
    }

    public void addRow(long timestamp, int serviceId, int level, int templateId, long sequence, byte[] rawBytes, int messageOffset, int length) {
        if (count == 0) firstAddedNanos = System.nanoTime();
        int i = count++;
        timestamps[i] = timestamp;
        serviceIds[i] = serviceId;
        levels[i] = level;
        templateIds[i] = templateId;
        sequences[i] = sequence;
        int msgLen = length - messageOffset;
        msgOffsets[i] = msgCursor;
        msgLengths[i] = msgLen;
        System.arraycopy(rawBytes, messageOffset, messageBuffer, msgCursor, msgLen);
        msgCursor += msgLen;
    }

    public void reset() {
        count = 0;
        msgCursor = 0;
        firstAddedNanos = 0;
    }

    public boolean isFull() { return count >= capacity; }
    public int count() { return count; }
    public boolean isEmpty() { return count == 0; }
    public long firstAddedNanos() { return firstAddedNanos; }
    public long timestamp(int i) { return timestamps[i]; }
    public int serviceId(int i) { return serviceIds[i]; }
    public int level(int i) { return levels[i]; }
    public int templateId(int i) { return templateIds[i]; }
    public long sequence(int i) { return sequences[i]; }
    public byte[] messageBuffer() { return messageBuffer; }
    public int messageOffset(int i) { return msgOffsets[i]; }
    public int messageLength(int i) { return msgLengths[i]; }

    public String dedupToken(String instanceId) {
        return instanceId + "-" + sequences[0] + "-" + sequences[count - 1];
    }
}
