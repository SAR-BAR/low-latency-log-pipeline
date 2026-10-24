package com.sarbar.pipeline.storage;

public final class ColumnarBatch {

    private final int capacity;
    private final long[] timestamps;
    private final int[] serviceIds;
    private final int[] levels;
    private final int[] templateIds;
    private final long[] sequences;
    private final byte[][] messages;
    private int count;
    private long firstAddedNanos;

    public ColumnarBatch(int capacity) {
        this.capacity = capacity;
        this.timestamps = new long[capacity];
        this.serviceIds = new int[capacity];
        this.levels = new int[capacity];
        this.templateIds = new int[capacity];
        this.sequences = new long[capacity];
        this.messages = new byte[capacity][];
    }

    public void addRow(long timestamp, int serviceId, int level, int templateId, long sequence, byte[] rawBytes, int messageOffset, int length) {
        if (count == 0) firstAddedNanos = System.nanoTime();
        int i = count++;
        timestamps[i] = timestamp;
        serviceIds[i] = serviceId;
        levels[i] = level;
        templateIds[i] = templateId;
        sequences[i] = sequence;
        messages[i] = new byte[length - messageOffset];
        System.arraycopy(rawBytes, messageOffset, messages[i], 0, messages[i].length);
    }

    public void reset() {
        for (int i = 0; i < count; i++) messages[i] = null;
        count = 0;
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
    public byte[] message(int i) { return messages[i]; }

    public String dedupToken() {
        return "batch-" + sequences[0] + "-" + sequences[count - 1];
    }
}
