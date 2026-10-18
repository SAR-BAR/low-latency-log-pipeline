package com.sarbar.agent.shm;

import com.sarbar.agent.sim.LogSink;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

/** The simulator's side of a mailbox: copies each log into the next free box and waits when all boxes are full. */
public final class ShmLogWriter implements LogSink {

    private final Mailbox mailbox;
    private final MemorySegment segment;
    private final long boxCount;
    private long writePosition;
    private long cachedReadPosition;

    public ShmLogWriter(Path file, int boxCount, int boxSize) throws IOException {
        this.mailbox = Mailbox.create(file, boxCount, boxSize);
        this.segment = mailbox.segment();
        this.boxCount = boxCount;
    }

    @Override
    public void write(byte[] line, int length, long sequence) {
        if (length > mailbox.maxPayload()) throw new IllegalArgumentException("log of " + length + " bytes exceeds box payload " + mailbox.maxPayload());
        // Re-read the reader's position only when the cached one says "full": saves a cross-core read per log.
        while (writePosition - cachedReadPosition >= boxCount) {
            cachedReadPosition = mailbox.readPosition();
            if (writePosition - cachedReadPosition >= boxCount) Thread.onSpinWait();
        }
        long box = mailbox.boxOffset(writePosition);
        MemorySegment.copy(line, 0, segment, ValueLayout.JAVA_BYTE, box + Mailbox.BOX_HEADER_BYTES, length);
        segment.set(ValueLayout.JAVA_INT, box, length);
        segment.set(ValueLayout.JAVA_LONG, box + 8, sequence);
        mailbox.publishWritePosition(++writePosition); // release: the reader sees the whole box once it sees this
    }

    @Override
    public void close() {
        mailbox.close();
    }
}
