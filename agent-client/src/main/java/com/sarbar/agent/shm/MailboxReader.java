package com.sarbar.agent.shm;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

/** The analyzer's side of a mailbox: hands each published log to a handler, then frees the boxes. */
public final class MailboxReader implements AutoCloseable {

    @FunctionalInterface
    public interface Handler {
        /** {@code bytes} is reused after this call returns, so copy what you need. */
        void onLog(byte[] bytes, int length, long sequence);
    }

    private final Mailbox mailbox;
    private final MemorySegment segment;
    private final byte[] scratch;
    private long readPosition;
    private long corruptBoxes;

    public MailboxReader(Path file) throws IOException {
        this.mailbox = Mailbox.open(file);
        this.segment = mailbox.segment();
        this.scratch = new byte[mailbox.maxPayload()];
        this.readPosition = mailbox.readPosition(); // resume where a previous reader stopped
    }

    /** Handles up to {@code max} waiting logs and returns how many boxes were consumed. */
    public int poll(Handler handler, int max) {
        long end = Math.min(mailbox.writePosition(), readPosition + max);
        int consumed = 0;
        for (long p = readPosition; p < end; p++) {
            long box = mailbox.boxOffset(p);
            int length = segment.get(ValueLayout.JAVA_INT, box);
            consumed++;
            if (length < 0 || length > scratch.length) {
                corruptBoxes++; // never trust the writer: skip a box with an impossible length
                continue;
            }
            long sequence = segment.get(ValueLayout.JAVA_LONG, box + 8);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, box + Mailbox.BOX_HEADER_BYTES, scratch, 0, length);
            handler.onLog(scratch, length, sequence);
        }
        if (consumed > 0) {
            readPosition = end;
            mailbox.publishReadPosition(end); // release: the writer may now reuse these boxes
        }
        return consumed;
    }

    public long corruptBoxes() {
        return corruptBoxes;
    }

    @Override
    public void close() {
        mailbox.close();
    }
}
