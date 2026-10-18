package com.sarbar.agent.shm;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** A memory-mapped mailbox file: header, then a ring of fixed-size boxes. One writer process, one reader process. */
public final class Mailbox implements AutoCloseable {

    static final int MAGIC = 0x4C4C504D; // "LLPM"

    static final long MAGIC_OFFSET = 0;
    static final long BOX_COUNT_OFFSET = 4;
    static final long BOX_SIZE_OFFSET = 8;
    // Each position gets its own 128-byte cache line so the two processes don't slow each other down.
    static final long WRITE_POSITION_OFFSET = 128;
    static final long READ_POSITION_OFFSET = 256;
    static final long BOXES_OFFSET = 384;

    /** Box layout: [int length][int unused][long sequence][bytes]. */
    public static final int BOX_HEADER_BYTES = 16;

    private static final VarHandle INT = ValueLayout.JAVA_INT.varHandle();
    private static final VarHandle LONG = ValueLayout.JAVA_LONG.varHandle();

    private final Arena arena;
    private final MemorySegment segment;
    private final int boxCount;
    private final int boxSize;

    private Mailbox(Arena arena, MemorySegment segment, int boxCount, int boxSize) {
        this.arena = arena;
        this.segment = segment;
        this.boxCount = boxCount;
        this.boxSize = boxSize;
    }

    /** The mailbox file of one simulator inside the shared directory; the analyzer finds files by this name. */
    public static Path file(Path directory, int sourceId) {
        return directory.resolve("source-" + sourceId + ".shm");
    }

    /** Creates a fresh mailbox file, replacing any old one. */
    public static Mailbox create(Path file, int boxCount, int boxSize) throws IOException {
        if (Integer.bitCount(boxCount) != 1) throw new IllegalArgumentException("boxCount must be a power of 2: " + boxCount);
        if (boxSize <= BOX_HEADER_BYTES || boxSize % 8 != 0) throw new IllegalArgumentException("boxSize must be a multiple of 8 above " + BOX_HEADER_BYTES + ": " + boxSize);
        Files.createDirectories(file.toAbsolutePath().getParent());
        Files.deleteIfExists(file);
        Arena arena = Arena.ofShared();
        MemorySegment s = map(file, arena, BOXES_OFFSET + (long) boxCount * boxSize, true);
        s.set(ValueLayout.JAVA_INT, BOX_COUNT_OFFSET, boxCount);
        s.set(ValueLayout.JAVA_INT, BOX_SIZE_OFFSET, boxSize);
        LONG.setRelease(s, WRITE_POSITION_OFFSET, 0L);
        LONG.setRelease(s, READ_POSITION_OFFSET, 0L);
        INT.setRelease(s, MAGIC_OFFSET, MAGIC); // last: a reader never accepts a half-created file
        return new Mailbox(arena, s, boxCount, boxSize);
    }

    /** Opens a mailbox created by {@link #create}; fails if the file is not (yet) a complete mailbox. */
    public static Mailbox open(Path file) throws IOException {
        long size = Files.size(file);
        if (size < BOXES_OFFSET) throw new IOException("not a mailbox (too small): " + file);
        Arena arena = Arena.ofShared();
        MemorySegment s = map(file, arena, size, false);
        int boxCount = s.get(ValueLayout.JAVA_INT, BOX_COUNT_OFFSET);
        int boxSize = s.get(ValueLayout.JAVA_INT, BOX_SIZE_OFFSET);
        if ((int) INT.getAcquire(s, MAGIC_OFFSET) != MAGIC || size != BOXES_OFFSET + (long) boxCount * boxSize) {
            arena.close();
            throw new IOException("not a complete mailbox: " + file);
        }
        return new Mailbox(arena, s, boxCount, boxSize);
    }

    private static MemorySegment map(Path file, Arena arena, long size, boolean create) throws IOException {
        StandardOpenOption[] options = create
                ? new StandardOpenOption[] {StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE}
                : new StandardOpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(file, options)) {
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
        } catch (IOException | RuntimeException e) {
            arena.close();
            throw e;
        }
    }

    MemorySegment segment() {
        return segment;
    }

    public int boxCount() {
        return boxCount;
    }

    public int maxPayload() {
        return boxSize - BOX_HEADER_BYTES;
    }

    long boxOffset(long position) {
        return BOXES_OFFSET + (position & (boxCount - 1)) * boxSize;
    }

    long writePosition() {
        return (long) LONG.getAcquire(segment, WRITE_POSITION_OFFSET);
    }

    void publishWritePosition(long position) {
        LONG.setRelease(segment, WRITE_POSITION_OFFSET, position);
    }

    long readPosition() {
        return (long) LONG.getAcquire(segment, READ_POSITION_OFFSET);
    }

    void publishReadPosition(long position) {
        LONG.setRelease(segment, READ_POSITION_OFFSET, position);
    }

    @Override
    public void close() {
        arena.close();
    }
}
