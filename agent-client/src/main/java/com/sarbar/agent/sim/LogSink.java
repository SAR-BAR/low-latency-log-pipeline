package com.sarbar.agent.sim;

import java.io.IOException;

/** Where the simulator sends each log line: a file or stdout for now, the shared mailbox file later. */
public interface LogSink extends AutoCloseable {

    /**
     * @param line     the line's bytes, without a trailing newline; only valid during this call
     * @param length   how many bytes of {@code line} to send
     * @param sequence this simulator's number for the line: 0, 1, 2, ... with no gaps
     */
    void write(byte[] line, int length, long sequence) throws IOException;

    @Override
    void close() throws IOException;
}
