package com.sarbar.agent.sim;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Writes one line per log to a stream (a file or stdout), buffered. */
public final class StreamLogSink implements LogSink {

    private final OutputStream out;

    public StreamLogSink(OutputStream out) {
        this.out = new BufferedOutputStream(out, 64 * 1024);
    }

    @Override
    public void write(byte[] line, int length, long sequence) throws IOException {
        out.write(line, 0, length);
        out.write('\n');
    }

    @Override
    public void close() throws IOException {
        out.flush();
        out.close();
    }
}
