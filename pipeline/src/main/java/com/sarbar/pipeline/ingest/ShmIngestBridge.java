package com.sarbar.pipeline.ingest;

import com.sarbar.agent.shm.MailboxReader;
import com.sarbar.pipeline.ring.LogRing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Platform thread that moves logs from every simulator's mailbox file (source-N.shm) onto the belt. */
public final class ShmIngestBridge implements Runnable {

    private static final Pattern FILE_NAME = Pattern.compile("source-(\\d+)\\.shm");
    private static final int BATCH_PER_MAILBOX = 256; // so one busy simulator can't starve the others
    private static final long RESCAN_NANOS = 1_000_000_000L;
    private static final long IDLE_NANOS = 10_000;

    private record Source(int id, MailboxReader reader, MailboxReader.Handler handler) {
    }

    private final Path directory;
    private final LogRing ring;
    private final List<Source> sources = new ArrayList<>();
    private final Set<Integer> knownIds = new HashSet<>();
    private volatile boolean running = true;

    public ShmIngestBridge(Path directory, LogRing ring) {
        this.directory = directory;
        this.ring = ring;
    }

    @Override
    public void run() {
        long nextScan = 0;
        while (running) {
            if (System.nanoTime() >= nextScan) {
                scan();
                nextScan = System.nanoTime() + RESCAN_NANOS;
            }
            if (pollAll() == 0) LockSupport.parkNanos(IDLE_NANOS);
        }
        while (pollAll() > 0) {
            // drain what is already in the mailboxes before stopping
        }
        for (Source s : sources) s.reader().close();
    }

    /** Stops after the mailboxes are drained; call {@code join()} on the bridge thread to wait for it. */
    public void stop() {
        running = false;
    }

    private int pollAll() {
        int moved = 0;
        for (int i = 0; i < sources.size(); i++) {
            Source s = sources.get(i);
            moved += s.reader().poll(s.handler(), BATCH_PER_MAILBOX);
        }
        return moved;
    }

    private void scan() {
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.toList()) {
                Matcher m = FILE_NAME.matcher(file.getFileName().toString());
                if (!m.matches()) continue;
                int id = Integer.parseInt(m.group(1));
                if (knownIds.contains(id)) continue;
                try {
                    MailboxReader reader = new MailboxReader(file);
                    sources.add(new Source(id, reader, (bytes, length, sequence) -> ring.publish(bytes, 0, length, id, sequence)));
                    knownIds.add(id);
                } catch (IOException notReadyYet) {
                    // the simulator is still creating the file; try again on the next scan
                }
            }
        } catch (IOException e) {
            // directory missing or unreadable: no simulators yet
        }
    }
}
