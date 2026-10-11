package com.sarbar.pipeline.processor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Gives every service name a small number (0, 1, 2, ...) so the rest of the pipeline works with
 * ints instead of Strings.
 *
 * <p>Keeps a short list of known names as bytes and compares the incoming bytes with each one.
 * Looking up a known name creates no objects; only the first sighting of a new service does.
 * A plain list is enough for tens of services; switch to a hash table only if measurements say so.
 *
 * <p>Written by the processor thread only. Workers that run after the processor may call
 * {@link #name(int)}: the processor registers a name before it finishes the slot, and the ring's
 * sequence hand-off makes that write visible to them.
 */
public final class ServiceRegistry {

    /** Returned when the registry is full; the caller treats the service as unknown. */
    public static final int TOO_MANY = -1;

    private final byte[][] nameBytes;
    private final String[] names;
    private int count;

    public ServiceRegistry(int maxServices) {
        this.nameBytes = new byte[maxServices][];
        this.names = new String[maxServices];
    }

    /** The id for the ASCII name in {@code b[from, to)}, registering it if new; {@link #TOO_MANY} if full. */
    public int idOf(byte[] b, int from, int to) {
        for (int id = 0; id < count; id++) {
            if (Arrays.equals(nameBytes[id], 0, nameBytes[id].length, b, from, to)) {
                return id;
            }
        }
        if (count == nameBytes.length) {
            return TOO_MANY;
        }
        nameBytes[count] = Arrays.copyOfRange(b, from, to);
        names[count] = new String(nameBytes[count], StandardCharsets.US_ASCII);
        return count++;
    }

    /** The name for an id returned by {@link #idOf}. */
    public String name(int id) {
        return names[id];
    }

    public int size() {
        return count;
    }
}
