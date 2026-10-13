package com.sarbar.pipeline.processor;

/**
 * Drain, part 1: splits a log message into words and turns each word into a number.
 *
 * <pre>
 * message: Timeout connecting to 10.0.3.17 after 2500ms
 * words:   Timeout | connecting | to | 10.0.3.17 | after | 2500ms
 * ids:     12      | 13         | 14 | WILDCARD  | 15    | WILDCARD
 * </pre>
 *
 * <p>Words are not copied: each one is stored as its start and end position in the original
 * byte array. A word containing a digit is treated as a changing value and becomes
 * {@link #WILDCARD} (shown as {@code <*>}); every other word gets an id from the word
 * {@link NameRegistry}. All arrays are created once and reused for every message, so nothing
 * is allocated per log once all words have been seen.
 *
 * <p>Not thread-safe: one instance per processor thread. After {@link #tokenize}, the getters
 * describe that message until the next call.
 */
public final class MessageTokenizer {

    /** Id of a word that is a changing value, written as {@code <*>} in patterns. */
    public static final int WILDCARD = -1;

    /** Most words kept per message; anything after the last one is folded into it. */
    public static final int MAX_WORDS = 64;

    private final NameRegistry words;
    private final int[] starts = new int[MAX_WORDS];
    private final int[] ends = new int[MAX_WORDS];
    private final int[] ids = new int[MAX_WORDS];
    private int count;

    /**
     * @param words where word ids come from; when it is full, new words become {@link #WILDCARD}
     *              (its {@link NameRegistry#TOO_MANY} has the same value), which is a safe fallback
     */
    public MessageTokenizer(NameRegistry words) {
        this.words = words;
    }

    /**
     * Splits {@code b[from, to)} at spaces. Several spaces in a row count as one separator;
     * leading and trailing spaces are ignored.
     *
     * @return the number of words
     */
    public int tokenize(byte[] b, int from, int to) {
        count = 0;
        int i = from;
        while (i < to) {
            // Skip spaces before the next word.
            while (i < to && b[i] == ' ') i++;
            if (i == to) break;

            int start = i;
            int end;
            if (count == MAX_WORDS - 1) {
                // Last free place: the rest of the message becomes one final word.
                end = to;
                while (b[end - 1] == ' ') end--;
            } else {
                while (i < to && b[i] != ' ') i++;
                end = i;
            }

            starts[count] = start;
            ends[count] = end;
            ids[count] = containsDigit(b, start, end) ? WILDCARD : words.idOf(b, start, end);
            count++;
            i = end;
            if (end == to || count == MAX_WORDS) break;
        }
        return count;
    }

    public int count() {
        return count;
    }

    /** Word id, or {@link #WILDCARD}. */
    public int id(int index) {
        return ids[index];
    }

    /** Position of the word's first byte. */
    public int start(int index) {
        return starts[index];
    }

    /** Position just after the word's last byte. */
    public int end(int index) {
        return ends[index];
    }

    /** The word ids of the last message; only the first {@link #count()} entries are valid. Do not modify. */
    int[] ids() {
        return ids;
    }

    private static boolean containsDigit(byte[] b, int from, int to) {
        for (int i = from; i < to; i++) {
            if (b[i] >= '0' && b[i] <= '9') return true;
        }
        return false;
    }
}
