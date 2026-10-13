package com.sarbar.pipeline.processor;

import com.sarbar.agent.sim.LogLineGenerator;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Check for {@link MessageTokenizer}:
 * <ol>
 *   <li>Same result as a simple String-based reference ({@code split} + "contains a digit?")
 *       on 10,000 generated messages.</li>
 *   <li>Edge cases: extra spaces, empty message, more than {@link MessageTokenizer#MAX_WORDS} words.</li>
 *   <li>Allocation: after warm-up, less than 1 byte per message.</li>
 * </ol>
 * Exit code 0 = PASS, 1 = FAIL.
 */
public final class MessageTokenizerCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final int LINES = 10_000;

    public static void main(String[] args) {
        boolean matches = checkAgainstReference();
        boolean edges = checkEdgeCases();
        boolean allocation = checkAllocation();
        boolean pass = matches && edges && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    /** The slow, obvious version the fast one must agree with. */
    static List<String> reference(String message) {
        List<String> out = new ArrayList<>();
        for (String word : message.trim().split(" +")) {
            if (word.isEmpty()) continue;
            out.add(word.chars().anyMatch(Character::isDigit) ? "<*>" : word);
        }
        return out;
    }

    /** What the tokenizer produced, as Strings (for comparing and printing only). */
    static List<String> actual(MessageTokenizer tokenizer, NameRegistry words) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < tokenizer.count(); i++) {
            int id = tokenizer.id(i);
            out.add(id == MessageTokenizer.WILDCARD ? "<*>" : words.name(id));
        }
        return out;
    }

    private static boolean checkAgainstReference() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        NameRegistry words = new NameRegistry(4096);
        MessageTokenizer tokenizer = new MessageTokenizer(words);
        LogParser parser = new LogParser(new NameRegistry(16));
        byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];
        int failures = 0;

        for (int i = 0; i < LINES; i++) {
            int length = generator.generate(line, System.currentTimeMillis(), generator.randomService(), generator.randomLevel(25));
            if (!parser.parse(line, length)) {
                failures += fail("parser rejected: " + new String(line, 0, length, StandardCharsets.US_ASCII));
                continue;
            }
            String message = new String(line, parser.messageOffset(), length - parser.messageOffset(), StandardCharsets.US_ASCII);
            tokenizer.tokenize(line, parser.messageOffset(), length);
            List<String> expected = reference(message);
            List<String> got = actual(tokenizer, words);
            if (!expected.equals(got)) {
                failures += fail("\"" + message + "\"\n    expected " + expected + "\n    got      " + got);
            }
        }
        System.out.printf("reference: %,d messages, %,d failures, %d distinct words%n", LINES, failures, words.size());
        return failures == 0;
    }

    private static boolean checkEdgeCases() {
        NameRegistry words = new NameRegistry(4096);
        MessageTokenizer tokenizer = new MessageTokenizer(words);
        int failures = 0;

        String[] cases = {"", "   ", "  Cache   refreshed  for region eu-1 ", "single", "42", "a b  c   d"};
        for (String message : cases) {
            byte[] b = message.getBytes(StandardCharsets.US_ASCII);
            tokenizer.tokenize(b, 0, b.length);
            List<String> expected = reference(message);
            List<String> got = actual(tokenizer, words);
            if (!expected.equals(got)) failures += fail("\"" + message + "\": expected " + expected + ", got " + got);
        }

        // More words than fit: the last place holds the rest of the message.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MessageTokenizer.MAX_WORDS + 10; i++) sb.append("w ");
        byte[] longMessage = sb.toString().getBytes(StandardCharsets.US_ASCII);
        int n = tokenizer.tokenize(longMessage, 0, longMessage.length);
        int last = MessageTokenizer.MAX_WORDS - 1;
        int expectedLastEnd = longMessage.length - 1; // the trailing space is not part of the word
        if (n != MessageTokenizer.MAX_WORDS || tokenizer.end(last) != expectedLastEnd) {
            failures += fail("long message: count " + n + ", last word ends at " + tokenizer.end(last) + " (expected " + MessageTokenizer.MAX_WORDS + ", " + expectedLastEnd + ")");
        }

        System.out.printf("edge cases: %d cases + 1 long message, %d failures%n", cases.length, failures);
        return failures == 0;
    }

    private static boolean checkAllocation() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 1);
        LogParser parser = new LogParser(new NameRegistry(16));
        byte[][] lines = new byte[LINES][LogLineGenerator.MAX_LINE_BYTES];
        int[] messageStarts = new int[LINES];
        int[] lengths = new int[LINES];
        long now = System.currentTimeMillis();
        for (int i = 0; i < LINES; i++) {
            lengths[i] = generator.generate(lines[i], now + i, generator.randomService(), generator.randomLevel(5));
            parser.parse(lines[i], lengths[i]);
            messageStarts[i] = parser.messageOffset();
        }
        MessageTokenizer tokenizer = new MessageTokenizer(new NameRegistry(4096));

        long checksum = 0;
        for (int round = 0; round < 200; round++) { // warm-up; also registers every word once
            checksum += tokenizeAll(tokenizer, lines, messageStarts, lengths);
        }

        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        int rounds = 100;
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int round = 0; round < rounds; round++) {
            checksum += tokenizeAll(tokenizer, lines, messageStarts, lengths);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        long messages = (long) rounds * LINES;
        double perMessage = (double) allocated / messages;
        System.out.printf("allocation: %,d bytes over %,d messages = %.4f bytes/message (checksum %d)%n", allocated, messages, perMessage, checksum);
        return perMessage < 1.0;
    }

    /** Tokenizes every message once; the returned sum stops the JIT from removing the work as unused. */
    private static long tokenizeAll(MessageTokenizer tokenizer, byte[][] lines, int[] messageStarts, int[] lengths) {
        long sum = 0;
        for (int i = 0; i < lines.length; i++) {
            int n = tokenizer.tokenize(lines[i], messageStarts[i], lengths[i]);
            sum += n + tokenizer.id(0);
        }
        return sum;
    }

    private static int fail(String message) {
        System.out.println("  FAIL " + message);
        return 1;
    }
}
