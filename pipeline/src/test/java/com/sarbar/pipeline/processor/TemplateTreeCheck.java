package com.sarbar.pipeline.processor;

import com.sarbar.agent.sim.LogLineGenerator;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Check for {@link TemplateTree}: agrees with a simple String-based reference, produces the 11
 * expected templates, handles edge cases, and allocates nothing per message. Exit code 0 = PASS.
 */
public final class TemplateTreeCheck {

    private static final String[] SERVICES = {"payment-service", "auth-service", "order-service", "inventory-service"};
    private static final int LINES = 10_000;
    private static final double THRESHOLD = 0.5;
    private static final int MAX_GROUPS = 100;

    private static final Set<String> EXPECTED_TEMPLATES = new TreeSet<>(List.of(
            "Fetched <*> rows from table orders in <*>",
            "User logged in successfully <*> <*>",
            "Order <*> created for customer <*> in <*>",
            "Cache refreshed for region <*> <*> entries",
            "Request timeout for API call <*> <*>",
            "Slow query took <*> on shard <*>",
            "Retrying payment <*> attempt <*> of <*>",
            "Database connection failed [exception=java.sql.SQLException: <*> <*>",
            "Database connection failed [exception=java.sql.SQLException: Invalid username or password]",
            "Payment <*> declined by gateway: insufficient funds",
            "Timeout connecting to <*> after <*>"));

    public static void main(String[] args) {
        boolean reference = checkAgainstReference();
        boolean cases = checkSmallCases();
        boolean allocation = checkAllocation();
        boolean pass = reference && cases && allocation;
        System.out.println(pass ? "PASS" : "FAIL");
        System.exit(pass ? 0 : 1);
    }

    /** Slow, obvious Drain with Strings and maps, following the same rules as {@link TemplateTree}. */
    static final class ReferenceDrain {
        private final Map<Integer, List<String>> firstWordsByCount = new HashMap<>();
        private final Map<String, List<Integer>> templateIdsByGroup = new HashMap<>();
        final List<List<String>> templates = new ArrayList<>();

        int match(List<String> words) {
            int count = words.size();
            String first = count == 0 ? "<*>" : words.get(0);
            List<String> firstWords = firstWordsByCount.computeIfAbsent(count, c -> new ArrayList<>());
            if (!firstWords.contains(first)) {
                if (firstWords.size() >= MAX_GROUPS && !first.equals("<*>")) first = "<*>";
                if (!firstWords.contains(first)) firstWords.add(first);
            }
            List<Integer> candidates = templateIdsByGroup.computeIfAbsent(count + "|" + first, k -> new ArrayList<>());

            int best = -1;
            double bestScore = -1;
            int bestWildcards = -1;
            for (int id : candidates) {
                List<String> t = templates.get(id);
                int equal = 0;
                int wildcards = 0;
                for (int i = 0; i < count; i++) {
                    if (t.get(i).equals(words.get(i))) equal++;
                    if (t.get(i).equals("<*>")) wildcards++;
                }
                double score = count == 0 ? 1.0 : (double) equal / count;
                if (score > bestScore || (score == bestScore && wildcards > bestWildcards)) {
                    best = id;
                    bestScore = score;
                    bestWildcards = wildcards;
                }
            }
            if (best >= 0 && bestScore >= THRESHOLD) {
                List<String> t = templates.get(best);
                for (int i = 0; i < count; i++) {
                    if (!t.get(i).equals(words.get(i))) t.set(i, "<*>");
                }
                return best;
            }
            templates.add(new ArrayList<>(words));
            candidates.add(templates.size() - 1);
            return templates.size() - 1;
        }

        String text(int id) {
            return String.join(" ", templates.get(id));
        }
    }

    private static boolean checkAgainstReference() {
        LogLineGenerator generator = new LogLineGenerator(SERVICES, 42);
        LogParser parser = new LogParser(new NameRegistry(16));
        NameRegistry words = new NameRegistry(4096);
        MessageTokenizer tokenizer = new MessageTokenizer(words);
        TemplateTree tree = new TemplateTree(1024, THRESHOLD, MAX_GROUPS);
        ReferenceDrain reference = new ReferenceDrain();
        byte[] line = new byte[LogLineGenerator.MAX_LINE_BYTES];
        int[] matchCounts = new int[1024];
        int failures = 0;

        for (int i = 0; i < LINES; i++) {
            int length = generator.generate(line, System.currentTimeMillis(), generator.randomService(), generator.randomLevel(25));
            parser.parse(line, length);
            String message = new String(line, parser.messageOffset(), length - parser.messageOffset(), StandardCharsets.US_ASCII);

            int n = tokenizer.tokenize(line, parser.messageOffset(), length);
            int id = tree.match(tokenizer.ids(), n);
            int refId = reference.match(MessageTokenizerCheck.reference(message));

            if (id != refId || !tree.text(id, words).equals(reference.text(refId))) {
                failures += fail("\"" + message + "\"\n    reference #" + refId + " " + reference.text(refId) + "\n    tree      #" + id + " " + (id < 0 ? "" : tree.text(id, words)));
            } else {
                matchCounts[id]++;
            }
        }

        Set<String> actual = new TreeSet<>();
        System.out.printf("reference: %,d messages, %,d failures, %d templates:%n", LINES, failures, tree.size());
        for (int id = 0; id < tree.size(); id++) {
            actual.add(tree.text(id, words));
            System.out.printf("    #%-2d %,6d x  %s%n", id, matchCounts[id], tree.text(id, words));
        }
        if (!actual.equals(EXPECTED_TEMPLATES)) {
            failures += fail("templates differ from expected:\n    expected " + EXPECTED_TEMPLATES + "\n    actual   " + actual);
        }
        return failures == 0;
    }

    private static boolean checkSmallCases() {
        int failures = 0;

        Fixture f = new Fixture(THRESHOLD, MAX_GROUPS, 1024);
        int alice = f.match("User alice not found");
        failures += expect("first user", f.text(alice), "User alice not found");
        int bob = f.match("User bob not found");
        failures += expect("second user, same id", bob, alice);
        failures += expect("second user, generalized", f.text(bob), "User <*> not found");

        int sql1 = f.match("SQL query failed");
        int sql2 = f.match("SQL query failed");
        failures += expect("fixed message, same id", sql2, sql1);
        failures += expect("fixed message, unchanged", f.text(sql1), "SQL query failed");

        int other = f.match("User session expired today");
        failures += expect("dissimilar message gets own id", other != alice, true);

        Fixture limited = new Fixture(THRESHOLD, 2, 1024);
        int a = limited.match("alpha done");
        int b = limited.match("beta done");
        int c = limited.match("gamma done");
        int d = limited.match("delta done");
        failures += expect("own groups", a != b, true);
        failures += expect("shared group, same id", d, c);
        failures += expect("shared group, generalized", limited.text(c), "<*> done");
        failures += expect("group count", limited.tree.groupCount(2), 3);

        int e1 = f.match("");
        int e2 = f.match("   ");
        failures += expect("empty message, same id", e2, e1);

        Fixture full = new Fixture(THRESHOLD, MAX_GROUPS, 1);
        int known = full.match("Cache warmed");
        failures += expect("full tree, new message", full.match("Disk almost full"), TemplateTree.NO_TEMPLATE);
        failures += expect("full tree, known message", full.match("Cache warmed"), known);

        System.out.printf("small cases: %d failures%n", failures);
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
        TemplateTree tree = new TemplateTree(1024, THRESHOLD, MAX_GROUPS);

        long checksum = 0;
        for (int round = 0; round < 200; round++) { // warm-up, also learns every template
            checksum += matchAll(tokenizer, tree, lines, messageStarts, lengths);
        }

        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        int rounds = 100;
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int round = 0; round < rounds; round++) {
            checksum += matchAll(tokenizer, tree, lines, messageStarts, lengths);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        long messages = (long) rounds * LINES;
        double perMessage = (double) allocated / messages;
        System.out.printf("allocation: %,d bytes over %,d messages = %.4f bytes/message (checksum %d)%n", allocated, messages, perMessage, checksum);
        return perMessage < 1.0;
    }

    private static long matchAll(MessageTokenizer tokenizer, TemplateTree tree, byte[][] lines, int[] messageStarts, int[] lengths) {
        long sum = 0;
        for (int i = 0; i < lines.length; i++) {
            int n = tokenizer.tokenize(lines[i], messageStarts[i], lengths[i]);
            sum += tree.match(tokenizer.ids(), n);
        }
        return sum;
    }

    /** Tokenizer + tree for matching plain String messages. */
    private static final class Fixture {
        final NameRegistry words = new NameRegistry(4096);
        final MessageTokenizer tokenizer = new MessageTokenizer(words);
        final TemplateTree tree;

        Fixture(double threshold, int maxGroups, int maxTemplates) {
            this.tree = new TemplateTree(maxTemplates, threshold, maxGroups);
        }

        int match(String message) {
            byte[] b = message.getBytes(StandardCharsets.US_ASCII);
            int n = tokenizer.tokenize(b, 0, b.length);
            return tree.match(tokenizer.ids(), n);
        }

        String text(int id) {
            return tree.text(id, words);
        }
    }

    private static int expect(String what, Object actual, Object expected) {
        return expected.equals(actual) ? 0 : fail(what + ": expected " + expected + ", got " + actual);
    }

    private static int fail(String message) {
        System.out.println("  FAIL " + message);
        return 1;
    }
}
