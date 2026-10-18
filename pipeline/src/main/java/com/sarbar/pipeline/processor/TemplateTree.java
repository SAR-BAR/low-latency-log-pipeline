package com.sarbar.pipeline.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Lookup is a fixed-depth tree, so the cost per message stays small however many templates exist:
 * <pre>
 * level 1: number of words  -> plain array index
 * level 2: first word id    -> short list of groups, scanned
 * leaf:    a few templates  -> compare the whole message with each one
 * </pre>
 */
public final class TemplateTree {

    public static final int NO_TEMPLATE = -1;

    private static final int WILDCARD = MessageTokenizer.WILDCARD;

    /** Templates that share a word count and a first word. */
    private static final class Group {
        final int firstWordId;
        final List<Integer> templateIds = new ArrayList<>();

        Group(int firstWordId) {
            this.firstWordId = firstWordId;
        }
    }

    private final double similarityThreshold;
    private final int maxGroupsPerWordCount;
    private final List<Group>[] groupsByWordCount;
    private final int[][] templates;
    private int templateCount;

    @SuppressWarnings("unchecked")
    public TemplateTree(int maxTemplates, double similarityThreshold, int maxGroupsPerWordCount) {
        this.similarityThreshold = similarityThreshold;
        this.maxGroupsPerWordCount = maxGroupsPerWordCount;
        this.templates = new int[maxTemplates][];
        this.groupsByWordCount = new List[MessageTokenizer.MAX_WORDS + 1];
        for (int i = 0; i < groupsByWordCount.length; i++) {
            groupsByWordCount[i] = new ArrayList<>();
        }
    }

    /**
     * Finds or creates the template for the first {@code count} entries of {@code ids}.
     *
     * @return the template id, or {@link #NO_TEMPLATE} if the tree is full and nothing matched
     */
    public int match(int[] ids, int count) {
        Group group = findOrCreateGroup(count, count == 0 ? WILDCARD : ids[0]);

        int best = NO_TEMPLATE;
        double bestScore = -1;
        int bestWildcards = -1;
        for (int k = 0; k < group.templateIds.size(); k++) {
            int id = group.templateIds.get(k);
            int[] template = templates[id];
            int equal = 0;
            int wildcards = 0;
            for (int i = 0; i < count; i++) {
                if (template[i] == ids[i]) equal++;
                if (template[i] == WILDCARD) wildcards++;
            }
            double score = count == 0 ? 1.0 : (double) equal / count;
            // On a tie, prefer the more general template, as Drain does.
            if (score > bestScore || (score == bestScore && wildcards > bestWildcards)) {
                best = id;
                bestScore = score;
                bestWildcards = wildcards;
            }
        }

        if (best != NO_TEMPLATE && bestScore >= similarityThreshold) {
            int[] template = templates[best];
            for (int i = 0; i < count; i++) {
                if (template[i] != ids[i]) template[i] = WILDCARD; // this position changes between messages
            }
            return best;
        }
        return create(group, ids, count);
    }

    public int size() {
        return templateCount;
    }

    /** Template as text, e.g. "Timeout connecting to <*> after <*>". Allocates; not for the hot path. */
    public String text(int templateId, NameRegistry words) {
        StringBuilder sb = new StringBuilder();
        for (int id : templates[templateId]) {
            if (!sb.isEmpty()) sb.append(' ');
            sb.append(id == WILDCARD ? "<*>" : words.name(id));
        }
        return sb.toString();
    }

    int groupCount(int wordCount) {
        return groupsByWordCount[wordCount].size();
    }

    private Group findOrCreateGroup(int count, int firstWordId) {
        List<Group> groups = groupsByWordCount[count];
        for (int i = 0; i < groups.size(); i++) {
            Group g = groups.get(i);
            if (g.firstWordId == firstWordId) return g;
        }
        // Too many first words for this length: share one wildcard group so the tree stays small.
        if (groups.size() >= maxGroupsPerWordCount && firstWordId != WILDCARD) {
            return findOrCreateGroup(count, WILDCARD);
        }
        Group g = new Group(firstWordId);
        groups.add(g);
        return g;
    }

    private int create(Group group, int[] ids, int count) {
        if (templateCount == templates.length) {
            return NO_TEMPLATE;
        }
        int id = templateCount++;
        templates[id] = Arrays.copyOf(ids, count);
        group.templateIds.add(id);
        return id;
    }
}
