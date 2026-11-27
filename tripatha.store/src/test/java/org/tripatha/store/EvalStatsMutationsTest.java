/*
 * Copyright 2026 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tripatha.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

import org.tripatha.api.domain.RyaIRI;
import org.tripatha.api.domain.RyaStatement;
import org.tripatha.api.domain.RyaType;
import org.tripatha.store.EvalStatsMutations.CounterBatch;
import org.tripatha.store.EvalStatsMutations.CounterPolicy;
import org.tripatha.store.spi.RowMutation;

class EvalStatsMutationsTest {

    /** Cells as "row|family|qualifier|visibility" → summed value. */
    private static Map<String, Long> cells(final List<RowMutation> mutations) {
        final Map<String, Long> out = new TreeMap<>();
        for (final RowMutation m : mutations) {
            final String row = new String(m.row(), StandardCharsets.UTF_8);
            for (final RowMutation.Update u : m.updates()) {
                final String key = row.replace('\u0000', '~') + "|" + new String(u.family(), StandardCharsets.UTF_8)
                        + "|" + new String(u.qualifier(), StandardCharsets.UTF_8)
                        + "|" + new String(u.visibility(), StandardCharsets.UTF_8);
                out.merge(key, u.delta(), Long::sum);
            }
        }
        return out;
    }

    private static RyaStatement st(final String s, final String p, final String o, final String graph, final String label) {
        return new RyaStatement(new RyaIRI(s), new RyaIRI(p), new RyaType(o), graph == null ? null : new RyaIRI(graph),
                null, label == null ? null : label.getBytes(StandardCharsets.UTF_8), null, 1L);
    }

    @Test
    void oneStatementAddsSixCountersOrTwelveInANamedGraph() {
        final Map<String, Long> plain = cells(EvalStatsMutations.counters(st("urn:s", "urn:p", "x", null, null), true));
        assertEquals(Map.of(
                "urn:s|s||", 1L, "urn:p|p||", 1L, "x|o||", 1L,
                "urn:s~urn:p|sp||", 1L, "urn:p~x|po||", 1L, "urn:s~x|so||", 1L), plain);

        final Map<String, Long> named = cells(EvalStatsMutations.counters(st("urn:s", "urn:p", "x", "urn:g", "A"), false));
        assertEquals(12, named.size());
        assertEquals(-1L, named.get("urn:p|p||A"));
        assertEquals(-1L, named.get("urn:p|p|urn:g|A"));
        assertTrue(named.values().stream().allMatch(v -> v == -1L));
    }

    @Test
    void aSubjectThatIsAlsoTheObjectKeepsBothCounters() {
        final Map<String, Long> same = cells(EvalStatsMutations.counters(
                new RyaStatement(new RyaIRI("urn:a"), new RyaIRI("urn:p"), new RyaIRI("urn:a")), true));
        assertEquals(1L, same.get("urn:a|s||"));
        assertEquals(1L, same.get("urn:a|o||"));
        assertEquals(6, same.size());
    }

    /** Summing many statements in a batch gives exactly the totals of their individual counters. */
    @Test
    void batchSumsEqualIndividualCounters() {
        final Random random = new Random(11);
        final EvalStatsMutations.CounterBatch batch = new EvalStatsMutations.CounterBatch();
        final Map<String, Long> expected = new HashMap<>();
        final Map<String, Long> actual = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            final RyaStatement s = st("urn:s" + random.nextInt(200), "urn:p" + random.nextInt(5), "o" + random.nextInt(50),
                    random.nextInt(3) == 0 ? "urn:g" + random.nextInt(2) : null,
                    random.nextInt(4) == 0 ? (random.nextBoolean() ? "A" : "A&B") : null);
            final boolean add = random.nextInt(10) != 0;
            cells(EvalStatsMutations.counters(s, add)).forEach((k, v) -> expected.merge(k, v, Long::sum));
            batch.add(s, add ? 1 : -1);
            if (batch.size() > 3000) {
                cells(batch.drain()).forEach((k, v) -> actual.merge(k, v, Long::sum));
            }
        }
        cells(batch.drain()).forEach((k, v) -> actual.merge(k, v, Long::sum));
        expected.values().removeIf(v -> v == 0);
        actual.values().removeIf(v -> v == 0);
        assertEquals(expected, actual);
        assertEquals(0, batch.size());
        assertTrue(batch.drain().isEmpty());
    }

    /**
     * Skipping a subject's ones leaves out exactly the s, sp and so cells whose sum is 1 and which
     * are their counter's only cell, and keeps every other counter, ones included. A 1 next to
     * another label's cell stays: a reader with both labels adds the two up.
     */
    @Test
    void skipSubjectOnesLeavesOutOnlyTheSubjectCountersOfOne() {
        final EvalStatsMutations.CounterBatch all = new EvalStatsMutations.CounterBatch();
        final EvalStatsMutations.CounterBatch skip = new EvalStatsMutations.CounterBatch();
        final List<RyaStatement> statements = List.of(
                st("urn:a", "urn:name", "Alice", "urn:g", null),
                st("urn:a", "urn:knows", "urn:b", "urn:g", null),
                st("urn:a", "urn:knows", "urn:c", "urn:g", null),
                st("urn:a", "urn:knows", "urn:c", "urn:g", "A"),
                st("urn:b", "urn:name", "Bob", null, null));
        for (final RyaStatement s : statements) {
            all.add(s, 1);
            skip.add(s, 1);
        }
        final Map<String, Long> every = cells(all.drain());
        final Map<String, Long> kept = cells(skip.drain(EvalStatsMutations.CounterPolicy.SKIP_SUBJECT_ONES));

        // Cells per counter: row, family and graph, over every label.
        final Map<String, Long> cellsOfCounter = new TreeMap<>();
        every.keySet().forEach(k -> cellsOfCounter.merge(k.substring(0, k.lastIndexOf('|')), 1L, Long::sum));
        final Map<String, Long> expected = new TreeMap<>(every);
        expected.entrySet().removeIf(e -> e.getValue() == 1 && isSubjectCounter(e.getKey())
                && cellsOfCounter.get(e.getKey().substring(0, e.getKey().lastIndexOf('|'))) == 1);
        assertEquals(expected, kept);
        // Kept: urn:a's totals above 1, and every non-subject counter (urn:b's object "Bob" included).
        assertEquals(3L, kept.get("urn:a|s||"));
        assertEquals(3L, kept.get("urn:a|s|urn:g|"));
        assertEquals(2L, kept.get("urn:a~urn:knows|sp||"));
        assertEquals(1L, kept.get("Bob|o||"));
        assertEquals(1L, kept.get("urn:name~Alice|po||"));
        // Kept too: label A's 1s next to the unlabelled cells of the same counters, which a reader
        // with A adds them to (urn:a's s counter reads 4 for it, not 3), and a counter of two 1s.
        assertEquals(1L, kept.get("urn:a|s||A"));
        assertEquals(1L, kept.get("urn:a~urn:knows|sp||A"));
        assertEquals(1L, kept.get("urn:a~urn:c|so||"));
        assertEquals(1L, kept.get("urn:a~urn:c|so||A"));
        // Left out: urn:b's subject cells, and the so cells of one with no other label.
        assertTrue(!kept.containsKey("urn:b|s||") && every.containsKey("urn:b|s||"));
        assertTrue(!kept.containsKey("urn:a~Alice|so||"));
        assertEquals(1L, every.get("urn:a~Alice|so||"));
        // The default drain, and the ALL policy, keep everything.
        assertTrue(every.values().stream().anyMatch(v -> v == 1));
        assertTrue(!EvalStatsMutations.CounterPolicy.ALL.skips(new EvalStatsMutations.CounterKey("urn:b", "s", "", ""), 1, 1));
        assertTrue(EvalStatsMutations.CounterPolicy.SKIP_SUBJECT_ONES.skips(new EvalStatsMutations.CounterKey("urn:b", "s", "", ""), 1, 1));
        assertTrue(!EvalStatsMutations.CounterPolicy.SKIP_SUBJECT_ONES.skips(new EvalStatsMutations.CounterKey("urn:b", "s", "", ""), 1, 2));
    }

    private static boolean isSubjectCounter(final String cell) {
        final String family = cell.split("\\|")[1];
        return family.equals("s") || family.equals("sp") || family.equals("so");
    }
    /** Cells by their exact bytes, "row|family|qualifier|visibility" in hex, summed. */
    private static Map<String, Long> byteCells(final List<RowMutation> mutations) {
        final HexFormat hex = HexFormat.of();
        final Map<String, Long> out = new TreeMap<>();
        for (final RowMutation m : mutations) {
            for (final RowMutation.Update u : m.updates()) {
                assertEquals(RowMutation.Op.COUNTER, u.op());
                out.merge(hex.formatHex(m.row()) + "|" + hex.formatHex(u.family()) + "|" + hex.formatHex(u.qualifier()) + "|"
                        + hex.formatHex(u.visibility()), u.delta(), Long::sum);
            }
        }
        return out;
    }

    /** One mutation per row, and one update per cell. */
    private static void assertGrouped(final List<RowMutation> mutations) {
        final HexFormat hex = HexFormat.of();
        final Set<String> rows = new HashSet<>();
        final Set<String> cells = new HashSet<>();
        for (final RowMutation m : mutations) {
            assertTrue(rows.add(hex.formatHex(m.row())), "a row in two mutations");
            for (final RowMutation.Update u : m.updates()) {
                assertTrue(cells.add(hex.formatHex(m.row()) + "|" + hex.formatHex(u.family()) + "|" + hex.formatHex(u.qualifier())
                        + "|" + hex.formatHex(u.visibility())), "a cell twice");
            }
        }
    }

    /** An IRI without the parse check, for a graph whose data is empty. */
    private static RyaIRI rawIri(final String data) {
        return new RyaIRI(XSD.ANYURI, data) {
        };
    }

    private static final String[] TEXTS = {"", "a", "urn:x", "caf\u00e9", "\u6771\u4eac", "\ud83d\ude00", "same-prefix-0123456789-",
        "same-prefix-0123456789-a", "same-prefix-0123456789-b", "urn:p", "x"};

    private static String text(final Random r) {
        final String t = TEXTS[r.nextInt(TEXTS.length)];
        return r.nextInt(3) == 0 ? t + r.nextInt(40) : t;
    }

    /**
     * Random statements: subjects and predicates often repeated (as in a block), objects that can
     * be a subject, empty, long with shared prefixes or outside ASCII; graphs absent, named or
     * empty (whose cell is the all-graphs one); labels absent, empty or two others.
     */
    private static RyaStatement randomStatement(final Random r, final boolean manyLabels) {
        final String s = "urn:s" + r.nextInt(r.nextBoolean() ? 3 : 300);
        final String p = r.nextInt(4) == 0 ? "urn:p" + r.nextInt(30) : "urn:p" + r.nextInt(3);
        final RyaType o = switch (r.nextInt(4)) {
            case 0 -> new RyaIRI(s);
            case 1 -> new RyaIRI("urn:s" + r.nextInt(300));
            default -> new RyaType(text(r));
        };
        final RyaIRI g = switch (r.nextInt(5)) {
            case 0 -> new RyaIRI("urn:g" + r.nextInt(3));
            case 1 -> rawIri("");
            default -> null;
        };
        final byte[] label = !manyLabels ? null : switch (r.nextInt(4)) {
            case 0 -> new byte[0];
            case 1 -> "A".getBytes(StandardCharsets.UTF_8);
            case 2 -> "(A&B)".getBytes(StandardCharsets.UTF_8);
            default -> null;
        };
        return new RyaStatement(new RyaIRI(s), new RyaIRI(p), o, g, null, label, null, 1L);
    }

    /**
     * The batch writes what the string-keyed batch it replaced wrote (O35): the same cells and
     * sums, grouped one mutation per row, with the same size after every statement, the same
     * reports, and the same cells left out, with one label or several, early drains included.
     */
    @Test
    void writesWhatTheStringKeyedBatchWrote() {
        for (int seed = 0; seed < 40; seed++) {
            final Random r = new Random(seed);
            final boolean manyLabels = seed % 2 == 1;
            final CounterBatch batch = new CounterBatch();
            final ReferenceCounterBatch reference = new ReferenceCounterBatch();
            for (int call = 0; call < 6; call++) {
                final CounterPolicy policy = r.nextBoolean() ? CounterPolicy.ALL : CounterPolicy.SKIP_SUBJECT_ONES;
                final int statements = r.nextInt(3) == 0 ? 1 + r.nextInt(3) : r.nextInt(4000);
                for (int i = 0; i < statements; i++) {
                    final RyaStatement st = randomStatement(r, manyLabels);
                    final long delta = r.nextInt(8) == 0 ? -1 : 1;
                    batch.add(st, delta);
                    reference.add(st, delta);
                    assertEquals(reference.size(), batch.size());
                    if (policy == CounterPolicy.ALL && r.nextInt(500) == 0) {
                        final List<RowMutation> early = batch.drain();
                        assertGrouped(early);
                        assertEquals(byteCells(reference.drain(CounterPolicy.ALL)), byteCells(early));
                    }
                }
                final Map<EvalStatsMutations.CounterKey, Long> sums = new HashMap<>();
                batch.forEachSum((k, v) -> assertEquals(null, sums.put(k, v)));
                final Map<EvalStatsMutations.CounterKey, Long> expectedSums = new HashMap<>();
                reference.forEachSum(expectedSums::put);
                assertEquals(expectedSums, sums);
                assertEquals(counts(reference.cellsPerCounter()), counts(batch.cellsPerCounter()));
                final List<RowMutation> drained = batch.drain(policy);
                assertGrouped(drained);
                assertEquals(byteCells(reference.drain(policy)), byteCells(drained), "seed " + seed + ", call " + call);
                assertEquals(0, batch.size());
            }
        }
    }

    private static Map<EvalStatsMutations.Counter, Integer> counts(final Map<EvalStatsMutations.Counter, int[]> cells) {
        final Map<EvalStatsMutations.Counter, Integer> out = new HashMap<>();
        cells.forEach((k, v) -> out.put(k, v[0]));
        return out;
    }

    /**
     * Texts that encode to the same bytes (a lone surrogate becomes '?') add to one cell, as the
     * store adds them up: so a subject's 1 and another 1 of the same bytes are a 2, which stays.
     */
    @Test
    void textsWithTheSameBytesAddToOneCell() {
        final CounterBatch batch = new CounterBatch();
        batch.add(new RyaStatement(new RyaIRI("urn:a"), new RyaIRI("urn:p"), new RyaType("x\ud800")), 1);
        batch.add(new RyaStatement(new RyaIRI("urn:a"), new RyaIRI("urn:p"), new RyaType("x?")), 1);
        final Map<String, Long> kept = cells(batch.drain(CounterPolicy.SKIP_SUBJECT_ONES));
        assertEquals(2L, kept.get("urn:a~x?|so||"));
        assertEquals(2L, kept.get("x?|o||"));
        assertEquals(2L, kept.get("urn:a|s||"));
    }

    /** A batch that held many cells and was drained serves one statement as a new one does, and as often. */
    @Test
    void aDrainedBatchStartsEmpty() {
        final CounterBatch batch = new CounterBatch();
        final Random r = new Random(5);
        for (int i = 0; i < 100_000; i++) {
            batch.add(randomStatement(r, true), 1);
        }
        assertTrue(batch.drain(CounterPolicy.SKIP_SUBJECT_ONES).size() > 1000);
        for (int i = 0; i < 1000; i++) {
            final RyaStatement st = st("urn:s" + i, "urn:p", "o" + (i % 7), i % 2 == 0 ? "urn:g" : null, i % 3 == 0 ? "A" : null);
            batch.add(st, 1);
            assertEquals(cells(EvalStatsMutations.counters(st, true)), cells(i % 2 == 0 ? batch.drain() : batch.drain(CounterPolicy.ALL)));
            assertEquals(0, batch.size());
        }
        // One label per call again: a skipped 1 is skipped after a drain of several labels.
        batch.add(st("urn:a", "urn:p", "x", null, "A"), 1);
        batch.add(st("urn:a", "urn:p", "y", null, null), 1);
        batch.drain(CounterPolicy.SKIP_SUBJECT_ONES);
        batch.add(st("urn:b", "urn:p", "x", null, "A"), 1);
        assertTrue(!cells(batch.drain(CounterPolicy.SKIP_SUBJECT_ONES)).containsKey("urn:b|s||A"));
    }

    /** A statement's label array can change after its add: the batch keeps what it was. */
    @Test
    void aLabelChangedAfterTheAddIsNotSeen() {
        final CounterBatch batch = new CounterBatch();
        final byte[] label = "A".getBytes(StandardCharsets.UTF_8);
        batch.add(new RyaStatement(new RyaIRI("urn:a"), new RyaIRI("urn:p"), new RyaType("x"), null, null, label, null, 1L), 1);
        label[0] = 'B';
        assertEquals(1L, cells(batch.drain()).get("urn:a|s||A"));
        final List<String> labels = new ArrayList<>();
        batch.add(new RyaStatement(new RyaIRI("urn:a"), new RyaIRI("urn:p"), new RyaType("x"), null, null, label, null, 1L), 1);
        batch.forEachSum((k, v) -> labels.add(k.visibility()));
        assertTrue(labels.stream().allMatch("B"::equals));
    }
}
