/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.tripatha.store;

import static org.tripatha.api.RdfCloudTripleStoreConstants.DELIM;
import static org.tripatha.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.tripatha.api.RdfCloudTripleStoreConstants.OBJ_CF;
import static org.tripatha.api.RdfCloudTripleStoreConstants.PREDOBJECT_CF;
import static org.tripatha.api.RdfCloudTripleStoreConstants.PRED_CF;
import static org.tripatha.api.RdfCloudTripleStoreConstants.SUBJECTOBJECT_CF;
import static org.tripatha.api.RdfCloudTripleStoreConstants.SUBJECTPRED_CF;
import static org.tripatha.api.RdfCloudTripleStoreConstants.SUBJECT_CF;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tripatha.api.domain.RyaStatement;
import org.tripatha.store.spi.RowMutation;

/**
 * Builds the cardinality-counter mutations for the eval table, and the named-graph markers.
 *
 * <p>Each statement adds +1 (or -1 on delete) to six counters: its subject, predicate, object,
 * subject+predicate, predicate+object and subject+object. The row is the values joined with
 * {@code DELIM}, the column family says which combination it is, and the column qualifier is
 * empty (all graphs) plus, for a named-graph statement, a second cell with the graph IRI.
 * The cell carries the statement's visibility, so a reader only counts what it may see.
 * The backend adds the deltas up server side (Accumulo's {@code SummingCombiner}).
 *
 * <p>A {@link CounterBatch} sums the counters of many statements first and writes one delta per
 * counter with the total, which the backend adds up exactly like the individual +1 deltas. It
 * keys them by the bytes it writes, without an object per counter.
 *
 * <p>The counts drive join ordering only, so they may drift (re-inserting an existing
 * statement counts it twice; dropping a graph does not adjust the all-graphs counters).
 *
 * <p>A batch may leave some counters out ({@link CounterPolicy}): the planner reads a missing
 * counter as 1, so a counter whose final total is 1 changes no estimate.
 */
public final class EvalStatsMutations {
    /** Column family of the named-graph marker rows. */
    public static final String GRAPH_CF = "g";
    public static final byte[] GRAPH_CF_BYTES = GRAPH_CF.getBytes(StandardCharsets.UTF_8);
    /** The column families of the six counters, which leave out the datatype markers ({@link DatatypeMarkers}). */
    public static final List<String> COUNTER_FAMILIES = List.of(SUBJECT_CF, PRED_CF, OBJ_CF, SUBJECTPRED_CF,
            PREDOBJECT_CF, SUBJECTOBJECT_CF);

    private static final byte[] EMPTY = new byte[0];

    private EvalStatsMutations() {
    }

    /**
     * One counter cell.
     *
     * @param row the counted values joined with {@code DELIM}
     * @param family which combination is counted (subject, predicate, ...)
     * @param graph the graph IRI, or empty for the all-graphs counter
     * @param visibility the counted statement's visibility, one char per byte (ISO-8859-1)
     */
    public record CounterKey(String row, String family, String graph, String visibility) {
        /** The counter this cell is one label's part of. */
        public Counter counter() {
            return new Counter(row, family, graph);
        }
    }

    /**
     * A counter as the planner reads it: a row, a family and a graph (empty for every graph). It
     * has one cell per label, and a reader adds up the cells it can see.
     */
    public record Counter(String row, String family, String graph) {
    }

    /**
     * Which counters a {@link CounterBatch} writes. The planner reads a missing counter as 1
     * ({@code RdfCloudTripleStoreEvaluationStatistics}), so leaving out a counter whose final
     * total is 1 changes no estimate. Most subject counters are such: a subject has a few
     * statements, and rarely two with the same predicate or the same object.
     */
    public enum CounterPolicy {
        /** Write every counter. */
        ALL,
        /**
         * Leave out a subject's counters ({@code s}, {@code sp}, {@code so}) whose sum is 1 and
         * which are their counter's only cell. Only exact when that sum is the final total: the
         * batch must hold every statement of each of its subjects, and the store none. The
         * loader's blocks hold whole entities, and an entity's subjects appear in no other entity.
         * A reader adds up the label cells it can see: a 1 left out next to another label's cell
         * would read as nothing, not as 1 more.
         */
        SKIP_SUBJECT_ONES;

        /**
         * Whether this policy leaves out the cell {@code key} with the sum {@code sum}, when its
         * counter has {@code cells} cells with a sum other than 0 (one per label, this one included).
         */
        public boolean skips(final CounterKey key, final long sum, final int cells) {
            return skips(isSubjectCounter(key.family()), sum, cells);
        }

        boolean skips(final boolean subjectCounter, final long sum, final int cells) {
            return this == SKIP_SUBJECT_ONES && sum == 1 && cells == 1 && subjectCounter;
        }

        /** Whether {@code family} is one of a subject's counters: {@code s}, {@code sp} or {@code so}. */
        public static boolean isSubjectCounter(final String family) {
            return family.equals(SUBJECT_CF) || family.equals(SUBJECTPRED_CF) || family.equals(SUBJECTOBJECT_CF);
        }
    }

    /** The counters a statement adds to: six, plus six more for a named-graph statement. */
    public static List<CounterKey> counterKeys(final RyaStatement stmt) {
        final String s = stmt.getSubject().getData();
        final String p = stmt.getPredicate().getData();
        final String o = stmt.getObject().getData();
        final String graph = stmt.getContext() == null ? null : stmt.getContext().getData();
        final byte[] label = stmt.getColumnVisibility();
        final String cv = label == null ? "" : new String(label, StandardCharsets.ISO_8859_1);
        final List<CounterKey> keys = new ArrayList<>(graph == null ? 6 : 12);
        addKeys(keys, s, SUBJECT_CF, graph, cv);
        addKeys(keys, p, PRED_CF, graph, cv);
        addKeys(keys, o, OBJ_CF, graph, cv);
        addKeys(keys, s + DELIM + p, SUBJECTPRED_CF, graph, cv);
        addKeys(keys, p + DELIM + o, PREDOBJECT_CF, graph, cv);
        addKeys(keys, s + DELIM + o, SUBJECTOBJECT_CF, graph, cv);
        return keys;
    }

    private static void addKeys(final List<CounterKey> keys, final String row, final String family, final String graph,
            final String cv) {
        keys.add(new CounterKey(row, family, "", cv));
        if (graph != null) {
            keys.add(new CounterKey(row, family, graph, cv));
        }
    }

    /**
     * Sums counter deltas in memory and writes one delta per counter. Not thread-safe: use one
     * per writer.
     *
     * <p>A cell is keyed by the bytes it is written with (row, family, graph, label), kept in
     * parallel arrays in the order the cells first came, with an open-addressing table over them
     * whose hash reads the bytes eight at a time (O35, ARCH.md 20.6.10). A statement encodes its
     * object and joins its three pair rows once, shared by its all-graphs and graph cells; adding
     * to a pending cell then creates no key, map entry or boxed sum. Two statements whose texts
     * encode to the same bytes add to one cell, as the store would add them up. The last subject,
     * predicate, graph and label are kept encoded: consecutive statements share them.
     */
    public static final class CounterBatch {
        private static final byte[][] FAMILIES = COUNTER_FAMILIES.stream()
                .map(f -> f.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
        private static final int S = COUNTER_FAMILIES.indexOf(SUBJECT_CF);
        private static final int P = COUNTER_FAMILIES.indexOf(PRED_CF);
        private static final int O = COUNTER_FAMILIES.indexOf(OBJ_CF);
        private static final int SP = COUNTER_FAMILIES.indexOf(SUBJECTPRED_CF);
        private static final int PO = COUNTER_FAMILIES.indexOf(PREDOBJECT_CF);
        private static final int SO = COUNTER_FAMILIES.indexOf(SUBJECTOBJECT_CF);
        /** Whether each family is one of a subject's counters, the ones a policy may leave out. */
        private static final boolean[] SUBJECT_FAMILY = new boolean[FAMILIES.length];
        private static final byte DELIMITER = DELIM_BYTES[0];
        private static final long EMPTY_HASH = hash(EMPTY);

        static {
            for (int f = 0; f < FAMILIES.length; f++) {
                SUBJECT_FAMILY[f] = CounterPolicy.isSubjectCounter(COUNTER_FAMILIES.get(f));
            }
        }

        // The cells, in the order they were first added.
        private byte[][] rows;
        private byte[] families;
        private byte[][] graphs;
        private byte[][] visibilities;
        private long[] sums;
        private long[] hashes;
        private long[] rowHashes;
        private int size;
        /** Each slot holds a cell's index plus one, or 0: twice as many slots as the arrays hold cells. */
        private int[] slots;

        private String subject;
        private byte[] subjectBytes;
        private long subjectHash;
        private String predicate;
        private byte[] predicateBytes;
        private long predicateHash;
        private String graph;
        private byte[] graphBytes;
        private long graphHash;
        /** A copy of the last statement's label (empty for none), shared by the cells that carry it. */
        private byte[] label;
        private long labelHash;
        /** Whether a statement was added since the last drain. */
        private boolean added;
        /** Whether those statements carry more than one label: only then can a counter have two cells. */
        private boolean labels;

        public CounterBatch() {
            allocate(16);
        }

        private void allocate(final int capacity) {
            rows = new byte[capacity][];
            families = new byte[capacity];
            graphs = new byte[capacity][];
            visibilities = new byte[capacity][];
            sums = new long[capacity];
            hashes = new long[capacity];
            rowHashes = new long[capacity];
            slots = new int[capacity * 2];
        }

        public void add(final RyaStatement stmt, final long delta) {
            final byte[] l = stmt.getColumnVisibility() == null ? EMPTY : stmt.getColumnVisibility();
            if (label == null || !Arrays.equals(label, l)) {
                labels |= added;
                label = l.length == 0 ? EMPTY : l.clone();
                labelHash = hash(label);
            }
            added = true;
            final String s = stmt.getSubject().getData();
            if (!s.equals(subject)) {
                subject = s;
                subjectBytes = s.getBytes(StandardCharsets.UTF_8);
                subjectHash = hash(subjectBytes);
            }
            final String p = stmt.getPredicate().getData();
            if (!p.equals(predicate)) {
                predicate = p;
                predicateBytes = p.getBytes(StandardCharsets.UTF_8);
                predicateHash = hash(predicateBytes);
            }
            byte[] g = null;
            final String data = stmt.getContext() == null ? null : stmt.getContext().getData();
            if (data != null) {
                if (!data.equals(graph)) {
                    graph = data;
                    graphBytes = data.getBytes(StandardCharsets.UTF_8);
                    graphHash = hash(graphBytes);
                }
                g = graphBytes;
            }
            final byte[] o = stmt.getObject().getData().getBytes(StandardCharsets.UTF_8);
            add(subjectBytes, subjectHash, S, g, delta);
            add(predicateBytes, predicateHash, P, g, delta);
            add(o, hash(o), O, g, delta);
            final byte[] sp = join(subjectBytes, predicateBytes);
            add(sp, hash(sp), SP, g, delta);
            final byte[] po = join(predicateBytes, o);
            add(po, hash(po), PO, g, delta);
            final byte[] so = join(subjectBytes, o);
            add(so, hash(so), SO, g, delta);
        }

        /** The row of a pair counter: the two values joined with {@code DELIM}, as the strings were. */
        private static byte[] join(final byte[] a, final byte[] b) {
            final byte[] row = new byte[a.length + 1 + b.length];
            System.arraycopy(a, 0, row, 0, a.length);
            row[a.length] = DELIMITER;
            System.arraycopy(b, 0, row, a.length + 1, b.length);
            return row;
        }

        /** The all-graphs cell, and the graph's when there is one. */
        private void add(final byte[] row, final long rowHash, final int family, final byte[] g, final long delta) {
            add(row, rowHash, family, EMPTY, EMPTY_HASH, delta);
            if (g != null) {
                add(row, rowHash, family, g, graphHash, delta);
            }
        }

        private void add(final byte[] row, final long rowHash, final int family, final byte[] g, final long gHash,
                final long delta) {
            final long h = mix(rowHash ^ (family + 1) * 0x9E37_79B9_7F4A_7C15L ^ gHash * 0xC2B2_AE3D_27D4_EB4FL
                    ^ labelHash * 0x1656_67B1_9E37_79F9L);
            final int mask = slots.length - 1;
            int i = (int) h & mask;
            while (true) {
                final int at = slots[i];
                if (at == 0) {
                    break;
                }
                final int c = at - 1;
                if (hashes[c] == h && families[c] == family && Arrays.equals(rows[c], row) && Arrays.equals(graphs[c], g)
                        && Arrays.equals(visibilities[c], label)) {
                    sums[c] += delta;
                    return;
                }
                i = (i + 1) & mask;
            }
            final int c = size++;
            rows[c] = row;
            families[c] = (byte) family;
            graphs[c] = g;
            visibilities[c] = label;
            sums[c] = delta;
            hashes[c] = h;
            rowHashes[c] = rowHash;
            slots[i] = c + 1;
            if (size == rows.length) {
                grow();
            }
        }

        private void grow() {
            final int capacity = rows.length * 2;
            rows = Arrays.copyOf(rows, capacity);
            families = Arrays.copyOf(families, capacity);
            graphs = Arrays.copyOf(graphs, capacity);
            visibilities = Arrays.copyOf(visibilities, capacity);
            sums = Arrays.copyOf(sums, capacity);
            hashes = Arrays.copyOf(hashes, capacity);
            rowHashes = Arrays.copyOf(rowHashes, capacity);
            slots = new int[capacity * 2];
            final int mask = slots.length - 1;
            for (int c = 0; c < size; c++) {
                int i = (int) hashes[c] & mask;
                while (slots[i] != 0) {
                    i = (i + 1) & mask;
                }
                slots[i] = c + 1;
            }
        }

        /** The number of distinct counters pending. */
        public int size() {
            return size;
        }

        private CounterKey key(final int c) {
            return new CounterKey(new String(rows[c], StandardCharsets.UTF_8), COUNTER_FAMILIES.get(families[c]),
                    new String(graphs[c], StandardCharsets.UTF_8), new String(visibilities[c], StandardCharsets.ISO_8859_1));
        }

        /** Each pending counter and its sum, without clearing them (for reports, such as the loader's dry run). */
        public void forEachSum(final java.util.function.ObjLongConsumer<CounterKey> action) {
            for (int c = 0; c < size; c++) {
                action.accept(key(c), sums[c]);
            }
        }

        /** How many cells each pending counter has with a sum other than 0: one per label. */
        public Map<Counter, int[]> cellsPerCounter() {
            final Map<Counter, int[]> cells = new HashMap<>();
            for (int c = 0; c < size; c++) {
                if (sums[c] != 0) {
                    cells.computeIfAbsent(key(c).counter(), k -> new int[1])[0]++;
                }
            }
            return cells;
        }

        /**
         * For each pending subject cell with a sum other than 0, how many such cells its counter
         * (row, family, graph) has: one per label. 0 for the other cells.
         */
        private int[] subjectCellsPerCounter() {
            final int[] counterOf = new int[size];
            final int[] count = new int[size];
            final int[] table = new int[Integer.highestOneBit(Math.max(1, size)) << 2];
            final int mask = table.length - 1;
            for (int c = 0; c < size; c++) {
                if (sums[c] == 0 || !SUBJECT_FAMILY[families[c]]) {
                    counterOf[c] = -1;
                    continue;
                }
                int i = (int) mix(rowHashes[c] ^ (families[c] + 1) * 0x9E37_79B9_7F4A_7C15L
                        ^ hash(graphs[c]) * 0xC2B2_AE3D_27D4_EB4FL) & mask;
                while (true) {
                    final int at = table[i];
                    if (at == 0) {
                        table[i] = c + 1;
                        counterOf[c] = c;
                        break;
                    }
                    final int r = at - 1;
                    if (families[r] == families[c] && Arrays.equals(rows[r], rows[c]) && Arrays.equals(graphs[r], graphs[c])) {
                        counterOf[c] = r;
                        break;
                    }
                    i = (i + 1) & mask;
                }
                count[counterOf[c]]++;
            }
            final int[] cells = new int[size];
            for (int c = 0; c < size; c++) {
                cells[c] = counterOf[c] < 0 ? 0 : count[counterOf[c]];
            }
            return cells;
        }

        /** The pending sums as mutations (one per row; zero sums are dropped), and clears them. */
        public List<RowMutation> drain() {
            return drain(CounterPolicy.ALL);
        }

        /**
         * The pending sums as mutations (one per row), without zero sums and without the counters
         * {@code policy} leaves out, and clears them.
         */
        public List<RowMutation> drain(final CounterPolicy policy) {
            try {
                // With one label for the whole batch every counter has one cell: no need to count them.
                final int[] cells = policy == CounterPolicy.ALL || !labels ? null : subjectCellsPerCounter();
                final List<RowMutation> out = new ArrayList<>();
                // The mutation of each row, by the row's bytes: index into out plus one.
                final int[] byRow = new int[Integer.highestOneBit(Math.max(1, size)) << 2];
                final int mask = byRow.length - 1;
                for (int c = 0; c < size; c++) {
                    final long sum = sums[c];
                    if (sum == 0 || policy.skips(SUBJECT_FAMILY[families[c]], sum, cells == null ? 1 : cells[c])) {
                        continue;
                    }
                    final byte[] row = rows[c];
                    int i = (int) mix(rowHashes[c]) & mask;
                    RowMutation m;
                    while (true) {
                        final int at = byRow[i];
                        if (at == 0) {
                            m = new RowMutation(row);
                            out.add(m);
                            byRow[i] = out.size();
                            break;
                        }
                        m = out.get(at - 1);
                        if (Arrays.equals(m.row(), row)) {
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                    m.addToCounter(FAMILIES[families[c]], graphs[c], visibilities[c], sum);
                }
                return out;
            } finally {
                clear();
            }
        }

        /** Empties it, keeping its capacity, at a cost of the cells it held when the table is sparse. */
        private void clear() {
            if (size * 8L < slots.length) {
                final int mask = slots.length - 1;
                for (int c = 0; c < size; c++) {
                    int i = (int) hashes[c] & mask;
                    while (slots[i] != c + 1) {
                        i = (i + 1) & mask;
                    }
                    slots[i] = 0;
                }
            } else {
                Arrays.fill(slots, 0);
            }
            Arrays.fill(rows, 0, size, null);
            Arrays.fill(graphs, 0, size, null);
            Arrays.fill(visibilities, 0, size, null);
            size = 0;
            added = false;
            labels = false;
        }
    }

    private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** The bytes mixed eight at a time, so a difference anywhere reaches every bit of the result. */
    static long hash(final byte[] bytes) {
        long h = bytes.length * 0x9E37_79B9_7F4A_7C15L;
        int i = 0;
        for (; i + Long.BYTES <= bytes.length; i += Long.BYTES) {
            h = mix(h ^ (long) LONGS.get(bytes, i));
        }
        long tail = 0;
        for (int s = 0; i < bytes.length; i++, s += 8) {
            tail |= (bytes[i] & 0xFFL) << s;
        }
        return mix(h ^ tail);
    }

    /** SplitMix64's finalizer. */
    static long mix(final long x) {
        long z = (x ^ (x >>> 30)) * 0xBF58_476D_1CE4_E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D0_49BB_1331_11EBL;
        return z ^ (z >>> 31);
    }

    /**
     * @param stmt the statement added or removed.
     * @param add true for an insert (+1), false for a delete (-1).
     */
    public static List<RowMutation> counters(final RyaStatement stmt, final boolean add) {
        final CounterBatch batch = new CounterBatch();
        batch.add(stmt, add ? 1 : -1);
        return batch.drain();
    }

    /**
     * @return a marker saying the statement's graph has data, or null for the default graph.
     *         Writing it again is harmless.
     */
    public static RowMutation graphMarker(final RyaStatement stmt) {
        if (stmt.getContext() == null) {
            return null;
        }
        return new RowMutation(stmt.getContext().getData().getBytes(StandardCharsets.UTF_8))
                .put(GRAPH_CF_BYTES, EMPTY, stmt.getColumnVisibility(), EMPTY);
    }
}
