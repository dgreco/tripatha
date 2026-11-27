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
package org.tripatha.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.SplittableRandom;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import scala.Tuple2;

/**
 * {@link FileSort} on local Spark returns each partition's cells in the order of their keys'
 * unsigned bytes, values intact, whether it sorts in memory or spills and merges back. The
 * ranges have bounds that share a prefix, rows that hold {@code 00} bytes, and rows equal to a
 * bound, so the comparison that skips the shared bytes is checked where it matters.
 */
class FileSortTest {
    private static final byte[][] BOUNDS = {
        "http://x/e/0".getBytes(), "http://x/e/3\u0000a".getBytes(), "http://x/e/3\u0000z".getBytes(), "http://x/e/8".getBytes()};

    /** A row in range {@code r} of {@link #BOUNDS}: {@code (BOUNDS[r-1], BOUNDS[r]]}. */
    private static byte[] row(final SplittableRandom random, final int r) {
        while (true) {
            final byte[] base = r == 0 ? "http://x/e/".getBytes() : r == BOUNDS.length ? "http://x/e/8".getBytes() : BOUNDS[r - 1];
            final byte[] tail = new byte[random.nextInt(0, 6)];
            for (int i = 0; i < tail.length; i++) {
                tail[i] = new byte[] {0, 1, 'a', 'z', (byte) 0xFF}[random.nextInt(5)];
            }
            final byte[] row = random.nextInt(20) == 0 && r < BOUNDS.length ? BOUNDS[r] : concat(base, tail);
            final boolean above = r == 0 || Arrays.compareUnsigned(row, BOUNDS[r - 1]) > 0;
            final boolean below = r == BOUNDS.length || Arrays.compareUnsigned(row, BOUNDS[r]) <= 0;
            if (above && below) {
                return row;
            }
        }
    }

    private static byte[] concat(final byte[] a, final byte[] b) {
        final byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static byte[] bytes(final SplittableRandom random, final int most) {
        final byte[] b = new byte[random.nextInt(most + 1)];
        random.nextBytes(b);
        return b;
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MAX_VALUE, 50})
    void sortsEachPartitionAsItsKeysBytes(final int spillEvery) {
        final Shuffle.Layout layout = new Shuffle.Layout(List.of("t_spo"), new boolean[] {false}, new byte[][][] {BOUNDS},
                new int[] {1});
        final SplittableRandom random = new SplittableRandom(spillEvery);
        final List<Tuple2<byte[], Shuffle.Value>> cells = new ArrayList<>();
        for (int i = 0; i < 4_000; i++) {
            final int r = random.nextInt(BOUNDS.length + 1);
            final byte[] row = row(random, r);
            assertEquals(r, layout.partition(0, row, new byte[0], new byte[0]));
            final byte[] key = Shuffle.Key.encode(r, row, bytes(random, 3), bytes(random, 12), random.nextBoolean() ? new byte[0]
                    : "A&B".getBytes());
            cells.add(new Tuple2<>(key, random.nextBoolean() ? Shuffle.Value.counter(random.nextLong(-5, 6))
                    : Shuffle.Value.put(random.nextLong(1, 4), bytes(random, 20))));
        }
        final SparkConf conf = SparkBuild.configure(new SparkConf().setMaster("local[2]").setAppName("FileSortTest")
                .set("spark.ui.enabled", "false").set("spark.shuffle.spill.numElementsForceSpillThreshold", Integer.toString(spillEvery)));
        final List<List<String>> sorted;
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sorted = JavaPairRDD.fromJavaRDD(sc.parallelize(cells, 3))
                    .partitionBy(new Shuffle.FilePartitioner(layout.partitions))
                    .mapPartitionsWithIndex((p, it) -> {
                        final List<List<String>> out = new ArrayList<>();
                        final List<String> part = new ArrayList<>();
                        FileSort.sorted(layout, p, it).forEachRemaining(c -> part.add(text(c)));
                        // The task's spills, as the last line.
                        part.add("spilled " + org.apache.spark.TaskContext.get().taskMetrics().diskBytesSpilled());
                        out.add(part);
                        return out.iterator();
                    }, true).collect();
        }
        long spilled = 0;
        for (final List<String> part : sorted) {
            spilled += Long.parseLong(part.remove(part.size() - 1).substring("spilled ".length()));
        }
        assertEquals(spillEvery < cells.size(), spilled > 0, "spilled " + spilled + " bytes");
        assertEquals(layout.partitions, sorted.size());
        for (int p = 0; p < layout.partitions; p++) {
            final int part = p;
            final List<Tuple2<byte[], Shuffle.Value>> expected = new ArrayList<>(cells.stream()
                    .filter(c -> Shuffle.Key.part(c._1()) == part).toList());
            expected.sort((a, b) -> Arrays.compareUnsigned(a._1(), b._1()));
            final List<String> got = sorted.get(p);
            assertEquals(expected.size(), got.size(), "partition " + p);
            // Equal keys may come in any order; the writer combines them whatever their order.
            for (int i = 0; i < got.size(); i++) {
                assertEquals(HexFormat.of().formatHex(expected.get(i)._1()), got.get(i).substring(0, got.get(i).indexOf(' ')),
                        "partition " + p + " at " + i);
            }
            final List<String> want = new ArrayList<>(expected.stream().map(FileSortTest::text).toList());
            final List<String> have = new ArrayList<>(got);
            want.sort(null);
            have.sort(null);
            assertEquals(want, have, "partition " + p + ": the same cells, values intact");
        }
        assertTrue(sorted.stream().allMatch(l -> !l.isEmpty()));
    }

    private static String text(final Tuple2<byte[], Shuffle.Value> c) {
        final Shuffle.Value v = c._2();
        return HexFormat.of().formatHex(c._1()) + " " + (v.counter ? "c" + v.delta : "p" + v.timestamp + ":"
                + HexFormat.of().formatHex(v.value));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 9, 31})
    void copiesAPageEightBytesAtATime(final int length) {
        final byte[] page = new byte[64];
        new SplittableRandom(length).nextBytes(page);
        assertArrayEquals(Arrays.copyOfRange(page, 3, 3 + length),
                FileSort.copy(page, org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET + 3, length));
    }
}
