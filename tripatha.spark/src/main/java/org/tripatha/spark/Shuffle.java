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

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Partitioner;

import org.tripatha.loader.bulk.CellKey;

/**
 * What Spark's shuffle moves and how it orders it: one record per cell, keyed by the partition
 * that writes it (a part of a table's tablet) and the cell's key, encoded together as one array
 * ({@link Key}), so each partition receives the cells of one file, sorted as the RFile needs
 * them. The values are a plain class with a no-argument constructor, for Kryo: no RDF4J object
 * crosses the shuffle.
 */
final class Shuffle {
    private Shuffle() {
    }

    /**
     * A cell's key without its timestamp, as one array whose unsigned byte order is the order the
     * file needs (O30): the partition (4 bytes, big-endian), then the row, the family and the
     * qualifier, each with its {@code 00} bytes written as {@code 00 FF} and closed by
     * {@code 00 01}, then the label as it is. A field that is a prefix of another sorts first, as
     * in Accumulo's key order, and two keys are equal exactly when their cells' keys are. So the
     * shuffle compares one array per record, and Kryo writes one object for the key, not five.
     */
    static final class Key {
        private static final byte ESCAPE = 0;
        private static final byte ESCAPED = (byte) 0xFF;
        private static final byte END = 1;
        private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private Key() {
        }

        static byte[] encode(final int part, final byte[] row, final byte[] family, final byte[] qualifier, final byte[] visibility) {
            final byte[] out = new byte[Integer.BYTES + escapedLength(row) + escapedLength(family) + escapedLength(qualifier)
                    + visibility.length];
            out[0] = (byte) (part >>> 24);
            out[1] = (byte) (part >>> 16);
            out[2] = (byte) (part >>> 8);
            out[3] = (byte) part;
            int at = escape(row, out, Integer.BYTES);
            at = escape(family, out, at);
            at = escape(qualifier, out, at);
            System.arraycopy(visibility, 0, out, at, visibility.length);
            return out;
        }

        /**
         * The bytes that begin every key of partition {@code part} whose rows begin with
         * {@code rowPrefix}: the partition, then the row prefix escaped, without its end.
         */
        static byte[] prefix(final int part, final byte[] rowPrefix) {
            final byte[] out = new byte[Integer.BYTES + escapedLength(rowPrefix) - 2];
            out[0] = (byte) (part >>> 24);
            out[1] = (byte) (part >>> 16);
            out[2] = (byte) (part >>> 8);
            out[3] = (byte) part;
            final byte[] withEnd = new byte[escapedLength(rowPrefix)];
            escape(rowPrefix, withEnd, 0);
            System.arraycopy(withEnd, 0, out, Integer.BYTES, withEnd.length - 2);
            return out;
        }

        /** The partition a key was encoded with. */
        static int part(final byte[] key) {
            return (key[0] & 0xFF) << 24 | (key[1] & 0xFF) << 16 | (key[2] & 0xFF) << 8 | key[3] & 0xFF;
        }

        /** The cell's key, in arrays of its own. */
        static CellKey decode(final byte[] key) {
            final int[] at = {Integer.BYTES};
            final byte[] row = unescape(key, at);
            final byte[] family = unescape(key, at);
            final byte[] qualifier = unescape(key, at);
            return new CellKey(row, family, qualifier, Arrays.copyOfRange(key, at[0], key.length));
        }

        private static int escapedLength(final byte[] b) {
            int n = b.length + 2;
            for (int z = indexOfZero(b, 0); z >= 0; z = indexOfZero(b, z + 1)) {
                n++;
            }
            return n;
        }

        private static int escape(final byte[] b, final byte[] out, final int to) {
            int at = to;
            int from = 0;
            for (int z = indexOfZero(b, 0); z >= 0; z = indexOfZero(b, z + 1)) {
                System.arraycopy(b, from, out, at, z + 1 - from);
                at += z + 1 - from;
                out[at++] = ESCAPED;
                from = z + 1;
            }
            System.arraycopy(b, from, out, at, b.length - from);
            at += b.length - from;
            out[at++] = ESCAPE;
            out[at++] = END;
            return at;
        }

        /** The field starting at {@code at[0]}, unescaped; {@code at[0]} moves past its end. */
        private static byte[] unescape(final byte[] key, final int[] at) {
            final int start = at[0];
            // Every 00 of a field is followed by FF (an escape) or 01 (its end).
            int end = indexOfZero(key, start);
            int escapes = 0;
            while (key[end + 1] != END) {
                escapes++;
                end = indexOfZero(key, end + 2);
            }
            final byte[] out = new byte[end - start - escapes];
            int o = 0;
            int from = start;
            for (int z = indexOfZero(key, from); z < end; z = indexOfZero(key, from)) {
                System.arraycopy(key, from, out, o, z + 1 - from);
                o += z + 1 - from;
                from = z + 2;
            }
            System.arraycopy(key, from, out, o, end - from);
            at[0] = end + 2;
            return out;
        }

        /**
         * The first {@code 00} byte of {@code b} at or after {@code from}, or -1: eight bytes at a
         * time, with the zero-byte test {@code (w - 0x01..01) & ~w & 0x80..80}. Read little-endian,
         * its lowest set bit is the first zero byte; a false hit can only follow a real one.
         */
        static int indexOfZero(final byte[] b, final int from) {
            int i = from;
            for (; i + Long.BYTES <= b.length; i += Long.BYTES) {
                final long w = (long) LONGS.get(b, i);
                final long t = (w - 0x0101_0101_0101_0101L) & ~w & 0x8080_8080_8080_8080L;
                if (t != 0) {
                    return i + (Long.numberOfTrailingZeros(t) >>> 3);
                }
            }
            for (; i < b.length; i++) {
                if (b[i] == 0) {
                    return i;
                }
            }
            return -1;
        }
    }

    /**
     * A cell's content: a put's timestamp and value, or a counter's delta. Two values of one key
     * combine as the store would: deltas add up, and the newest put wins (on a tie, the greater
     * value, so a task run twice combines the same way).
     */
    static final class Value implements Serializable {
        private static final long serialVersionUID = 1L;
        boolean counter;
        long delta;
        long timestamp;
        byte[] value;

        Value() {
        }

        static Value put(final long timestamp, final byte[] value) {
            final Value v = new Value();
            v.timestamp = timestamp;
            v.value = value;
            return v;
        }

        static Value counter(final long delta) {
            final Value v = new Value();
            v.counter = true;
            v.delta = delta;
            return v;
        }

        /** {@code a} and {@code b} combined; may return (and change) {@code a}. */
        static Value combine(final Value a, final Value b) {
            if (a.counter != b.counter) {
                throw new IllegalStateException("a counter delta and a put for one cell");
            }
            if (a.counter) {
                a.delta = Math.addExact(a.delta, b.delta);
                return a;
            }
            if (b.timestamp > a.timestamp || b.timestamp == a.timestamp && Arrays.compareUnsigned(b.value, a.value) > 0) {
                return b;
            }
            return a;
        }
    }

    /** The partition {@link Layout} gave each key. */
    static final class FilePartitioner extends Partitioner {
        private static final long serialVersionUID = 1L;
        private final int partitions;

        FilePartitioner(final int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int numPartitions() {
            return partitions;
        }

        @Override
        public int getPartition(final Object key) {
            return Key.part((byte[]) key);
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof FilePartitioner p && p.partitions == partitions;
        }

        @Override
        public int hashCode() {
            return partitions;
        }
    }

    /**
     * Where each table's cells go, fixed by the driver before anything is written: the tables in
     * order, which hold counters, and each table's points. The points are its tablets' split rows
     * plus rows between them, so each file falls in one tablet; a point ends its range, as a split
     * row ends its tablet on Accumulo, so range {@code i} holds the rows in
     * {@code (point[i-1], point[i]]}. A table whose rows are few and long (the entity index: one
     * row per shard) also spreads each range over {@code buckets} files by the hash of the column.
     */
    static final class Layout implements Serializable {
        private static final long serialVersionUID = 1L;
        final List<String> tables;
        final boolean[] counters;
        final byte[][][] points;
        final int[] buckets;
        final int[] offsets;
        final int partitions;

        Layout(final List<String> tables, final boolean[] counters, final byte[][][] points, final int[] buckets) {
            this.tables = List.copyOf(tables);
            this.counters = counters.clone();
            this.points = points;
            this.buckets = buckets.clone();
            this.offsets = new int[tables.size()];
            int total = 0;
            for (int t = 0; t < tables.size(); t++) {
                offsets[t] = total;
                total = Math.addExact(total, Math.multiplyExact(points[t].length + 1, buckets[t]));
            }
            this.partitions = total;
        }

        int table(final String name) {
            return tables.indexOf(name);
        }

        /** The files of table {@code t}. */
        int files(final int t) {
            return (points[t].length + 1) * buckets[t];
        }

        /** The partition of a cell of table {@code t}. */
        int partition(final int t, final byte[] row, final byte[] family, final byte[] qualifier) {
            final byte[][] s = points[t];
            int lo = 0;
            int hi = s.length;
            while (lo < hi) {
                final int mid = (lo + hi) >>> 1;
                if (Arrays.compareUnsigned(s[mid], row) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            final int b = buckets[t];
            if (b == 1) {
                return offsets[t] + lo;
            }
            final int h = 31 * Arrays.hashCode(family) + Arrays.hashCode(qualifier);
            return offsets[t] + lo * b + Math.floorMod(h, b);
        }

        /**
         * The bytes every encoded key of a partition begins with: the partition, then the common
         * prefix of its range's bounds, since a row in {@code (lo, hi]} begins with every byte
         * {@code lo} and {@code hi} share. The first and last ranges of a table are open, so
         * their keys share only the partition.
         */
        byte[] sharedKeyBytes(final int partition) {
            final int t = tableOf(partition);
            final int range = fileOf(partition) / buckets[t];
            final byte[][] s = points[t];
            if (range == 0 || range == s.length) {
                return Key.prefix(partition, new byte[0]);
            }
            final byte[] lo = s[range - 1];
            final byte[] hi = s[range];
            final int common = Math.max(0, Arrays.mismatch(lo, hi));
            return Key.prefix(partition, Arrays.copyOf(lo, common));
        }

        /** The table a partition belongs to. */
        int tableOf(final int partition) {
            int t = 0;
            while (t + 1 < offsets.length && offsets[t + 1] <= partition) {
                t++;
            }
            return t;
        }

        /** The partition's file, within its table. */
        int fileOf(final int partition) {
            return partition - offsets[tableOf(partition)];
        }
    }
}
