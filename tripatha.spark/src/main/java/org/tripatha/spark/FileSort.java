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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

import scala.Tuple2;

/**
 * One file's cells sorted in the reduce task (O30), as Spark SQL sorts rows: each cell is copied
 * as bytes into Spark's memory pages, and an array of (pointer, 8-byte prefix) pairs is sorted,
 * comparing the records themselves only when their prefixes are equal. The pages are the task's
 * execution memory, so the sorter spills to the local disk when they run out, and merges back.
 * Spark's own sort of a shuffle (its {@code ExternalSorter}) sorts objects instead: every
 * comparison follows two references to keys scattered over the heap, which took about half the
 * reduce stage's CPU.
 *
 * <p>Every key of a partition starts with the same bytes: its partition, then the bytes its
 * range's bounds share (the rows of {@code (point[i-1], point[i]]} share their bounds' common
 * prefix). The prefix is the 8 bytes after those, where the keys differ, and comparisons start
 * there.
 *
 * <p>This uses Spark's unsafe sorter, an internal class whose constructor changed in 4.2.0:
 * upgrading Spark means checking {@link #sorter} again.
 */
final class FileSort {
    /** A record: key length (4 bytes), the key, then the value's kind, its number and, for a put, its bytes. */
    private static final byte PUT = 0;
    private static final byte COUNTER = 1;

    private FileSort() {
    }

    /**
     * The cells of partition {@code partition}, in the order of their keys' unsigned bytes.
     *
     * @throws IllegalStateException if a key doesn't start with the partition's shared bytes
     */
    static Iterator<Tuple2<byte[], Shuffle.Value>> sorted(final Shuffle.Layout layout, final int partition,
            final Iterator<Tuple2<byte[], Shuffle.Value>> cells) {
        if (!cells.hasNext()) {
            return cells;
        }
        final byte[] shared = layout.sharedKeyBytes(partition);
        final UnsafeExternalSorter sorter = sorter(shared.length);
        byte[] record = new byte[256];
        try {
            while (cells.hasNext()) {
                final Tuple2<byte[], Shuffle.Value> c = cells.next();
                final byte[] key = c._1();
                if (key.length < shared.length || !Arrays.equals(key, 0, shared.length, shared, 0, shared.length)) {
                    throw new IllegalStateException("a cell of partition " + partition + " outside its range");
                }
                final Shuffle.Value v = c._2();
                final int length = Integer.BYTES + key.length + 1 + Long.BYTES + (v.counter ? 0 : v.value.length);
                if (record.length < length) {
                    record = new byte[Math.max(length, record.length * 2)];
                }
                Platform.putInt(record, Platform.BYTE_ARRAY_OFFSET, key.length);
                System.arraycopy(key, 0, record, Integer.BYTES, key.length);
                int at = Integer.BYTES + key.length;
                record[at++] = v.counter ? COUNTER : PUT;
                Platform.putLong(record, Platform.BYTE_ARRAY_OFFSET + at, v.counter ? v.delta : v.timestamp);
                at += Long.BYTES;
                if (!v.counter) {
                    System.arraycopy(v.value, 0, record, at, v.value.length);
                }
                sorter.insertRecord(record, Platform.BYTE_ARRAY_OFFSET, length, prefix(key, shared.length), false);
            }
            return new Sorted(sorter.getSortedIterator(), sorter);
        } catch (final IOException e) {
            sorter.cleanupResources();
            throw new UncheckedIOException(e);
        } catch (final RuntimeException e) {
            sorter.cleanupResources();
            throw e;
        }
    }

    private static UnsafeExternalSorter sorter(final int skip) {
        final TaskContext task = TaskContext.get();
        final SparkEnv env = SparkEnv.get();
        final TaskMemoryManager memory = task.taskMemoryManager();
        return UnsafeExternalSorter.create(memory, env.blockManager(), env.serializerManager(), task, () -> new KeyOrder(skip),
                PrefixComparators.BINARY, 1 << 16, memory.pageSizeBytes(),
                env.conf().getInt("spark.shuffle.spill.numElementsForceSpillThreshold", Integer.MAX_VALUE), Long.MAX_VALUE,
                env.conf().getInt("spark.unsafe.sorter.spill.merge.factor", -1),
                // Radix sort would order the records by their prefixes alone.
                false);
    }

    /** The 8 bytes of the key after {@code skip}, big-endian, padded with zeros. */
    static long prefix(final byte[] key, final int skip) {
        long p = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            p = p << 8 | (skip + i < key.length ? key[skip + i] & 0xFF : 0);
        }
        return p;
    }

    /** Two records' keys as unsigned bytes, from the first byte the partition's keys don't share. */
    private static final class KeyOrder extends RecordComparator {
        private final int skip;

        KeyOrder(final int skip) {
            this.skip = skip;
        }

        @Override
        public int compare(final Object left, final long leftOffset, final int leftLength, final Object right, final long rightOffset,
                final int rightLength) {
            final int l = Platform.getInt(left, leftOffset) - skip;
            final int r = Platform.getInt(right, rightOffset) - skip;
            return compareUnsigned(left, leftOffset + Integer.BYTES + skip, l, right, rightOffset + Integer.BYTES + skip, r);
        }
    }

    /** Unsigned bytes, eight at a time. */
    static int compareUnsigned(final Object left, final long leftOffset, final int leftLength, final Object right,
            final long rightOffset, final int rightLength) {
        final int n = Math.min(leftLength, rightLength);
        int i = 0;
        for (; i + Long.BYTES <= n; i += Long.BYTES) {
            final long a = Platform.getLong(left, leftOffset + i);
            final long b = Platform.getLong(right, rightOffset + i);
            if (a != b) {
                return Long.compareUnsigned(Long.reverseBytes(a), Long.reverseBytes(b));
            }
        }
        for (; i < n; i++) {
            final int c = (Platform.getByte(left, leftOffset + i) & 0xFF) - (Platform.getByte(right, rightOffset + i) & 0xFF);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    /** The sorted records, as cells again; frees the sorter's memory when read to the end. */
    private static final class Sorted implements Iterator<Tuple2<byte[], Shuffle.Value>> {
        private final UnsafeSorterIterator records;
        private final UnsafeExternalSorter sorter;
        private boolean done;

        Sorted(final UnsafeSorterIterator records, final UnsafeExternalSorter sorter) {
            this.records = records;
            this.sorter = sorter;
        }

        @Override
        public boolean hasNext() {
            if (!done && !records.hasNext()) {
                done = true;
                // The task's peak execution memory, as Spark's own sorters report it: the executors' sizing reads it.
                TaskContext.get().taskMetrics().incPeakExecutionMemory(sorter.getPeakMemoryUsedBytes());
                sorter.cleanupResources();
            }
            return !done;
        }

        @Override
        public Tuple2<byte[], Shuffle.Value> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                records.loadNext();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            final Object base = records.getBaseObject();
            final long offset = records.getBaseOffset();
            final int length = records.getRecordLength();
            final int keyLength = Platform.getInt(base, offset);
            final byte[] key = copy(base, offset + Integer.BYTES, keyLength);
            long at = offset + Integer.BYTES + keyLength;
            final boolean counter = Platform.getByte(base, at) == COUNTER;
            final long number = Platform.getLong(base, at + 1);
            at += 1 + Long.BYTES;
            if (counter) {
                return new Tuple2<>(key, Shuffle.Value.counter(number));
            }
            return new Tuple2<>(key, Shuffle.Value.put(number, copy(base, at, (int) (offset + length - at))));
        }
    }

    /**
     * {@code length} bytes of a page, as an array: eight at a time. {@code Platform.copyMemory}
     * checks its pointers on every call, which on short keys cost more than the copy.
     */
    static byte[] copy(final Object base, final long offset, final int length) {
        final byte[] out = new byte[length];
        int i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            Platform.putLong(out, Platform.BYTE_ARRAY_OFFSET + i, Platform.getLong(base, offset + i));
        }
        for (; i < length; i++) {
            out[i] = Platform.getByte(base, offset + i);
        }
        return out;
    }
}
