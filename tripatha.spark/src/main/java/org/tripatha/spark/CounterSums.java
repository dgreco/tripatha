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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Counter deltas added up by encoded key ({@link Shuffle.Key}), for a map task (O32): open
 * addressing over two parallel arrays, the keys and their sums, so a counter costs no entry
 * object and no boxed sum, and its key is hashed eight bytes at a time. Keys are compared by
 * content and kept as given: the caller must not change an array it added. Not thread-safe.
 */
final class CounterSums {
    private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private byte[][] keys;
    private long[] sums;
    private int size;

    CounterSums() {
        this(1 << 10);
    }

    CounterSums(final int expected) {
        final int capacity = Integer.highestOneBit(Math.max(16, expected * 2 - 1)) << 1;
        keys = new byte[capacity][];
        sums = new long[capacity];
    }

    int size() {
        return size;
    }

    /** Adds {@code delta} to the sum of {@code key}. */
    void add(final byte[] key, final long delta) {
        final int mask = keys.length - 1;
        int i = hash(key) & mask;
        while (true) {
            final byte[] k = keys[i];
            if (k == null) {
                keys[i] = key;
                sums[i] = delta;
                if (++size * 4 > keys.length * 3) {
                    grow();
                }
                return;
            }
            if (Arrays.equals(k, key)) {
                sums[i] = Math.addExact(sums[i], delta);
                return;
            }
            i = (i + 1) & mask;
        }
    }

    /** Adds every sum of {@code other} to this one's. */
    void addAll(final CounterSums other) {
        for (int i = 0; i < other.keys.length; i++) {
            if (other.keys[i] != null) {
                add(other.keys[i], other.sums[i]);
            }
        }
    }

    /** What {@link #forEach} hands each counter to. */
    interface Visitor {
        void accept(byte[] key, long sum);
    }

    void forEach(final Visitor visitor) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                visitor.accept(keys[i], sums[i]);
            }
        }
    }

    /** Empties it, keeping its capacity. */
    void clear() {
        if (size > 0) {
            Arrays.fill(keys, null);
            size = 0;
        }
    }

    private void grow() {
        final byte[][] oldKeys = keys;
        final long[] oldSums = sums;
        keys = new byte[oldKeys.length * 2][];
        sums = new long[oldKeys.length * 2];
        size = 0;
        final int mask = keys.length - 1;
        for (int j = 0; j < oldKeys.length; j++) {
            final byte[] k = oldKeys[j];
            if (k != null) {
                int i = hash(k) & mask;
                while (keys[i] != null) {
                    i = (i + 1) & mask;
                }
                keys[i] = k;
                sums[i] = oldSums[j];
                size++;
            }
        }
    }

    /**
     * The key's bytes mixed eight at a time. Each word is folded in with a multiply and a shift
     * down, so a difference in a word's high bytes reaches the low bits the table uses.
     */
    static int hash(final byte[] key) {
        long h = key.length * 0x9E37_79B9_7F4A_7C15L;
        int i = 0;
        for (; i + Long.BYTES <= key.length; i += Long.BYTES) {
            h = mix(h ^ (long) LONGS.get(key, i));
        }
        long tail = 0;
        for (int s = 0; i < key.length; i++, s += 8) {
            tail |= (key[i] & 0xFFL) << s;
        }
        h = mix(h ^ tail);
        return (int) (h ^ (h >>> 32));
    }

    private static long mix(final long x) {
        long z = (x ^ (x >>> 30)) * 0xBF58_476D_1CE4_E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D0_49BB_1331_11EBL;
        return z ^ (z >>> 31);
    }
}
