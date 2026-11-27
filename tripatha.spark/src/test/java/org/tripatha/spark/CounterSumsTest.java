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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

/** {@link CounterSums} adds up as a map of keys compared by content does, through growth, merges and clears. */
class CounterSumsTest {
    private static Map<ByteBuffer, Long> contents(final CounterSums sums) {
        final Map<ByteBuffer, Long> out = new HashMap<>();
        sums.forEach((key, sum) -> assertEquals(null, out.put(ByteBuffer.wrap(key), sum), "a key twice"));
        assertEquals(out.size(), sums.size());
        return out;
    }

    /** Keys of 0 to 40 bytes from a small alphabet, so many share prefixes and some are equal arrays that aren't the same. */
    private static byte[] key(final SplittableRandom r) {
        final byte[] k = new byte[r.nextInt(41)];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) r.nextInt(3);
        }
        return k;
    }

    @Test
    void addsUpAsAMapByContent() {
        final SplittableRandom r = new SplittableRandom(32);
        final CounterSums sums = new CounterSums(4);
        final CounterSums other = new CounterSums();
        final Map<ByteBuffer, Long> expected = new HashMap<>();
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 5_000; i++) {
                final byte[] k = key(r);
                final long d = r.nextLong(-3, 4);
                if (r.nextBoolean()) {
                    sums.add(k, d);
                } else {
                    other.add(k.clone(), d);
                }
                expected.merge(ByteBuffer.wrap(k.clone()), d, Long::sum);
            }
            sums.addAll(other);
            other.clear();
            assertEquals(0, other.size());
            assertEquals(expected, contents(sums));
        }
        sums.clear();
        assertEquals(Map.of(), contents(sums));
        sums.add(new byte[] {1}, 5);
        assertEquals(Map.of(ByteBuffer.wrap(new byte[] {1}), 5L), contents(sums));
    }

    @Test
    void anOverflowStopsTheTask() {
        final CounterSums sums = new CounterSums();
        sums.add(new byte[] {1}, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> sums.add(new byte[] {1}, 1));
    }

    @Test
    void hashesTellLongKeysApartInTheirLowBits() {
        // Keys that differ only in one byte past the first word must still spread over a small table.
        final java.util.Set<Integer> buckets = new java.util.HashSet<>();
        for (int b = 0; b < 256; b++) {
            final byte[] k = new byte[20];
            k[13] = (byte) b;
            buckets.add(CounterSums.hash(k) & 255);
        }
        assertEquals(true, buckets.size() > 140, buckets.size() + " of 256 buckets");
    }
}
