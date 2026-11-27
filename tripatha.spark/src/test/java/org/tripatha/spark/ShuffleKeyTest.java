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

import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

import org.tripatha.loader.bulk.CellKey;

/**
 * The shuffle's encoded key sorts as the cells' keys do (partition, then row, family, qualifier
 * and label, each as unsigned bytes), is equal exactly when they are, and decodes back.
 */
class ShuffleKeyTest {
    /** The bytes that matter to the escaping, and a few others. */
    private static final byte[] ALPHABET = {0x00, 0x01, 0x02, 0x7F, (byte) 0x80, (byte) 0xFE, (byte) 0xFF, 'a'};

    private record Cell(int part, byte[] row, byte[] family, byte[] qualifier, byte[] visibility) {
        byte[] encoded() {
            return Shuffle.Key.encode(part, row, family, qualifier, visibility);
        }
    }

    /** The order the shuffle had before O30, field by field: the oracle. */
    private static int fieldOrder(final Cell a, final Cell b) {
        int c = Integer.compare(a.part, b.part);
        if (c == 0) {
            c = Arrays.compareUnsigned(a.row, b.row);
        }
        if (c == 0) {
            c = Arrays.compareUnsigned(a.family, b.family);
        }
        if (c == 0) {
            c = Arrays.compareUnsigned(a.qualifier, b.qualifier);
        }
        if (c == 0) {
            c = Arrays.compareUnsigned(a.visibility, b.visibility);
        }
        return c;
    }

    private static byte[] field(final SplittableRandom r) {
        final byte[] b = new byte[r.nextInt(4) == 0 ? 0 : r.nextInt(1, 5)];
        for (int i = 0; i < b.length; i++) {
            b[i] = ALPHABET[r.nextInt(ALPHABET.length)];
        }
        return b;
    }

    /** A cell like {@code c}, with one field cut short or extended: a prefix of the other's. */
    private static Cell near(final Cell c, final SplittableRandom r) {
        final byte[][] f = {c.row.clone(), c.family.clone(), c.qualifier.clone(), c.visibility.clone()};
        final int i = r.nextInt(f.length);
        if (f[i].length > 0 && r.nextBoolean()) {
            f[i] = Arrays.copyOf(f[i], r.nextInt(f[i].length));
        } else {
            f[i] = Arrays.copyOf(f[i], f[i].length + 1);
            f[i][f[i].length - 1] = ALPHABET[r.nextInt(ALPHABET.length)];
        }
        return new Cell(c.part, f[0], f[1], f[2], f[3]);
    }

    private static Cell cell(final SplittableRandom r) {
        return new Cell(r.nextInt(3) == 0 ? r.nextInt() & Integer.MAX_VALUE : r.nextInt(3), field(r), field(r), field(r), field(r));
    }

    @Test
    void sortsAsTheCellsKeysDo() {
        final SplittableRandom r = new SplittableRandom(30);
        for (int i = 0; i < 200_000; i++) {
            final Cell a = cell(r);
            final Cell b = r.nextBoolean() ? near(a, r) : cell(r);
            final int expected = Integer.signum(fieldOrder(a, b));
            assertEquals(expected, Integer.signum(Arrays.compareUnsigned(a.encoded(), b.encoded())), () -> a + " against " + b);
            assertEquals(expected == 0, Arrays.equals(a.encoded(), b.encoded()));
        }
    }

    @Test
    void decodesBack() {
        final SplittableRandom r = new SplittableRandom(31);
        for (int i = 0; i < 100_000; i++) {
            final Cell c = cell(r);
            final byte[] key = c.encoded();
            assertEquals(c.part, Shuffle.Key.part(key));
            final CellKey k = Shuffle.Key.decode(key);
            assertArrayEquals(c.row, k.row());
            assertArrayEquals(c.family, k.family());
            assertArrayEquals(c.qualifier, k.qualifier());
            assertArrayEquals(c.visibility, k.visibility());
        }
    }

    @Test
    void findsTheFirstZeroByte() {
        final SplittableRandom r = new SplittableRandom(32);
        for (int i = 0; i < 100_000; i++) {
            final byte[] b = new byte[r.nextInt(40)];
            for (int j = 0; j < b.length; j++) {
                b[j] = r.nextInt(6) == 0 ? 0 : ALPHABET[r.nextInt(ALPHABET.length)];
            }
            final int from = b.length == 0 ? 0 : r.nextInt(b.length + 1);
            int expected = -1;
            for (int j = from; j < b.length; j++) {
                if (b[j] == 0) {
                    expected = j;
                    break;
                }
            }
            assertEquals(expected, Shuffle.Key.indexOfZero(b, from), () -> Arrays.toString(b) + " from " + from);
        }
    }

    /** Long fields, with runs of 00 and of bytes next to it, across the eight-byte words. */
    @Test
    void longFieldsDecodeBack() {
        final SplittableRandom r = new SplittableRandom(33);
        for (int i = 0; i < 20_000; i++) {
            final byte[][] f = new byte[4][];
            for (int k = 0; k < f.length; k++) {
                f[k] = new byte[r.nextInt(80)];
                for (int j = 0; j < f[k].length; j++) {
                    f[k][j] = r.nextInt(5) == 0 ? 0 : r.nextInt(5) == 0 ? (byte) 0x80 : (byte) r.nextInt(1, 256);
                }
            }
            final CellKey k = Shuffle.Key.decode(Shuffle.Key.encode(7, f[0], f[1], f[2], f[3]));
            assertArrayEquals(f[0], k.row());
            assertArrayEquals(f[1], k.family());
            assertArrayEquals(f[2], k.qualifier());
            assertArrayEquals(f[3], k.visibility());
        }
    }

    @Test
    void thePartitionerReadsThePartition() {
        final Shuffle.FilePartitioner p = new Shuffle.FilePartitioner(300);
        for (final int part : new int[] {0, 1, 255, 256, 299}) {
            assertEquals(part, p.getPartition(Shuffle.Key.encode(part, new byte[] {0}, new byte[0], new byte[] {1}, new byte[0])));
        }
    }
}
