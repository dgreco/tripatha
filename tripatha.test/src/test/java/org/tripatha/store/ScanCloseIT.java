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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.tripatha.store.spi.Auths;
import org.tripatha.store.spi.CellScanner;
import org.tripatha.store.spi.CellWriter;
import org.tripatha.store.spi.KeyRange;
import org.tripatha.store.spi.RowMutation;
import org.tripatha.store.spi.ScanSpec;
import org.tripatha.store.spi.TableKind;
import org.tripatha.store.spi.WriterOptions;
import org.tripatha.test.StoreITBase;

/**
 * A read closed from another thread while it is being read ends quietly, on every backend. RDF4J's
 * query timeouts close a query's iterations from a timer thread, and the query thread may then be
 * inside one of the store's reads: on Accumulo the read used to throw (the batch scanner's threads
 * interrupted), on RocksDB the native iterator could be freed under the reader.
 */
class ScanCloseIT extends StoreITBase {
    private static final int ROWS = 40_000;
    private static final int ROUNDS = 20;
    private static final ScanSpec ALL = ScanSpec.of(Auths.ALL_HELD);

    private String table;

    @BeforeEach
    void write() {
        table = getTablePrefix() + "spo";
        getStore().tables().create(table, TableKind.STATEMENTS);
        final SortedSet<byte[]> splits = new TreeSet<>(java.util.Arrays::compareUnsigned);
        for (int i = 1; i < 4; i++) {
            splits.add(b(row(i * ROWS / 4)));
        }
        getStore().tables().addSplits(table, splits);
        try (CellWriter w = getStore().newWriter(WriterOptions.DEFAULT)) {
            for (int i = 0; i < ROWS; i++) {
                w.table(table).write(new RowMutation(b(row(i))).put(b(""), b(""), b(""), b("v")));
            }
        }
    }

    @Test
    void aScanClosedWhileItIsReadEndsQuietly() {
        closeWhileReading(() -> getStore().scan(table, KeyRange.all(), ALL));
    }

    @Test
    void aBatchScanClosedWhileItIsReadEndsQuietly() {
        final List<KeyRange> ranges = new ArrayList<>();
        for (int i = 0; i < ROWS; i += 100) {
            ranges.add(KeyRange.closed(b(row(i)), b(row(i + 99))));
        }
        closeWhileReading(() -> getStore().batchScan(table, ranges, ALL));
    }

    /**
     * Each round, a reader reads the whole scan while this thread closes it from outside, at a
     * different point each time. The reader must end, with no exception but the end of the cells,
     * and the store must still read afterwards.
     */
    private void closeWhileReading(final Supplier<CellScanner> open) {
        final int[] cut = new int[1];
        assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
            for (int round = 0; round < ROUNDS; round++) {
                final CellScanner scanner = open.get();
                final AtomicLong read = new AtomicLong();
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final CountDownLatch reading = new CountDownLatch(1);
                final Thread reader = new Thread(() -> {
                    try {
                        while (scanner.hasNext()) {
                            scanner.next();
                            if (read.incrementAndGet() == 10) {
                                reading.countDown();
                            }
                        }
                    } catch (final NoSuchElementException closedBetweenHasNextAndNext) {
                        // allowed: the close came between hasNext and next
                    } catch (final Throwable t) {
                        failure.set(t);
                    } finally {
                        reading.countDown();
                    }
                }, "scan-reader-" + round);
                reader.start();
                assertTrue(reading.await(60, TimeUnit.SECONDS), "the reader never started reading");
                if (round % 2 == 1) {
                    java.util.concurrent.locks.LockSupport.parkNanos(200_000L * round);
                }
                scanner.close();
                reader.join(60_000);
                assertFalse(reader.isAlive(), "round " + round + ": the reader didn't end after the close");
                final int r = round;
                assertNull(failure.get(), () -> "round " + r + ": the reader failed: " + failure.get());
                if (read.get() < ROWS) {
                    cut[0]++;
                }
                scanner.close(); // closing again is harmless
            }
        });
        assertTrue(cut[0] > 0, "no round closed the read before its end, so the test checked nothing");
        long n = 0;
        try (CellScanner s = getStore().scan(table, KeyRange.all(), ALL)) {
            while (s.hasNext()) {
                s.next();
                n++;
            }
        }
        assertEquals(ROWS, n);
    }

    private static String row(final int i) {
        return String.format("s%06d", i);
    }

    private static byte[] b(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
