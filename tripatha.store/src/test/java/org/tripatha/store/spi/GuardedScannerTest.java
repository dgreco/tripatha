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
package org.tripatha.store.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

class GuardedScannerTest {

    /** A backend read that fails the test if it is closed while a call is inside it. */
    private static final class Backend implements CellScanner {
        private final long cells;
        private long read;
        volatile boolean inside;
        volatile CountDownLatch entered;
        volatile CountDownLatch release;
        final AtomicInteger closes = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<String> violation = new AtomicReference<>();
        volatile Thread closedBy;

        Backend(final long cells) {
            this.cells = cells;
        }

        @Override
        public boolean hasNext() {
            enter();
            try {
                if (entered != null) {
                    entered.countDown();
                }
                if (release != null) {
                    try {
                        release.await();
                    } catch (final InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return read < cells;
            } finally {
                inside = false;
            }
        }

        @Override
        public Cell next() {
            enter();
            try {
                if (read >= cells) {
                    throw new NoSuchElementException();
                }
                read++;
                return new Cell(new byte[] {1}, new byte[0], new byte[0], new byte[0], 1, new byte[0]);
            } finally {
                inside = false;
            }
        }

        private void enter() {
            calls.incrementAndGet();
            if (closes.get() > 0) {
                violation.compareAndSet(null, "read after close");
            }
            inside = true;
        }

        @Override
        public void close() {
            if (inside) {
                violation.compareAndSet(null, "closed while a read was inside it");
            }
            closes.incrementAndGet();
            closedBy = Thread.currentThread();
        }
    }

    @Test
    void aCloseWhileAReaderIsInsideIsLeftToTheReader() throws Exception {
        final Backend backend = new Backend(10);
        backend.entered = new CountDownLatch(1);
        backend.release = new CountDownLatch(1);
        final CellScanner guarded = GuardedScanner.of(backend);
        final AtomicReference<Boolean> more = new AtomicReference<>();
        final Thread reader = new Thread(() -> more.set(guarded.hasNext()), "reader");
        reader.start();
        assertTrue(backend.entered.await(10, TimeUnit.SECONDS));

        guarded.close(); // returns at once, without touching the backend
        assertEquals(0, backend.closes.get());

        backend.release.countDown();
        reader.join(10_000);
        assertFalse(more.get(), "a hasNext the close overtook says the read ended");
        assertEquals(1, backend.closes.get());
        assertSame(reader, backend.closedBy, "the reader closed it as it left");

        final int calls = backend.calls.get();
        assertFalse(guarded.hasNext());
        assertThrows(NoSuchElementException.class, guarded::next);
        guarded.close();
        assertEquals(calls, backend.calls.get(), "nothing reaches the backend after the close");
        assertEquals(1, backend.closes.get());
        assertNull(backend.violation.get());
    }

    @Test
    void aCloseWithNoReaderInsideClosesAtOnce() {
        final Backend backend = new Backend(3);
        final CellScanner guarded = GuardedScanner.of(backend);
        assertTrue(guarded.hasNext());
        guarded.next();
        guarded.close();
        assertEquals(1, backend.closes.get());
        assertSame(Thread.currentThread(), backend.closedBy);
        assertFalse(guarded.hasNext());
        guarded.close();
        assertEquals(1, backend.closes.get());
        assertNull(backend.violation.get());
    }

    @Test
    void readsThroughToTheBackendUntilClosed() {
        final Backend backend = new Backend(1000);
        long n = 0;
        try (CellScanner guarded = GuardedScanner.of(backend)) {
            while (guarded.hasNext()) {
                guarded.next();
                n++;
            }
        }
        assertEquals(1000, n);
        assertEquals(1, backend.closes.get());
        assertNull(backend.violation.get());
    }

    /** Many readers closed at random points: no close ever reaches the backend while a read is inside it. */
    @Test
    void noCloseReachesTheBackendWhileAReadIsInside() {
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            for (int round = 0; round < 2000; round++) {
                final Backend backend = new Backend(Long.MAX_VALUE);
                final CellScanner guarded = GuardedScanner.of(backend);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread reader = new Thread(() -> {
                    try {
                        while (guarded.hasNext()) {
                            guarded.next();
                        }
                    } catch (final NoSuchElementException closedBetweenTheTwo) {
                        // allowed
                    } catch (final Throwable t) {
                        failure.set(t);
                    }
                });
                reader.start();
                LockSupport.parkNanos(round % 50 * 1_000L);
                guarded.close();
                reader.join(10_000);
                assertFalse(reader.isAlive(), "round " + round + ": the reader didn't end");
                assertNull(failure.get());
                assertNull(backend.violation.get(), "round " + round);
                assertEquals(1, backend.closes.get(), "round " + round + ": closed exactly once");
            }
        });
    }

    @Test
    void aFailingReadIsPassedOnAndTheReadStillCloses() {
        final CellScanner failing = new CellScanner() {
            int closes;

            @Override
            public boolean hasNext() {
                throw new IllegalStateException("backend failure");
            }

            @Override
            public Cell next() {
                throw new IllegalStateException("backend failure");
            }

            @Override
            public void close() {
                if (++closes > 1) {
                    throw new AssertionError("closed twice");
                }
            }
        };
        final CellScanner guarded = GuardedScanner.of(failing);
        assertThrows(IllegalStateException.class, guarded::hasNext);
        assertThrows(IllegalStateException.class, guarded::next);
        guarded.close();
        guarded.close();
    }

    @Test
    void guardsOnce() {
        final CellScanner guarded = GuardedScanner.of(new Backend(0));
        assertSame(guarded, GuardedScanner.of(guarded));
    }
}
