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

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A read that may be closed from another thread while it is being read.
 *
 * <p>RDF4J's query timeouts close a query's iterations from a timer thread
 * ({@code TimeLimitIteration}), and the query thread, or a thread reading a merge join's slice, may
 * then be inside one of the store's reads. No backend takes that well: Accumulo's scanners throw
 * ("Scanner was closed", "The BatchScanner was unexpectedly closed"), which reached the caller instead
 * of the query's timeout; RocksDB would free its native iterator under the reader; HBase's batch scan
 * cancels the workers its reader waits on.
 *
 * <p>So no backend ever sees a close while a read is inside it. {@link #close()} closes the read at
 * once when no thread is inside it, and otherwise leaves it to the reader, which closes it as it leaves
 * {@link #hasNext()} or {@link #next()}. {@code close()} never waits: RDF4J's timer thread is shared by
 * every query. After a close, {@code hasNext()} is false and {@code next()} throws
 * {@link NoSuchElementException}, without reaching the backend, which is how RDF4J expects a closed
 * iteration to end. A {@code hasNext()} that a close overtook returns false too. {@code hasNext()}
 * reads the next cell ahead, so a cell read before a close may still be returned after it.
 *
 * <p>One reader at a time, as for any scanner. {@code TracedScans.open} guards every read it opens.
 */
public final class GuardedScanner implements CellScanner {
    private static final int IDLE = 0;
    private static final int READING = 1;
    /** Closed while a reader was inside: the reader closes the read as it leaves. */
    private static final int CLOSING = 2;
    private static final int CLOSED = 3;

    private final CellScanner real;
    private final AtomicInteger state = new AtomicInteger(IDLE);
    /**
     * The next cell, read by {@link #hasNext()} within its guarded call, so that {@link #next()} reaches
     * neither the backend nor the state: one guarded call per cell instead of two. Only the reader
     * touches it.
     */
    private Cell buffered;

    private GuardedScanner(final CellScanner real) {
        this.real = real;
    }

    /** {@code real}, guarded (once). */
    public static CellScanner of(final CellScanner real) {
        return real instanceof GuardedScanner ? real : new GuardedScanner(real);
    }

    @Override
    public boolean hasNext() {
        if (buffered != null) {
            return true;
        }
        if (!state.compareAndSet(IDLE, READING)) {
            return false;
        }
        final Cell cell;
        try {
            cell = real.hasNext() ? real.next() : null;
        } catch (final RuntimeException | Error e) {
            leave();
            throw e;
        }
        if (!leave()) {
            return false;
        }
        buffered = cell;
        return cell != null;
    }

    @Override
    public Cell next() {
        if (!hasNext()) {
            throw new NoSuchElementException("The read is over or was closed");
        }
        final Cell cell = buffered;
        buffered = null;
        return cell;
    }

    /** Leaves a read call: false if a close came meanwhile, which this thread then carries out. */
    private boolean leave() {
        if (state.compareAndSet(READING, IDLE)) {
            return true;
        }
        state.set(CLOSED);
        real.close();
        return false;
    }

    @Override
    public void close() {
        while (true) {
            final int s = state.get();
            if (s == IDLE) {
                if (state.compareAndSet(IDLE, CLOSED)) {
                    real.close();
                    return;
                }
            } else if (s == READING) {
                if (state.compareAndSet(READING, CLOSING)) {
                    return;
                }
            } else {
                return;
            }
        }
    }
}
