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
package org.tripatha.loader.bench;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;

import org.tripatha.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.tripatha.api.resolver.RyaTripleContext;
import org.tripatha.loader.gen.EntityGenerator;
import org.tripatha.loader.gen.GenConfig;
import org.tripatha.store.StoreRdfConfiguration;
import org.tripatha.store.spi.Auths;
import org.tripatha.store.spi.Cell;
import org.tripatha.store.spi.CellScanner;
import org.tripatha.store.spi.KeyRange;
import org.tripatha.store.spi.ScanSpec;
import org.tripatha.store.spi.Store;

/**
 * One client's scan rates on {@code spo} (ARCH.md 20.1): what the query path's reads cost on
 * a store, before any query. Three measurements, through the storage SPI the queries use:
 * <ul>
 * <li><b>One long scan</b> (Accumulo's Scanner): cells and bytes per second over one range, for up
 * to {@value #LONG_SCAN_CELLS} cells or {@value #LONG_SCAN_SECONDS} s;</li>
 * <li><b>Lookups one at a time:</b> one entity's statements per scan, the latency of a round trip,
 * as a nested-loop join without batching pays it;</li>
 * <li><b>Batches of lookups</b> (Accumulo's BatchScanner, HBase's multi-get): many entities per
 * call, as a batched join or an entity prefetch asks, at several thread counts.</li>
 * </ul>
 * The entities are drawn at random, afresh for each measurement, so each mostly reads blocks no
 * earlier measurement cached; the first run after the servers start is the cold one.
 */
final class ScanRates {
    static final int LONG_SCAN_CELLS = 20_000_000;
    static final int LONG_SCAN_SECONDS = 60;
    /** The thread counts tried for batches: the default ({@code ScanSpec}'s), and more. */
    static final int[] THREADS = {1, 4, 16};

    private final Store store;
    private final String spo;
    private final GenConfig gen;
    private final EntityGenerator generator;
    private final RyaTripleContext context;
    private final int lookups;
    private final PrintStream out;
    private final SplittableRandom random = new SplittableRandom(System.nanoTime());

    ScanRates(final Store store, final StoreRdfConfiguration conf, final GenConfig gen, final int lookups, final PrintStream out) {
        this.store = store;
        this.spo = conf.getTableLayoutStrategy().getSpo();
        this.gen = gen;
        this.generator = new EntityGenerator(gen);
        this.context = RyaTripleContext.getInstance(conf);
        this.lookups = lookups;
        this.out = out;
    }

    void run(final Path dir, final String where) throws IOException {
        final StringBuilder md = new StringBuilder();
        md.append("# Scan rates, ").append(where).append("\n\n");
        md.append(String.format(Locale.ROOT, "%s, %,d entities, `%s` in %,d tablets or regions on %d servers, one client with %d "
                + "processors.%n%n", LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS), gen.entities(), spo,
                store.tables().splitCount(spo) + 1, store.tables().serverCount(), Runtime.getRuntime().availableProcessors()));
        md.append("| Read | Calls | Cells | MB | Seconds | Cells/s | MB/s | Per call, ms (p50 / p90 / p99) |\n");
        md.append("|---|---|---|---|---|---|---|---|\n");

        out.println("One long scan…");
        md.append(longScan());
        out.println("Lookups one at a time…");
        md.append(singleLookups());
        for (final int threads : THREADS) {
            out.println("Batches of " + lookups + " lookups on " + threads + " threads…");
            md.append(batches(threads));
        }
        md.append("\nCells and MB count what came back (row, columns, label and value). Lookups read one entity's statements "
                + "each, entities drawn at random afresh for every row of the table.\n");
        final Path file = dir.resolve("scan-rates-" + where + "-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern(
                "yyyyMMdd-HHmmss", Locale.ROOT)) + ".md");
        Files.writeString(file, md, StandardCharsets.UTF_8);
        out.println();
        out.println(md);
        out.println("Report: " + file.toAbsolutePath());
    }

    /** The spo prefix of one entity's statements: its subject's bytes and the delimiter after them. */
    private byte[] subjectPrefix(final long entity) {
        try {
            final byte[] row = context.serializeTriple(generator.generate(entity).get(0)).get(TABLE_LAYOUT.SPO).getRow();
            for (int i = 0; i < row.length; i++) {
                if (row[i] == 0) {
                    return Arrays.copyOf(row, i + 1);
                }
            }
            throw new IllegalStateException("an spo row without a delimiter");
        } catch (final Exception e) {
            throw new IllegalStateException("could not encode entity " + entity, e);
        }
    }

    private List<KeyRange> randomEntities(final int n) {
        final List<KeyRange> ranges = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ranges.add(KeyRange.prefix(subjectPrefix(random.nextLong(gen.entities()))));
        }
        return ranges;
    }

    private static long bytes(final Cell c) {
        return (long) c.row().length + c.family().length + c.qualifier().length + c.visibility().length + c.value().length;
    }

    private String longScan() {
        final ScanSpec spec = ScanSpec.of(Auths.ALL_HELD);
        // From a random entity on, so the scan doesn't always read the first tablet.
        final KeyRange from = KeyRange.atLeast(subjectPrefix(random.nextLong(gen.entities())));
        long cells = 0;
        long bytes = 0;
        final long t0 = System.nanoTime();
        final long end = t0 + LONG_SCAN_SECONDS * 1_000_000_000L;
        long first = -1;
        try (CellScanner scan = store.scan(spo, from, spec)) {
            while (scan.hasNext() && cells < LONG_SCAN_CELLS) {
                final Cell c = scan.next();
                if (first < 0) {
                    first = System.nanoTime() - t0;
                }
                cells++;
                bytes += bytes(c);
                if ((cells & 0xFFFF) == 0 && System.nanoTime() > end) {
                    break;
                }
            }
        }
        final double s = (System.nanoTime() - t0) / 1e9;
        return row("One long scan", 1, cells, bytes, s, String.format(Locale.ROOT, "first cell %.1f", first / 1e6));
    }

    private String singleLookups() {
        final ScanSpec spec = ScanSpec.of(Auths.ALL_HELD);
        final List<KeyRange> ranges = randomEntities(lookups);
        final double[] ms = new double[ranges.size()];
        long cells = 0;
        long bytes = 0;
        final long t0 = System.nanoTime();
        for (int i = 0; i < ranges.size(); i++) {
            final long s = System.nanoTime();
            try (CellScanner scan = store.scan(spo, ranges.get(i), spec)) {
                while (scan.hasNext()) {
                    final Cell c = scan.next();
                    cells++;
                    bytes += bytes(c);
                }
            }
            ms[i] = (System.nanoTime() - s) / 1e6;
        }
        return row("Lookups one at a time", ranges.size(), cells, bytes, (System.nanoTime() - t0) / 1e9, percentiles(ms));
    }

    private String batches(final int threads) {
        final ScanSpec spec = ScanSpec.builder(Auths.ALL_HELD).threads(threads).build();
        final int batches = 10;
        final double[] ms = new double[batches];
        long cells = 0;
        long bytes = 0;
        double seconds = 0;
        for (int b = 0; b < batches; b++) {
            final List<KeyRange> ranges = randomEntities(lookups);
            final long s = System.nanoTime();
            try (CellScanner scan = store.batchScan(spo, ranges, spec)) {
                while (scan.hasNext()) {
                    final Cell c = scan.next();
                    cells++;
                    bytes += bytes(c);
                }
            }
            ms[b] = (System.nanoTime() - s) / 1e6;
            seconds += ms[b] / 1000;
        }
        return row(String.format(Locale.ROOT, "Batches of %,d lookups, %d threads", lookups, threads), batches, cells, bytes, seconds,
                percentiles(ms));
    }

    private static String percentiles(final double[] ms) {
        final double[] sorted = ms.clone();
        Arrays.sort(sorted);
        return String.format(Locale.ROOT, "%.2f / %.2f / %.2f", at(sorted, 0.5), at(sorted, 0.9), at(sorted, 0.99));
    }

    private static double at(final double[] sorted, final double q) {
        return sorted.length == 0 ? 0 : sorted[Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1)];
    }

    private static String row(final String what, final long calls, final long cells, final long bytes, final double seconds,
            final String perCall) {
        final double s = Math.max(seconds, 1e-9);
        return String.format(Locale.ROOT, "| %s | %,d | %,d | %.1f | %.2f | %,.0f | %.1f | %s |%n", what, calls, cells, bytes / 1e6,
                seconds, cells / s, bytes / 1e6 / s, perCall);
    }
}
