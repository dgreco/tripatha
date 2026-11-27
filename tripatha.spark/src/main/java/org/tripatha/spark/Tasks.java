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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.tripatha.accumulo.AccumuloRFiles;
import org.tripatha.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.tripatha.api.domain.RyaStatement;
import org.tripatha.api.resolver.RyaTripleContext;
import org.tripatha.api.resolver.triple.TripleRow;
import org.tripatha.loader.BulkBuild;
import org.tripatha.loader.bulk.CellKey;
import org.tripatha.loader.gen.EntityGenerator;
import org.tripatha.loader.gen.SplitMix64;
import org.tripatha.store.spi.BulkImport;

import scala.Tuple2;

/** What the tasks of a build run: statements to cells, and sorted cells to files. */
final class Tasks {
    private static final Logger log = LoggerFactory.getLogger(Tasks.class);

    /** The statements of one write call, for files: as many as the DAO combines counters over. */
    static final int BATCH = 20_000;
    /** The lines parsed in one call of the parser. */
    static final int CHUNK = 1_000;
    /**
     * The counters a task holds to add up across its write calls before handing them to the
     * shuffle (about 100 bytes each): the DAO adds them up within a call, the task across its
     * calls (30 % fewer counter cells for one task at 10M), and the files' writer what is left.
     */
    static final int COUNTERS = 1 << 19;

    private Tasks() {
    }

    // ------------------------------------------------------------------ the generator

    /** The cells of the generator's blocks: one write call per block, as {@code load} and {@code build} make. */
    static Iterator<Tuple2<byte[], Shuffle.Value>> generated(final JobSpec spec, final Iterator<Long> blocks,
            final LongAccumulator statements) {
        final EntityGenerator generator = new EntityGenerator(spec.options().genConfig());
        final Iterator<List<RyaStatement>> batches = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return blocks.hasNext();
            }

            @Override
            public List<RyaStatement> next() {
                final long from = spec.first + blocks.next() * spec.blockSize;
                final long end = Math.min(from + spec.blockSize, spec.last);
                final List<RyaStatement> out = new ArrayList<>((int) (end - from) * 9);
                for (long e = from; e < end; e++) {
                    out.addAll(generator.generate(e));
                }
                return out;
            }
        };
        return new CellIterator(spec, batches, null, statements);
    }

    // ------------------------------------------------------------------ RDF files

    /** The cells of one split of an N-Triples or N-Quads file. */
    static Iterator<Tuple2<byte[], Shuffle.Value>> fromLines(final JobSpec spec, final InputSplit split,
            final Iterator<Tuple2<LongWritable, Text>> lines, final LongAccumulator statements, final LongAccumulator badLines) {
        final BadLines bad = new BadLines(spec.maxBadLines, badLines);
        return new CellIterator(spec, new LineBatches(spec, split, lines, bad), bad, statements);
    }

    /**
     * The statements of one split as their spo cells, for {@code --distinct}: two statements are
     * the same exactly when their spo cells are (subject, predicate, object, graph, label).
     */
    static Iterator<Bytes> spoCells(final JobSpec spec, final InputSplit split, final Iterator<Tuple2<LongWritable, Text>> lines,
            final LongAccumulator badLines) {
        final BadLines bad = new BadLines(spec.maxBadLines, badLines);
        final LineBatches batches = new LineBatches(spec, split, lines, bad);
        final RyaTripleContext context = RyaTripleContext.getInstance(spec.conf());
        return new Iterator<>() {
            private final List<Bytes> buffer = new ArrayList<>();
            private int pos;

            @Override
            public boolean hasNext() {
                while (pos >= buffer.size()) {
                    buffer.clear();
                    pos = 0;
                    if (!batches.hasNext()) {
                        return false;
                    }
                    for (final RyaStatement st : batches.next()) {
                        try {
                            buffer.add(Bytes.of(context.serializeTriple(st).get(TABLE_LAYOUT.SPO)));
                        } catch (final Exception e) {
                            bad.add(String.valueOf(st), e);
                        }
                    }
                }
                return true;
            }

            @Override
            public Bytes next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return buffer.get(pos++);
            }
        };
    }

    /** The cells of distinct statements, read back from their spo cells. */
    static Iterator<Tuple2<byte[], Shuffle.Value>> fromSpoCells(final JobSpec spec, final Iterator<Bytes> cells,
            final LongAccumulator statements, final LongAccumulator badLines) {
        final RyaTripleContext context = RyaTripleContext.getInstance(spec.conf());
        final BadLines bad = new BadLines(spec.maxBadLines, badLines);
        final Iterator<List<RyaStatement>> batches = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return cells.hasNext();
            }

            @Override
            public List<RyaStatement> next() {
                final List<RyaStatement> out = new ArrayList<>(BATCH);
                while (cells.hasNext() && out.size() < BATCH) {
                    try {
                        out.add(context.deserializeTriple(TABLE_LAYOUT.SPO, cells.next().row()));
                    } catch (final Exception e) {
                        throw new IllegalStateException("an spo cell that doesn't read back", e);
                    }
                }
                return out;
            }
        };
        return new CellIterator(spec, batches, bad, statements);
    }

    /** One split's lines, parsed into batches of statements; a line that doesn't parse is a bad line. */
    private static final class LineBatches implements Iterator<List<RyaStatement>> {
        private final Iterator<Tuple2<LongWritable, Text>> lines;
        private final BadLines bad;
        private final RdfLines parser;
        private final String path;
        private List<RyaStatement> next;

        LineBatches(final JobSpec spec, final InputSplit split, final Iterator<Tuple2<LongWritable, Text>> lines, final BadLines bad) {
            this.lines = lines;
            this.bad = bad;
            final Path file = ((FileSplit) split).getPath();
            this.path = file.toString();
            try {
                final FileSystem fs = file.getFileSystem(spec.hadoopConf());
                this.parser = new RdfLines(RdfLines.fileId(fs.getFileStatus(file)), spec.jobTime, spec.visibility);
            } catch (final IOException e) {
                throw new UncheckedIOException("could not read " + file, e);
            }
        }

        @Override
        public boolean hasNext() {
            final List<String> chunk = new ArrayList<>(CHUNK);
            final List<Long> offsets = new ArrayList<>(CHUNK);
            while (next == null && lines.hasNext()) {
                final List<RyaStatement> batch = new ArrayList<>(BATCH);
                while (lines.hasNext() && batch.size() < BATCH) {
                    chunk.clear();
                    offsets.clear();
                    while (lines.hasNext() && chunk.size() < CHUNK) {
                        final Tuple2<LongWritable, Text> line = lines.next();
                        final String text = line._2().toString();
                        if (!RdfLines.isBlank(text)) {
                            chunk.add(text);
                            offsets.add(line._1().get());
                        }
                    }
                    try {
                        parser.parseAll(chunk, batch);
                    } catch (final RuntimeException e) {
                        // A bad line in the chunk: read it again line by line, to keep the others.
                        for (int i = 0; i < chunk.size(); i++) {
                            try {
                                parser.parse(chunk.get(i), batch);
                            } catch (final RuntimeException bad1) {
                                bad.add(path + " at byte " + offsets.get(i) + ": " + abbreviate(chunk.get(i)), bad1);
                            }
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    next = batch;
                }
            }
            return next != null;
        }

        @Override
        public List<RyaStatement> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final List<RyaStatement> out = next;
            next = null;
            return out;
        }
    }

    /** A task's bad lines: counted, the first few logged, and the task stopped past the most allowed. */
    static final class BadLines {
        private final long max;
        private final LongAccumulator total;
        private long count;

        BadLines(final long max, final LongAccumulator total) {
            this.max = max;
            this.total = total;
        }

        void add(final String where, final Exception e) {
            count++;
            total.add(1);
            if (count <= 5) {
                log.warn("Bad input, left out: {}: {}", where, e.toString());
            }
            if (count > max) {
                throw new IllegalStateException("More than " + max + " bad lines (--max-bad-lines): " + where, e);
            }
        }
    }

    static String abbreviate(final String s) {
        return s.length() <= 200 ? s : s.substring(0, 200) + "…";
    }

    /** Whether a sampled line is kept: a hash of its file and offset, below the fraction. */
    static boolean sampled(final String fileId, final long offset, final double fraction) {
        if (fraction >= 1) {
            return true;
        }
        final long h = SplitMix64.mix64(fileId.hashCode() * 0x9E37_79B9_7F4A_7C15L ^ offset);
        return (h >>> 11) < (long) (fraction * (1L << 53));
    }

    // ------------------------------------------------------------------ cells

    /**
     * Batches of statements written through a {@link CellMaker}, as cells keyed for the shuffle.
     * With {@code bad} (files), a batch the DAO refuses is written again one statement at a time,
     * and the statements it still refuses are bad lines; the generator's are never refused.
     */
    private static final class CellIterator implements Iterator<Tuple2<byte[], Shuffle.Value>>, CellMaker.Sink {
        private final JobSpec spec;
        private final Iterator<List<RyaStatement>> batches;
        private final BadLines bad;
        private final LongAccumulator statements;
        private final CellMaker maker;
        /** The puts of the calls written, until the shuffle takes them; then the counters drained. */
        private final List<Tuple2<byte[], Shuffle.Value>> buffer = new ArrayList<>();
        /** The counter deltas of the call being written: added to the task's once the call succeeds. */
        private final CounterSums call = new CounterSums();
        /** The task's counter sums across its calls (O32): the shuffle gets one cell per key and drain. */
        private final CounterSums counters = new CounterSums(COUNTERS);
        private int pos;
        private boolean closed;

        CellIterator(final JobSpec spec, final Iterator<List<RyaStatement>> batches, final BadLines bad,
                final LongAccumulator statements) {
            this.spec = spec;
            this.batches = batches;
            this.bad = bad;
            this.statements = statements;
            this.maker = new CellMaker(spec.conf(), java.util.Set.copyOf(spec.layout.tables), spec.existing, this, spec.jobTime);
        }

        @Override
        public boolean hasNext() {
            while (pos >= buffer.size()) {
                buffer.clear();
                pos = 0;
                if (closed) {
                    return false;
                }
                if (!batches.hasNext()) {
                    closed = true;
                    maker.close();
                    drainCounters();
                    if (buffer.isEmpty()) {
                        return false;
                    }
                    continue;
                }
                write(batches.next());
                if (counters.size() >= COUNTERS) {
                    drainCounters();
                }
            }
            return true;
        }

        /**
         * Writes a batch as one call. A call that fails may have handed part of its cells over:
         * its puts are taken off the buffer and its counters dropped, so only whole calls count.
         */
        private void write(final List<RyaStatement> batch) {
            try {
                maker.add(batch, spec.addPolicy);
                counters.addAll(call);
                call.clear();
                statements.add(batch.size());
                return;
            } catch (final RuntimeException e) {
                if (bad == null) {
                    throw e;
                }
                buffer.clear();
                call.clear();
            }
            for (final RyaStatement st : batch) {
                final int mark = buffer.size();
                try {
                    maker.add(List.of(st), spec.addPolicy);
                    counters.addAll(call);
                    statements.add(1);
                } catch (final RuntimeException e) {
                    buffer.subList(mark, buffer.size()).clear();
                    bad.add(abbreviate(String.valueOf(st)), e);
                } finally {
                    call.clear();
                }
            }
        }

        private void drainCounters() {
            counters.forEach((key, sum) -> buffer.add(new Tuple2<>(key, Shuffle.Value.counter(sum))));
            counters.clear();
        }

        @Override
        public Tuple2<byte[], Shuffle.Value> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.get(pos++);
        }

        @Override
        public void put(final String table, final byte[] row, final byte[] family, final byte[] qualifier, final byte[] visibility,
                final long timestamp, final byte[] value) {
            final int t = spec.layout.table(table);
            buffer.add(new Tuple2<>(Shuffle.Key.encode(spec.layout.partition(t, row, family, qualifier), row, family, qualifier,
                    visibility), Shuffle.Value.put(timestamp, value)));
        }

        @Override
        public void counter(final String table, final byte[] row, final byte[] family, final byte[] qualifier, final byte[] visibility,
                final long delta) {
            final int t = spec.layout.table(table);
            call.add(Shuffle.Key.encode(spec.layout.partition(t, row, family, qualifier), row, family, qualifier, visibility), delta);
        }
    }

    // ------------------------------------------------------------------ files

    /** A file a task wrote. A plain class, for Kryo. */
    static final class FileWritten implements Serializable {
        private static final long serialVersionUID = 1L;
        String table;
        String path;
        byte[] firstRow;
        byte[] lastRow;
        long cells;

        FileWritten() {
        }

        FileWritten(final String table, final BulkImport.Written w) {
            this.table = table;
            this.path = w.path();
            this.firstRow = w.firstRow();
            this.lastRow = w.lastRow();
            this.cells = w.cells();
        }

        BulkImport.Written written() {
            return new BulkImport.Written(path, firstRow, lastRow, cells);
        }
    }

    /**
     * Writes one partition's cells, combined and sorted, to its table's file; counters whose total
     * the build leaves out are left out. An empty partition writes no file.
     */
    static Iterator<FileWritten> write(final JobSpec spec, final int partition, final Iterator<Tuple2<byte[], Shuffle.Value>> cells) {
        final Shuffle.Layout layout = spec.layout;
        final String table = layout.tables.get(layout.tableOf(partition));
        final String name = String.format(java.util.Locale.ROOT, "%06d", layout.fileOf(partition));
        // The file, opened by the first cell kept: an empty partition writes none.
        final BulkImport.BulkFile[] file = new BulkImport.BulkFile[1];
        final java.util.function.Supplier<BulkImport.BulkFile> open = () -> {
            if (file[0] == null) {
                file[0] = AccumuloRFiles.open(spec.tableProperties.get(table), spec.hadoopConf(), spec.filesDir + "/" + table, name,
                        spec.jobTime);
            }
            return file[0];
        };
        // A counter's cells (one per label) decided together: whether a 1 stays out depends on the others.
        // The group holds a counter's keys until it decides, so each key is decoded into arrays of its own.
        final BulkBuild.CounterGroup<Tuple2<CellKey, Long>> group = spec.leftOut()
                .group(kv -> open.get().counter(kv._1().row(), kv._1().family(), kv._1().qualifier(), kv._1().visibility(), kv._2()));
        try {
            // Sorted, so the cells of one key are next to each other: added up, or the newest put kept.
            byte[] k = null;
            Shuffle.Value v = null;
            while (true) {
                final Tuple2<byte[], Shuffle.Value> c = cells.hasNext() ? cells.next() : null;
                if (c != null && k != null && Arrays.equals(k, c._1())) {
                    v = Shuffle.Value.combine(v, c._2());
                    continue;
                }
                if (k != null) {
                    final CellKey key = Shuffle.Key.decode(k);
                    if (v.counter) {
                        group.add(key.row(), key.family(), key.qualifier(), v.delta, new Tuple2<>(key, v.delta));
                    } else {
                        group.flush();
                        open.get().put(key.row(), key.family(), key.qualifier(), key.visibility(), v.timestamp, v.value);
                    }
                }
                if (c == null) {
                    break;
                }
                k = c._1();
                v = c._2();
            }
            group.flush();
            if (file[0] == null) {
                return java.util.Collections.emptyIterator();
            }
            final BulkImport.Written w = file[0].finish();
            file[0] = null;
            final List<FileWritten> out = new ArrayList<>(1);
            if (w != null) {
                out.add(new FileWritten(table, w));
            }
            return out.iterator();
        } finally {
            if (file[0] != null) {
                file[0].close();
            }
        }
    }

    // ------------------------------------------------------------------ bytes

    /** An spo cell as one comparable value, for {@code distinct}. A plain class, for Kryo. */
    static final class Bytes implements Serializable {
        private static final long serialVersionUID = 1L;
        byte[] b;

        Bytes() {
        }

        static Bytes of(final TripleRow row) {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(row.getRow().length + 64);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                for (final byte[] part : new byte[][] {row.getRow(), row.getColumnFamily(), row.getColumnQualifier(),
                    row.getColumnVisibility(), row.getValue()}) {
                    if (part == null) {
                        out.writeInt(-1);
                    } else {
                        out.writeInt(part.length);
                        out.write(part);
                    }
                }
                out.writeLong(row.getTimestamp() == null ? Long.MIN_VALUE : row.getTimestamp());
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            final Bytes b = new Bytes();
            b.b = bytes.toByteArray();
            return b;
        }

        TripleRow row() {
            try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(b))) {
                final byte[][] parts = new byte[5][];
                for (int i = 0; i < parts.length; i++) {
                    final int n = in.readInt();
                    if (n >= 0) {
                        parts[i] = in.readNBytes(n);
                    }
                }
                final long ts = in.readLong();
                return new TripleRow(parts[0], parts[1], parts[2], ts == Long.MIN_VALUE ? null : ts, parts[3], parts[4]);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof Bytes x && Arrays.equals(b, x.b);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(b);
        }
    }
}
